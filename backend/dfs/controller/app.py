"""Controlador + nó de metadados (mesmo processo, papéis em módulos separados).

  store.py       -> nó de metadados (árvore, versões, blocos, réplicas; SQLite)
  cluster.py     -> controlador: registro, heartbeats, falhas, posicionamento
  replication.py -> controlador: re-replicação, trim, drain, GC, reconciliação
  app.py (este)  -> API REST (plano de controle). NENHUM byte de arquivo passa por aqui.

Todos os handlers são `async def` e rodam no mesmo event loop, sem `await` dentro
de transações: as mutações de metadados ficam naturalmente serializadas.
"""
from __future__ import annotations

import asyncio
import json
import base64
import math
import os
import random
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..common import crypto
from ..common import paths as P
from ..common.config import chunk_size_bytes
from ..common.logs import EventLogger
from ..common.metrics import Metrics
from .cluster import ClusterRegistry
from .replication import GarbageCollector, ReplicationManager, reconcile_block_report
from .store import FSError, MetadataStore

STATE_LABELS = {
    "healthy": "íntegro",
    "under_replicated": "sub-replicado",
    "recovering": "em recuperação",
    "unavailable": "indisponível",
    "writing": "gravando",
}


# ------------------------------------------------------------------ modelos
class MkdirReq(BaseModel):
    path: str
    parents: bool = False
    replication: int | None = Field(default=None, ge=1)


class RenameReq(BaseModel):
    src: str
    dst: str


class SetRepReq(BaseModel):
    path: str
    replication: int = Field(ge=1)


class UploadReq(BaseModel):
    path: str
    size: int = Field(ge=0)
    replication: int | None = Field(default=None, ge=1)
    overwrite: bool = False


class AllocateReq(BaseModel):
    index: int
    exclude_nodes: list[str] = []
    have: list[dict] = []          # réplicas já confirmadas [{node_id, disk_id}]


class CommitChunk(BaseModel):
    index: int
    size: int
    hash: str
    replicas: list[dict]           # [{node_id, disk_id}]


class CommitReq(BaseModel):
    file_hash: str
    chunks: list[CommitChunk]


class CorruptReq(BaseModel):
    chunk_id: str
    node_id: str
    disk_id: str | None = None
    reporter: str = "client"


# --------------------------------------------------------------------- app
def create_app(cfg: dict) -> FastAPI:
    log = EventLogger("controller", "controller", file=cfg["logging"].get("file"))
    events: deque = deque(maxlen=300)
    log.sink = events.append
    metrics = Metrics()
    store = MetadataStore(cfg["controller"]["db_path"])
    cluster = ClusterRegistry(cfg, store, log)
    http = httpx.AsyncClient(timeout=float(cfg["controller"]["node_request_timeout_s"]))
    repl = ReplicationManager(cfg, store, cluster, log, http, metrics)
    reservations: dict[str, list[tuple[str, int]]] = {}
    master_key: dict[str, bytes | None] = {"key": None}

    def get_master() -> bytes:
        if master_key["key"] is None:
            master_key["key"] = crypto.load_or_create_master_key(cfg["encryption"]["master_key_file"])
        return master_key["key"]

    def release_upload(upload_id: str) -> None:
        for disk_id, size in reservations.pop(upload_id, []):
            cluster.release(disk_id, size)

    gc = GarbageCollector(cfg, store, cluster, repl, log, release_upload)

    async def liveness_loop() -> None:
        while True:
            await asyncio.sleep(1.0)
            for nid, old, new in cluster.check_liveness():
                if new == "dead":
                    repl.trigger(f"node_dead:{nid}")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if cfg["encryption"]["enabled"]:
            get_master()
        log.info("controller_started", replication=cfg["cluster"]["replication_factor"],
                 write_quorum=cfg["cluster"]["write_quorum"], encryption=cfg["encryption"]["enabled"],
                 chunk_size=chunk_size_bytes(cfg), db=cfg["controller"]["db_path"], **store.counts())
        tasks = [asyncio.create_task(liveness_loop()), asyncio.create_task(repl.run()),
                 asyncio.create_task(gc.run())]
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            await http.aclose()

    app = FastAPI(title="DComp Drive — controlador", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    app.state.store, app.state.cluster, app.state.repl = store, cluster, repl

    @app.exception_handler(FSError)
    async def fs_error(_: Request, exc: FSError):
        return JSONResponse(status_code=exc.status, content={"error": exc.message})

    @app.exception_handler(P.PathError)
    async def path_error(_: Request, exc: P.PathError):
        return JSONResponse(status_code=400, content={"error": str(exc)})

    @app.middleware("http")
    async def timing(request: Request, call_next):
        t0 = time.perf_counter()
        resp = await call_next(request)
        route = request.scope.get("route")
        name = f"{request.method} {route.path if route else request.url.path}"
        metrics.observe(name, (time.perf_counter() - t0) * 1000, error=resp.status_code >= 500)
        return resp

    # ----------------------------------------------------------- helpers
    def replica_view(rep) -> dict:
        n = cluster.nodes.get(rep["node_id"])
        d = n.disks.get(rep["disk_id"]) if n else None
        state = n.state if n else "unknown"
        if n and n.draining:
            state = "draining"
        if d and d.status != "ok":
            state = "disk_failed"
        return {"node_id": rep["node_id"], "disk_id": rep["disk_id"], "state": state,
                "url": n.public_url if n else None, "internal_url": n.internal_url if n else None,
                "zone": n.zone if n else None}

    def file_state(version_id: str | None, health: dict, inflight: set[str]) -> str:
        if not version_id or version_id not in health:
            return "writing"
        h = health[version_id]
        if h["nchunks"] == 0:
            return "healthy"
        if (h["min_live"] or 0) == 0:
            return "unavailable"
        if h["min_live"] < h["rep"]:
            return "recovering" if version_id in inflight else "under_replicated"
        return "healthy"

    def target_view(n, d) -> dict:
        return {"node_id": n.id, "disk_id": d.id, "url": n.public_url, "internal_url": n.internal_url,
                "zone": n.zone}

    # ======================================================= namespace (RF1)
    @app.get("/api/fs/list")
    async def list_dir(path: str = "/"):
        path = P.norm(path)
        rows = store.list_dir(path)
        health = store.version_health([r["version_id"] for r in rows if r["version_id"]])
        inflight = repl.inflight_versions()
        uploading = {u["path"] for u in store.all_uploads()}
        items = []
        for r in rows:
            item = {"name": P.name(r["path"]), "path": r["path"], "type": r["type"],
                    "modified_at": r["modified_at"], "created_at": r["created_at"]}
            if r["type"] == "file":
                st = file_state(r["version_id"], health, inflight)
                item.update(size=r["size"], hash=r["file_hash"], replication=r["v_replication"],
                            encrypted=bool(r["encrypted"]), state=st, state_label=STATE_LABELS[st],
                            live_replicas=health.get(r["version_id"], {}).get("min_live"),
                            being_written=r["path"] in uploading)
            else:
                item["replication"] = r["replication"]
            items.append(item)
        # arquivos novos ainda em gravação aparecem como "gravando"
        for u in store.all_uploads():
            if P.parent(u["path"]) == path and not any(i["path"] == u["path"] for i in items):
                items.append({"name": P.name(u["path"]), "path": u["path"], "type": "file", "size": u["size"],
                              "state": "writing", "state_label": STATE_LABELS["writing"],
                              "replication": u["replication"], "being_written": True})
        return {"path": path, "items": items}

    @app.post("/api/fs/mkdir")
    async def mkdir(req: MkdirReq):
        path = P.norm(req.path)
        created = store.mkdir(path, req.replication, req.parents)
        for d in created:
            log.info("dir_created", path=d)
        return {"path": path, "created": created}

    @app.delete("/api/fs/dir")
    async def rmdir(path: str, recursive: bool = False):
        res = store.rmdir(P.norm(path), recursive, float(cfg["gc"]["delete_grace_s"]))
        log.info("dir_removed", **res)
        return res

    @app.delete("/api/fs/file")
    async def rm(path: str):
        res = store.delete_file(P.norm(path), float(cfg["gc"]["delete_grace_s"]))
        log.info("file_deleted", **res)
        return res

    @app.post("/api/fs/rename")
    async def rename(req: RenameReq):
        res = store.rename(P.norm(req.src), P.norm(req.dst))
        log.info("renamed", **res)
        return res

    @app.post("/api/fs/setrep")
    async def setrep(req: SetRepReq):
        res = store.set_replication(P.norm(req.path), req.replication)
        log.info("replication_changed", **res)
        repl.trigger(f"setrep:{res['path']}")
        return res

    @app.get("/api/fs/stat")
    async def stat(path: str):
        path = P.norm(path)
        e = store.get_entry(path)
        if e is None:
            raise FSError(404, f"não existe: {path}")
        out = {"path": path, "type": e["type"], "created_at": e["created_at"], "modified_at": e["modified_at"]}
        if e["type"] == "dir":
            out["replication"] = store.effective_replication(path, int(cfg["cluster"]["replication_factor"]))
            out["replication_explicit"] = e["replication"]
            out["children"] = len(store.list_dir(path))
            return out
        v = store.get_version(e["version_id"])
        health = store.version_health([v["id"]])
        st = file_state(v["id"], health, repl.inflight_versions())
        reps = store.replicas_for_version(v["id"])
        chunks = []
        for c in store.version_chunks(v["id"]):
            views = [replica_view(r) for r in reps.get(c["id"], [])]
            live = sum(1 for x in views if x["state"] in ("alive", "suspect"))
            chunks.append({"index": c["idx"], "chunk_id": c["id"], "size": c["size"], "hash": c["hash"],
                           "live_replicas": live, "replicas": views})
        out.update({
            "version_id": v["id"], "size": v["size"], "hash": v["file_hash"], "hash_alg": v["hash_alg"],
            "chunk_size": v["chunk_size"], "replication": v["replication"], "encrypted": bool(v["encrypted"]),
            "state": st, "state_label": STATE_LABELS[st],
            "live_replicas_min": health.get(v["id"], {}).get("min_live"),
            "replica_nodes": sorted({x["node_id"] for c in chunks for x in c["replicas"]}),
            "chunks": chunks,
        })
        return out

    # ===================================================== escrita (RF3/RF8)
    @app.post("/api/uploads")
    async def open_upload(req: UploadReq):
        path = P.norm(req.path)
        if path == "/":
            raise FSError(400, "caminho inválido")
        store.require_dir(P.parent(path))
        e = store.get_entry(path)
        if e is not None and e["type"] == "dir":
            raise FSError(409, f"{path} é um diretório")
        if e is not None and not req.overwrite:
            raise FSError(409, f"arquivo já existe: {path} (use overwrite)")
        default_rep = int(cfg["cluster"]["replication_factor"])
        rep = req.replication or store.effective_replication(P.parent(path), default_rep)
        csize = chunk_size_bytes(cfg)
        nchunks = math.ceil(req.size / csize) if req.size else 0
        enc = bool(cfg["encryption"]["enabled"])
        overhead = 28 if enc else 0           # nonce + tag do AES-GCM
        upload_id, version_id = uuid.uuid4().hex, uuid.uuid4().hex
        chunk_ids = [uuid.uuid4().hex for _ in range(nchunks)]

        warnings: list[str] = []
        chunk_plan, reserved = [], []
        min_targets = rep
        any_fallback = False
        for i in range(nchunks):
            size = min(csize, req.size - i * csize) + overhead
            targets, fallback = cluster.choose_targets(rep, size)
            any_fallback |= fallback
            for _, d in targets:
                cluster.reserve(d.id, size)
                reserved.append((d.id, size))
            min_targets = min(min_targets, len(targets))
            chunk_plan.append({"index": i, "chunk_id": chunk_ids[i], "size": size,
                               "targets": [target_view(n, d) for n, d in targets]})

        def fail(status: int, msg: str):
            for disk_id, size in reserved:
                cluster.release(disk_id, size)
            log.warning("upload_rejected", path=path, reason=msg)
            raise FSError(status, msg)

        alive = sum(1 for n in cluster.nodes.values() if n.state == "alive" and not n.draining)
        if nchunks and min_targets == 0:
            fail(507, f"nenhum disco disponível (nós ativos: {alive}; discos podem estar acima do limite de uso)")
        quorum = min(int(cfg["cluster"]["write_quorum"]), rep)
        if nchunks and min_targets < rep:
            msg = (f"apenas {min_targets} destino(s) disponível(is) para N={rep} réplicas "
                   f"(nós ativos: {alive})")
            if cfg["cluster"]["under_replication_policy"] == "reject":
                fail(507, msg + " — gravação rejeitada pela política 'reject'")
            warnings.append(msg + " — arquivo será gravado SUB-REPLICADO e completado quando houver nós")
            quorum = min(quorum, min_targets)
        if any_fallback:
            warnings.append("menos máquinas que N: algumas réplicas ficaram em discos distintos da mesma máquina")

        wrapped, dek = None, None
        if enc:
            dek = crypto.generate_key()
            wrapped = crypto.wrap_key(get_master(), dek)
        lease = float(cfg["leases"]["write_lease_s"])
        try:
            store.create_upload({
                "id": upload_id, "path": path, "version_id": version_id, "size": req.size, "chunk_size": csize,
                "hash_alg": cfg["cluster"]["hash_algorithm"], "replication": rep, "write_quorum": quorum,
                "overwrite": int(req.overwrite), "encrypted": int(enc), "wrapped_key": wrapped,
                "chunk_ids": chunk_ids, "lease_expires": time.time() + lease, "created_at": time.time(),
            })
        except FSError as exc:
            fail(exc.status, exc.message)
        reservations[upload_id] = reserved
        log.info("upload_opened", upload_id=upload_id, path=path, size=req.size, chunks=nchunks,
                 replication=rep, write_quorum=quorum, encrypted=enc, warnings=warnings or None)
        return {
            "upload_id": upload_id, "version_id": version_id, "path": path, "chunk_size": csize,
            "hash_alg": cfg["cluster"]["hash_algorithm"], "replication": rep, "write_quorum": quorum,
            "lease_s": lease, "warnings": warnings,
            "encryption": {"enabled": enc, "algorithm": cfg["encryption"]["algorithm"] if enc else None,
                           "key": base64.b64encode(dek).decode() if dek else None},
            "chunks": chunk_plan,
        }

    def get_live_upload(upload_id: str):
        up = store.get_upload(upload_id)
        if up is None:
            raise FSError(410, "sessão de escrita não existe ou expirou")
        return up

    @app.post("/api/uploads/{upload_id}/renew")
    async def renew(upload_id: str):
        up = get_live_upload(upload_id)
        store.renew_upload(upload_id, time.time() + float(cfg["leases"]["write_lease_s"]))
        return {"upload_id": up["id"], "lease_s": cfg["leases"]["write_lease_s"]}

    @app.post("/api/uploads/{upload_id}/allocate")
    async def allocate(upload_id: str, req: AllocateReq):
        """Novos destinos para um bloco cujo pipeline falhou (nó lento/caído no meio da gravação)."""
        up = get_live_upload(upload_id)
        ids = json.loads(up["chunk_ids"])
        if not 0 <= req.index < len(ids):
            raise FSError(400, "índice de bloco inválido")
        have_nodes = {h["node_id"] for h in req.have}
        have_disks = {h["disk_id"] for h in req.have}
        zones = {cluster.nodes[x].zone for x in have_nodes if x in cluster.nodes}
        size = min(up["chunk_size"], up["size"] - req.index * up["chunk_size"]) + (28 if up["encrypted"] else 0)
        need = max(1, up["replication"] - len(req.have))
        targets, _ = cluster.choose_targets(need, size, exclude_disks=have_disks, used_nodes=have_nodes,
                                            used_zones=zones, exclude_nodes=set(req.exclude_nodes))
        log.info("chunk_reallocated", upload_id=upload_id, chunk_id=ids[req.index], index=req.index,
                 excluded=req.exclude_nodes, new_targets=[n.id for n, _ in targets])
        return {"index": req.index, "chunk_id": ids[req.index], "targets": [target_view(n, d) for n, d in targets]}

    @app.post("/api/uploads/{upload_id}/commit")
    async def commit(upload_id: str, req: CommitReq):
        up = get_live_upload(upload_id)
        ids = json.loads(up["chunk_ids"])
        by_idx = {c.index: c for c in req.chunks}
        if sorted(by_idx) != list(range(len(ids))):
            raise FSError(400, f"commit deve conter os blocos 0..{len(ids) - 1}")
        node_of_disk = cluster.node_of_disk()
        plan, weak = [], 0
        for i, cid in enumerate(ids):
            c = by_idx[i]
            disks = sorted({r["disk_id"] for r in c.replicas if r.get("disk_id") in node_of_disk})
            if len(disks) < up["write_quorum"]:
                raise FSError(409, f"quórum de escrita não atingido no bloco {i}: {len(disks)}/{up['write_quorum']}")
            if len(disks) < up["replication"]:
                weak += 1
            plan.append({"chunk_id": cid, "index": i, "size": c.size, "hash": c.hash, "disks": disks})
        res = store.commit_upload(up, req.file_hash, plan, node_of_disk, float(cfg["gc"]["delete_grace_s"]))
        release_upload(upload_id)
        log.info("file_committed", path=up["path"], version_id=up["version_id"], size=up["size"],
                 chunks=len(ids), file_hash=req.file_hash, replication=up["replication"],
                 under_replicated_chunks=weak, replaced_version=res["replaced_version"])
        if weak:
            repl.trigger(f"commit_below_N:{up['path']}")
        return {**res, "under_replicated_chunks": weak, "replication": up["replication"]}

    @app.delete("/api/uploads/{upload_id}")
    async def abort(upload_id: str):
        up = get_live_upload(upload_id)
        store.delete_upload(upload_id)
        release_upload(upload_id)
        log.warning("upload_aborted", upload_id=upload_id, path=up["path"], reason="client")
        return {"aborted": upload_id}

    # ============================================================ leitura
    @app.get("/api/fs/locate")
    async def locate(path: str):
        path = P.norm(path)
        e = store.get_entry(path)
        if e is None or e["type"] != "file" or not e["version_id"]:
            raise FSError(404, f"arquivo não existe: {path}")
        v = store.get_version(e["version_id"])
        reps = store.replicas_for_version(v["id"])
        chunks = []
        rank = {"alive": 0, "suspect": 1, "draining": 2}
        for c in store.version_chunks(v["id"]):
            views = [x for x in (replica_view(r) for r in reps.get(c["id"], [])) if x["state"] in rank]
            # réplica mais "próxima": viva, menos carregada; desempate aleatório espalha a leitura
            random.shuffle(views)
            views.sort(key=lambda x: (rank[x["state"]], cluster.nodes[x["node_id"]].active_ops))
            chunks.append({"index": c["idx"], "chunk_id": c["id"], "size": c["size"], "hash": c["hash"],
                           "replicas": views})
        key = None
        if v["encrypted"]:
            key = base64.b64encode(crypto.unwrap_key(get_master(), v["wrapped_key"])).decode()
        return {"path": path, "version_id": v["id"], "size": v["size"], "file_hash": v["file_hash"],
                "hash_alg": v["hash_alg"], "chunk_size": v["chunk_size"], "encrypted": bool(v["encrypted"]),
                "key": key, "chunks": chunks}

    @app.post("/api/chunks/corrupt")
    async def report_corrupt(req: CorruptReq):
        """Relato de corrupção. Se veio do cliente, o controlador pede ao nó para
        reverificar (um cliente com bug não consegue apagar réplicas boas)."""
        n = cluster.nodes.get(req.node_id)
        disk_id = req.disk_id
        confirmed = req.reporter == "node"
        if not confirmed and n is not None:
            try:
                r = await http.post(f"{n.internal_url}/chunks/{req.chunk_id}/verify", timeout=30)
                body = r.json()
                confirmed = r.status_code == 200 and body.get("corrupt", False) or r.status_code == 404
                disk_id = disk_id or body.get("disk_id")
            except Exception:
                confirmed = False
        if confirmed and disk_id:
            store.remove_replica(req.chunk_id, disk_id)
            log.warning("replica_corrupt", chunk_id=req.chunk_id, node_id=req.node_id, disk_id=disk_id,
                        reporter=req.reporter)
            repl.trigger(f"corruption:{req.chunk_id}")
        return {"confirmed": confirmed}

    # ======================================================= nós (internos)
    @app.post("/api/internal/register")
    async def register(body: dict):
        n = cluster.register(body)
        repl.trigger(f"node_registered:{n.id}")
        hb = cfg["heartbeat"]
        return {"ok": True, "node_id": n.id, "config": {
            "heartbeat_interval_s": hb["interval_s"], "block_report_interval_s": hb["block_report_interval_s"],
            "scrub_interval_s": cfg["storage"]["scrub_interval_s"]}}

    @app.post("/api/internal/heartbeat")
    async def heartbeat(body: dict):
        res = cluster.heartbeat(body)
        if res is None:
            return JSONResponse(status_code=404, content={"error": "nó desconhecido", "reregister": True})
        if res["changed"]:
            repl.trigger(f"node_change:{body.get('node_id')}")
        return {"ok": True, "block_report": res["block_report"]}

    @app.post("/api/internal/blockreport")
    async def block_report(body: dict):
        if body.get("node_id") not in cluster.nodes:
            return JSONResponse(status_code=404, content={"error": "nó desconhecido", "reregister": True})
        res = reconcile_block_report(cfg, store, cluster, log, body["node_id"], body.get("disks_ok", []),
                                     body.get("chunks", []))
        if res.get("changed"):
            repl.trigger(f"block_report:{body['node_id']}")
        return res

    # ===================================================== administração
    @app.post("/api/nodes/{node_id}/drain")
    async def drain(node_id: str):
        n = cluster.nodes.get(node_id)
        if n is None:
            raise FSError(404, "nó desconhecido")
        n.draining, n.drained = True, False
        store.set_node_drain(node_id, True, False)
        log.info("node_drain_started", node_id=node_id, replicas=len(store.chunk_ids_on_node(node_id)))
        repl.trigger(f"drain:{node_id}")
        return {"node_id": node_id, "draining": True}

    @app.post("/api/nodes/{node_id}/undrain")
    async def undrain(node_id: str):
        n = cluster.nodes.get(node_id)
        if n is None:
            raise FSError(404, "nó desconhecido")
        n.draining = n.drained = False
        store.set_node_drain(node_id, False, False)
        log.info("node_drain_cancelled", node_id=node_id)
        return {"node_id": node_id, "draining": False}

    @app.delete("/api/nodes/{node_id}")
    async def forget(node_id: str):
        n = cluster.nodes.get(node_id)
        if n is None:
            raise FSError(404, "nó desconhecido")
        if n.state != "dead" and not n.drained:
            raise FSError(409, "só é possível remover nós mortos ou drenados")
        removed = store.remove_node(node_id)
        cluster.nodes.pop(node_id, None)
        log.info("node_removed", node_id=node_id, replica_rows=removed)
        repl.trigger(f"node_removed:{node_id}")
        return {"node_id": node_id, "replica_rows_removed": removed}

    # ============================================= visibilidade (RF11) e métricas
    def files_by_state() -> dict:
        health = store.version_health()
        inflight = repl.inflight_versions()
        counts = {k: 0 for k in STATE_LABELS}
        for vid in health:
            counts[file_state(vid, health, inflight)] += 1
        counts["writing"] = len(store.all_uploads())
        return counts

    @app.get("/api/cluster/status")
    async def cluster_status():
        nodes = cluster.summary(store.replica_counts_by_node())
        by_state = {"alive": 0, "suspect": 0, "dead": 0}
        for n in nodes:
            by_state[n["state"]] = by_state.get(n["state"], 0) + 1
        return {
            "time": time.time(),
            "nodes": nodes,
            "nodes_by_state": by_state,
            "files_by_state": files_by_state(),
            "state_labels": STATE_LABELS,
            "totals": store.counts(),
            "replication": repl.status(),
            "config": public_config(),
        }

    @app.get("/api/events")
    async def recent_events(limit: int = 100):
        return {"events": list(events)[-limit:][::-1]}

    def public_config() -> dict:
        return {
            "replication_factor": cfg["cluster"]["replication_factor"],
            "write_quorum": cfg["cluster"]["write_quorum"],
            "under_replication_policy": cfg["cluster"]["under_replication_policy"],
            "chunk_size": chunk_size_bytes(cfg), "hash_algorithm": cfg["cluster"]["hash_algorithm"],
            "encryption": cfg["encryption"]["enabled"], "placement": cfg["placement"],
            "heartbeat": cfg["heartbeat"], "disk_usage_limit": cfg["storage"]["disk_usage_limit"],
            "rereplication": cfg["rereplication"],
        }

    @app.get("/api/config")
    async def get_config():
        return public_config()

    @app.get("/metrics")
    async def get_metrics():
        snap = metrics.snapshot()
        snap["files_by_state"] = files_by_state()
        snap["nodes_by_state"] = {s: sum(1 for n in cluster.nodes.values() if n.state == s)
                                  for s in ("alive", "suspect", "dead")}
        snap["totals"] = store.counts()
        snap["replication"] = repl.status()
        snap["failure_detection"] = cluster.detection_times[-20:]
        return snap

    @app.get("/health")
    async def health():
        return {"ok": True, "role": "controller+metadata"}

    return app

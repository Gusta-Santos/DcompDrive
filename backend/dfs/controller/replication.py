"""Re-replicação (RF5), trim de excesso, drain (RF7), coleta de lixo e reconciliação (RF6).

Laço principal (a cada `check_interval_s`, ou imediatamente quando algo muda):
  1. SQL calcula os blocos cuja contagem de réplicas VIVAS != fator desejado.
  2. Sub-replicados são ordenados por réplicas vivas (ascendente): quem tem
     menos cópias é recuperado primeiro (prioridade exigida pelo RF5).
  3. Para cada um, escolhe origem (réplica viva menos carregada) e destino
     (choose_targets respeitando a regra de não repetição) e manda o NÓ DESTINO
     puxar o bloco do NÓ ORIGEM. O dado não passa pelo controlador.
  4. Limites: `max_concurrent` cópias no cluster, `max_per_node` por nó e
     `bandwidth_mbps` por cópia (o nó destino limita a taxa de leitura).
  5. Super-replicados (ex.: nó que voltou) perdem as réplicas excedentes.
  6. Nós em drain cujos blocos já têm N cópias fora deles viram "drained".
"""
from __future__ import annotations

import asyncio
import time

import httpx

from ..common.logs import EventLogger
from ..common.metrics import Metrics
from .cluster import ClusterRegistry
from .store import MetadataStore


class ReplicationManager:
    def __init__(self, cfg: dict, store: MetadataStore, cluster: ClusterRegistry, log: EventLogger,
                 http: httpx.AsyncClient, metrics: Metrics):
        self.cfg = cfg
        self.store = store
        self.cluster = cluster
        self.log = log
        self.http = http
        self.metrics = metrics
        self.wake = asyncio.Event()
        self.inflight: dict[str, dict] = {}     # chunk_id -> tarefa em andamento
        self.node_busy: dict[str, int] = {}
        self.completed = 0
        self.failed = 0
        self.bytes_copied = 0
        self.under = 0
        self.lost = 0
        self.over = 0
        self.episode: dict | None = None
        self.episodes: list[dict] = []
        self.last_cause = "startup"
        self._warned_no_target: set[str] = set()
        self._warned_lost: set[str] = set()

    def trigger(self, cause: str | None = None) -> None:
        if cause:
            self.last_cause = cause
        self.wake.set()

    def inflight_versions(self) -> set[str]:
        return {t["version_id"] for t in self.inflight.values()}

    # ------------------------------------------------------------- laço
    async def run(self) -> None:
        interval = float(self.cfg["rereplication"]["check_interval_s"])
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            try:
                await self.tick()
            except Exception as exc:  # o laço nunca pode morrer
                self.log.error("replication_tick_error", error=repr(exc))

    async def tick(self) -> None:
        rows = self.store.deficient_chunks()
        under = [r for r in rows if r["live"] < r["replication"]]
        over = [r for r in rows if r["live"] > r["replication"]]
        recoverable = [r for r in under if r["live"] > 0]
        self.under, self.over = len(under), len(over)
        self.lost = len(under) - len(recoverable)
        self._track_episode(len(recoverable))

        for r in under:
            if r["live"] == 0 and r["chunk_id"] not in self._warned_lost:
                self._warned_lost.add(r["chunk_id"])
                self.log.error("chunk_unavailable", chunk_id=r["chunk_id"], version_id=r["version_id"],
                               detail="nenhuma réplica viva; aguardando o nó voltar")

        rr = self.cfg["rereplication"]
        recoverable.sort(key=lambda r: (r["live"], r["live"] - r["replication"]))
        for r in recoverable:
            if len(self.inflight) >= int(rr["max_concurrent"]):
                break
            if r["chunk_id"] in self.inflight:
                continue
            self._schedule(r)

        for r in over:
            if r["chunk_id"] not in self.inflight:
                await self._trim(r)

        await self._check_drains({r["chunk_id"] for r in under})

    # -------------------------------------------------------- re-replicação
    def _schedule(self, r) -> None:
        cid = r["chunk_id"]
        max_per_node = int(self.cfg["rereplication"]["max_per_node"])
        replicas = self.store.replicas_of(cid)
        sources = []
        for rep in replicas:
            n = self.cluster.nodes.get(rep["node_id"])
            if n and n.state == "alive" and self.cluster.is_readable(n.id, rep["disk_id"]):
                if self.node_busy.get(n.id, 0) < max_per_node:
                    sources.append(n)
        if not sources:
            return
        sources.sort(key=lambda n: (self.node_busy.get(n.id, 0), n.active_ops))
        used_nodes = {rep["node_id"] for rep in replicas}
        used_zones = {self.cluster.nodes[x].zone for x in used_nodes if x in self.cluster.nodes}
        busy = {nid for nid, c in self.node_busy.items() if c >= max_per_node}
        targets, fallback = self.cluster.choose_targets(
            1, r["size"], exclude_disks={rep["disk_id"] for rep in replicas},
            used_nodes=used_nodes, used_zones=used_zones, exclude_nodes=busy,
        )
        if not targets:
            if cid not in self._warned_no_target:
                self._warned_no_target.add(cid)
                self.log.warning("rereplication_no_target", chunk_id=cid, live=r["live"], wanted=r["replication"])
            return
        self._warned_no_target.discard(cid)
        tnode, tdisk = targets[0]
        task = {
            "chunk_id": cid, "version_id": r["version_id"], "size": r["size"], "live": r["live"],
            "wanted": r["replication"], "source": sources[0].id, "target": tnode.id, "target_disk": tdisk.id,
            "started": time.time(),
        }
        self.inflight[cid] = task
        for nid in {sources[0].id, tnode.id}:
            self.node_busy[nid] = self.node_busy.get(nid, 0) + 1
        self.cluster.reserve(tdisk.id, r["size"])
        self.log.info("rereplication_started", chunk_id=cid, source=sources[0].id, target=tnode.id,
                      disk_id=tdisk.id, live=r["live"], wanted=r["replication"], same_machine_fallback=fallback)
        asyncio.create_task(self._copy(task, r, [s.internal_url for s in sources], tnode, tdisk))

    async def _copy(self, task: dict, r, source_urls: list[str], tnode, tdisk) -> None:
        cid = task["chunk_id"]
        ok = False
        try:
            resp = await self.http.post(
                f"{tnode.internal_url}/replicate",
                json={"chunk_id": cid, "hash": r["hash"], "hash_alg": r["hash_alg"], "disk_id": tdisk.id,
                      "sources": source_urls, "bandwidth_mbps": float(self.cfg["rereplication"]["bandwidth_mbps"])},
                timeout=max(60.0, r["size"] / 1e5),
            )
            if resp.status_code == 200:
                info = self.store.chunk_info(cid)
                if info is not None and info["vstatus"] == "committed":
                    self.store.add_replica(cid, tdisk.id, tnode.id)
                    ok = True
                else:  # arquivo foi apagado enquanto copiávamos
                    await self._send_delete(tnode, cid, tdisk.id)
            else:
                self.log.warning("rereplication_failed", chunk_id=cid, target=tnode.id, status=resp.status_code,
                                 detail=resp.text[:200])
        except Exception as exc:
            self.log.warning("rereplication_failed", chunk_id=cid, target=tnode.id, error=repr(exc))
        finally:
            dur = time.time() - task["started"]
            self.inflight.pop(cid, None)
            for nid in {task["source"], task["target"]}:
                self.node_busy[nid] = max(0, self.node_busy.get(nid, 1) - 1)
            self.cluster.release(tdisk.id, r["size"])
            if ok:
                self.completed += 1
                self.bytes_copied += r["size"]
                if self.episode:
                    self.episode["chunks_copied"] += 1
                    self.episode["bytes_copied"] += r["size"]
                self.metrics.observe("rereplication", dur * 1000, r["size"])
                self.log.info("rereplication_completed", chunk_id=cid, target=tnode.id, disk_id=tdisk.id,
                              bytes=r["size"], duration_ms=round(dur * 1000, 1))
            else:
                self.failed += 1
            self.wake.set()

    # ------------------------------------------------------------ excesso
    async def _trim(self, r) -> None:
        """Remove réplicas excedentes, preferindo as que violam a regra de máquina e
        depois as que estão no disco mais cheio."""
        cid = r["chunk_id"]
        live = []
        for rep in self.store.replicas_of(cid):
            n = self.cluster.nodes.get(rep["node_id"])
            d = n.disks.get(rep["disk_id"]) if n else None
            if n and d and n.state == "alive" and not n.draining and d.status == "ok":
                live.append((n, d))
        excess = len(live) - r["replication"]
        if excess <= 0:
            return
        per_node: dict[str, int] = {}
        for n, _ in live:
            per_node[n.id] = per_node.get(n.id, 0) + 1
        live.sort(key=lambda nd: (per_node[nd[0].id] > 1, nd[1].projected_usage()), reverse=True)
        for n, d in live[:excess]:
            if await self._send_delete(n, cid, d.id):
                self.store.remove_replica(cid, d.id)
                self.log.info("replica_trimmed", chunk_id=cid, node_id=n.id, disk_id=d.id,
                              live=len(live), wanted=r["replication"])

    async def _send_delete(self, node, chunk_id: str, disk_id: str | None) -> bool:
        try:
            params = {"disk": disk_id} if disk_id else None
            resp = await self.http.delete(f"{node.internal_url}/chunks/{chunk_id}", params=params, timeout=10)
            return resp.status_code in (200, 404)
        except Exception:
            return False

    # -------------------------------------------------------------- drain
    async def _check_drains(self, under_ids: set[str]) -> None:
        for n in list(self.cluster.nodes.values()):
            if not n.draining or n.drained:
                continue
            on_node = self.store.chunk_ids_on_node(n.id)
            pending = (on_node & under_ids) | (on_node & set(self.inflight))
            if pending:
                continue
            for rep in self.store.q("SELECT chunk_id, disk_id FROM replicas WHERE node_id=?", (n.id,)):
                if n.state != "dead":
                    await self._send_delete(n, rep["chunk_id"], rep["disk_id"])
                self.store.remove_replica(rep["chunk_id"], rep["disk_id"])
            n.drained = True
            self.store.set_node_drain(n.id, True, True)
            self.log.info("node_drained", node_id=n.id, chunks_moved=len(on_node),
                          detail="nó pode ser desligado com segurança")

    # ------------------------------------------------------ episódios (métrica)
    def _track_episode(self, n_under: int) -> None:
        now = time.time()
        if n_under > 0 and self.episode is None:
            self.episode = {"started": now, "cause": self.last_cause, "initial_under_replicated": n_under,
                            "chunks_copied": 0, "bytes_copied": 0}
            self.log.info("recovery_started", cause=self.last_cause, under_replicated=n_under)
        elif n_under == 0 and self.episode is not None:
            ep = self.episode
            ep["finished"] = now
            ep["duration_s"] = round(now - ep["started"], 3)
            self.episodes.append(ep)
            self.episodes = self.episodes[-50:]
            self.episode = None
            self.log.info("recovery_completed", cause=ep["cause"], duration_s=ep["duration_s"],
                          chunks_copied=ep["chunks_copied"], bytes_copied=ep["bytes_copied"])
            self.last_cause = "unknown"

    def status(self) -> dict:
        return {
            "in_progress": [{**t, "elapsed_s": round(time.time() - t["started"], 2)} for t in self.inflight.values()],
            "under_replicated_chunks": self.under,
            "over_replicated_chunks": self.over,
            "unavailable_chunks": self.lost,
            "completed_total": self.completed,
            "failed_total": self.failed,
            "bytes_copied_total": self.bytes_copied,
            "current_episode": self.episode,
            "recent_episodes": self.episodes[-10:],
        }


class GarbageCollector:
    """Apaga blocos de arquivos removidos/sobrescritos (após carência) e aborta
    gravações cujo lease expirou ("gravações incompletas devem ser desfeitas")."""

    def __init__(self, cfg, store: MetadataStore, cluster: ClusterRegistry, repl: ReplicationManager,
                 log: EventLogger, release_upload):
        self.cfg = cfg
        self.store = store
        self.cluster = cluster
        self.repl = repl
        self.log = log
        self.release_upload = release_upload

    async def run(self) -> None:
        while True:
            await asyncio.sleep(float(self.cfg["gc"]["interval_s"]))
            try:
                await self.tick()
            except Exception as exc:
                self.log.error("gc_error", error=repr(exc))

    async def tick(self) -> None:
        now = time.time()
        for up in self.store.expired_uploads(now):
            self.store.delete_upload(up["id"])
            self.release_upload(up["id"])
            self.log.warning("upload_aborted", upload_id=up["id"], path=up["path"], reason="lease_expired",
                             detail="blocos órfãos serão removidos via block report")
        for cid in self.store.due_garbage(now):
            for rep in self.store.replicas_of(cid):
                n = self.cluster.nodes.get(rep["node_id"])
                if n and n.state != "dead":
                    await self.repl._send_delete(n, cid, rep["disk_id"])
            self.store.purge_chunk(cid)
            self.log.info("chunk_garbage_collected", chunk_id=cid)


def reconcile_block_report(cfg, store: MetadataStore, cluster: ClusterRegistry, log: EventLogger,
                           node_id: str, disks_ok: list[str], items: list[dict]) -> dict:
    """Reconciliação (RF6): compara o que o nó TEM com o que os metadados DIZEM.

      * bloco conhecido, versão ativa, hash confere  -> mantém (re-anexa se faltava a linha)
      * bloco conhecido mas hash diferente            -> réplica corrompida: apaga
      * bloco de arquivo apagado/sobrescrito          -> obsoleto: apaga
      * bloco desconhecido (órfão de gravação abortada) -> apaga após carência
      * linha nos metadados sem o bloco no nó         -> remove a linha (réplica perdida)
    """
    now = time.time()
    node = cluster.nodes.get(node_id)
    if node is None:
        return {"delete": []}
    orphan_grace = float(cfg["gc"]["orphan_grace_s"])
    uploading = store.active_upload_chunk_ids()
    deletes: list[dict] = []
    seen: set[tuple[str, str]] = set()
    reattached = obsolete = orphans = mismatched = 0

    for it in items:
        cid, did = it["chunk_id"], it["disk_id"]
        if did not in node.disks:
            continue
        if node.drained:
            deletes.append({"chunk_id": cid, "disk_id": did})
            continue
        info = store.chunk_info(cid)
        if info is None:
            if cid in uploading:
                continue
            if float(it.get("age_s", 0)) > orphan_grace:
                deletes.append({"chunk_id": cid, "disk_id": did})
                orphans += 1
            continue
        if info["vstatus"] != "committed":
            if store.has_replica(cid, did):
                seen.add((cid, did))   # o GC vai apagá-lo quando a carência acabar
            else:
                deletes.append({"chunk_id": cid, "disk_id": did})
                obsolete += 1
            continue
        if it.get("hash") != info["hash"]:
            store.remove_replica(cid, did)
            deletes.append({"chunk_id": cid, "disk_id": did})
            mismatched += 1
            log.warning("replica_hash_mismatch", chunk_id=cid, node_id=node_id, disk_id=did)
            continue
        seen.add((cid, did))
        if not store.has_replica(cid, did):
            store.add_replica(cid, did, node_id)
            reattached += 1

    missing = 0
    for r in store.replicas_on_disks(disks_ok):
        if (r["chunk_id"], r["disk_id"]) not in seen and now - r["added_at"] > 30:
            store.remove_replica(r["chunk_id"], r["disk_id"])
            missing += 1

    log.info("block_report_reconciled", node_id=node_id, reported=len(items), reattached=reattached,
             obsolete=obsolete, orphans=orphans, hash_mismatch=mismatched, missing=missing,
             to_delete=len(deletes))
    return {"delete": deletes, "changed": bool(reattached or mismatched or missing)}

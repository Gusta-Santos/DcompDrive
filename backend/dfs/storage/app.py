"""Nó de armazenamento (plano de dados).

Responsabilidades:
  * registrar-se no controlador e enviar heartbeats com espaço por disco (RF6/RF7);
  * receber blocos do cliente, verificar hash, gravar e propagar em PIPELINE para
    a próxima réplica (estilo HDFS) — o cliente só envia o bloco uma vez;
  * servir blocos, verificando o hash em toda leitura (RF4b);
  * scrubbing periódico (RF4c): réplica corrompida é apagada e reportada;
  * copiar blocos de outro nó quando o controlador pedir (re-replicação), com
    limite de banda;
  * enviar block reports para reconciliação (RF6).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from ..common.hashing import hash_bytes
from ..common.logs import EventLogger
from ..common.metrics import Metrics
from .chunkstore import ChunkError, ChunkStore, Corrupt, HashMismatch


class ReplicateReq(BaseModel):
    chunk_id: str
    hash: str
    hash_alg: str = "sha256"
    disk_id: str | None = None
    sources: list[str]
    bandwidth_mbps: float = 0


def create_app(settings: dict) -> FastAPI:
    node_id = settings["node_id"]
    log = EventLogger("storage", node_id, file=settings.get("log_file"))
    metrics = Metrics()
    store = ChunkStore(node_id, settings["disks"], settings.get("capacity_bytes"))
    controller = settings["controller_url"].rstrip("/")
    state = {"registered": False, "hb": 2.0, "br": 30.0, "scrub": 60.0, "want_report": True,
             "active": 0, "last_report": 0.0}
    http = httpx.AsyncClient(timeout=30)

    # ------------------------------------------------------ controle (loops)
    def disk_payload() -> list[dict]:
        return store.disk_stats()

    async def register() -> None:
        body = {"node_id": node_id, "internal_url": settings["internal_url"],
                "public_url": settings["public_url"], "zone": settings["zone"], "disks": disk_payload()}
        resp = await http.post(f"{controller}/api/internal/register", json=body, timeout=10)
        resp.raise_for_status()
        conf = resp.json().get("config", {})
        state["hb"] = float(conf.get("heartbeat_interval_s", state["hb"]))
        state["br"] = float(conf.get("block_report_interval_s", state["br"]))
        state["scrub"] = float(settings.get("scrub_interval_s") or conf.get("scrub_interval_s", state["scrub"]))
        state["registered"] = True
        state["want_report"] = True
        log.info("registered", controller=controller, disks=[d["id"] for d in body["disks"]],
                 heartbeat_s=state["hb"])

    async def send_block_report() -> None:
        items = await run_in_threadpool(store.report)
        disks_ok = [d.id for d in store.disks.values() if d.status == "ok"]
        resp = await http.post(f"{controller}/api/internal/blockreport",
                               json={"node_id": node_id, "disks_ok": disks_ok, "chunks": items}, timeout=60)
        if resp.status_code == 404:
            state["registered"] = False
            return
        resp.raise_for_status()
        deletes = resp.json().get("delete", [])
        for d in deletes:
            await run_in_threadpool(store.delete, d["chunk_id"], d.get("disk_id"))
        state["last_report"] = time.time()
        state["want_report"] = False
        log.info("block_report_sent", chunks=len(items), deleted=len(deletes))

    async def control_loop() -> None:
        while True:
            try:
                if not state["registered"]:
                    await register()
                for disk_id, new in store.check_disks():
                    (log.warning if new != "ok" else log.info)("disk_status_changed", disk_id=disk_id, status=new)
                    if new == "ok":
                        state["want_report"] = True
                resp = await http.post(f"{controller}/api/internal/heartbeat", json={
                    "node_id": node_id, "disks": disk_payload(), "active_ops": state["active"],
                    "chunk_count": store.chunk_count()}, timeout=5)
                if resp.status_code == 404:
                    state["registered"] = False
                    continue
                if resp.json().get("block_report"):
                    state["want_report"] = True
                if state["want_report"] or time.time() - state["last_report"] > state["br"]:
                    await send_block_report()
            except Exception as exc:
                log.warning("controller_unreachable", error=repr(exc)[:200])
                state["registered"] = state["registered"] and not isinstance(exc, httpx.HTTPStatusError)
            await asyncio.sleep(state["hb"])

    async def report_corrupt(cid: str, disk_id: str, reason: str) -> None:
        metrics.inc("corrupt_detected")
        log.warning("chunk_corrupt_detected", chunk_id=cid, disk_id=disk_id, reason=reason,
                    detail="réplica descartada")
        try:
            await http.post(f"{controller}/api/chunks/corrupt",
                            json={"chunk_id": cid, "node_id": node_id, "disk_id": disk_id, "reporter": "node"})
        except Exception as exc:
            log.warning("corrupt_report_failed", chunk_id=cid, error=repr(exc))

    async def scrub_loop() -> None:
        """RF4(c): relê todos os blocos e confere com o hash gravado no .meta."""
        await asyncio.sleep(5)
        while True:
            t0 = time.time()
            checked = bad = 0
            for cid, did in store.all_chunks():
                if store.disks[did].status != "ok":
                    continue
                for disk_id, ok in await run_in_threadpool(store.verify, cid, did):
                    checked += 1
                    if not ok:
                        bad += 1
                        await run_in_threadpool(store.delete, cid, disk_id)
                        await report_corrupt(cid, disk_id, "scrub")
                await asyncio.sleep(0.005)
            metrics.inc("scrub_runs")
            metrics.inc("scrub_chunks_checked", checked)
            log.info("scrub_completed", checked=checked, corrupt=bad, duration_ms=round((time.time() - t0) * 1000))
            await asyncio.sleep(state["scrub"])

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("storage_started", disks=[d.id for d in store.disks.values()], chunks=store.chunk_count(),
                 internal_url=settings["internal_url"], public_url=settings["public_url"], zone=settings["zone"])
        tasks = [asyncio.create_task(control_loop()), asyncio.create_task(scrub_loop())]
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            await http.aclose()

    app = FastAPI(title=f"DComp Drive — nó {node_id}", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                       expose_headers=["X-Chunk-Hash", "X-Disk-Id"])

    @app.exception_handler(ChunkError)
    async def chunk_error(_: Request, exc: ChunkError):
        return JSONResponse(status_code=exc.status, content={"error": str(exc)})

    # --------------------------------------------------------- plano de dados
    @app.put("/chunks/{chunk_id}")
    async def put_chunk(chunk_id: str, request: Request, disk: str | None = None):
        """Grava o bloco localmente e propaga para o restante do pipeline.

        Cabeçalhos: X-Chunk-Hash, X-Hash-Alg, X-Pipeline (JSON com os próximos destinos).
        Resposta: {acks:[{node_id,disk_id}], errors:[...]} — o cliente sabe exatamente
        quais réplicas foram gravadas E verificadas.
        """
        state["active"] += 1
        t0 = time.perf_counter()
        try:
            data = await request.body()
            expected = request.headers.get("x-chunk-hash", "")
            alg = request.headers.get("x-hash-alg", "sha256")
            pipeline = json.loads(request.headers.get("x-pipeline") or "[]")
            acks, errors = [], []
            try:
                did = await run_in_threadpool(store.put, disk, chunk_id, data, expected, alg)
                acks.append({"node_id": node_id, "disk_id": did})
                log.info("chunk_stored", chunk_id=chunk_id, disk_id=did, bytes=len(data),
                         pipeline_remaining=len(pipeline))
            except HashMismatch as exc:
                # dado chegou corrompido: não adianta propagar
                log.warning("chunk_rejected", chunk_id=chunk_id, reason=str(exc))
                return JSONResponse(status_code=422, content={"error": str(exc), "acks": [], "errors": []})
            except ChunkError as exc:
                errors.append({"node_id": node_id, "disk_id": disk, "error": str(exc)})
                log.warning("chunk_store_failed", chunk_id=chunk_id, disk_id=disk, error=str(exc))

            # pipeline: tenta o próximo; se ele falhar, pula para o seguinte
            remaining = list(pipeline)
            while remaining:
                nxt, rest = remaining[0], remaining[1:]
                try:
                    r = await http.put(f"{nxt['internal_url']}/chunks/{chunk_id}", params={"disk": nxt["disk_id"]},
                                       content=data, headers={"X-Chunk-Hash": expected, "X-Hash-Alg": alg,
                                                              "X-Pipeline": json.dumps(rest)}, timeout=60)
                    body = r.json()
                    acks += body.get("acks", [])
                    errors += body.get("errors", [])
                    if r.status_code == 200:
                        break
                    errors.append({"node_id": nxt["node_id"], "disk_id": nxt["disk_id"], "error": body.get("error")})
                except Exception as exc:
                    errors.append({"node_id": nxt["node_id"], "disk_id": nxt["disk_id"], "error": repr(exc)[:200]})
                    log.warning("pipeline_forward_failed", chunk_id=chunk_id, next_node=nxt["node_id"],
                                error=repr(exc)[:200])
                remaining = rest
            ms = (time.perf_counter() - t0) * 1000
            metrics.observe("put_chunk", ms, len(data))
            return {"acks": acks, "errors": errors}
        finally:
            state["active"] -= 1

    @app.get("/chunks/{chunk_id}")
    async def get_chunk(chunk_id: str, disk: str | None = None):
        state["active"] += 1
        t0 = time.perf_counter()
        try:
            data, meta, did = await run_in_threadpool(store.read, chunk_id, disk)
            metrics.observe("get_chunk", (time.perf_counter() - t0) * 1000, len(data))
            return Response(content=data, media_type="application/octet-stream",
                            headers={"X-Chunk-Hash": meta["hash"], "X-Disk-Id": did})
        except Corrupt as exc:
            await report_corrupt(exc.chunk_id, exc.disk_id, "read")
            return JSONResponse(status_code=409, content={"error": str(exc), "corrupt": True})
        finally:
            state["active"] -= 1

    @app.post("/chunks/{chunk_id}/verify")
    async def verify_chunk(chunk_id: str):
        results = await run_in_threadpool(store.verify, chunk_id)
        if not results:
            return JSONResponse(status_code=404, content={"error": "não encontrado", "corrupt": False})
        bad = [d for d, ok in results if not ok]
        for d in bad:
            await run_in_threadpool(store.delete, chunk_id, d)
            await report_corrupt(chunk_id, d, "verify")
        return {"corrupt": bool(bad), "disk_id": bad[0] if bad else results[0][0], "results": results}

    @app.delete("/chunks/{chunk_id}")
    async def delete_chunk(chunk_id: str, disk: str | None = None):
        n = await run_in_threadpool(store.delete, chunk_id, disk)
        if n:
            log.info("chunk_deleted", chunk_id=chunk_id, disk_id=disk)
        return {"deleted": n}

    @app.post("/replicate")
    async def replicate(req: ReplicateReq):
        """Puxa o bloco de outro nó (re-replicação), respeitando o limite de banda."""
        state["active"] += 1
        t0 = time.perf_counter()
        try:
            data = None
            for src in req.sources:
                try:
                    buf = bytearray()
                    start = time.monotonic()
                    async with http.stream("GET", f"{src}/chunks/{req.chunk_id}", timeout=120) as r:
                        if r.status_code != 200:
                            continue
                        async for piece in r.aiter_bytes(256 * 1024):
                            buf += piece
                            if req.bandwidth_mbps > 0:
                                should = len(buf) / (req.bandwidth_mbps * 1024 * 1024)
                                elapsed = time.monotonic() - start
                                if should > elapsed:
                                    await asyncio.sleep(should - elapsed)
                    if hash_bytes(bytes(buf), req.hash_alg) == req.hash:
                        data = bytes(buf)
                        break
                    log.warning("replicate_source_bad_hash", chunk_id=req.chunk_id, source=src)
                except Exception as exc:
                    log.warning("replicate_source_failed", chunk_id=req.chunk_id, source=src, error=repr(exc)[:200])
            if data is None:
                return JSONResponse(status_code=502, content={"error": "nenhuma origem forneceu cópia íntegra"})
            did = await run_in_threadpool(store.put, req.disk_id, req.chunk_id, data, req.hash, req.hash_alg)
            ms = (time.perf_counter() - t0) * 1000
            metrics.observe("replicate_in", ms, len(data))
            log.info("chunk_replicated", chunk_id=req.chunk_id, disk_id=did, bytes=len(data), duration_ms=round(ms, 1))
            return {"ok": True, "disk_id": did, "bytes": len(data)}
        finally:
            state["active"] -= 1

    # ------------------------------------------------------------ observação
    @app.get("/health")
    async def health():
        return {"ok": True, "node_id": node_id, "registered": state["registered"]}

    @app.get("/info")
    async def info():
        return {"node_id": node_id, "zone": settings["zone"], "disks": disk_payload(), "chunks": store.chunk_count()}

    @app.get("/metrics")
    async def get_metrics():
        snap = metrics.snapshot()
        snap["node_id"] = node_id
        snap["disks"] = disk_payload()
        snap["chunks"] = store.chunk_count()
        snap["active_ops"] = state["active"]
        return snap

    # ------------------------------------- simulação de falhas (Parte 2 / demo)
    if settings.get("debug_endpoints", True):
        @app.post("/admin/chunks/{chunk_id}/corrupt")
        async def corrupt(chunk_id: str):
            did = await run_in_threadpool(store.corrupt, chunk_id)
            log.warning("debug_chunk_corrupted", chunk_id=chunk_id, disk_id=did)
            return {"corrupted": chunk_id, "disk_id": did}

        @app.post("/admin/disks/{disk}/fail")
        async def fail_disk(disk: str):
            d = store.disks.get(disk) or next((x for x in store.disks.values() if x.name == disk), None)
            if d is None:
                return JSONResponse(status_code=404, content={"error": "disco desconhecido"})
            d.simulated_failure = True
            store.check_disks()
            log.warning("debug_disk_failed", disk_id=d.id)
            return {"disk_id": d.id, "status": d.status}

        @app.post("/admin/disks/{disk}/recover")
        async def recover_disk(disk: str):
            d = store.disks.get(disk) or next((x for x in store.disks.values() if x.name == disk), None)
            if d is None:
                return JSONResponse(status_code=404, content={"error": "disco desconhecido"})
            d.simulated_failure = False
            store.check_disks()
            state["want_report"] = True
            log.info("debug_disk_recovered", disk_id=d.id)
            return {"disk_id": d.id, "status": d.status}

        @app.get("/admin/chunks")
        async def list_chunks():
            return {"chunks": store.all_chunks()}

    return app

"""Biblioteca cliente.

Fluxo de escrita (put):
  1. POST /api/uploads ao controlador -> recebe upload_id, tamanho do bloco,
     destinos (N por bloco), quórum W e, se houver cifra, a chave do arquivo.
  2. Para cada bloco: (cifra) -> calcula hash -> PUT no 1º nó com o resto do pipeline.
     A resposta diz quais réplicas foram gravadas e verificadas. Se < W, pede novos
     destinos (/allocate) excluindo os nós que falharam e tenta de novo.
  3. POST /commit com o hash do arquivo e as réplicas de cada bloco. O controlador
     publica a versão atomicamente.
  O conteúdo trafega SOMENTE cliente <-> nós; o controlador só vê metadados.

Fluxo de leitura (get):
  1. GET /api/fs/locate -> blocos e réplicas ordenadas (viva e menos carregada primeiro).
  2. Para cada bloco tenta as réplicas em ordem; confere o hash; se estiver errado,
     reporta a corrupção ao controlador e tenta a próxima réplica.
  3. Decifra (se for o caso), grava em arquivo temporário, confere o hash do arquivo
     inteiro e só então renomeia para o destino.
"""
from __future__ import annotations

import base64
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from ..common import crypto
from ..common.hashing import hash_bytes, new_hasher


class DFSError(Exception):
    pass


class _StaleVersion(Exception):
    pass


class DFSClient:
    def __init__(self, controller: str | None = None, workers: int = 4, timeout: float = 120.0,
                 address: str | None = None):
        self.controller = (controller or os.environ.get("DFS_CONTROLLER", "http://localhost:8000")).rstrip("/")
        self.workers = workers
        self.address = address or os.environ.get("DFS_CLIENT_ADDRESS", "public")
        self.http = httpx.Client(timeout=timeout)

    # -------------------------------------------------------------- util
    def _api(self, method: str, path: str, **kw):
        try:
            r = self.http.request(method, f"{self.controller}{path}", **kw)
        except httpx.HTTPError as exc:
            raise DFSError(f"controlador inacessível em {self.controller}: {exc}") from exc
        if r.status_code >= 400:
            try:
                msg = r.json().get("error") or r.json().get("detail") or r.text
            except ValueError:
                msg = r.text
            raise DFSError(f"[{r.status_code}] {msg}")
        return r.json()

    def _url(self, target: dict) -> str:
        if self.address == "internal":
            return target.get("internal_url") or target["url"]
        return target.get("url") or target["internal_url"]

    # ------------------------------------------------------ namespace / info
    def ls(self, path: str = "/"):
        return self._api("GET", "/api/fs/list", params={"path": path})

    def mkdir(self, path: str, parents: bool = False, replication: int | None = None):
        return self._api("POST", "/api/fs/mkdir", json={"path": path, "parents": parents, "replication": replication})

    def rmdir(self, path: str, recursive: bool = False):
        return self._api("DELETE", "/api/fs/dir", params={"path": path, "recursive": str(recursive).lower()})

    def rm(self, path: str):
        return self._api("DELETE", "/api/fs/file", params={"path": path})

    def mv(self, src: str, dst: str):
        return self._api("POST", "/api/fs/rename", json={"src": src, "dst": dst})

    def stat(self, path: str):
        return self._api("GET", "/api/fs/stat", params={"path": path})

    def setrep(self, path: str, replication: int):
        return self._api("POST", "/api/fs/setrep", json={"path": path, "replication": replication})

    def status(self):
        return self._api("GET", "/api/cluster/status")

    def events(self, limit: int = 30):
        return self._api("GET", "/api/events", params={"limit": limit})

    def drain(self, node_id: str):
        return self._api("POST", f"/api/nodes/{node_id}/drain")

    def undrain(self, node_id: str):
        return self._api("POST", f"/api/nodes/{node_id}/undrain")

    def forget(self, node_id: str):
        return self._api("DELETE", f"/api/nodes/{node_id}")

    def metrics(self):
        return self._api("GET", "/metrics")

    # ---------------------------------------------------------------- put
    def put(self, local: str, remote: str, replication: int | None = None, overwrite: bool = False,
            progress=None) -> dict:
        size = os.path.getsize(local)
        t0 = time.perf_counter()
        sess = self._api("POST", "/api/uploads", json={"path": remote, "size": size, "replication": replication,
                                                       "overwrite": overwrite})
        upload_id = sess["upload_id"]
        key = base64.b64decode(sess["encryption"]["key"]) if sess["encryption"]["enabled"] else None
        alg = sess["hash_alg"]

        stop = threading.Event()

        def renew_loop():
            while not stop.wait(max(1.0, sess["lease_s"] / 3)):
                try:
                    self._api("POST", f"/api/uploads/{upload_id}/renew")
                except DFSError:
                    return

        renewer = threading.Thread(target=renew_loop, daemon=True)
        renewer.start()
        file_hash = new_hasher(alg)
        results: list[dict] = []
        done_bytes = 0
        try:
            with open(local, "rb") as fh, ThreadPoolExecutor(self.workers) as pool:
                pending = []
                for ch in sess["chunks"]:
                    data = fh.read(sess["chunk_size"])
                    file_hash.update(data)
                    pending.append(pool.submit(self._upload_chunk, sess, ch, data, key))
                    if len(pending) >= self.workers * 2:
                        res = pending.pop(0).result()
                        results.append(res)
                        done_bytes += res["plain"]
                        if progress:
                            progress(done_bytes, size)
                for f in pending:
                    res = f.result()
                    results.append(res)
                    done_bytes += res["plain"]
                    if progress:
                        progress(done_bytes, size)
            commit = self._api("POST", f"/api/uploads/{upload_id}/commit", json={
                "file_hash": file_hash.hexdigest(),
                "chunks": [{"index": r["index"], "size": r["size"], "hash": r["hash"], "replicas": r["replicas"]}
                           for r in results]})
        except BaseException:
            stop.set()
            try:
                self._api("DELETE", f"/api/uploads/{upload_id}")
            except DFSError:
                pass
            raise
        finally:
            stop.set()
        elapsed = time.perf_counter() - t0
        return {
            "path": sess["path"], "size": size, "chunks": len(results), "file_hash": file_hash.hexdigest(),
            "replication": sess["replication"], "write_quorum": sess["write_quorum"],
            "encrypted": bool(key), "warnings": sess["warnings"],
            "under_replicated_chunks": commit["under_replicated_chunks"],
            "replicas_per_chunk": [len(r["replicas"]) for r in results],
            "elapsed_s": round(elapsed, 3), "throughput_MBps": round(size / elapsed / 1e6, 3) if elapsed else None,
        }

    def _send_pipeline(self, targets: list[dict], chunk_id: str, payload: bytes, h: str, alg: str):
        """Envia ao 1º destino com o resto como pipeline. Se o 1º não responder,
        o próximo vira cabeça. Retorna (acks, nós que falharam)."""
        failed: list[str] = []
        for i, head in enumerate(targets):
            chain = [{"node_id": t["node_id"], "disk_id": t["disk_id"], "internal_url": t["internal_url"]}
                     for t in targets[i + 1:]]
            try:
                r = self.http.put(f"{self._url(head)}/chunks/{chunk_id}", params={"disk": head["disk_id"]},
                                  content=payload, headers={"X-Chunk-Hash": h, "X-Hash-Alg": alg,
                                                            "X-Pipeline": json.dumps(chain)})
                if r.status_code == 422:
                    raise DFSError(f"nó {head['node_id']} rejeitou o bloco: {r.json().get('error')}")
                body = r.json()
                failed += [e["node_id"] for e in body.get("errors", [])]
                return body.get("acks", []), failed
            except httpx.HTTPError:
                failed.append(head["node_id"])
        return [], failed

    def _upload_chunk(self, sess: dict, ch: dict, data: bytes, key: bytes | None) -> dict:
        cid = ch["chunk_id"]
        payload = crypto.encrypt_chunk(key, cid, data) if key else data
        h = hash_bytes(payload, sess["hash_alg"])
        acks: list[dict] = []
        excluded: set[str] = set()
        targets = ch["targets"]
        for _ in range(4):
            if targets:
                new_acks, failed = self._send_pipeline(targets, cid, payload, h, sess["hash_alg"])
                have = {a["disk_id"] for a in acks}
                acks += [a for a in new_acks if a["disk_id"] not in have]
                excluded |= set(failed)
            if len(acks) >= sess["write_quorum"]:
                break
            alloc = self._api("POST", f"/api/uploads/{sess['upload_id']}/allocate",
                              json={"index": ch["index"], "exclude_nodes": sorted(excluded), "have": acks})
            targets = alloc["targets"]
            if not targets:
                break
        if len(acks) < sess["write_quorum"]:
            raise DFSError(f"bloco {ch['index']}: só {len(acks)} réplica(s) confirmada(s), quórum W="
                           f"{sess['write_quorum']}")
        return {"index": ch["index"], "size": len(payload), "hash": h, "replicas": acks, "plain": len(data)}

    # ---------------------------------------------------------------- get
    def _fetch_chunk(self, loc: dict, ch: dict, key: bytes | None) -> bytes:
        errors = []
        for rep in ch["replicas"]:
            try:
                r = self.http.get(f"{self._url(rep)}/chunks/{ch['chunk_id']}", params={"disk": rep["disk_id"]})
            except httpx.HTTPError as exc:
                errors.append(f"{rep['node_id']}: {exc.__class__.__name__}")
                continue
            if r.status_code == 409:
                errors.append(f"{rep['node_id']}: réplica corrompida")
                continue
            if r.status_code != 200:
                errors.append(f"{rep['node_id']}: HTTP {r.status_code}")
                continue
            data = r.content
            if hash_bytes(data, loc["hash_alg"]) != ch["hash"]:
                errors.append(f"{rep['node_id']}: hash incorreto")
                try:
                    self._api("POST", "/api/chunks/corrupt", json={"chunk_id": ch["chunk_id"], "node_id": rep["node_id"],
                                                                  "disk_id": rep["disk_id"], "reporter": "client"})
                except DFSError:
                    pass
                continue
            return crypto.decrypt_chunk(key, ch["chunk_id"], data) if key else data
        raise DFSError(f"bloco {ch['index']} indisponível em todas as réplicas: {errors}")

    def get(self, remote: str, local: str, progress=None) -> dict:
        """Se o arquivo for sobrescrito durante uma leitura longa, os blocos da versão
        antiga podem ser coletados (após a carência). Nesse caso a leitura recomeça
        com a versão nova — o resultado é sempre UMA versão inteira, nunca uma mistura."""
        for attempt in range(3):
            try:
                return self._get_once(remote, local, progress)
            except _StaleVersion:
                continue
        raise DFSError("arquivo mudou repetidamente durante a leitura")

    def _get_once(self, remote: str, local: str, progress=None) -> dict:
        t0 = time.perf_counter()
        loc = self._api("GET", "/api/fs/locate", params={"path": remote})
        key = base64.b64decode(loc["key"]) if loc["encrypted"] else None
        tmp = f"{local}.part"
        file_hash = new_hasher(loc["hash_alg"])
        done = 0
        try:
            with open(tmp, "wb") as out, ThreadPoolExecutor(self.workers) as pool:
                chunks = loc["chunks"]
                window = []
                i = 0
                while i < len(chunks) or window:
                    while i < len(chunks) and len(window) < self.workers * 2:
                        window.append(pool.submit(self._fetch_chunk, loc, chunks[i], key))
                        i += 1
                    data = window.pop(0).result()
                    out.write(data)
                    file_hash.update(data)
                    done += len(data)
                    if progress:
                        progress(done, loc["size"])
            if file_hash.hexdigest() != loc["file_hash"]:
                raise DFSError("hash do arquivo completo não confere — download descartado")
            os.replace(tmp, local)
        except DFSError:
            try:
                now = self._api("GET", "/api/fs/locate", params={"path": remote})["version_id"]
            except DFSError:
                now = None
            if now and now != loc["version_id"]:
                raise _StaleVersion()
            raise
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        elapsed = time.perf_counter() - t0
        return {"path": loc["path"], "local": local, "size": loc["size"], "file_hash": loc["file_hash"],
                "verified": True, "encrypted": loc["encrypted"], "elapsed_s": round(elapsed, 3),
                "throughput_MBps": round(loc["size"] / elapsed / 1e6, 3) if elapsed else None}

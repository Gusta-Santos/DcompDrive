"""Armazenamento local de blocos em um ou mais discos (pastas de dados simulam discos).

Layout de um disco:
  <disco>/chunks/ab/abcdef...        conteúdo do bloco (bytes exatamente como o cliente enviou)
  <disco>/chunks/ab/abcdef....meta   JSON {hash, alg, size, created}
  <disco>/tmp/                       gravações em andamento

Gravação atômica: escreve em tmp/, faz fsync, RELÊ e confere o hash (RF4a), grava o
.meta e só então `os.replace` para o nome final (rename atômico no mesmo sistema de
arquivos). Uma queda no meio deixa só lixo em tmp/, que é apagado ao reiniciar —
nunca um bloco pela metade com nome válido.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
import uuid

from ..common.hashing import hash_bytes, hash_file

CHUNK_RE = re.compile(r"^[0-9a-f]{16,64}$")


class ChunkError(Exception):
    status = 500


class HashMismatch(ChunkError):
    status = 422


class DiskUnavailable(ChunkError):
    status = 503


class DiskFull(ChunkError):
    status = 507


class NotFound(ChunkError):
    status = 404


class Corrupt(ChunkError):
    status = 409

    def __init__(self, chunk_id: str, disk_id: str):
        super().__init__(f"bloco {chunk_id} corrompido em {disk_id}")
        self.chunk_id, self.disk_id = chunk_id, disk_id


class Disk:
    def __init__(self, node_id: str, name: str, path: str, capacity: int | None):
        self.name = name
        self.id = f"{node_id}:{name}"
        self.path = os.path.abspath(path)
        self.capacity = capacity
        self.used = 0
        self.status = "ok"
        self.simulated_failure = False


class ChunkStore:
    def __init__(self, node_id: str, disk_paths: list[str], capacity_bytes: int | None = None):
        self.node_id = node_id
        self.lock = threading.RLock()
        self.disks: dict[str, Disk] = {}
        self.index: dict[str, set[str]] = {}
        names: set[str] = set()
        for i, p in enumerate(disk_paths):
            name = os.path.basename(os.path.normpath(p)) or f"disk{i + 1}"
            if name in names:
                name = f"{name}-{i + 1}"
            names.add(name)
            d = Disk(node_id, name, p, capacity_bytes)
            self.disks[d.id] = d
        for d in self.disks.values():
            self._init_disk(d)

    # --------------------------------------------------------------- disco
    def _init_disk(self, d: Disk) -> None:
        os.makedirs(os.path.join(d.path, "chunks"), exist_ok=True)
        tmp = os.path.join(d.path, "tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        os.makedirs(tmp, exist_ok=True)
        used = 0
        for root, _, files in os.walk(os.path.join(d.path, "chunks")):
            for f in files:
                full = os.path.join(root, f)
                if f.endswith(".meta"):
                    if not os.path.exists(full[:-5]):
                        os.remove(full)
                    continue
                if not os.path.exists(full + ".meta") or not CHUNK_RE.match(f):
                    os.remove(full)
                    continue
                used += os.path.getsize(full)
                self.index.setdefault(f, set()).add(d.id)
        d.used = used

    def _chunk_path(self, d: Disk, cid: str) -> str:
        return os.path.join(d.path, "chunks", cid[:2], cid)

    def check_disks(self) -> list[tuple[str, str]]:
        """Detecta falha de disco: pasta sumiu, não é gravável, ou falha simulada."""
        changes = []
        for d in self.disks.values():
            ok = not d.simulated_failure and os.path.isdir(os.path.join(d.path, "chunks"))
            if ok:
                probe = os.path.join(d.path, "tmp", f".probe-{uuid.uuid4().hex}")
                try:
                    with open(probe, "wb") as fh:
                        fh.write(b"x")
                    os.remove(probe)
                except OSError:
                    ok = False
            new = "ok" if ok else "failed"
            if new != d.status:
                changes.append((d.id, new))
                d.status = new
        return changes

    def disk_stats(self) -> list[dict]:
        out = []
        for d in self.disks.values():
            try:
                real = shutil.disk_usage(d.path)
                real_total, real_free = real.total, real.free
            except OSError:
                real_total = real_free = 0
            if d.capacity:
                total = d.capacity
                used = d.used
                free = max(0, min(d.capacity - d.used, real_free))
            else:
                total, free = real_total, real_free
                used = real_total - real_free
            out.append({"id": d.id, "name": d.name, "path": d.path, "total": total, "used": used,
                        "free": free, "status": d.status, "chunks_bytes": d.used})
        return out

    def _disk(self, disk_id: str | None) -> Disk:
        if disk_id is None:
            ok = [d for d in self.disks.values() if d.status == "ok"]
            if not ok:
                raise DiskUnavailable("nenhum disco disponível")
            return max(ok, key=lambda d: (d.capacity or 0) - d.used)
        d = self.disks.get(disk_id) or next((x for x in self.disks.values() if x.name == disk_id), None)
        if d is None:
            raise NotFound(f"disco desconhecido: {disk_id}")
        if d.status != "ok":
            raise DiskUnavailable(f"disco {d.id} indisponível")
        return d

    # ------------------------------------------------------------ operações
    def put(self, disk_id: str | None, cid: str, data: bytes, expected_hash: str, alg: str) -> str:
        if not CHUNK_RE.match(cid):
            raise ChunkError("chunk_id inválido")
        actual = hash_bytes(data, alg)
        if actual != expected_hash:
            raise HashMismatch(f"hash recebido não confere ({actual[:12]} != {expected_hash[:12]})")
        d = self._disk(disk_id)
        final = self._chunk_path(d, cid)
        with self.lock:
            previous = os.path.getsize(final) if os.path.exists(final) else 0
            if d.capacity and d.used - previous + len(data) > d.capacity:
                raise DiskFull(f"disco {d.id} cheio")
        tmp = os.path.join(d.path, "tmp", f"{cid}.{uuid.uuid4().hex}.tmp")
        try:
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            if hash_file(tmp, alg) != expected_hash:
                raise HashMismatch("hash do arquivo gravado não confere")
            os.makedirs(os.path.dirname(final), exist_ok=True)
            meta_tmp = tmp + ".meta"
            with open(meta_tmp, "w", encoding="utf-8") as fh:
                json.dump({"hash": expected_hash, "alg": alg, "size": len(data), "created": time.time()}, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(meta_tmp, final + ".meta")
            os.replace(tmp, final)
        except OSError as exc:
            raise DiskUnavailable(f"erro de E/S em {d.id}: {exc}") from exc
        finally:
            for p in (tmp, tmp + ".meta"):
                if os.path.exists(p):
                    os.remove(p)
        with self.lock:
            d.used += len(data) - previous
            self.index.setdefault(cid, set()).add(d.id)
        return d.id

    def _meta(self, d: Disk, cid: str) -> dict:
        with open(self._chunk_path(d, cid) + ".meta", encoding="utf-8") as fh:
            return json.load(fh)

    def locations(self, cid: str) -> list[Disk]:
        with self.lock:
            ids = list(self.index.get(cid, ()))
        return [self.disks[i] for i in ids if self.disks[i].status == "ok"]

    def read(self, cid: str, disk_id: str | None = None) -> tuple[bytes, dict, str]:
        """Lê e VERIFICA o hash antes de entregar (RF4b). Réplica corrompida é descartada."""
        if not CHUNK_RE.match(cid):
            raise NotFound("chunk_id inválido")
        disks = self.locations(cid)
        if disk_id:
            disks = [d for d in disks if d.id == disk_id or d.name == disk_id]
        if not disks:
            raise NotFound(f"bloco {cid} não está neste nó")
        last: Exception | None = None
        for d in disks:
            try:
                meta = self._meta(d, cid)
                with open(self._chunk_path(d, cid), "rb") as fh:
                    data = fh.read()
            except OSError as exc:
                last = DiskUnavailable(str(exc))
                continue
            if hash_bytes(data, meta["alg"]) != meta["hash"]:
                self.delete(cid, d.id)
                raise Corrupt(cid, d.id)
            return data, meta, d.id
        raise last or NotFound(cid)

    def verify(self, cid: str, disk_id: str | None = None) -> list[tuple[str, bool]]:
        out = []
        for d in self.locations(cid):
            if disk_id and d.id != disk_id:
                continue
            try:
                meta = self._meta(d, cid)
                ok = hash_file(self._chunk_path(d, cid), meta["alg"]) == meta["hash"]
            except (OSError, ValueError, KeyError):
                ok = False
            out.append((d.id, ok))
        return out

    def delete(self, cid: str, disk_id: str | None = None) -> int:
        if not CHUNK_RE.match(cid):
            return 0
        removed = 0
        with self.lock:
            targets = list(self.index.get(cid, ()))
            for did in targets:
                d = self.disks[did]
                if disk_id and did != disk_id and d.name != disk_id:
                    continue
                path = self._chunk_path(d, cid)
                try:
                    size = os.path.getsize(path)
                    os.remove(path)
                    d.used -= size
                    removed += 1
                except OSError:
                    pass
                try:
                    os.remove(path + ".meta")
                except OSError:
                    pass
                self.index[cid].discard(did)
            if not self.index.get(cid):
                self.index.pop(cid, None)
        return removed

    def report(self) -> list[dict]:
        """Block report: tudo que este nó tem em discos saudáveis."""
        now = time.time()
        out = []
        with self.lock:
            items = [(cid, did) for cid, dids in self.index.items() for did in dids]
        for cid, did in items:
            d = self.disks[did]
            if d.status != "ok":
                continue
            try:
                meta = self._meta(d, cid)
                mtime = os.path.getmtime(self._chunk_path(d, cid))
            except (OSError, ValueError):
                continue
            out.append({"chunk_id": cid, "disk_id": did, "hash": meta["hash"], "size": meta["size"],
                        "age_s": round(now - mtime, 1)})
        return out

    def all_chunks(self) -> list[tuple[str, str]]:
        with self.lock:
            return [(cid, did) for cid, dids in self.index.items() for did in dids]

    def chunk_count(self) -> int:
        with self.lock:
            return sum(len(v) for v in self.index.values())

    # -------------------------------------------------- simulação de falhas
    def corrupt(self, cid: str) -> str:
        disks = self.locations(cid)
        if not disks:
            raise NotFound(cid)
        path = self._chunk_path(disks[0], cid)
        with open(path, "r+b") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            pos = size // 2
            fh.seek(pos)
            b = fh.read(1) or b"\x00"
            fh.seek(pos)
            fh.write(bytes([b[0] ^ 0xFF]))
        return disks[0].id

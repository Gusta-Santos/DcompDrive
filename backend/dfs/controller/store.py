"""Nó de metadados: persistência em SQLite (RF10).

Por que SQLite?
  * Transações ACID: cada operação de metadados (commit de arquivo, rename de
    diretório, remoção recursiva) é aplicada inteira ou não é aplicada.
  * Modo WAL + synchronous=FULL: após o COMMIT o dado está no disco; ao reiniciar,
    o controlador reabre o banco e o estado está lá (sem recriação manual).
  * É o "log de operações com snapshots" pronto: o WAL é o log, e o checkpoint
    do WAL para o arquivo principal é o snapshot.

Concorrência: o controlador roda num único event loop asyncio e TODOS os acessos
ao banco acontecem nesse thread, sem `await` no meio de uma transação. Logo, as
mutações de metadados são serializadas (equivalente ao lock global do NameNode
do HDFS) — não há condição de corrida entre duas operações de metadados.

Modelo de dados:
  entries   : árvore de nomes (path -> dir|file, versão atual do arquivo)
  versions  : cada gravação completa cria uma versão IMUTÁVEL de um arquivo
  chunks    : blocos de uma versão (id, índice, tamanho, hash)
  replicas  : onde cada bloco está (chunk_id, disk_id, node_id)
  nodes/disks: nós de armazenamento conhecidos e seus discos
  uploads   : sessões de escrita em andamento (lease de escrita)
  garbage   : blocos a apagar (com carência para leitores em andamento)
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterable

from ..common import paths as P

SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
    path        TEXT PRIMARY KEY,
    parent      TEXT,
    type        TEXT NOT NULL CHECK (type IN ('dir','file')),
    replication INTEGER,
    version_id  TEXT,
    created_at  REAL NOT NULL,
    modified_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent);

CREATE TABLE IF NOT EXISTS versions (
    id          TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    file_hash   TEXT NOT NULL,
    hash_alg    TEXT NOT NULL,
    chunk_size  INTEGER NOT NULL,
    replication INTEGER NOT NULL,
    encrypted   INTEGER NOT NULL DEFAULT 0,
    wrapped_key BLOB,
    status      TEXT NOT NULL,          -- committed | deleted
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_versions_status ON versions(status);

CREATE TABLE IF NOT EXISTS chunks (
    id         TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    idx        INTEGER NOT NULL,
    size       INTEGER NOT NULL,
    hash       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_version ON chunks(version_id);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id TEXT NOT NULL,
    disk_id  TEXT NOT NULL,
    node_id  TEXT NOT NULL,
    added_at REAL NOT NULL,
    PRIMARY KEY (chunk_id, disk_id)
);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
CREATE INDEX IF NOT EXISTS idx_replicas_disk ON replicas(disk_id);

CREATE TABLE IF NOT EXISTS nodes (
    id            TEXT PRIMARY KEY,
    internal_url  TEXT NOT NULL,
    public_url    TEXT NOT NULL,
    zone          TEXT,
    state         TEXT NOT NULL,
    draining      INTEGER NOT NULL DEFAULT 0,
    drained       INTEGER NOT NULL DEFAULT 0,
    registered_at REAL NOT NULL,
    last_seen     REAL
);

CREATE TABLE IF NOT EXISTS disks (
    id      TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    path    TEXT,
    total   INTEGER NOT NULL DEFAULT 0,
    used    INTEGER NOT NULL DEFAULT 0,
    free    INTEGER NOT NULL DEFAULT 0,
    status  TEXT NOT NULL DEFAULT 'ok'
);

CREATE TABLE IF NOT EXISTS uploads (
    id            TEXT PRIMARY KEY,
    path          TEXT NOT NULL UNIQUE,   -- UNIQUE = no máximo 1 escritor por caminho
    version_id    TEXT NOT NULL,
    size          INTEGER NOT NULL,
    chunk_size    INTEGER NOT NULL,
    hash_alg      TEXT NOT NULL,
    replication   INTEGER NOT NULL,
    write_quorum  INTEGER NOT NULL,
    overwrite     INTEGER NOT NULL,
    encrypted     INTEGER NOT NULL,
    wrapped_key   BLOB,
    chunk_ids     TEXT NOT NULL,
    lease_expires REAL NOT NULL,
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS garbage (
    chunk_id     TEXT PRIMARY KEY,
    delete_after REAL NOT NULL
);

-- discos cujas réplicas contam como "vivas" para o nível de replicação
CREATE VIEW IF NOT EXISTS live_disks AS
    SELECT d.id AS id FROM disks d JOIN nodes n ON n.id = d.node_id
    WHERE n.state IN ('alive','suspect') AND n.draining = 0 AND d.status = 'ok';
"""


class FSError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class MetadataStore:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)
        now = time.time()
        self.db.execute(
            "INSERT OR IGNORE INTO entries(path,parent,type,created_at,modified_at) VALUES('/',NULL,'dir',?,?)",
            (now, now),
        )

    # ------------------------------------------------------------------ util
    @contextmanager
    def tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def q(self, sql: str, args: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return self.db.execute(sql, tuple(args)).fetchall()

    def q1(self, sql: str, args: Iterable[Any] = ()) -> sqlite3.Row | None:
        return self.db.execute(sql, tuple(args)).fetchone()

    # ------------------------------------------------------------- namespace
    def get_entry(self, path: str) -> sqlite3.Row | None:
        return self.q1("SELECT * FROM entries WHERE path=?", (path,))

    def require_dir(self, path: str) -> sqlite3.Row:
        e = self.get_entry(path)
        if e is None:
            raise FSError(404, f"diretório não existe: {path}")
        if e["type"] != "dir":
            raise FSError(409, f"não é diretório: {path}")
        return e

    def list_dir(self, path: str) -> list[sqlite3.Row]:
        self.require_dir(path)
        return self.q(
            """SELECT e.*, v.size AS size, v.file_hash AS file_hash, v.replication AS v_replication,
                      v.encrypted AS encrypted
               FROM entries e LEFT JOIN versions v ON v.id = e.version_id
               WHERE e.parent=? ORDER BY e.type, e.path""",
            (path,),
        )

    def effective_replication(self, path: str, default: int) -> int:
        """Fator de replicação herdado: arquivo > diretório mais próximo > padrão do cluster."""
        p = path
        while True:
            e = self.get_entry(p)
            if e is not None and e["replication"]:
                return int(e["replication"])
            if p == "/":
                return default
            p = P.parent(p)

    def active_upload_under(self, path: str) -> sqlite3.Row | None:
        if path == "/":
            return self.q1("SELECT * FROM uploads LIMIT 1")
        return self.q1(
            "SELECT * FROM uploads WHERE path=? OR substr(path,1,?)=?",
            (path, len(path) + 1, path + "/"),
        )

    def mkdir(self, path: str, replication: int | None, parents: bool) -> list[str]:
        created: list[str] = []
        with self.tx():
            e = self.get_entry(path)
            if e is not None:
                if e["type"] == "dir" and parents:
                    return created
                raise FSError(409, f"já existe: {path}")
            if self.q1("SELECT 1 FROM uploads WHERE path=?", (path,)):
                raise FSError(409, f"há uma gravação em andamento em {path}")
            chain = []
            p = path
            while self.get_entry(p) is None:
                chain.append(p)
                p = P.parent(p)
            if self.get_entry(p)["type"] != "dir":
                raise FSError(409, f"não é diretório: {p}")
            if len(chain) > 1 and not parents:
                raise FSError(404, f"diretório pai não existe: {P.parent(path)}")
            now = time.time()
            for d in reversed(chain):
                rep = replication if d == path else None
                self.db.execute(
                    "INSERT INTO entries(path,parent,type,replication,created_at,modified_at) VALUES(?,?,?,?,?,?)",
                    (d, P.parent(d), "dir", rep, now, now),
                )
                created.append(d)
            self._touch(P.parent(chain[-1]), now)
        return created

    def _touch(self, path: str, now: float) -> None:
        self.db.execute("UPDATE entries SET modified_at=? WHERE path=?", (now, path))

    def _garbage_version(self, version_id: str, delete_after: float) -> int:
        """Marca uma versão como apagada e agenda seus blocos para remoção."""
        self.db.execute("UPDATE versions SET status='deleted' WHERE id=?", (version_id,))
        rows = self.q("SELECT id FROM chunks WHERE version_id=?", (version_id,))
        for r in rows:
            self.db.execute(
                "INSERT OR REPLACE INTO garbage(chunk_id, delete_after) VALUES(?,?)", (r["id"], delete_after)
            )
        if not rows:
            self.db.execute("DELETE FROM versions WHERE id=?", (version_id,))
        return len(rows)

    def delete_file(self, path: str, grace_s: float) -> dict:
        with self.tx():
            e = self.get_entry(path)
            if e is None:
                raise FSError(404, f"não existe: {path}")
            if e["type"] != "file":
                raise FSError(409, f"é um diretório (use rmdir): {path}")
            if self.q1("SELECT 1 FROM uploads WHERE path=?", (path,)):
                raise FSError(409, f"arquivo está sendo gravado: {path}")
            n = 0
            if e["version_id"]:
                n = self._garbage_version(e["version_id"], time.time() + grace_s)
            self.db.execute("DELETE FROM entries WHERE path=?", (path,))
            self._touch(P.parent(path), time.time())
        return {"path": path, "chunks_scheduled": n}

    def rmdir(self, path: str, recursive: bool, grace_s: float) -> dict:
        if path == "/":
            raise FSError(400, "não é possível remover a raiz")
        with self.tx():
            self.require_dir(path)
            children = self.q(
                "SELECT * FROM entries WHERE substr(path,1,?)=?", (len(path) + 1, path + "/")
            )
            if children and not recursive:
                raise FSError(409, f"diretório não está vazio: {path}")
            if self.active_upload_under(path):
                raise FSError(409, f"há gravações em andamento dentro de {path}")
            after = time.time() + grace_s
            files = 0
            for c in children:
                if c["type"] == "file" and c["version_id"]:
                    self._garbage_version(c["version_id"], after)
                    files += 1
            self.db.execute("DELETE FROM entries WHERE path=? OR substr(path,1,?)=?", (path, len(path) + 1, path + "/"))
            self._touch(P.parent(path), time.time())
        return {"path": path, "removed_entries": len(children) + 1, "files": files}

    def rename(self, src: str, dst: str) -> dict:
        if src == "/":
            raise FSError(400, "não é possível mover a raiz")
        if P.is_under(dst, src):
            raise FSError(400, "não é possível mover um diretório para dentro dele mesmo")
        with self.tx():
            e = self.get_entry(src)
            if e is None:
                raise FSError(404, f"não existe: {src}")
            if self.get_entry(dst) is not None:
                raise FSError(409, f"destino já existe: {dst}")
            self.require_dir(P.parent(dst))
            if self.active_upload_under(src) or self.q1("SELECT 1 FROM uploads WHERE path=?", (dst,)):
                raise FSError(409, "há gravação em andamento na origem ou no destino")
            rows = [e]
            if e["type"] == "dir":
                rows += self.q("SELECT * FROM entries WHERE substr(path,1,?)=?", (len(src) + 1, src + "/"))
            now = time.time()
            # ordena do mais raso para o mais profundo para manter consistência
            for r in sorted(rows, key=lambda r: r["path"].count("/")):
                new_path = dst + r["path"][len(src):]
                self.db.execute(
                    "UPDATE entries SET path=?, parent=? WHERE path=?",
                    (new_path, P.parent(new_path), r["path"]),
                )
            self.db.execute("UPDATE entries SET modified_at=? WHERE path=?", (now, dst))
            self._touch(P.parent(src), now)
            self._touch(P.parent(dst), now)
        return {"src": src, "dst": dst, "moved_entries": len(rows)}

    def set_replication(self, path: str, replication: int) -> dict:
        with self.tx():
            e = self.get_entry(path)
            if e is None:
                raise FSError(404, f"não existe: {path}")
            self.db.execute("UPDATE entries SET replication=? WHERE path=?", (replication, path))
            if e["type"] == "file":
                vids = [e["version_id"]] if e["version_id"] else []
            else:
                prefix = path.rstrip("/") + "/"
                vids = [
                    r["version_id"]
                    for r in self.q(
                        "SELECT version_id FROM entries WHERE type='file' AND version_id IS NOT NULL AND substr(path,1,?)=?",
                        (len(prefix), prefix),
                    )
                ]
            for vid in vids:
                self.db.execute("UPDATE versions SET replication=? WHERE id=?", (replication, vid))
        return {"path": path, "replication": replication, "files_updated": len(vids)}

    # ---------------------------------------------------------------- uploads
    def create_upload(self, rec: dict) -> None:
        with self.tx():
            if self.q1("SELECT 1 FROM uploads WHERE path=?", (rec["path"],)):
                raise FSError(409, f"outro cliente já está gravando {rec['path']} (lease de escrita ativo)")
            self.db.execute(
                """INSERT INTO uploads(id,path,version_id,size,chunk_size,hash_alg,replication,write_quorum,
                   overwrite,encrypted,wrapped_key,chunk_ids,lease_expires,created_at)
                   VALUES(:id,:path,:version_id,:size,:chunk_size,:hash_alg,:replication,:write_quorum,
                   :overwrite,:encrypted,:wrapped_key,:chunk_ids,:lease_expires,:created_at)""",
                {**rec, "chunk_ids": json.dumps(rec["chunk_ids"])},
            )

    def get_upload(self, upload_id: str) -> sqlite3.Row | None:
        return self.q1("SELECT * FROM uploads WHERE id=?", (upload_id,))

    def renew_upload(self, upload_id: str, expires: float) -> bool:
        cur = self.db.execute("UPDATE uploads SET lease_expires=? WHERE id=?", (expires, upload_id))
        return cur.rowcount > 0

    def delete_upload(self, upload_id: str) -> None:
        self.db.execute("DELETE FROM uploads WHERE id=?", (upload_id,))

    def expired_uploads(self, now: float) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM uploads WHERE lease_expires < ?", (now,))

    def all_uploads(self) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM uploads")

    def active_upload_chunk_ids(self) -> set[str]:
        ids: set[str] = set()
        for r in self.q("SELECT chunk_ids FROM uploads"):
            ids.update(json.loads(r["chunk_ids"]))
        return ids

    def commit_upload(self, up: sqlite3.Row, file_hash: str, chunks: list[dict], node_of_disk: dict,
                      grace_s: float) -> dict:
        """Publica a nova versão de forma ATÔMICA (uma única transação).

        Leitores veem a versão anterior inteira até o COMMIT e a nova inteira depois:
        uma leitura nunca devolve um arquivo parcialmente escrito (RF8).
        """
        path = up["path"]
        now = time.time()
        with self.tx():
            if self.get_upload(up["id"]) is None:
                raise FSError(410, "sessão de escrita expirou ou foi abortada")
            self.require_dir(P.parent(path))
            e = self.get_entry(path)
            if e is not None and e["type"] == "dir":
                raise FSError(409, f"{path} virou um diretório")
            if e is not None and not up["overwrite"]:
                raise FSError(409, f"arquivo já existe: {path}")
            self.db.execute(
                """INSERT INTO versions(id,size,file_hash,hash_alg,chunk_size,replication,encrypted,wrapped_key,
                   status,created_at) VALUES(?,?,?,?,?,?,?,?,'committed',?)""",
                (up["version_id"], up["size"], file_hash, up["hash_alg"], up["chunk_size"], up["replication"],
                 up["encrypted"], up["wrapped_key"], now),
            )
            for c in chunks:
                self.db.execute(
                    "INSERT INTO chunks(id,version_id,idx,size,hash) VALUES(?,?,?,?,?)",
                    (c["chunk_id"], up["version_id"], c["index"], c["size"], c["hash"]),
                )
                for disk_id in c["disks"]:
                    self.db.execute(
                        "INSERT OR IGNORE INTO replicas(chunk_id,disk_id,node_id,added_at) VALUES(?,?,?,?)",
                        (c["chunk_id"], disk_id, node_of_disk[disk_id], now),
                    )
            old_version = None
            if e is not None:
                old_version = e["version_id"]
                if old_version:
                    self._garbage_version(old_version, now + grace_s)
                self.db.execute(
                    "UPDATE entries SET version_id=?, modified_at=? WHERE path=?", (up["version_id"], now, path)
                )
            else:
                self.db.execute(
                    "INSERT INTO entries(path,parent,type,version_id,created_at,modified_at) VALUES(?,?,?,?,?,?)",
                    (path, P.parent(path), "file", up["version_id"], now, now),
                )
            self._touch(P.parent(path), now)
            self.db.execute("DELETE FROM uploads WHERE id=?", (up["id"],))
        return {"path": path, "version_id": up["version_id"], "replaced_version": old_version}

    # --------------------------------------------------------------- leitura
    def get_version(self, version_id: str) -> sqlite3.Row | None:
        return self.q1("SELECT * FROM versions WHERE id=?", (version_id,))

    def version_chunks(self, version_id: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM chunks WHERE version_id=? ORDER BY idx", (version_id,))

    def replicas_of(self, chunk_id: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM replicas WHERE chunk_id=?", (chunk_id,))

    def replicas_for_version(self, version_id: str) -> dict[str, list[sqlite3.Row]]:
        out: dict[str, list[sqlite3.Row]] = {}
        for r in self.q(
            "SELECT r.* FROM replicas r JOIN chunks c ON c.id=r.chunk_id WHERE c.version_id=?", (version_id,)
        ):
            out.setdefault(r["chunk_id"], []).append(r)
        return out

    # ----------------------------------------------------- réplicas / saúde
    LIVE_COUNT = "(SELECT COUNT(*) FROM replicas r JOIN live_disks l ON l.id=r.disk_id WHERE r.chunk_id=c.id)"

    def deficient_chunks(self) -> list[sqlite3.Row]:
        """Blocos de versões ativas cuja contagem de réplicas vivas difere do desejado."""
        return self.q(
            f"""SELECT * FROM (
                    SELECT c.id AS chunk_id, c.size, c.hash, c.version_id, v.hash_alg, v.replication,
                           {self.LIVE_COUNT} AS live
                    FROM chunks c JOIN versions v ON v.id=c.version_id
                    WHERE v.status='committed')
                WHERE live <> replication"""
        )

    def version_health(self, version_ids: list[str] | None = None) -> dict[str, dict]:
        where = "v.status='committed'"
        args: list[Any] = []
        if version_ids is not None:
            if not version_ids:
                return {}
            where += f" AND v.id IN ({','.join('?' * len(version_ids))})"
            args = list(version_ids)
        rows = self.q(
            f"""SELECT v.id AS vid, v.replication AS rep, COUNT(x.cid) AS nchunks, MIN(x.live) AS min_live
                FROM versions v LEFT JOIN (
                    SELECT c.id AS cid, c.version_id AS version_id, {self.LIVE_COUNT} AS live FROM chunks c
                ) x ON x.version_id = v.id
                WHERE {where} GROUP BY v.id""",
            args,
        )
        return {r["vid"]: {"rep": r["rep"], "nchunks": r["nchunks"], "min_live": r["min_live"]} for r in rows}

    def chunk_info(self, chunk_id: str) -> sqlite3.Row | None:
        return self.q1(
            "SELECT c.*, v.status AS vstatus FROM chunks c JOIN versions v ON v.id=c.version_id WHERE c.id=?",
            (chunk_id,),
        )

    def has_replica(self, chunk_id: str, disk_id: str) -> bool:
        return self.q1("SELECT 1 FROM replicas WHERE chunk_id=? AND disk_id=?", (chunk_id, disk_id)) is not None

    def add_replica(self, chunk_id: str, disk_id: str, node_id: str) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO replicas(chunk_id,disk_id,node_id,added_at) VALUES(?,?,?,?)",
            (chunk_id, disk_id, node_id, time.time()),
        )

    def remove_replica(self, chunk_id: str, disk_id: str) -> bool:
        return self.db.execute("DELETE FROM replicas WHERE chunk_id=? AND disk_id=?", (chunk_id, disk_id)).rowcount > 0

    def replicas_on_disks(self, disk_ids: list[str]) -> list[sqlite3.Row]:
        if not disk_ids:
            return []
        return self.q(f"SELECT * FROM replicas WHERE disk_id IN ({','.join('?' * len(disk_ids))})", disk_ids)

    def chunk_ids_on_node(self, node_id: str) -> set[str]:
        return {r["chunk_id"] for r in self.q("SELECT chunk_id FROM replicas WHERE node_id=?", (node_id,))}

    def replica_counts_by_node(self) -> dict[str, int]:
        return {r["node_id"]: r["n"] for r in self.q("SELECT node_id, COUNT(*) AS n FROM replicas GROUP BY node_id")}

    # ------------------------------------------------------------------- GC
    def due_garbage(self, now: float, limit: int = 500) -> list[str]:
        return [r["chunk_id"] for r in self.q(
            "SELECT chunk_id FROM garbage WHERE delete_after <= ? LIMIT ?", (now, limit))]

    def purge_chunk(self, chunk_id: str) -> None:
        with self.tx():
            row = self.q1("SELECT version_id FROM chunks WHERE id=?", (chunk_id,))
            self.db.execute("DELETE FROM replicas WHERE chunk_id=?", (chunk_id,))
            self.db.execute("DELETE FROM chunks WHERE id=?", (chunk_id,))
            self.db.execute("DELETE FROM garbage WHERE chunk_id=?", (chunk_id,))
            if row and not self.q1("SELECT 1 FROM chunks WHERE version_id=?", (row["version_id"],)):
                self.db.execute("DELETE FROM versions WHERE id=? AND status='deleted'", (row["version_id"],))

    # ---------------------------------------------------------- nós / discos
    def upsert_node(self, n: dict) -> None:
        self.db.execute(
            """INSERT INTO nodes(id,internal_url,public_url,zone,state,draining,drained,registered_at,last_seen)
               VALUES(:id,:internal_url,:public_url,:zone,:state,:draining,:drained,:registered_at,:last_seen)
               ON CONFLICT(id) DO UPDATE SET internal_url=excluded.internal_url, public_url=excluded.public_url,
                 zone=excluded.zone, state=excluded.state, draining=excluded.draining, drained=excluded.drained,
                 last_seen=excluded.last_seen""",
            n,
        )

    def set_node_state(self, node_id: str, state: str, last_seen: float | None = None) -> None:
        self.db.execute("UPDATE nodes SET state=?, last_seen=COALESCE(?, last_seen) WHERE id=?",
                        (state, last_seen, node_id))

    def set_node_drain(self, node_id: str, draining: bool, drained: bool) -> None:
        self.db.execute("UPDATE nodes SET draining=?, drained=? WHERE id=?", (int(draining), int(drained), node_id))

    def upsert_disk(self, d: dict) -> None:
        self.db.execute(
            """INSERT INTO disks(id,node_id,path,total,used,free,status) VALUES(:id,:node_id,:path,:total,:used,:free,:status)
               ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id, path=excluded.path, total=excluded.total,
                 used=excluded.used, free=excluded.free, status=excluded.status""",
            d,
        )

    def set_disk_status(self, disk_id: str, status: str) -> None:
        self.db.execute("UPDATE disks SET status=? WHERE id=?", (status, disk_id))

    def load_nodes(self) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
        return self.q("SELECT * FROM nodes"), self.q("SELECT * FROM disks")

    def remove_node(self, node_id: str) -> int:
        with self.tx():
            n = self.db.execute("DELETE FROM replicas WHERE node_id=?", (node_id,)).rowcount
            self.db.execute("DELETE FROM disks WHERE node_id=?", (node_id,))
            self.db.execute("DELETE FROM nodes WHERE id=?", (node_id,))
        return n

    # ------------------------------------------------------------ contagens
    def counts(self) -> dict:
        r = self.q1(
            """SELECT (SELECT COUNT(*) FROM entries WHERE type='file') AS files,
                      (SELECT COUNT(*) FROM entries WHERE type='dir') AS dirs,
                      (SELECT COUNT(*) FROM chunks c JOIN versions v ON v.id=c.version_id WHERE v.status='committed') AS chunks,
                      (SELECT COUNT(*) FROM replicas) AS replicas,
                      (SELECT COALESCE(SUM(size),0) FROM versions WHERE status='committed') AS bytes,
                      (SELECT COUNT(*) FROM garbage) AS garbage,
                      (SELECT COUNT(*) FROM uploads) AS uploads"""
        )
        return dict(r)

"""Teste ponta a ponta: sobe 1 controlador + 5 nós locais com tempos curtos,
exercita os requisitos e injeta falhas. Saída: lista de verificações PASSOU/FALHOU.

  python scripts/e2e_test.py

Não precisa de Docker. Usa portas 8000 e 9001-9005 e a pasta data-e2e/.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(ROOT, "backend")
sys.path.insert(0, BACKEND)

import httpx

from dfs.client.lib import DFSClient, DFSError

DATA = os.path.join(ROOT, "data-e2e")
ENV = {**os.environ, "PYTHONPATH": BACKEND, "PYTHONUNBUFFERED": "1"}
FAST = ["heartbeat.interval_s=0.5", "heartbeat.suspect_after_s=2", "heartbeat.dead_after_s=4",
        "heartbeat.block_report_interval_s=5", "cluster.chunk_size_mb=0.25", "storage.scrub_interval_s=3",
        "gc.interval_s=1", "gc.delete_grace_s=1", "gc.orphan_grace_s=5", "rereplication.check_interval_s=0.5",
        "leases.write_lease_s=10",
        f"controller.db_path={DATA}/controller/metadata.db",
        f"encryption.master_key_file={DATA}/controller/master.key",
        f"logging.file={DATA}/logs/controller.jsonl"]
procs: dict[str, subprocess.Popen] = {}
results: list[tuple[str, bool, str]] = []
shared: dict[str, str] = {}


def start_controller(extra: list[str] = ()) -> None:
    cmd = [sys.executable, "-m", "dfs.controller", "--config", os.path.join(BACKEND, "config", "config.yaml")]
    for s in [*FAST, *extra]:
        cmd += ["--set", s]
    procs["controller"] = subprocess.Popen(cmd, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    wait_for(lambda: httpx.get("http://localhost:8000/health").status_code == 200, 15, "controlador subir")


def start_node(i: int) -> None:
    nid, port = f"node{i}", 9000 + i
    cmd = [sys.executable, "-m", "dfs.storage", "--node-id", nid, "--port", str(port),
           "--internal-url", f"http://localhost:{port}", "--zone", ["zona-a", "zona-b", "zona-c"][(i - 1) % 3],
           "--capacity-mb", "256", "--log-file", f"{DATA}/logs/{nid}.jsonl",
           "--disk", f"{DATA}/{nid}/disk1", "--disk", f"{DATA}/{nid}/disk2"]
    procs[nid] = subprocess.Popen(cmd, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def stop(name: str) -> None:
    p = procs.pop(name, None)
    if p:
        p.kill()
        p.wait()


def wait_for(cond, timeout: float, what: str, interval: float = 0.25):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            v = cond()
            if v:
                return v
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise TimeoutError(f"timeout esperando: {what} ({last!r})")


def check(name: str, fn) -> None:
    t0 = time.time()
    try:
        fn()
        results.append((name, True, f"{time.time() - t0:.1f}s"))
        print(f"PASSOU  {name}  ({time.time() - t0:.1f}s)", flush=True)
    except Exception as exc:
        results.append((name, False, repr(exc)))
        print(f"FALHOU  {name}: {exc!r}", flush=True)
        traceback.print_exc()


def make_file(path: str, size: int) -> str:
    with open(path, "wb") as fh:
        fh.write(os.urandom(size))
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def sha(path: str) -> str:
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def all_healthy(c: DFSClient, path: str, n: int | None = None) -> bool:
    st = c.stat(path)
    want = n or st["replication"]
    return st["state"] == "healthy" and all(ch["live_replicas"] == want for ch in st["chunks"])


def main() -> int:
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(f"{DATA}/logs", exist_ok=True)
    tmp = os.path.join(DATA, "tmp")
    os.makedirs(tmp)
    start_controller()
    for i in range(1, 6):
        start_node(i)
    c = DFSClient(workers=4)
    wait_for(lambda: c.status()["nodes_by_state"].get("alive") == 5, 20, "5 nós vivos")
    print("cluster no ar: 1 controlador + 5 nós", flush=True)

    src = os.path.join(tmp, "a.bin")
    ref = make_file(src, 1_300_000)

    # ---------------------------------------------------------- RF1/RF3/RF4
    def t_put_get():
        c.mkdir("/docs")
        r = c.put(src, "/docs/a.bin")
        assert r["file_hash"] == ref and r["chunks"] == 5, r
        st = c.stat("/docs/a.bin")
        assert st["state"] == "healthy", st["state"]
        for ch in st["chunks"]:
            nodes = [x["node_id"] for x in ch["replicas"]]
            disks = [x["disk_id"] for x in ch["replicas"]]
            assert len(nodes) == 3 and len(set(nodes)) == 3 and len(set(disks)) == 3, ch
            zones = {x["zone"] for x in ch["replicas"]}
            assert len(zones) == 3, f"réplicas deveriam estar em 3 zonas: {zones}"
        out = os.path.join(tmp, "a.out")
        c.get("/docs/a.bin", out)
        assert sha(out) == ref
    check("RF1/RF3: put/get com 3 réplicas em nós, discos e zonas distintos", t_put_get)

    def t_namespace():
        c.mkdir("/x/y/z", parents=True)
        c.put(src, "/x/y/z/f.bin")
        c.mv("/x/y", "/x/w")
        assert any(i["name"] == "f.bin" for i in c.ls("/x/w/z")["items"])
        try:
            c.put(src, "/x/w/z/f.bin")
            raise AssertionError("deveria recusar sobrescrever sem -f")
        except DFSError as e:
            assert "409" in str(e)
        c.put(src, "/x/w/z/f.bin", overwrite=True)
        c.mv("/x/w/z/f.bin", "/x/g.bin")
        c.rm("/x/g.bin")
        try:
            c.rmdir("/x")
            raise AssertionError("rmdir não recursivo de dir não vazio deveria falhar")
        except DFSError:
            pass
        c.rmdir("/x", recursive=True)
        assert not any(i["name"] == "x" for i in c.ls("/")["items"])
    check("RF1: mkdir -p, ls, mv (arquivo e diretório), sobrescrita, rm, rmdir", t_namespace)

    def t_gc():
        time.sleep(3)
        total = c.status()["totals"]
        assert total["garbage"] == 0, total
        reps = c.status()["totals"]["replicas"]
        assert reps == 15, f"esperado 15 linhas de réplica (5 blocos x 3), obtido {reps}"
    check("GC: blocos de arquivos removidos/sobrescritos são apagados", t_gc)

    def t_concurrent_writers():
        big = os.path.join(tmp, "big.bin")
        make_file(big, 3_000_000)
        errs, oks = [], []

        def w():
            try:
                oks.append(c.put(big, "/docs/conc.bin"))
            except DFSError as e:
                errs.append(str(e))
        ts = [threading.Thread(target=w) for _ in range(3)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert len(oks) >= 1 and all("409" in e for e in errs), (oks, errs)
    check("RF8: escritores concorrentes no mesmo arquivo (lease exclusivo)", t_concurrent_writers)

    def t_read_during_overwrite():
        v1 = os.path.join(tmp, "v1.bin")
        v2 = os.path.join(tmp, "v2.bin")
        h1, h2 = make_file(v1, 2_000_000), make_file(v2, 2_500_000)
        c.put(v1, "/docs/v.bin")
        seen, stop_flag = set(), threading.Event()

        def reader():
            rc = DFSClient()
            while not stop_flag.is_set():
                o = os.path.join(tmp, f"r{threading.get_ident()}.bin")
                rc.get("/docs/v.bin", o)
                seen.add(sha(o))
        th = threading.Thread(target=reader)
        th.start()
        c.put(v2, "/docs/v.bin", overwrite=True)
        time.sleep(0.5)
        stop_flag.set()
        th.join()
        assert seen <= {h1, h2} and seen, seen
    check("RF8: leitura durante sobrescrita nunca vê arquivo parcial", t_read_during_overwrite)

    # --------------------------------------------------------- RF5 falha de nó
    def t_node_failure():
        st = c.stat("/docs/a.bin")
        victim = st["chunks"][0]["replicas"][0]["node_id"]
        shared["victim"] = victim
        t0 = time.time()
        stop(victim)
        wait_for(lambda: next(n for n in c.status()["nodes"] if n["id"] == victim)["state"] == "dead", 15,
                 "nó declarado morto")
        detect = time.time() - t0
        out = os.path.join(tmp, "a2.out")
        c.get("/docs/a.bin", out)
        assert sha(out) == ref
        wait_for(lambda: all_healthy(c, "/docs/a.bin"), 30, "re-replicação")
        st = c.stat("/docs/a.bin")
        for ch in st["chunks"]:
            live = [x for x in ch["replicas"] if x["state"] in ("alive", "suspect")]
            assert len({x["node_id"] for x in live}) == 3
        print(f"        detecção em {detect:.1f}s; recuperação total em {time.time() - t0:.1f}s", flush=True)
    check("RF5: falha de nó detectada por heartbeat e nível de replicação restaurado", t_node_failure)

    def t_write_with_dead_node():
        r = c.put(src, "/docs/during-failure.bin")
        assert r["under_replicated_chunks"] == 0
    check("Tolerância: escrita continua com 1 nó fora (4 vivos >= N)", t_write_with_dead_node)

    def t_node_returns():
        victim = shared["victim"]
        start_node(int(victim[-1]))
        wait_for(lambda: next(n for n in c.status()["nodes"] if n["id"] == victim)["state"] == "alive", 15,
                 "nó voltou")
        wait_for(lambda: c.status()["replication"]["over_replicated_chunks"] == 0 and
                 all_healthy(c, "/docs/a.bin"), 30, "reconciliação")
    check("RF6: nó que volta é reconhecido e seus blocos reconciliados (excesso removido)", t_node_returns)

    # ------------------------------------------------------ RF4 corrupção
    def t_corrupt_on_read():
        st = c.stat("/docs/a.bin")
        ch = st["chunks"][1]
        first = c._api("GET", "/api/fs/locate", params={"path": "/docs/a.bin"})["chunks"][1]["replicas"][0]
        httpx.post(f"{first['url']}/admin/chunks/{ch['chunk_id']}/corrupt").raise_for_status()
        out = os.path.join(tmp, "a3.out")
        c.get("/docs/a.bin", out)
        assert sha(out) == ref
        wait_for(lambda: all_healthy(c, "/docs/a.bin"), 30, "réplica corrompida substituída")
    check("RF4b: corrupção detectada na leitura, réplica descartada e substituída", t_corrupt_on_read)

    def t_corrupt_scrub():
        st = c.stat("/docs/a.bin")
        ch = st["chunks"][2]
        rep = ch["replicas"][0]
        url = next(n for n in c.status()["nodes"] if n["id"] == rep["node_id"])["public_url"]
        httpx.post(f"{url}/admin/chunks/{ch['chunk_id']}/corrupt").raise_for_status()
        wait_for(lambda: any(e["event"] == "replica_corrupt" and e.get("chunk_id") == ch["chunk_id"]
                             for e in c.events(200)["events"]), 20, "scrubbing detectar")
        wait_for(lambda: all_healthy(c, "/docs/a.bin"), 30, "réplica substituída")
    check("RF4c: scrubbing periódico detecta corrupção sem leitura do usuário", t_corrupt_scrub)

    def t_disk_failure():
        st = c.stat("/docs/a.bin")
        rep = st["chunks"][3]["replicas"][0]
        url = next(n for n in c.status()["nodes"] if n["id"] == rep["node_id"])["public_url"]
        disk = rep["disk_id"].split(":")[1]
        httpx.post(f"{url}/admin/disks/{disk}/fail").raise_for_status()
        wait_for(lambda: any(d["id"] == rep["disk_id"] and d["status"] == "failed"
                             for n in c.status()["nodes"] for d in n["disks"]), 10, "controlador ver disco falho")
        wait_for(lambda: all_healthy(c, "/docs/a.bin"), 30, "re-replicação após falha de disco")
        st = c.stat("/docs/a.bin")
        assert all(x["disk_id"] != rep["disk_id"] or x["state"] == "disk_failed"
                   for ch in st["chunks"] for x in ch["replicas"])
        httpx.post(f"{url}/admin/disks/{disk}/recover").raise_for_status()
    check("RF5: falha de DISCO detectada e réplicas recriadas em outro disco", t_disk_failure)

    # ------------------------------------------------------- RF3 política
    def t_under_replicated():
        r = c.put(src, "/docs/n12.bin", replication=12)
        assert r["warnings"] and r["under_replicated_chunks"] > 0, r
        assert c.stat("/docs/n12.bin")["state"] in ("under_replicated", "recovering")
        c.setrep("/docs/n12.bin", 3)
        wait_for(lambda: all_healthy(c, "/docs/n12.bin", 3), 30, "trim para N=3")
    check("RF3: nós insuficientes -> gravado como sub-replicado com aviso; setrep reduz", t_under_replicated)

    def t_dir_replication():
        c.mkdir("/rep2", replication=2)
        c.put(src, "/rep2/f.bin")
        st = c.stat("/rep2/f.bin")
        assert st["replication"] == 2 and all(len(ch["replicas"]) == 2 for ch in st["chunks"])
    check("Config: fator de replicação por diretório", t_dir_replication)

    # ---------------------------------------------------------- RF7 drain
    def t_drain():
        c.drain("node5")
        wait_for(lambda: next(n for n in c.status()["nodes"] if n["id"] == "node5")["drained"], 40, "drain")
        for p in ("/docs/a.bin", "/docs/v.bin"):
            st = c.stat(p)
            assert all("node5" not in [x["node_id"] for x in ch["replicas"]] for ch in st["chunks"])
            assert st["state"] == "healthy"
        r = c.put(src, "/docs/after-drain.bin")
        assert "node5" not in c.stat("/docs/after-drain.bin")["replica_nodes"]
        c.undrain("node5")
    check("RF7: drain move todas as réplicas e o nó não recebe novas", t_drain)

    # ---------------------------------------------------------- RF10
    def t_controller_restart():
        stop("controller")
        time.sleep(1)
        start_controller()
        wait_for(lambda: c.status()["nodes_by_state"].get("alive") == 5, 20, "nós se reconectarem")
        out = os.path.join(tmp, "a4.out")
        c.get("/docs/a.bin", out)
        assert sha(out) == ref
        assert c.status()["replication"]["under_replicated_chunks"] == 0
    check("RF10: controlador reinicia e reconstrói o estado sem intervenção", t_controller_restart)

    # ---------------------------------------------------------- RF9
    def t_encryption():
        stop("controller")
        time.sleep(1)
        start_controller(["encryption.enabled=true"])
        wait_for(lambda: c.status()["nodes_by_state"].get("alive") == 5, 20, "nós se reconectarem")
        txt = os.path.join(tmp, "secret.txt")
        with open(txt, "wb") as fh:
            fh.write(b"SEGREDO-" * 50000)
        r = c.put(txt, "/docs/secret.txt")
        assert r["encrypted"]
        out = os.path.join(tmp, "secret.out")
        c.get("/docs/secret.txt", out)
        assert sha(out) == sha(txt)
        st = c.stat("/docs/secret.txt")
        cid = st["chunks"][0]["chunk_id"]
        found = False
        for root, _, files in os.walk(DATA):
            if cid in files:
                found = True
                assert b"SEGREDO" not in open(os.path.join(root, cid), "rb").read()
        assert found
        o2 = os.path.join(tmp, "a5.out")
        c.get("/docs/a.bin", o2)
        assert sha(o2) == ref
    check("RF9: criptografia AES-256-GCM no cliente; nós guardam só texto cifrado", t_encryption)

    def t_metrics():
        m = c.metrics()
        assert "operations" in m and "files_by_state" in m and "replication" in m
        nm = httpx.get("http://localhost:9001/metrics").json()
        assert "disks" in nm
        lines = open(f"{DATA}/logs/controller.jsonl", encoding="utf-8").read().splitlines()
        import json
        evs = {json.loads(line)["event"] for line in lines}
        for e in ("node_dead", "recovery_completed", "file_committed", "replica_corrupt"):
            assert e in evs, e
    check("RF11/RF12/Obs.: status, /metrics e logs JSON estruturados", t_metrics)

    for name in list(procs):
        stop(name)
    ok = sum(1 for _, passed, _ in results if passed)
    print(f"\n{ok}/{len(results)} verificações passaram")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        for p in procs.values():
            if isinstance(p, subprocess.Popen):
                p.kill()

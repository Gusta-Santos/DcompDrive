"""Sobe 1 controlador + N nós de armazenamento como processos locais (sem Docker).

  python scripts/run_local.py                 # 5 nós, 2 discos cada
  python scripts/run_local.py --nodes 6 --set cluster.replication_factor=2
  python scripts/run_local.py --clean         # apaga data/ antes

Útil para desenvolvimento e para testar rápido no Windows/Linux/macOS.
Ctrl+C encerra tudo. Os logs JSON vão para data/logs/.
"""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(ROOT, "backend")
ZONES = ["zona-a", "zona-b", "zona-c"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--disks", type=int, default=2)
    ap.add_argument("--capacity-mb", type=float, default=1024)
    ap.add_argument("--data", default=os.path.join(ROOT, "data"))
    ap.add_argument("--clean", action="store_true")
    ap.add_argument("--set", action="append", default=[])
    a = ap.parse_args()

    if a.clean and os.path.exists(a.data):
        shutil.rmtree(a.data)
    logs = os.path.join(a.data, "logs")
    os.makedirs(logs, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": BACKEND, "PYTHONUNBUFFERED": "1"}
    procs: dict[str, subprocess.Popen] = {}

    sets = [f"controller.db_path={a.data}/controller/metadata.db",
            f"encryption.master_key_file={a.data}/controller/master.key",
            f"logging.file={logs}/controller.jsonl", *a.set]
    cmd = [sys.executable, "-m", "dfs.controller", "--config", os.path.join(BACKEND, "config", "config.yaml")]
    for s in sets:
        cmd += ["--set", s]
    procs["controller"] = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL)
    time.sleep(1.5)

    for i in range(1, a.nodes + 1):
        nid, port = f"node{i}", 9000 + i
        cmd = [sys.executable, "-m", "dfs.storage", "--node-id", nid, "--port", str(port),
               "--controller", "http://localhost:8000", "--internal-url", f"http://localhost:{port}",
               "--zone", ZONES[(i - 1) % len(ZONES)], "--capacity-mb", str(a.capacity_mb),
               "--log-file", f"{logs}/{nid}.jsonl"]
        for d in range(1, a.disks + 1):
            cmd += ["--disk", os.path.join(a.data, nid, f"disk{d}")]
        procs[nid] = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL)

    print(f"controlador: http://localhost:8000   nós: 9001..{9000 + a.nodes}   logs: {logs}")
    print("Ctrl+C para encerrar.")
    reported: set[str] = set()
    try:
        while True:
            time.sleep(1)
            for name, p in procs.items():
                if p.poll() is not None and name not in reported:
                    reported.add(name)
                    print(f"[aviso] {name} terminou com código {p.returncode}")
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs.values():
            if os.name == "nt":
                p.terminate()
            else:
                p.send_signal(signal.SIGINT)
        for p in procs.values():
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()

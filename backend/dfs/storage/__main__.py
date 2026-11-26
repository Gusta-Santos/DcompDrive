"""python -m dfs.storage --node-id n1 --port 9001 --disk data/n1/disk1 --disk data/n1/disk2

Todos os parâmetros também podem vir de variáveis de ambiente (usadas no Docker):
  NODE_ID, PORT, DISKS (separados por vírgula), CONTROLLER_URL, INTERNAL_URL,
  PUBLIC_URL, ZONE, DISK_CAPACITY_MB, SCRUB_INTERVAL_S, LOG_FILE, DEBUG_ENDPOINTS
"""
import argparse
import os
import socket
import uuid

import uvicorn

from .app import create_app


def _persistent_id(first_disk: str) -> str:
    """Sem NODE_ID explícito, gera um id e o grava no primeiro disco: o nó mantém
    a mesma identidade ao reiniciar e o controlador o reconhece (RF6)."""
    os.makedirs(first_disk, exist_ok=True)
    path = os.path.join(first_disk, ".node_id")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return fh.read().strip()
    nid = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(nid)
    return nid


def main() -> None:
    env = os.environ.get
    ap = argparse.ArgumentParser(description="Nó de armazenamento do DComp Drive")
    ap.add_argument("--node-id", default=env("NODE_ID"))
    ap.add_argument("--host", default=env("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(env("PORT", "9001")))
    ap.add_argument("--disk", action="append", default=None, help="pasta de dados (repita para vários discos)")
    ap.add_argument("--controller", default=env("CONTROLLER_URL", "http://localhost:8000"))
    ap.add_argument("--internal-url", default=env("INTERNAL_URL"), help="URL que outros nós/controlador usam")
    ap.add_argument("--public-url", default=env("PUBLIC_URL"), help="URL que os clientes usam")
    ap.add_argument("--zone", default=env("ZONE", "default"), help="domínio de falha (rack/zona)")
    ap.add_argument("--capacity-mb", type=float, default=float(env("DISK_CAPACITY_MB", "0")),
                    help="capacidade simulada por disco (0 = usar o tamanho real do sistema de arquivos)")
    ap.add_argument("--scrub-interval", type=float, default=float(env("SCRUB_INTERVAL_S", "0")) or None)
    ap.add_argument("--log-file", default=env("LOG_FILE"))
    args = ap.parse_args()

    disks = args.disk or [d for d in env("DISKS", "data/node/disk1").split(",") if d]
    node_id = args.node_id or _persistent_id(disks[0])
    internal = args.internal_url or f"http://{socket.gethostname()}:{args.port}"
    settings = {
        "node_id": node_id,
        "disks": disks,
        "controller_url": args.controller,
        "internal_url": internal,
        "public_url": args.public_url or internal,
        "zone": args.zone,
        "capacity_bytes": int(args.capacity_mb * 1024 * 1024) or None,
        "scrub_interval_s": args.scrub_interval,
        "log_file": args.log_file,
        "debug_endpoints": env("DEBUG_ENDPOINTS", "true").lower() == "true",
    }
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()

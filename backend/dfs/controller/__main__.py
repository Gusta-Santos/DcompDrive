"""python -m dfs.controller [--config arquivo.yaml] [--set secao.chave=valor ...]"""
import argparse

import uvicorn

from ..common.config import load_config
from .app import create_app


def main() -> None:
    ap = argparse.ArgumentParser(description="Controlador / nó de metadados do DComp Drive")
    ap.add_argument("--config", help="arquivo YAML de configuração (ou env DFS_CONFIG)")
    ap.add_argument("--set", action="append", default=[], metavar="secao.chave=valor",
                    help="sobrescreve uma configuração, ex.: --set cluster.replication_factor=2")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    args = ap.parse_args()
    cfg = load_config(args.config, args.set)
    host = args.host or cfg["controller"]["host"]
    port = args.port or int(cfg["controller"]["port"])
    uvicorn.run(create_app(cfg), host=host, port=port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()

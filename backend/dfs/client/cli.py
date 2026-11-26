"""CLI do DComp Drive (RF2).

  python -m dfs.client [--controller URL] [--json] <comando> ...

Comandos: put get ls mkdir rmdir rm mv stat setrep status events drain undrain forget metrics
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime

from .lib import DFSClient, DFSError


def human(n) -> str:
    if n is None:
        return "-"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def ts(t) -> str:
    return datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S") if t else "-"


def table(rows: list[list], headers: list[str]) -> str:
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c.ljust(w) for c, w in zip(r, widths)) for r in rows]
    return "\n".join(out)


def progress_bar(done: int, total: int) -> None:
    if not sys.stderr.isatty() or not total:
        return
    pct = done / total
    bar = "#" * int(pct * 30)
    sys.stderr.write(f"\r[{bar:<30}] {pct * 100:5.1f}% {human(done)}/{human(total)}")
    if done >= total:
        sys.stderr.write("\n")
    sys.stderr.flush()


def print_status(st: dict) -> None:
    nb = st["nodes_by_state"]
    print(f"Nós: {nb.get('alive', 0)} vivos, {nb.get('suspect', 0)} suspeitos, {nb.get('dead', 0)} mortos")
    rows = []
    for n in st["nodes"]:
        for i, d in enumerate(n["disks"]):
            rows.append([n["id"] if i == 0 else "", n["label"] if i == 0 else "", n["zone"] if i == 0 else "",
                         d["id"].split(":", 1)[-1], d["status"],
                         f"{human(d['used'])}/{human(d['total'])}",
                         f"{(d['usage'] or 0) * 100:.1f}%", n["replicas"] if i == 0 else "",
                         n["last_heartbeat_age_s"] if i == 0 else ""])
    print(table(rows, ["NÓ", "ESTADO", "ZONA", "DISCO", "DISCO_OK", "USO", "%", "RÉPLICAS", "HB(s)"]))
    labels = st["state_labels"]
    fs = st["files_by_state"]
    print("\nArquivos por estado: " + ", ".join(f"{labels[k]}={v}" for k, v in fs.items()))
    rp = st["replication"]
    print(f"Blocos: sub-replicados={rp['under_replicated_chunks']}  super-replicados={rp['over_replicated_chunks']}"
          f"  indisponíveis={rp['unavailable_chunks']}")
    print(f"Re-replicações: em andamento={len(rp['in_progress'])}  concluídas={rp['completed_total']}"
          f"  falhas={rp['failed_total']}  bytes copiados={human(rp['bytes_copied_total'])}")
    for t in rp["in_progress"]:
        print(f"  -> {t['chunk_id'][:12]} {t['source']} => {t['target']} ({human(t['size'])}, {t['elapsed_s']}s)")
    if rp["current_episode"]:
        ep = rp["current_episode"]
        print(f"Recuperação em curso desde {ts(ep['started'])} (causa: {ep['cause']})")
    for ep in rp["recent_episodes"][-3:]:
        print(f"  recuperação concluída: causa={ep['cause']} duração={ep['duration_s']}s "
              f"blocos={ep['chunks_copied']} bytes={human(ep['bytes_copied'])}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="dfs", description="Cliente do DComp Drive")
    ap.add_argument("--controller", help="URL do controlador (padrão: env DFS_CONTROLLER ou http://localhost:8000)")
    ap.add_argument("--json", action="store_true", help="saída em JSON")
    ap.add_argument("--workers", type=int, default=4, help="blocos transferidos em paralelo")
    ap.add_argument("--internal", action="store_true",
                    help="usar endereços internos dos nós (cliente dentro da rede Docker)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put", help="envia arquivo")
    p.add_argument("local")
    p.add_argument("remote")
    p.add_argument("-r", "--replication", type=int)
    p.add_argument("-f", "--force", action="store_true", help="sobrescreve se existir")
    p = sub.add_parser("get", help="baixa arquivo")
    p.add_argument("remote")
    p.add_argument("local")
    p = sub.add_parser("ls", help="lista diretório")
    p.add_argument("path", nargs="?", default="/")
    p = sub.add_parser("mkdir", help="cria diretório")
    p.add_argument("path")
    p.add_argument("-p", "--parents", action="store_true")
    p.add_argument("-r", "--replication", type=int, help="fator de replicação padrão do diretório")
    p = sub.add_parser("rmdir", help="remove diretório")
    p.add_argument("path")
    p.add_argument("-R", "--recursive", action="store_true")
    p = sub.add_parser("rm", help="remove arquivo")
    p.add_argument("path")
    p = sub.add_parser("mv", help="renomeia/move arquivo ou diretório")
    p.add_argument("src")
    p.add_argument("dst")
    p = sub.add_parser("stat", help="metadados e estado do arquivo")
    p.add_argument("path")
    p = sub.add_parser("setrep", help="altera o fator de replicação")
    p.add_argument("path")
    p.add_argument("replication", type=int)
    p = sub.add_parser("status", help="painel do cluster (RF11)")
    p.add_argument("-w", "--watch", type=float, help="atualiza a cada N segundos")
    p = sub.add_parser("events", help="eventos recentes do controlador")
    p.add_argument("-n", type=int, default=30)
    p = sub.add_parser("drain", help="esvazia um nó antes de retirá-lo")
    p.add_argument("node_id")
    p = sub.add_parser("undrain", help="cancela o drain")
    p.add_argument("node_id")
    p = sub.add_parser("forget", help="remove do cluster um nó morto/drenado")
    p.add_argument("node_id")
    sub.add_parser("metrics", help="métricas do controlador (JSON)")

    a = ap.parse_args(argv)
    c = DFSClient(a.controller, workers=a.workers, address="internal" if a.internal else None)

    def out(obj, text: str | None = None):
        if a.json or text is None:
            print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))
        else:
            print(text)

    try:
        if a.cmd == "put":
            r = c.put(a.local, a.remote, a.replication, a.force, progress=None if a.json else progress_bar)
            msg = [f"OK  {r['path']}  {human(r['size'])}  {r['chunks']} bloco(s)  N={r['replication']} "
                   f"W={r['write_quorum']}  {r['throughput_MBps']} MB/s",
                   f"    {r['file_hash']}"]
            for w in r["warnings"]:
                msg.append(f"    AVISO: {w}")
            if r["under_replicated_chunks"]:
                msg.append(f"    {r['under_replicated_chunks']} bloco(s) abaixo de N: o controlador completará "
                           f"as réplicas em segundo plano")
            out(r, "\n".join(msg))
        elif a.cmd == "get":
            r = c.get(a.remote, a.local, progress=None if a.json else progress_bar)
            out(r, f"OK  {r['path']} -> {r['local']}  {human(r['size'])}  hash verificado  {r['throughput_MBps']} MB/s")
        elif a.cmd == "ls":
            r = c.ls(a.path)
            rows = [[("d " if i["type"] == "dir" else "- ") + i["name"] + ("/" if i["type"] == "dir" else ""),
                     human(i.get("size")) if i["type"] == "file" else "",
                     i.get("replication") or "", i.get("state_label", ""), ts(i.get("modified_at"))]
                    for i in r["items"]]
            out(r, table(rows, ["NOME", "TAMANHO", "N", "ESTADO", "MODIFICADO"]))
        elif a.cmd == "mkdir":
            out(c.mkdir(a.path, a.parents, a.replication), f"OK  {a.path}")
        elif a.cmd == "rmdir":
            r = c.rmdir(a.path, a.recursive)
            out(r, f"OK  {a.path} removido ({r['removed_entries']} entradas)")
        elif a.cmd == "rm":
            out(c.rm(a.path), f"OK  {a.path} removido")
        elif a.cmd == "mv":
            out(c.mv(a.src, a.dst), f"OK  {a.src} -> {a.dst}")
        elif a.cmd == "setrep":
            r = c.setrep(a.path, a.replication)
            out(r, f"OK  {a.path} N={a.replication} ({r['files_updated']} arquivo(s))")
        elif a.cmd == "stat":
            r = c.stat(a.path)
            if r["type"] == "dir":
                out(r, f"{r['path']}  diretório  {r['children']} itens  N efetivo={r['replication']}")
            else:
                lines = [f"Caminho : {r['path']}", f"Tamanho : {human(r['size'])} ({r['size']} bytes)",
                         f"Criado  : {ts(r['created_at'])}   Modificado: {ts(r['modified_at'])}",
                         f"Hash    : {r['hash_alg']} {r['hash']}",
                         f"Réplicas: N={r['replication']}  mínimo vivo={r['live_replicas_min']}  "
                         f"cifrado={r['encrypted']}",
                         f"Estado  : {r['state_label']}",
                         f"Nós     : {', '.join(r['replica_nodes'])}", ""]
                rows = [[ch["index"], ch["chunk_id"][:12], human(ch["size"]), ch["hash"][:16],
                         f"{ch['live_replicas']}/{r['replication']}",
                         ", ".join(f"{x['disk_id']}({x['state']})" for x in ch["replicas"])] for ch in r["chunks"]]
                out(r, "\n".join(lines) + table(rows, ["#", "BLOCO", "TAM", "HASH", "VIVAS", "LOCALIZAÇÃO"]))
        elif a.cmd == "status":
            while True:
                r = c.status()
                if a.json:
                    out(r)
                else:
                    if a.watch:
                        print("\033[2J\033[H", end="")
                    print_status(r)
                if not a.watch:
                    break
                time.sleep(a.watch)
        elif a.cmd == "events":
            r = c.events(a.n)
            rows = [[e["ts"], e["level"], e["event"],
                     " ".join(f"{k}={v}" for k, v in e.items() if k not in ("ts", "level", "event", "component", "instance"))]
                    for e in r["events"]]
            out(r, table(rows, ["QUANDO", "NÍVEL", "EVENTO", "DETALHES"]))
        elif a.cmd == "drain":
            out(c.drain(a.node_id), f"OK  {a.node_id} em drain: réplicas serão copiadas para outros nós")
        elif a.cmd == "undrain":
            out(c.undrain(a.node_id), f"OK  drain de {a.node_id} cancelado")
        elif a.cmd == "forget":
            out(c.forget(a.node_id), f"OK  {a.node_id} removido do cluster")
        elif a.cmd == "metrics":
            out(c.metrics())
    except DFSError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0

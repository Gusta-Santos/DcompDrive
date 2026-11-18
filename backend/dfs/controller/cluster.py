"""Plano de controle: registro de nós, heartbeats, detecção de falhas e posicionamento.

Descoberta (RF6): "registro ativo". O nó de armazenamento conhece apenas o endereço
do controlador (variável CONTROLLER_URL) e se registra sozinho ao iniciar. Nenhum
arquivo central precisa ser editado para adicionar uma máquina.

Detecção de falhas (RF5): cada nó envia heartbeat a cada `interval_s`.
  sem heartbeat por > suspect_after_s  -> SUSPECT (ainda conta como réplica viva)
  sem heartbeat por > dead_after_s     -> DEAD    (réplicas deixam de contar => re-replicação)
O estado intermediário SUSPECT evita re-replicar em massa por causa de uma
pausa curta (GC, rede lenta), que é o maior custo de um falso positivo.
"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field

from ..common.logs import EventLogger
from .store import MetadataStore


@dataclass
class DiskInfo:
    id: str
    node_id: str
    path: str = ""
    total: int = 0
    used: int = 0
    free: int = 0
    status: str = "ok"
    reserved: int = 0

    def projected_usage(self, extra: int = 0) -> float:
        if self.total <= 0:
            return 1.0
        return (self.used + self.reserved + extra) / self.total

    def as_dict(self) -> dict:
        return {
            "id": self.id, "path": self.path, "total": self.total, "used": self.used, "free": self.free,
            "reserved": self.reserved, "status": self.status,
            "usage": round(self.used / self.total, 4) if self.total else None,
        }


@dataclass
class NodeInfo:
    id: str
    internal_url: str
    public_url: str
    zone: str = "default"
    state: str = "alive"
    draining: bool = False
    drained: bool = False
    registered_at: float = 0.0
    last_seen: float = 0.0
    dead_since: float | None = None
    active_ops: int = 0
    chunk_count: int = 0
    disks: dict[str, DiskInfo] = field(default_factory=dict)


class ClusterRegistry:
    def __init__(self, cfg: dict, store: MetadataStore, log: EventLogger):
        self.cfg = cfg
        self.store = store
        self.log = log
        self.nodes: dict[str, NodeInfo] = {}
        self.detection_times: list[dict] = []
        self._load()

    # ---------------------------------------------------------- persistência
    def _load(self) -> None:
        """Ao reiniciar o controlador, os nós conhecidos voltam como SUSPECT com o relógio
        "zerado": eles têm `dead_after_s` para mandar heartbeat antes de serem declarados
        mortos. Isso evita uma re-replicação em massa só porque o controlador reiniciou."""
        nodes, disks = self.store.load_nodes()
        now = time.time()
        for r in nodes:
            n = NodeInfo(
                id=r["id"], internal_url=r["internal_url"], public_url=r["public_url"], zone=r["zone"] or "default",
                state="dead" if r["state"] == "dead" else "suspect",
                draining=bool(r["draining"]), drained=bool(r["drained"]),
                registered_at=r["registered_at"], last_seen=now if r["state"] != "dead" else (r["last_seen"] or 0),
            )
            self.nodes[n.id] = n
            self.store.set_node_state(n.id, n.state)
        for d in disks:
            if d["node_id"] in self.nodes:
                self.nodes[d["node_id"]].disks[d["id"]] = DiskInfo(
                    id=d["id"], node_id=d["node_id"], path=d["path"] or "", total=d["total"], used=d["used"],
                    free=d["free"], status=d["status"],
                )
        if self.nodes:
            self.log.info("cluster_state_loaded", nodes=len(self.nodes))

    def _persist_node(self, n: NodeInfo) -> None:
        self.store.upsert_node({
            "id": n.id, "internal_url": n.internal_url, "public_url": n.public_url, "zone": n.zone,
            "state": n.state, "draining": int(n.draining), "drained": int(n.drained),
            "registered_at": n.registered_at, "last_seen": n.last_seen,
        })

    # ------------------------------------------------------------- registro
    def register(self, body: dict) -> NodeInfo:
        now = time.time()
        nid = body["node_id"]
        existing = self.nodes.get(nid)
        n = existing or NodeInfo(id=nid, internal_url="", public_url="", registered_at=now)
        was_state = existing.state if existing else None
        n.internal_url = body["internal_url"].rstrip("/")
        n.public_url = (body.get("public_url") or body["internal_url"]).rstrip("/")
        n.zone = body.get("zone") or "default"
        n.last_seen = now
        n.state = "alive"
        n.dead_since = None
        self.nodes[nid] = n
        self._persist_node(n)
        self._update_disks(n, body.get("disks", []), persist=True)
        if existing is None:
            self.log.info("node_registered", node_id=nid, zone=n.zone, url=n.internal_url, disks=len(n.disks))
        else:
            self.log.info("node_reregistered", node_id=nid, previous_state=was_state)
        return n

    def _update_disks(self, n: NodeInfo, disks: list[dict], persist: bool) -> list[tuple[str, str, str]]:
        """Atualiza estatísticas de espaço. Retorna mudanças de estado de disco."""
        changes = []
        for d in disks:
            info = n.disks.get(d["id"])
            if info is None:
                info = DiskInfo(id=d["id"], node_id=n.id)
                n.disks[d["id"]] = info
                persist = True
            old = info.status
            info.path = d.get("path", info.path)
            info.total, info.used, info.free = int(d["total"]), int(d["used"]), int(d["free"])
            info.status = d.get("status", "ok")
            if old != info.status:
                changes.append((info.id, old, info.status))
            if persist or old != info.status:
                self.store.upsert_disk({"id": info.id, "node_id": n.id, "path": info.path, "total": info.total,
                                        "used": info.used, "free": info.free, "status": info.status})
        return changes

    def heartbeat(self, body: dict) -> dict | None:
        """Processa heartbeat. Retorna None se o nó for desconhecido (ele deve se registrar)."""
        n = self.nodes.get(body.get("node_id", ""))
        if n is None:
            return None
        now = time.time()
        prev = n.state
        n.last_seen = now
        n.active_ops = int(body.get("active_ops", 0))
        n.chunk_count = int(body.get("chunk_count", 0))
        changed = False
        want_report = False
        for disk_id, old, new in self._update_disks(n, body.get("disks", []), persist=False):
            changed = True
            if new != "ok":
                self.log.warning("disk_failed", node_id=n.id, disk_id=disk_id)
            else:
                self.log.info("disk_recovered", node_id=n.id, disk_id=disk_id)
                want_report = True
        if prev != "alive":
            n.state = "alive"
            self.store.set_node_state(n.id, "alive", now)
            changed = True
            if prev == "dead":
                down = now - (n.dead_since or now)
                self.log.info("node_recovered", node_id=n.id, was_dead_for_s=round(down, 2))
                want_report = True   # reconcilia blocos do nó que voltou (RF6)
            else:
                self.log.info("node_alive", node_id=n.id, previous_state=prev)
            n.dead_since = None
        return {"changed": changed, "block_report": want_report}

    # ---------------------------------------------------- detecção de falhas
    def check_liveness(self) -> list[tuple[str, str, str]]:
        hb = self.cfg["heartbeat"]
        now = time.time()
        transitions = []
        for n in self.nodes.values():
            silent = now - n.last_seen
            new = n.state
            if silent > hb["dead_after_s"]:
                new = "dead"
            elif silent > hb["suspect_after_s"]:
                new = "suspect" if n.state != "dead" else "dead"
            if new != n.state:
                old, n.state = n.state, new
                self.store.set_node_state(n.id, new)
                transitions.append((n.id, old, new))
                if new == "dead":
                    n.dead_since = now
                    rec = {"node_id": n.id, "last_heartbeat_age_s": round(silent, 3), "detected_at": now}
                    self.detection_times.append(rec)
                    self.detection_times = self.detection_times[-100:]
                    self.log.warning("node_dead", node_id=n.id, silent_for_s=round(silent, 3))
                else:
                    self.log.warning("node_suspect", node_id=n.id, silent_for_s=round(silent, 3))
        return transitions

    # ------------------------------------------------------------- consultas
    def disk(self, disk_id: str) -> DiskInfo | None:
        for n in self.nodes.values():
            if disk_id in n.disks:
                return n.disks[disk_id]
        return None

    def node_of_disk(self) -> dict[str, str]:
        return {d: n.id for n in self.nodes.values() for d in n.disks}

    def is_readable(self, node_id: str, disk_id: str) -> bool:
        n = self.nodes.get(node_id)
        if n is None or n.state == "dead":
            return False
        d = n.disks.get(disk_id)
        return d is not None and d.status == "ok"

    def reserve(self, disk_id: str, nbytes: int) -> None:
        d = self.disk(disk_id)
        if d:
            d.reserved += nbytes

    def release(self, disk_id: str, nbytes: int) -> None:
        d = self.disk(disk_id)
        if d:
            d.reserved = max(0, d.reserved - nbytes)

    # ----------------------------------------------------------- posicionamento
    def _weight(self, n: NodeInfo, d: DiskInfo) -> float:
        policy = self.cfg["placement"]["policy"]
        if policy == "random":
            return 1.0
        if policy == "load":
            return 1.0 / (1 + n.active_ops)
        # free_space: probabilidade proporcional ao espaço livre
        return max(1.0, float(d.total - d.used - d.reserved))

    def choose_targets(self, count: int, size: int, exclude_disks=(), used_nodes=(), used_zones=(),
                       exclude_nodes=()) -> tuple[list[tuple[NodeInfo, DiskInfo]], bool]:
        """Escolhe `count` discos para um bloco.

        Regras (em ordem de prioridade):
          1. só nós ALIVE, que não estejam em drain, com disco OK;
          2. nunca dois blocos no mesmo disco (RF3);
          3. disco não pode passar do limite de uso (ex.: 90%) com este bloco (RF7);
          4. preferir máquinas distintas E zonas distintas (domínio de falha);
          5. depois, máquinas distintas em qualquer zona;
          6. só se faltar máquina e `allow_same_machine_fallback` = true: discos
             distintos de uma máquina já usada (retorna fallback=True).
        Entre os candidatos, a ordem é um sorteio ponderado (Efraimidis-Spirakis):
        discos com mais espaço livre têm mais chance, mas não recebem tudo — isso
        espalha os blocos de um mesmo arquivo e equilibra o uso.
        """
        limit = float(self.cfg["storage"]["disk_usage_limit"])
        exclude_disks, exclude_nodes = set(exclude_disks), set(exclude_nodes)
        cands: list[tuple[float, NodeInfo, DiskInfo]] = []
        for n in self.nodes.values():
            if n.state != "alive" or n.draining or n.id in exclude_nodes:
                continue
            for d in n.disks.values():
                if d.status != "ok" or d.id in exclude_disks or d.total <= 0:
                    continue
                if d.projected_usage(size) > limit or (d.free - d.reserved) < size:
                    continue
                key = math.log(random.random() or 1e-300) / self._weight(n, d)
                cands.append((key, n, d))
        cands.sort(key=lambda t: t[0], reverse=True)

        chosen: list[tuple[NodeInfo, DiskInfo]] = []
        nodes_used, zones_used, disks_used = set(used_nodes), set(used_zones), set()

        def pick(cond) -> None:
            for _, n, d in cands:
                if len(chosen) >= count:
                    return
                if d.id in disks_used or not cond(n):
                    continue
                chosen.append((n, d))
                disks_used.add(d.id)
                nodes_used.add(n.id)
                zones_used.add(n.zone)

        if self.cfg["placement"]["zone_aware"]:
            pick(lambda n: n.id not in nodes_used and n.zone not in zones_used)
        pick(lambda n: n.id not in nodes_used)
        fallback = False
        if len(chosen) < count and self.cfg["cluster"]["allow_same_machine_fallback"]:
            before = len(chosen)
            pick(lambda n: True)
            fallback = len(chosen) > before
        return chosen, fallback

    # --------------------------------------------------------------- painel
    def summary(self, replica_counts: dict[str, int]) -> list[dict]:
        now = time.time()
        out = []
        for n in sorted(self.nodes.values(), key=lambda n: n.id):
            state = n.state
            if n.drained:
                state_label = "drained"
            elif n.draining:
                state_label = "draining"
            else:
                state_label = state
            out.append({
                "id": n.id, "state": state, "label": state_label, "zone": n.zone,
                "draining": n.draining, "drained": n.drained,
                "internal_url": n.internal_url, "public_url": n.public_url,
                "last_heartbeat_age_s": round(now - n.last_seen, 1) if n.last_seen else None,
                "active_ops": n.active_ops, "replicas": replica_counts.get(n.id, 0),
                "disks": [d.as_dict() for d in sorted(n.disks.values(), key=lambda d: d.id)],
            })
        return out

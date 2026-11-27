"""Carregamento de configuração.

Ordem de precedência (do mais fraco para o mais forte):
  1. DEFAULTS (abaixo)
  2. arquivo YAML (--config ou variável DFS_CONFIG)
  3. variáveis de ambiente no formato DFS__SECAO__CHAVE=valor
     ex.: DFS__CLUSTER__REPLICATION_FACTOR=2
  4. parâmetros de linha de comando --set secao.chave=valor

Nada precisa ser recompilado: basta mudar o YAML / env / argumento e reiniciar.
"""
from __future__ import annotations

import copy
import os
from typing import Any, Iterable

import yaml

DEFAULTS: dict[str, Any] = {
    "cluster": {
        "replication_factor": 3,
        "write_quorum": 2,
        "under_replication_policy": "accept",
        "allow_same_machine_fallback": True,
        "chunk_size_mb": 4,
        "hash_algorithm": "sha256",
    },
    "encryption": {
        "enabled": False,
        "algorithm": "AES-256-GCM",
        "master_key_file": "data/controller/master.key",
    },
    "heartbeat": {
        "interval_s": 2.0,
        "suspect_after_s": 6.0,
        "dead_after_s": 12.0,
        "block_report_interval_s": 30.0,
    },
    "storage": {
        "disk_usage_limit": 0.90,
        "scrub_interval_s": 60.0,
    },
    "placement": {
        "policy": "free_space",
        "zone_aware": True,
    },
    "rereplication": {
        "max_concurrent": 4,
        "max_per_node": 2,
        "bandwidth_mbps": 50.0,
        "check_interval_s": 2.0,
    },
    "gc": {
        "interval_s": 5.0,
        "delete_grace_s": 30.0,
        "orphan_grace_s": 120.0,
    },
    "leases": {
        "write_lease_s": 60.0,
    },
    "controller": {
        "host": "0.0.0.0",
        "port": 8000,
        "db_path": "data/controller/metadata.db",
        "node_request_timeout_s": 10.0,
    },
    "logging": {
        "file": None,
    },
    "debug_endpoints": True,
}

SUPPORTED_HASHES = ("sha256", "sha512")
SUPPORTED_POLICIES = ("free_space", "random", "load")


def _deep_merge(base: dict, extra: dict) -> dict:
    for key, value in (extra or {}).items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _set_path(cfg: dict, dotted: list[str], raw: str) -> None:
    node = cfg
    for part in dotted[:-1]:
        node = node.setdefault(part, {})
    node[dotted[-1]] = yaml.safe_load(raw) if raw != "" else None


def load_config(path: str | None = None, overrides: Iterable[str] | None = None) -> dict:
    cfg = copy.deepcopy(DEFAULTS)
    path = path or os.environ.get("DFS_CONFIG")
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            _deep_merge(cfg, yaml.safe_load(fh) or {})

    for key, value in os.environ.items():
        if key.startswith("DFS__"):
            _set_path(cfg, key[5:].lower().split("__"), value)

    for item in overrides or []:
        if "=" not in item:
            raise ValueError(f"--set espera chave=valor, recebeu {item!r}")
        k, v = item.split("=", 1)
        _set_path(cfg, k.strip().split("."), v.strip())

    validate(cfg)
    return cfg


def validate(cfg: dict) -> None:
    c = cfg["cluster"]
    if int(c["replication_factor"]) < 1:
        raise ValueError("replication_factor deve ser >= 1")
    if int(c["write_quorum"]) < 1:
        raise ValueError("write_quorum deve ser >= 1")
    if c["hash_algorithm"] not in SUPPORTED_HASHES:
        raise ValueError(f"hash_algorithm deve ser um de {SUPPORTED_HASHES}")
    if c["under_replication_policy"] not in ("accept", "reject"):
        raise ValueError("under_replication_policy deve ser accept ou reject")
    if cfg["placement"]["policy"] not in SUPPORTED_POLICIES:
        raise ValueError(f"placement.policy deve ser um de {SUPPORTED_POLICIES}")
    if cfg["encryption"]["algorithm"] != "AES-256-GCM":
        raise ValueError("único algoritmo de cifra suportado: AES-256-GCM")
    if not 0 < float(cfg["storage"]["disk_usage_limit"]) <= 1:
        raise ValueError("disk_usage_limit deve estar em (0, 1]")


def chunk_size_bytes(cfg: dict) -> int:
    return max(1024, int(float(cfg["cluster"]["chunk_size_mb"]) * 1024 * 1024))

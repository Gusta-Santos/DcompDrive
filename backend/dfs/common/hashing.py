"""Funções de hash (RF4). Usa somente a biblioteca padrão (hashlib)."""
from __future__ import annotations

import hashlib

from .config import SUPPORTED_HASHES


def new_hasher(alg: str = "sha256"):
    if alg not in SUPPORTED_HASHES:
        raise ValueError(f"algoritmo de hash não suportado: {alg}")
    return hashlib.new(alg)


def hash_bytes(data: bytes, alg: str = "sha256") -> str:
    h = new_hasher(alg)
    h.update(data)
    return h.hexdigest()


def hash_file(path: str, alg: str = "sha256", block: int = 1024 * 1024) -> str:
    h = new_hasher(alg)
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(block)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()

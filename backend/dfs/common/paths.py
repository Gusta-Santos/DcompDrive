"""Normalização de caminhos do espaço de nomes (sempre absolutos, estilo POSIX)."""
from __future__ import annotations

import posixpath


class PathError(ValueError):
    pass


def norm(path: str) -> str:
    if path is None:
        raise PathError("caminho vazio")
    p = path.strip().replace("\\", "/")
    if not p:
        raise PathError("caminho vazio")
    p = posixpath.normpath("/" + p.lstrip("/"))
    if p.startswith("//"):
        p = "/" + p.lstrip("/")
    if any(ch in p for ch in ("\x00",)):
        raise PathError("caractere inválido no caminho")
    return p


def parent(path: str) -> str:
    return posixpath.dirname(path) or "/"


def name(path: str) -> str:
    return posixpath.basename(path) or "/"


def is_under(path: str, ancestor: str) -> bool:
    """True se `path` == ancestor ou está dentro dele."""
    if ancestor == "/":
        return True
    return path == ancestor or path.startswith(ancestor + "/")

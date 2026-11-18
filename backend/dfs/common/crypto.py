"""Criptografia (RF9) — AES-256-GCM via biblioteca `cryptography` (não implementamos cifra própria).

Modelo de chaves ("envelope encryption", como um KMS):
  * O controlador guarda uma chave-mestra (KEK) num arquivo separado do banco de metadados.
  * Cada versão de arquivo recebe uma chave de dados aleatória (DEK) de 256 bits.
  * A DEK é guardada nos metadados *cifrada* pela KEK (wrapped key).
  * O cliente recebe a DEK do controlador e cifra/decifra os blocos ele mesmo.
    => Os nós de armazenamento nunca veem texto claro nem chaves.
    => O controlador nunca vê o conteúdo dos arquivos.

Formato de um bloco cifrado: nonce(12 bytes) || ciphertext || tag(16 bytes)
O chunk_id é usado como "associated data", amarrando o conteúdo ao bloco
(impede trocar blocos de lugar sem detecção).
"""
from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_LEN = 12
KEY_LEN = 32


def generate_key() -> bytes:
    return AESGCM.generate_key(bit_length=256)


def load_or_create_master_key(path: str) -> bytes:
    if os.path.exists(path):
        with open(path, "rb") as fh:
            key = fh.read()
        if len(key) != KEY_LEN:
            raise ValueError(f"chave-mestra em {path} tem tamanho inválido")
        return key
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    key = generate_key()
    with open(path, "wb") as fh:
        fh.write(key)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return key


def wrap_key(master: bytes, dek: bytes) -> bytes:
    nonce = os.urandom(NONCE_LEN)
    return nonce + AESGCM(master).encrypt(nonce, dek, b"dfs-dek")


def unwrap_key(master: bytes, wrapped: bytes) -> bytes:
    return AESGCM(master).decrypt(wrapped[:NONCE_LEN], wrapped[NONCE_LEN:], b"dfs-dek")


def encrypt_chunk(dek: bytes, chunk_id: str, plaintext: bytes) -> bytes:
    nonce = os.urandom(NONCE_LEN)
    return nonce + AESGCM(dek).encrypt(nonce, plaintext, chunk_id.encode())


def decrypt_chunk(dek: bytes, chunk_id: str, blob: bytes) -> bytes:
    return AESGCM(dek).decrypt(blob[:NONCE_LEN], blob[NONCE_LEN:], chunk_id.encode())

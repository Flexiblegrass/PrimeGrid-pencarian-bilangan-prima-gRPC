"""Utilitas bersama: pemanggilan RPC dengan timeout + retry (exponential backoff + jitter)."""
import logging
import random
import time
import uuid

import grpc

log = logging.getLogger("rpc_util")

# Hanya kegagalan yang bersifat sementara yang layak diulang.
KODE_ULANG = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


def buat_request_id() -> str:
    return str(uuid.uuid4())


def panggil_dengan_retry(fn, request, timeout=5.0, maks=3, dasar=0.5, nama=""):
    """Panggil `fn(request, timeout=...)`.

    * Objek `request` yang SAMA (dan karena itu request_id yang SAMA) dipakai di
      setiap percobaan -> server dapat menyaring duplikat (at-most-once).
    * Jeda antar percobaan: dasar * 2^(n-1) dikali jitter acak 0.5-1.0.
    * Kode selain UNAVAILABLE / DEADLINE_EXCEEDED (mis. INVALID_ARGUMENT,
      NOT_FOUND) langsung dilempar karena mengulang tidak akan membantu.
    """
    for percobaan in range(1, maks + 1):
        try:
            return fn(request, timeout=timeout)
        except grpc.RpcError as e:
            kode = e.code()
            if kode not in KODE_ULANG or percobaan == maks:
                raise
            jeda = dasar * (2 ** (percobaan - 1)) * random.uniform(0.5, 1.0)
            log.warning("%s percobaan %d/%d gagal (%s); ulang dalam %.2f s",
                        nama or getattr(fn, "__name__", "rpc"), percobaan, maks,
                        kode.name, jeda)
            time.sleep(jeda)

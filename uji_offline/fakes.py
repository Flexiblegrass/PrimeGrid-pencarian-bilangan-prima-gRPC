import enum, sys, types

# ---------------- grpc palsu ----------------
grpc = types.ModuleType("grpc")
class StatusCode(enum.Enum):
    OK=0; INVALID_ARGUMENT=3; DEADLINE_EXCEEDED=4; NOT_FOUND=5; UNAVAILABLE=14
class RpcError(Exception):
    def __init__(self, code, details=""): self._c, self._d = code, details
    def code(self): return self._c
    def details(self): return self._d
grpc.StatusCode, grpc.RpcError = StatusCode, RpcError
sys.modules["grpc"] = grpc

# ---------------- pb2 palsu ----------------
pb = types.ModuleType("primes_pb2")
class Msg:
    def __init__(self, **kw): self.__dict__.update(kw)
    def __getattr__(self, k):
        if k.startswith("__"): raise AttributeError(k)
        return 0
for n in ["Kosong","PingResponse","SegmenRequest","SegmenResponse","WorkerInfo","WorkerStatus",
          "WorkerList","StatusResponse","JobRequest","JobAck","JobId","JobStatus","JobProgress","JobHasil"]:
    setattr(pb, n, type(n, (Msg,), {}))
for i, n in enumerate(["JOB_TIDAK_DIKETAHUI","JOB_ANTRI","JOB_BERJALAN","JOB_SELESAI","JOB_GAGAL"]):
    setattr(pb, n, i)
sys.modules["primes_pb2"] = pb
pbg = types.ModuleType("primes_pb2_grpc")
pbg.WorkerServicer = pbg.MasterServicer = object
sys.modules["primes_pb2_grpc"] = pbg

class Ctx:
    def __init__(self): self.aktif = True
    def abort(self, code, det): raise RpcError(code, det)
    def is_active(self): return self.aktif
    def time_remaining(self): return None

class StubWorker:
    """Stub palsu yang memanggil servicer Worker secara langsung (tanpa jaringan)."""
    def __init__(self, servicer, mati_setelah=None, gagal_awal=0):
        self.s, self.mati_setelah, self.gagal_awal = servicer, mati_setelah, gagal_awal
        self.n = 0; self.mati = False
    def HitungSegmen(self, req, timeout=None):
        if self.mati: raise RpcError(StatusCode.UNAVAILABLE, "mati")
        if self.gagal_awal > 0:
            self.gagal_awal -= 1; raise RpcError(StatusCode.DEADLINE_EXCEEDED, "lambat")
        r = self.s.HitungSegmen(req, Ctx())
        self.n += 1
        if self.mati_setelah is not None and self.n >= self.mati_setelah:
            self.mati = True; raise RpcError(StatusCode.UNAVAILABLE, "crash, balasan hilang")
        return r
    def Ping(self, req, timeout=None):
        if self.mati: raise RpcError(StatusCode.UNAVAILABLE, "mati")
        return self.s.Ping(req, Ctx())

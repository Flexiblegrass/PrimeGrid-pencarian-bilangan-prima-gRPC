#!/usr/bin/env python3
"""MASTER: membagi rentang bilangan menjadi chunk, membagikannya ke banyak worker
secara paralel, lalu menggabungkan hasilnya.

Strategi penjadwalan: *dynamic work queue*. Semua chunk dimasukkan ke antrian;
setiap worker punya satu thread di master yang terus mengambil chunk berikutnya
begitu worker-nya selesai. Worker yang cepat otomatis mengerjakan lebih banyak
chunk (load balancing), dan chunk dari worker yang mati dikembalikan ke antrian
(failover) sehingga hasil akhir tetap lengkap dan benar.

Contoh:
  python master.py --port 50050
  python master.py --port 50050 --workers w1=localhost:50061,w2=localhost:50062
"""
import argparse
import logging
import queue
import threading
import time
import uuid
from collections import defaultdict
from concurrent import futures

import grpc

import prima
import primes_pb2 as pb
import primes_pb2_grpc as pb_grpc
from rpc_util import panggil_dengan_retry

log = logging.getLogger("master")

STATE_AKHIR = (pb.JOB_SELESAI, pb.JOB_GAGAL)
BATAS_JOB_TERSIMPAN = 5000


# ======================================================================
#  Registri worker
# ======================================================================
class WorkerRef:
    def __init__(self, worker_id, alamat, stub, channel=None):
        self.worker_id = worker_id
        self.alamat = alamat
        self.stub = stub
        self.channel = channel
        self.hidup = True
        self.gagal_ping = 0
        self.chunk_selesai = 0
        self._lock = threading.Lock()

    def tambah_selesai(self):
        with self._lock:
            self.chunk_selesai += 1


# ======================================================================
#  Pekerjaan (job)
# ======================================================================
def bagi_rentang(awal, akhir, n):
    """Bagi [awal, akhir] menjadi n sub-rentang berurutan, tanpa tumpang tindih."""
    total = akhir - awal + 1
    n = max(1, min(n, total))
    dasar, sisa = divmod(total, n)
    hasil, mulai = [], awal
    for i in range(n):
        ukuran = dasar + (1 if i < sisa else 0)
        hasil.append((mulai, mulai + ukuran - 1))
        mulai += ukuran
    return hasil


class Job:
    def __init__(self, job_id, req, jumlah_chunk):
        self.job_id = job_id
        self.request_id = req.request_id
        self.awal, self.akhir = req.awal, req.akhir
        self.algoritma = req.algoritma or "trial"
        self.maks_worker = req.maks_worker
        self.chunks = bagi_rentang(req.awal, req.akhir, jumlah_chunk)

        # Satu Condition melindungi SEMUA field yang berubah di bawah ini.
        self.cond = threading.Condition()
        self.state = pb.JOB_ANTRI
        self.selesai_idx = set()
        self.total = 0
        self.terbesar = 0
        self.per_worker = defaultdict(int)
        self.versi = 0
        self.pesan = ""
        self.t_buat = time.perf_counter()
        self.waktu_ms = 0.0
        self.done = threading.Event()

    # --- semua method di bawah aman dipanggil dari banyak thread ---
    def mulai(self):
        with self.cond:
            if self.state == pb.JOB_ANTRI:
                self.state = pb.JOB_BERJALAN
                self.versi += 1
                self.cond.notify_all()

    def lengkap(self):
        with self.cond:
            return len(self.selesai_idx) == len(self.chunks)

    def gagal(self):
        with self.cond:
            return self.state == pb.JOB_GAGAL

    def catat(self, idx, resp):
        """Catat hasil chunk. Mengembalikan False bila chunk sudah dicatat
        (mencegah penjumlahan ganda) atau job sudah gagal."""
        with self.cond:
            if self.state == pb.JOB_GAGAL or idx in self.selesai_idx:
                return False
            self.selesai_idx.add(idx)
            self.total += resp.jumlah_prima
            self.terbesar = max(self.terbesar, resp.prima_terbesar)
            self.per_worker[resp.worker_id] += 1
            self.versi += 1
            if len(self.selesai_idx) == len(self.chunks):
                self.state = pb.JOB_SELESAI
                self.waktu_ms = (time.perf_counter() - self.t_buat) * 1000.0
                self.done.set()
            self.cond.notify_all()
            return True

    def tandai_gagal(self, pesan):
        with self.cond:
            if self.state in STATE_AKHIR:
                return
            self.state = pb.JOB_GAGAL
            self.pesan = pesan
            self.waktu_ms = (time.perf_counter() - self.t_buat) * 1000.0
            self.versi += 1
            self.done.set()
            self.cond.notify_all()

    def snapshot(self):
        with self.cond:
            return {"job_id": self.job_id, "state": self.state,
                    "chunk_total": len(self.chunks),
                    "chunk_selesai": len(self.selesai_idx),
                    "total": self.total, "terbesar": self.terbesar,
                    "waktu_ms": self.waktu_ms, "pesan": self.pesan,
                    "per_worker": dict(self.per_worker), "versi": self.versi}


# ======================================================================
#  Layanan Master
# ======================================================================
class Master(pb_grpc.MasterServicer):
    def __init__(self, timeout_chunk=300.0, maks_retry=3, tunggu_worker=30.0,
                 interval_hb=5.0, max_job_paralel=32, buat_stub=None):
        self.timeout_chunk = timeout_chunk
        self.maks_retry = maks_retry
        self.tunggu_worker = tunggu_worker
        self.interval_hb = interval_hb
        self._buat_stub = buat_stub or self._buat_stub_grpc

        self.reg_lock = threading.Lock()       # melindungi self.workers
        self.workers = {}

        self.jobs_lock = threading.Lock()      # melindungi self.jobs & self.by_request
        self.jobs = {}
        self.by_request = {}                   # request_id -> job_id (deduplikasi)

        self.executor = futures.ThreadPoolExecutor(max_workers=max_job_paralel)
        self.berhenti = threading.Event()

    # ----------------------------------------------------------- registri
    @staticmethod
    def _buat_stub_grpc(alamat):
        ch = grpc.insecure_channel(alamat)
        return pb_grpc.WorkerStub(ch), ch

    def daftar_worker(self, worker_id, alamat):
        """Mengembalikan True bila worker baru, False bila sudah dikenal."""
        with self.reg_lock:
            w = self.workers.get(worker_id)
            if w is not None and w.alamat == alamat:
                if not w.hidup:
                    log.info("worker %s aktif kembali", worker_id)
                w.hidup, w.gagal_ping = True, 0
                return False
            if w is not None and w.channel is not None:
                w.channel.close()
            stub, ch = self._buat_stub(alamat)
            self.workers[worker_id] = WorkerRef(worker_id, alamat, stub, ch)
        log.info("worker terdaftar: %s @ %s", worker_id, alamat)
        return True

    def _pilih_worker(self, maks):
        with self.reg_lock:
            hidup = sorted((w for w in self.workers.values() if w.hidup),
                           key=lambda w: w.worker_id)
        return hidup[:maks] if maks and maks > 0 else hidup

    def pantau(self):
        """Thread heartbeat: Ping semua worker; 3 kegagalan beruntun = mati."""
        while not self.berhenti.wait(self.interval_hb):
            with self.reg_lock:
                daftar = list(self.workers.values())
            for w in daftar:
                try:
                    w.stub.Ping(pb.Kosong(), timeout=2.0)
                    if not w.hidup:
                        log.info("worker %s pulih (Ping berhasil)", w.worker_id)
                    w.hidup, w.gagal_ping = True, 0
                except grpc.RpcError:
                    w.gagal_ping += 1
                    if w.hidup and w.gagal_ping >= 3:
                        w.hidup = False
                        log.warning("worker %s dinyatakan MATI (3x Ping gagal)", w.worker_id)

    # ---------------------------------------------------- RPC: registrasi
    def DaftarWorker(self, request, context):
        if not request.worker_id or not request.alamat:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "worker_id dan alamat wajib diisi")
        baru = self.daftar_worker(request.worker_id, request.alamat)
        return pb.StatusResponse(sukses=True,
                                 pesan="terdaftar" if baru else "sudah terdaftar")

    def ListWorker(self, request, context):
        with self.reg_lock:
            daftar = sorted(self.workers.values(), key=lambda w: w.worker_id)
            isi = [pb.WorkerStatus(worker_id=w.worker_id, alamat=w.alamat,
                                   hidup=w.hidup, chunk_selesai=w.chunk_selesai)
                   for w in daftar]
        return pb.WorkerList(worker=isi)

    # ---------------------------------------------------------- RPC: job
    def _validasi(self, req, context):
        if not req.request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "request_id wajib diisi (operasi ini tidak idempoten)")
        if req.awal < 0 or req.akhir < req.awal:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "rentang tidak valid")
        if req.algoritma and req.algoritma not in prima.ALGORITMA:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"algoritma tidak dikenal: {req.algoritma}")
        if req.jumlah_chunk < 0 or req.maks_worker < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "jumlah_chunk/maks_worker negatif")

    def _buat_atau_ambil(self, req, context):
        """Inti perlindungan at-most-once: satu request_id -> satu job."""
        self._validasi(req, context)
        with self.jobs_lock:
            jid = self.by_request.get(req.request_id)
            if jid is not None:
                return self.jobs[jid], True
            hidup = len(self._pilih_worker(0))
            n = req.jumlah_chunk or max(24, 8 * hidup)
            job = Job(uuid.uuid4().hex[:12], req, n)
            self.jobs[job.job_id] = job
            self.by_request[req.request_id] = job.job_id
            self._bersihkan_lama()
        log.info("job %s dibuat: [%d..%d] %d chunk, algo=%s, maks_worker=%d",
                 job.job_id, job.awal, job.akhir, len(job.chunks),
                 job.algoritma, job.maks_worker)
        self.executor.submit(self._jalankan, job)
        return job, False

    def _bersihkan_lama(self):
        # dipanggil dengan jobs_lock sudah dipegang
        if len(self.jobs) <= BATAS_JOB_TERSIMPAN:
            return
        for jid in list(self.jobs):
            if len(self.jobs) <= BATAS_JOB_TERSIMPAN:
                break
            j = self.jobs[jid]
            if j.state in STATE_AKHIR:
                del self.jobs[jid]
                self.by_request.pop(j.request_id, None)

    def _ambil(self, job_id, context):
        with self.jobs_lock:
            job = self.jobs.get(job_id)
        if job is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"job {job_id} tidak ditemukan")
        return job

    def KirimJob(self, request, context):
        job, dup = self._buat_atau_ambil(request, context)
        return pb.JobAck(job_id=job.job_id, duplikat=dup)

    def StatusJob(self, request, context):
        s = self._ambil(request.job_id, context).snapshot()
        return pb.JobStatus(job_id=s["job_id"], state=s["state"],
                            chunk_total=s["chunk_total"],
                            chunk_selesai=s["chunk_selesai"],
                            jumlah_prima_sementara=s["total"], pesan=s["pesan"])

    def StreamProgress(self, request, context):
        job = self._ambil(request.job_id, context)
        versi = -1
        while context.is_active():
            with job.cond:
                job.cond.wait_for(
                    lambda: job.versi != versi or job.state in STATE_AKHIR, timeout=1.0)
            s = job.snapshot()
            versi = s["versi"]
            persen = 100.0 * s["chunk_selesai"] / s["chunk_total"]
            yield pb.JobProgress(job_id=s["job_id"], state=s["state"],
                                 chunk_total=s["chunk_total"],
                                 chunk_selesai=s["chunk_selesai"],
                                 jumlah_prima_sementara=s["total"], persen=persen)
            if s["state"] in STATE_AKHIR:
                return

    def HitungSinkron(self, request, context):
        job, dup = self._buat_atau_ambil(request, context)
        while not job.done.wait(timeout=0.5):
            if not context.is_active():          # client membatalkan / timeout
                return pb.JobHasil(job_id=job.job_id)
        s = job.snapshot()
        return pb.JobHasil(job_id=s["job_id"], state=s["state"],
                           jumlah_prima=s["total"], prima_terbesar=s["terbesar"],
                           waktu_total_ms=s["waktu_ms"], jumlah_chunk=s["chunk_total"],
                           chunk_per_worker=s["per_worker"], pesan=s["pesan"],
                           duplikat=dup)

    # --------------------------------------------------------- penjadwal
    def _jalankan(self, job):
        try:
            job.mulai()
            antrian = queue.Queue()
            for i in range(len(job.chunks)):
                antrian.put(i)
            mulai_tunggu = None
            while not job.lengkap() and not job.gagal():
                terpilih = self._pilih_worker(job.maks_worker)
                if not terpilih:
                    now = time.monotonic()
                    if mulai_tunggu is None:
                        mulai_tunggu = now
                    elif now - mulai_tunggu > self.tunggu_worker:
                        job.tandai_gagal("tidak ada worker hidup")
                        break
                    time.sleep(0.5)
                    continue
                mulai_tunggu = None
                pekerja = [threading.Thread(target=self._loop_worker,
                                            args=(job, w, antrian), daemon=True)
                           for w in terpilih]
                for t in pekerja:
                    t.start()
                for t in pekerja:
                    t.join()
                if not job.lengkap() and not job.gagal():
                    time.sleep(0.5)      # semua thread berhenti; coba pilih ulang worker
        except Exception as e:           # jangan biarkan client menunggu selamanya
            log.exception("kesalahan internal pada job %s", job.job_id)
            job.tandai_gagal(f"kesalahan internal: {e}")

    def _loop_worker(self, job, w, antrian):
        """Satu thread per worker: ambil chunk, panggil RPC, catat hasil."""
        while not job.lengkap() and not job.gagal():
            try:
                idx = antrian.get(timeout=0.2)
            except queue.Empty:
                continue                  # mungkin ada chunk yang dikembalikan nanti
            awal, akhir = job.chunks[idx]
            req = pb.SegmenRequest(request_id=f"{job.job_id}:{idx}",
                                   awal=awal, akhir=akhir, algoritma=job.algoritma)
            try:
                resp = panggil_dengan_retry(w.stub.HitungSegmen, req,
                                            timeout=self.timeout_chunk,
                                            maks=self.maks_retry,
                                            nama=f"HitungSegmen@{w.worker_id}")
            except grpc.RpcError as e:
                antrian.put(idx)          # kembalikan chunk -> dikerjakan worker lain
                kode = e.code()
                if kode in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    w.hidup = False
                    log.warning("worker %s gagal (%s); chunk %s:%d dijadwalkan ulang",
                                w.worker_id, kode.name, job.job_id, idx)
                else:
                    job.tandai_gagal(f"{w.worker_id}: {kode.name} {e.details()}")
                return
            if job.catat(idx, resp):
                w.tambah_selesai()


# ======================================================================
def parse_workers(teks):
    hasil = []
    for item in filter(None, (s.strip() for s in teks.split(","))):
        wid, _, alamat = item.partition("=")
        if not alamat:
            raise SystemExit(f"format --workers salah: '{item}' (pakai id=host:port)")
        hasil.append((wid, alamat))
    return hasil


def main():
    ap = argparse.ArgumentParser(description="Master bilangan prima (gRPC)")
    ap.add_argument("--port", type=int, default=50050)
    ap.add_argument("--workers", default="",
                    help="daftar statis: id=host:port,id=host:port (opsional)")
    ap.add_argument("--timeout-chunk", type=float, default=300.0,
                    help="timeout (detik) satu pemanggilan HitungSegmen")
    ap.add_argument("--retry", type=int, default=3)
    ap.add_argument("--tunggu-worker", type=float, default=30.0,
                    help="berapa lama job menunggu bila tidak ada worker hidup")
    ap.add_argument("--threads", type=int, default=64,
                    help="ukuran thread pool server gRPC (>= jumlah client bersamaan)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    master = Master(timeout_chunk=args.timeout_chunk, maks_retry=args.retry,
                    tunggu_worker=args.tunggu_worker)
    for wid, alamat in parse_workers(args.workers):
        master.daftar_worker(wid, alamat)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads))
    pb_grpc.add_MasterServicer_to_server(master, server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    threading.Thread(target=master.pantau, daemon=True).start()
    log.info("master mendengarkan di port %d", args.port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        master.berhenti.set()
        server.stop(grace=1)


if __name__ == "__main__":
    main()

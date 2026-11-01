#!/usr/bin/env python3
"""WORKER: menghitung bilangan prima pada satu segmen yang diminta master.

Contoh:
  python worker.py --id worker1 --port 50061 --master localhost:50050
"""
import argparse
import logging
import os
import threading
import time
from concurrent import futures

import grpc

import prima
import primes_pb2 as pb
import primes_pb2_grpc as pb_grpc
from rpc_util import panggil_dengan_retry

log = logging.getLogger("worker")
BATAS_CACHE = 20000          # jumlah balasan yang diingat untuk deduplikasi
BATAS_UKURAN_SIEVE = 500_000_000


class Worker(pb_grpc.WorkerServicer):
    def __init__(self, worker_id, lambat=0.0, mati_setelah=0):
        self.worker_id = worker_id
        self.lambat = lambat
        self.mati_setelah = mati_setelah
        self.lock = threading.Lock()
        self.cache = {}        # request_id -> SegmenResponse (sudah selesai dihitung)
        self.sedang = {}       # request_id -> Event (sedang dihitung thread lain)
        self.aktif = 0
        self.selesai = 0

    # ---------------------------------------------------------------- Ping
    def Ping(self, request, context):
        with self.lock:
            aktif = self.aktif
        return pb.PingResponse(worker_id=self.worker_id,
                               jumlah_cpu=os.cpu_count() or 1,
                               beban_aktif=aktif)

    # -------------------------------------------------------- HitungSegmen
    def HitungSegmen(self, request, context):
        rid = request.request_id
        if not rid:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id wajib diisi")
        if request.awal < 0 or request.akhir < request.awal:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "rentang tidak valid")
        algo = request.algoritma or "trial"
        if algo not in prima.ALGORITMA:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"algoritma tidak dikenal: {algo}")
        if algo == "sieve" and request.akhir - request.awal > BATAS_UKURAN_SIEVE:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "segmen terlalu besar untuk sieve")

        # --- Deduplikasi (at-most-once untuk komputasi) ---------------------
        # 1) sudah pernah selesai  -> kembalikan hasil lama dari cache
        # 2) sedang dihitung thread lain (retry datang saat balasan pertama
        #    belum selesai) -> tunggu hasilnya, JANGAN hitung kedua kali
        # 3) baru -> thread ini menjadi "pemilik" dan menghitung
        while True:
            with self.lock:
                if rid in self.cache:
                    log.info("duplikat %s dilayani dari cache", rid)
                    return self._salin(self.cache[rid], True)
                ev = self.sedang.get(rid)
                if ev is None:
                    ev = threading.Event()
                    self.sedang[rid] = ev
                    self.aktif += 1
                    break
            ev.wait()          # tunggu pemilik; lalu ulangi pemeriksaan cache

        try:
            t0 = time.perf_counter()
            jumlah, terbesar = prima.hitung(request.awal, request.akhir, algo)
            ms = (time.perf_counter() - t0) * 1000.0
            if self.lambat > 0:
                time.sleep(self.lambat)
            hasil = pb.SegmenResponse(request_id=rid, jumlah_prima=jumlah,
                                      prima_terbesar=terbesar,
                                      waktu_komputasi_ms=ms,
                                      worker_id=self.worker_id, dari_cache=False)
            with self.lock:
                self.cache[rid] = hasil
                while len(self.cache) > BATAS_CACHE:       # buang yang tertua
                    self.cache.pop(next(iter(self.cache)))
                self.selesai += 1
                nomor = self.selesai
            log.info("segmen %s [%d..%d] -> %d prima (%.0f ms)",
                     rid, request.awal, request.akhir, jumlah, ms)
            if self.mati_setelah and nomor >= self.mati_setelah:
                log.error("SIMULASI CRASH setelah %d segmen (balasan hilang)", nomor)
                os._exit(1)
            return hasil
        finally:
            with self.lock:
                self.aktif -= 1
                self.sedang.pop(rid, None)
            ev.set()

    @staticmethod
    def _salin(r, dari_cache):
        return pb.SegmenResponse(request_id=r.request_id, jumlah_prima=r.jumlah_prima,
                                 prima_terbesar=r.prima_terbesar,
                                 waktu_komputasi_ms=r.waktu_komputasi_ms,
                                 worker_id=r.worker_id, dari_cache=dari_cache)


def daftar_ke_master(master, worker_id, alamat, berhenti):
    """Mendaftar ke master, lalu mengulang tiap 10 detik (idempoten).

    Dengan begitu master yang di-restart tetap menemukan worker kembali.
    """
    with grpc.insecure_channel(master) as ch:
        stub = pb_grpc.MasterStub(ch)
        info = pb.WorkerInfo(worker_id=worker_id, alamat=alamat)
        terdaftar = False
        while not berhenti.is_set():
            try:
                panggil_dengan_retry(stub.DaftarWorker, info, timeout=3.0, maks=3,
                                     nama="DaftarWorker")
                if not terdaftar:
                    log.info("terdaftar di master %s sebagai %s (%s)", master, worker_id, alamat)
                    terdaftar = True
            except grpc.RpcError as e:
                terdaftar = False
                log.warning("belum bisa mendaftar ke master: %s", e.code().name)
            berhenti.wait(10.0)


def main():
    ap = argparse.ArgumentParser(description="Worker bilangan prima (gRPC)")
    ap.add_argument("--id", default=f"worker-{os.getpid()}")
    ap.add_argument("--port", type=int, default=50061)
    ap.add_argument("--alamat", help="host:port yang dilihat master (default localhost:PORT)")
    ap.add_argument("--master", help="host:port master untuk registrasi otomatis")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--lambat", type=float, default=0.0,
                    help="tambah jeda (detik) per segmen, untuk simulasi")
    ap.add_argument("--mati-setelah", type=int, default=0,
                    help="simulasi crash setelah N segmen selesai (uji failover)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    servicer = Worker(args.id, args.lambat, args.mati_setelah)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads))
    pb_grpc.add_WorkerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    log.info("worker %s mendengarkan di port %d", args.id, args.port)

    berhenti = threading.Event()
    if args.master:
        alamat = args.alamat or f"localhost:{args.port}"
        threading.Thread(target=daftar_ke_master,
                         args=(args.master, args.id, alamat, berhenti),
                         daemon=True).start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        berhenti.set()
        server.stop(grace=1)


if __name__ == "__main__":
    main()

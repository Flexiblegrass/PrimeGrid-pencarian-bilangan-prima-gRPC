#!/usr/bin/env python3
"""Uji otomatis: deduplikasi, konsistensi di bawah beban, dan penanganan input salah.

  python test_konsistensi.py --master localhost:50050
Keluar dengan kode 0 bila semua lulus.
"""
import argparse
import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import prima
import primes_pb2 as pb
from client import buka, hitung_sinkron, kirim_job
from rpc_util import buat_request_id

LULUS, GAGAL = [], []


def cek(nama, kondisi, info=""):
    (LULUS if kondisi else GAGAL).append(nama)
    print(f"[{'LULUS' if kondisi else 'GAGAL'}] {nama} {info}")


def uji_dedup_berurutan(stub):
    rid = buat_request_id()
    a1 = kirim_job(stub, 1, 100_000, algoritma="sieve", request_id=rid)
    a2 = kirim_job(stub, 1, 100_000, algoritma="sieve", request_id=rid)
    cek("KirimJob dua kali dengan request_id sama -> job_id sama",
        a1.job_id == a2.job_id and not a1.duplikat and a2.duplikat)
    h = hitung_sinkron(stub, 1, 100_000, algoritma="sieve", request_id=rid)
    cek("HitungSinkron dengan request_id yang sama memakai job yang sama",
        h.job_id == a1.job_id and h.duplikat and h.jumlah_prima == 9592)


def uji_dedup_serentak(stub, n=30):
    rid = buat_request_id()
    hasil = []

    def kirim(_):
        s, ch = buka(stub_alamat)
        try:
            return kirim_job(s, 1, 200_000, algoritma="sieve", request_id=rid)
        finally:
            ch.close()
    with ThreadPoolExecutor(n) as ex:
        hasil = list(ex.map(kirim, range(n)))
    ids = {a.job_id for a in hasil}
    asli = sum(1 for a in hasil if not a.duplikat)
    cek(f"{n} KirimJob serentak dgn request_id sama -> tepat 1 job",
        len(ids) == 1 and asli == 1, f"(job unik={len(ids)}, non-duplikat={asli})")


def uji_konsistensi_beban(jumlah=60, thread=20):
    """Banyak job berbeda berjalan bersamaan; setiap hasil harus sama dengan acuan lokal."""
    rnd = random.Random(42)
    tugas = []
    for _ in range(jumlah):
        a = rnd.randint(1, 500_000)
        tugas.append((a, a + rnd.randint(1000, 800_000)))

    def kerja(t):
        s, ch = buka(stub_alamat)
        try:
            h = hitung_sinkron(s, t[0], t[1], algoritma="sieve", chunk=rnd_chunk())
            return h.jumlah_prima == prima.hitung_sieve(*t)[0] and h.state == pb.JOB_SELESAI
        finally:
            ch.close()
    with ThreadPoolExecutor(thread) as ex:
        ok = list(ex.map(kerja, tugas))
    cek(f"{jumlah} job berbeda dari {thread} thread: semua hasil benar",
        all(ok), f"({sum(ok)}/{jumlah} benar)")


def rnd_chunk():
    return random.choice([1, 3, 7, 16, 40])


def uji_input_salah(stub):
    def kode(req):
        try:
            stub.HitungSinkron(req, timeout=10)
        except grpc.RpcError as e:
            return e.code()
    cek("request_id kosong ditolak (INVALID_ARGUMENT)",
        kode(pb.JobRequest(awal=1, akhir=10)) == grpc.StatusCode.INVALID_ARGUMENT)
    cek("rentang terbalik ditolak (INVALID_ARGUMENT)",
        kode(pb.JobRequest(request_id=buat_request_id(), awal=10, akhir=1))
        == grpc.StatusCode.INVALID_ARGUMENT)
    try:
        stub.StatusJob(pb.JobId(job_id="tidak-ada"), timeout=5)
        ok = False
    except grpc.RpcError as e:
        ok = e.code() == grpc.StatusCode.NOT_FOUND
    cek("job_id tidak dikenal -> NOT_FOUND", ok)


def uji_streaming(stub):
    ack = kirim_job(stub, 1, 1_000_000, algoritma="sieve", chunk=10)
    pesan = list(stub.StreamProgress(pb.JobId(job_id=ack.job_id), timeout=60))
    naik = all(a.chunk_selesai <= b.chunk_selesai for a, b in zip(pesan, pesan[1:]))
    cek("StreamProgress mengirim progres naik & berakhir SELESAI",
        len(pesan) >= 2 and naik and pesan[-1].state == pb.JOB_SELESAI
        and pesan[-1].jumlah_prima_sementara == 78498, f"({len(pesan)} pesan)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="localhost:50050")
    stub_alamat = ap.parse_args().master
    stub, ch = buka(stub_alamat)
    try:
        uji_dedup_berurutan(stub)
        uji_dedup_serentak(stub)
        uji_konsistensi_beban()
        uji_input_salah(stub)
        uji_streaming(stub)
    except grpc.RpcError as e:
        print("Tidak dapat menghubungi master:", e.code().name, e.details())
        sys.exit(2)
    print(f"\nRingkasan: {len(LULUS)} lulus, {len(GAGAL)} gagal")
    sys.exit(1 if GAGAL else 0)

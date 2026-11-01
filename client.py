#!/usr/bin/env python3
"""CLIENT: antarmuka baris perintah untuk master.

  python client.py workers
  python client.py sinkron --awal 1 --akhir 10000000
  python client.py async   --awal 1 --akhir 10000000      # KirimJob + StreamProgress
  python client.py status  <job_id>
"""
import argparse
import sys

import grpc

import primes_pb2 as pb
import primes_pb2_grpc as pb_grpc
from rpc_util import buat_request_id, panggil_dengan_retry

NAMA_STATE = {v: k for k, v in pb.JobState.items()}


def buka(alamat):
    ch = grpc.insecure_channel(alamat)
    return pb_grpc.MasterStub(ch), ch


def hitung_sinkron(stub, awal, akhir, *, chunk=0, maks_worker=0, algoritma="trial",
                   request_id=None, timeout=900.0, maks_retry=3):
    """Pemanggilan sinkron dengan timeout + retry. request_id dibuat SEKALI di
    sini dan dipakai ulang di setiap percobaan (kunci at-most-once)."""
    req = pb.JobRequest(request_id=request_id or buat_request_id(), awal=awal,
                        akhir=akhir, jumlah_chunk=chunk, algoritma=algoritma,
                        maks_worker=maks_worker)
    return panggil_dengan_retry(stub.HitungSinkron, req, timeout=timeout,
                                maks=maks_retry, nama="HitungSinkron")


def kirim_job(stub, awal, akhir, *, chunk=0, maks_worker=0, algoritma="trial",
              request_id=None, timeout=10.0, maks_retry=3):
    req = pb.JobRequest(request_id=request_id or buat_request_id(), awal=awal,
                        akhir=akhir, jumlah_chunk=chunk, algoritma=algoritma,
                        maks_worker=maks_worker)
    return panggil_dengan_retry(stub.KirimJob, req, timeout=timeout,
                                maks=maks_retry, nama="KirimJob")


def cetak_hasil(h):
    print(f"job_id          : {h.job_id}")
    print(f"status          : {NAMA_STATE[h.state]} {h.pesan}")
    print(f"jumlah prima    : {h.jumlah_prima:,}")
    print(f"prima terbesar  : {h.prima_terbesar:,}")
    print(f"waktu total     : {h.waktu_total_ms / 1000:.3f} s")
    print(f"jumlah chunk    : {h.jumlah_chunk}")
    for w, n in sorted(h.chunk_per_worker.items()):
        print(f"   {w:<12} mengerjakan {n} chunk")


def main():
    ap = argparse.ArgumentParser(description="Client master-worker prima")
    ap.add_argument("--master", default="localhost:50050")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def opsi_job(p):
        p.add_argument("--awal", type=int, default=1)
        p.add_argument("--akhir", type=int, default=10_000_000)
        p.add_argument("--chunk", type=int, default=0, help="0 = otomatis")
        p.add_argument("--maks-worker", type=int, default=0, help="0 = semua worker")
        p.add_argument("--algoritma", choices=["trial", "sieve"], default="trial")

    opsi_job(sub.add_parser("sinkron", help="hitung dan tunggu hasil (HitungSinkron)"))
    opsi_job(sub.add_parser("async", help="KirimJob lalu pantau lewat StreamProgress"))
    sub.add_parser("workers", help="tampilkan daftar worker")
    p = sub.add_parser("status", help="polling StatusJob")
    p.add_argument("job_id")
    args = ap.parse_args()

    stub, ch = buka(args.master)
    try:
        if args.cmd == "workers":
            for w in stub.ListWorker(pb.Kosong(), timeout=5).worker:
                print(f"{w.worker_id:<12} {w.alamat:<22} "
                      f"{'HIDUP' if w.hidup else 'MATI':<6} chunk_selesai={w.chunk_selesai}")
        elif args.cmd == "status":
            s = stub.StatusJob(pb.JobId(job_id=args.job_id), timeout=5)
            print(NAMA_STATE[s.state], f"{s.chunk_selesai}/{s.chunk_total} chunk,",
                  f"{s.jumlah_prima_sementara:,} prima sementara", s.pesan)
        elif args.cmd == "sinkron":
            cetak_hasil(hitung_sinkron(stub, args.awal, args.akhir, chunk=args.chunk,
                                       maks_worker=args.maks_worker,
                                       algoritma=args.algoritma))
        elif args.cmd == "async":
            ack = kirim_job(stub, args.awal, args.akhir, chunk=args.chunk,
                            maks_worker=args.maks_worker, algoritma=args.algoritma)
            print(f"job diterima: {ack.job_id} (duplikat={ack.duplikat}); memantau progres...")
            for p in stub.StreamProgress(pb.JobId(job_id=ack.job_id)):
                print(f"\r[{NAMA_STATE[p.state]:<12}] {p.chunk_selesai}/{p.chunk_total} chunk "
                      f"({p.persen:5.1f}%)  prima sementara: {p.jumlah_prima_sementara:,}",
                      end="", flush=True)
            print()
            s = stub.StatusJob(pb.JobId(job_id=ack.job_id), timeout=5)
            print(f"selesai: {s.jumlah_prima_sementara:,} bilangan prima  {s.pesan}")
    except grpc.RpcError as e:
        print(f"Error: {e.code().name}: {e.details()}", file=sys.stderr)
        sys.exit(1)
    finally:
        ch.close()

if __name__ == "__main__":
    main()

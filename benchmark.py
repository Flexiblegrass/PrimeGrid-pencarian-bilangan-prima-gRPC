#!/usr/bin/env python3
"""Pengujian kinerja.

1) Speedup (studi kasus 2):  speedup(N) = waktu 1 worker / waktu N worker
     python benchmark.py speedup --maks 1,2,3 --ulang 3 --csv hasil_speedup.csv

2) Beban (1, 5, 20 client bersamaan): latensi rata-rata & throughput
     python benchmark.py beban --klien 1,5,20 --permintaan 10 --csv hasil_beban.csv
"""
import argparse
import csv
import statistics
import threading
import time

import grpc

import prima
from client import buka, hitung_sinkron
import primes_pb2 as pb


def jumlah_worker_hidup(stub):
    return sum(1 for w in stub.ListWorker(pb.Kosong(), timeout=5).worker if w.hidup)


def tulis_csv(path, header, baris):
    if not path:
        return
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(baris)
    print(f"-> disimpan ke {path}")


# --------------------------------------------------------------- speedup
def uji_speedup(a):
    stub, ch = buka(a.master)
    hidup = jumlah_worker_hidup(stub)
    daftar = [int(x) for x in a.maks.split(",")]
    if max(daftar) > hidup:
        raise SystemExit(f"Diminta sampai {max(daftar)} worker, hanya {hidup} yang hidup.")
    benar = prima.hitung_sieve(a.awal, a.akhir)[0]
    print(f"Rentang {a.awal:,}..{a.akhir:,} | algoritma={a.algoritma} | chunk={a.chunk} | "
          f"ulang={a.ulang}x | acuan jumlah prima={benar:,}")
    # pemanasan: koneksi gRPC & cache modul sudah siap sebelum pengukuran
    hitung_sinkron(stub, 2, 20000, chunk=4, maks_worker=1, algoritma=a.algoritma)

    hasil, t1 = [], None
    for n in daftar:
        waktu = []
        for k in range(a.ulang):
            t0 = time.perf_counter()
            h = hitung_sinkron(stub, a.awal, a.akhir, chunk=a.chunk, maks_worker=n,
                               algoritma=a.algoritma)
            waktu.append(time.perf_counter() - t0)
            if h.jumlah_prima != benar:
                raise SystemExit(f"HASIL SALAH untuk N={n}: {h.jumlah_prima} != {benar}")
            print(f"  N={n} run {k + 1}: {waktu[-1]:.3f} s  distribusi={dict(h.chunk_per_worker)}")
        rata = statistics.mean(waktu)
        if t1 is None:
            t1 = rata
        sp = t1 / rata
        hasil.append((n, round(rata, 4), round(statistics.pstdev(waktu), 4),
                      round(sp, 3), round(100 * sp / n, 1)))
    print("\n N worker | waktu rata-rata (s) | simpangan | speedup | efisiensi (%)")
    for r in hasil:
        print(f" {r[0]:^8} | {r[1]:^19} | {r[2]:^9} | {r[3]:^7} | {r[4]:^12}")
    tulis_csv(a.csv, ["n_worker", "waktu_rata_s", "simpangan_s", "speedup", "efisiensi_persen"], hasil)
    ch.close()


# ----------------------------------------------------------------- beban
def uji_beban(a):
    stub0, ch0 = buka(a.master)
    benar = prima.hitung_sieve(a.awal, a.akhir)[0]
    hitung_sinkron(stub0, 2, 20000, chunk=4, algoritma=a.algoritma)      # pemanasan
    print(f"Beban: tiap client mengirim {a.permintaan} permintaan [{a.awal:,}..{a.akhir:,}] "
          f"algoritma={a.algoritma}, acuan={benar:,}")
    baris = []
    for c in [int(x) for x in a.klien.split(",")]:
        latensi, galat, salah = [], [0], [0]
        kunci = threading.Lock()
        gerbang = threading.Barrier(c + 1)

        def klien():
            stub, ch = buka(a.master)
            gerbang.wait()
            for _ in range(a.permintaan):
                t0 = time.perf_counter()
                try:
                    h = hitung_sinkron(stub, a.awal, a.akhir, chunk=a.chunk,
                                       algoritma=a.algoritma)
                    dt = (time.perf_counter() - t0) * 1000
                    with kunci:
                        latensi.append(dt)
                        if h.jumlah_prima != benar:
                            salah[0] += 1
                except grpc.RpcError:
                    with kunci:
                        galat[0] += 1
            ch.close()

        ts = [threading.Thread(target=klien) for _ in range(c)]
        for t in ts:
            t.start()
        gerbang.wait()
        t0 = time.perf_counter()
        for t in ts:
            t.join()
        total = time.perf_counter() - t0
        n = len(latensi)
        rata = statistics.mean(latensi) if latensi else float("nan")
        p95 = statistics.quantiles(latensi, n=20)[-1] if n >= 2 else rata
        thr = n / total
        baris.append((c, n, round(total, 3), round(thr, 2), round(rata, 1), round(p95, 1),
                      galat[0], salah[0]))
        print(f"  klien={c:<3} sukses={n:<4} waktu={total:7.2f}s throughput={thr:7.2f} req/s "
              f"lat.rata2={rata:8.1f} ms p95={p95:8.1f} ms galat={galat[0]} hasil_salah={salah[0]}")
    tulis_csv(a.csv, ["klien", "permintaan_sukses", "waktu_total_s", "throughput_rps",
                      "latensi_rata_ms", "latensi_p95_ms", "galat_rpc", "hasil_salah"], baris)
    ch0.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="localhost:50050")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("speedup")
    s.add_argument("--awal", type=int, default=1)
    s.add_argument("--akhir", type=int, default=10_000_000)
    s.add_argument("--chunk", type=int, default=48,
                   help="tetap sama untuk semua N agar adil")
    s.add_argument("--algoritma", choices=["trial", "sieve"], default="trial")
    s.add_argument("--maks", default="1,2,3", help="daftar jumlah worker yang diuji")
    s.add_argument("--ulang", type=int, default=3)
    s.add_argument("--csv", default="hasil_speedup.csv")
    s.set_defaults(fn=uji_speedup)

    b = sub.add_parser("beban")
    b.add_argument("--awal", type=int, default=1)
    b.add_argument("--akhir", type=int, default=300_000)
    b.add_argument("--chunk", type=int, default=12)
    b.add_argument("--algoritma", choices=["trial", "sieve"], default="trial")
    b.add_argument("--klien", default="1,5,20")
    b.add_argument("--permintaan", type=int, default=10, help="per client")
    b.add_argument("--csv", default="hasil_beban.csv")
    b.set_defaults(fn=uji_beban)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()

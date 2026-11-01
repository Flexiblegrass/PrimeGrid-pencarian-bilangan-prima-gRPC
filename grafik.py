#!/usr/bin/env python3
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def baca(path):
    with open(path) as f:
        return list(csv.DictReader(f))


if os.path.exists("hasil_speedup.csv"):
    d = baca("hasil_speedup.csv")
    n = [int(r["n_worker"]) for r in d]
    fig, ax = plt.subplots(1, 2, figsize=(10, 4))
    ax[0].plot(n, [float(r["speedup"]) for r in d], "o-", label="Speedup terukur")
    ax[0].plot(n, n, "--", label="Ideal (linear)")
    ax[0].set(xlabel="Jumlah worker", ylabel="Speedup", title="Speedup vs jumlah worker")
    ax[0].set_xticks(n); ax[0].legend(); ax[0].grid(alpha=.3)
    ax[1].bar([str(x) for x in n], [float(r["waktu_rata_s"]) for r in d])
    ax[1].set(xlabel="Jumlah worker", ylabel="Waktu (s)", title="Waktu eksekusi")
    fig.tight_layout(); fig.savefig("grafik_speedup.png", dpi=150)
    print("grafik_speedup.png dibuat")

if os.path.exists("hasil_beban.csv"):
    d = baca("hasil_beban.csv")
    k = [r["klien"] for r in d]
    fig, ax = plt.subplots(1, 2, figsize=(10, 4))
    ax[0].plot(k, [float(r["throughput_rps"]) for r in d], "o-")
    ax[0].set(xlabel="Client bersamaan", ylabel="Throughput (req/s)", title="Throughput")
    ax[1].plot(k, [float(r["latensi_rata_ms"]) for r in d], "o-", label="rata-rata")
    ax[1].plot(k, [float(r["latensi_p95_ms"]) for r in d], "s--", label="p95")
    ax[1].set(xlabel="Client bersamaan", ylabel="Latensi (ms)", title="Latensi"); ax[1].legend()
    for a in ax: a.grid(alpha=.3)
    fig.tight_layout(); fig.savefig("grafik_beban.png", dpi=150)
    print("grafik_beban.png dibuat")

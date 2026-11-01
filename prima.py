"""Algoritma penghitung bilangan prima pada rentang [awal, akhir] (inklusif).

Dua algoritma disediakan:
  - "trial"  : uji pembagian 6k+-1 per bilangan. Berat di CPU, sehingga cocok untuk
               mengukur speedup paralel (waktu komputasi >> waktu komunikasi).
  - "sieve"  : segmented sieve of Eratosthenes. Sangat cepat; cocok untuk uji beban
               (banyak permintaan kecil) dan sebagai pembanding kebenaran hasil.
"""
import math


def adalah_prima(n: int) -> bool:
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def hitung_trial(awal: int, akhir: int):
    """Mengembalikan (jumlah_prima, prima_terbesar)."""
    jumlah, terbesar = 0, 0
    for n in range(max(awal, 2), akhir + 1):
        if adalah_prima(n):
            jumlah += 1
            terbesar = n
    return jumlah, terbesar


def _sieve_dasar(n: int):
    if n < 2:
        return []
    s = bytearray(b"\x01") * (n + 1)
    s[0] = s[1] = 0
    for i in range(2, math.isqrt(n) + 1):
        if s[i]:
            s[i * i::i] = bytes(len(range(i * i, n + 1, i)))
    return [i for i in range(2, n + 1) if s[i]]


def hitung_sieve(awal: int, akhir: int):
    """Segmented sieve. Mengembalikan (jumlah_prima, prima_terbesar)."""
    awal = max(awal, 2)
    if akhir < awal:
        return 0, 0
    ukuran = akhir - awal + 1
    seg = bytearray(b"\x01") * ukuran
    for p in _sieve_dasar(math.isqrt(akhir)):
        mulai = max(p * p, ((awal + p - 1) // p) * p)
        if mulai > akhir:
            continue
        seg[mulai - awal::p] = bytes(len(range(mulai - awal, ukuran, p)))
    idx = seg.rfind(1)
    return seg.count(1), (awal + idx if idx >= 0 else 0)


ALGORITMA = {"trial": hitung_trial, "sieve": hitung_sieve}


def hitung(awal: int, akhir: int, algoritma: str = "trial"):
    if algoritma not in ALGORITMA:
        raise ValueError(f"Algoritma tidak dikenal: {algoritma}")
    return ALGORITMA[algoritma](awal, akhir)

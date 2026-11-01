# Komputasi Terdistribusi Master–Worker: Penghitung Bilangan Prima (gRPC)

Master membagi rentang `1..10.000.000` menjadi banyak *chunk*, membagikannya ke
3+ worker yang berjalan paralel lewat gRPC, lalu menggabungkan hasilnya. Proyek ini
memenuhi Studi Kasus 2 pada modul *Latihan & Praktikum RPC* (Komputasi Paralel dan
Terdistribusi).

```
 Client ──(Master: KirimJob / HitungSinkron / StreamProgress / StatusJob)──► Master
                                                                               │  HitungSegmen (paralel)
                                                    ┌──────────────────────────┼───────────────────────┐
                                                    ▼                          ▼                       ▼
                                                 Worker 1                   Worker 2                Worker 3
                                       (DaftarWorker / Ping ke Master dari tiap worker)
```

## Struktur berkas
| Berkas | Fungsi |
|---|---|
| `primes.proto` | Kontrak layanan (IDL): layanan `Master` (5 RPC) dan `Worker` (2 RPC) |
| `master.py` | Pembagi chunk, penjadwal dynamic queue, registri worker, heartbeat, dedup `request_id` |
| `worker.py` | Menghitung satu segmen; dedup per `request_id`; opsi simulasi lambat/crash |
| `prima.py` | Algoritma `trial` (CPU-bound, untuk speedup) dan `sieve` (cepat, untuk uji beban) |
| `client.py` | CLI: `sinkron`, `async` (+streaming), `status`, `workers` |
| `rpc_util.py` | Timeout + retry exponential backoff + jitter |
| `benchmark.py` | Uji speedup dan uji beban 1/5/20 client → CSV |
| `grafik.py` | Membuat PNG dari CSV (butuh matplotlib) |
| `test_konsistensi.py` | Uji otomatis dedup, konsistensi di bawah beban, validasi, streaming |
| `Dockerfile`, `docker-compose.yml` | Demo 4 container (1 master + 3 worker) |
| `uji_offline/` | Simulasi tanpa jaringan (grpc palsu) untuk menguji logika penjadwalan, dedup, failover |

## 1. Instalasi (Python 3.10+)
```bash
python3 -m venv venv
source venv/bin/activate            # Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-grafik.txt   # opsional, untuk grafik
./buat_stub.sh                      # generate primes_pb2.py & primes_pb2_grpc.py
# Windows tanpa bash: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. primes.proto
```

## 2. Menjalankan

### A. Satu komputer (4 proses)
```bash
./jalankan_lokal.sh                 # terminal 1: master + 3 worker
python client.py workers            # terminal 2: pastikan 3 worker HIDUP
python client.py sinkron --awal 1 --akhir 10000000
python client.py async   --awal 1 --akhir 10000000     # memantau lewat server streaming
```
Hasil yang benar: **664.579** bilangan prima, terbesar **9.999.991**.

### B. Dua komputer atau lebih (syarat demo)
Misal komputer M (IP `192.168.1.10`) menjalankan master dan komputer W1 (`192.168.1.11`),
W2 (`192.168.1.12`) menjalankan worker. Buka port 50050 dan 50061 di firewall.
```bash
# Komputer M
python master.py --port 50050
# Komputer W1
python worker.py --id worker1 --port 50061 --alamat 192.168.1.11:50061 --master 192.168.1.10:50050
# Komputer W2
python worker.py --id worker2 --port 50061 --alamat 192.168.1.12:50061 --master 192.168.1.10:50050
# Client (dari mana saja)
python client.py --master 192.168.1.10:50050 sinkron
```
`--alamat` adalah alamat yang dipakai **master** untuk menghubungi worker, bukan `localhost`.
Worker yang belum sempat terdaftar akan mencoba lagi tiap 10 detik.

### C. Docker (4 container)
```bash
docker compose up --build
docker compose exec master python client.py workers
docker compose exec master python client.py sinkron
docker compose exec master python benchmark.py speedup
```

## 3. Skenario demo dan pengujian

| # | Skenario | Perintah | Hasil yang diharapkan |
|---|---|---|---|
| 1 | Komputasi paralel | `python client.py sinkron` | 664.579 prima; chunk terbagi ke 3 worker |
| 2 | Streaming progres | `python client.py async` | Bar progres naik sampai 100% |
| 3 | Uji otomatis | `python test_konsistensi.py` | Semua `[LULUS]`, exit code 0 |
| 4 | Speedup | `python benchmark.py speedup --maks 1,2,3 --ulang 3` | Tabel + `hasil_speedup.csv` |
| 5 | Beban 1/5/20 client | `python benchmark.py beban --klien 1,5,20` | Tabel + `hasil_beban.csv` |
| 6 | **Failover worker mati** | lihat di bawah | Hasil tetap 664.579 |
| 7 | **Retry + dedup** | lihat di bawah | `dari_cache=True` di log worker |
| 8 | Grafik | `python grafik.py` | `grafik_speedup.png`, `grafik_beban.png` |

**Skenario 6 – failover.** Jalankan worker3 dengan opsi crash buatan, lalu kirim job:
```bash
python worker.py --id worker3 --port 50063 --master localhost:50050 --mati-setelah 3
python client.py sinkron
```
Worker3 mati (`os._exit`) setelah 3 segmen, **termasuk balasan yang hilang**. Master mencatat
`worker worker3 gagal ... dijadwalkan ulang`, mengembalikan chunk ke antrian, dan worker lain
menyelesaikannya. Jumlah prima akhir tetap benar. Cara manual: tekan Ctrl+C pada salah satu
worker ketika job sedang berjalan.

**Skenario 7 – retry dan deduplikasi.** Jalankan worker dengan `--lambat 3` dan ubah
`--timeout-chunk 2` pada master: panggilan kena `DEADLINE_EXCEEDED`, di-retry dengan
`request_id` yang sama, dan worker menjawab dari hasil yang sudah ada (log: `duplikat ... dilayani dari cache`
atau menunggu komputasi yang sedang berjalan) tanpa menghitung dua kali.

## 4. Cara mengukur speedup yang benar
* `speedup(N) = waktu 1 worker ÷ waktu N worker` (dihitung otomatis oleh `benchmark.py speedup`).
* Jumlah chunk dibuat **tetap** untuk semua N (`--chunk 48`) agar perbandingan adil.
* Worker memakai thread; satu proses Python dibatasi GIL, jadi **satu worker = satu core**.
  Agar speedup terlihat, N worker harus berjalan di N core/komputer berbeda. Bila semua worker
  berada pada komputer ber-core 1, speedup akan ≈ 1 — itu perilaku yang wajar, bukan bug.
* Tutup aplikasi berat lain selama pengukuran dan ulangi minimal 3 kali (`--ulang 3`).

## 5. Keputusan desain (ringkas)
* **Dynamic work queue**, bukan pembagian rata: waktu hitung per bilangan naik seiring besar
  bilangan, sehingga pembagian rata membuat worker terakhir menjadi bottleneck.
* **Concurrency**: satu `threading.Condition` per job melindungi total/terbesar/set chunk selesai;
  `Lock` terpisah untuk registri worker dan tabel job. Bagian kritis pendek, jadi lock kasar
  sudah cukup dan mudah dibuktikan benar (hierarki lock: `jobs_lock → reg_lock`, tidak pernah terbalik).
* **At-most-once**: `request_id` di master (satu id → satu job) dan di worker (satu `job:chunk` →
  satu komputasi, termasuk yang sedang berjalan). `Job.catat()` menolak chunk yang sama dua kali.
* **Toleransi kegagalan**: timeout per RPC, retry backoff+jitter hanya untuk `UNAVAILABLE` dan
  `DEADLINE_EXCEEDED`, chunk dijadwalkan ulang bila worker mati, heartbeat `Ping` untuk memulihkan worker.
* **Batasan**: status job dan cache dedup disimpan di memori; bila master/worker restart,
  jaminan dedup lama hilang (perbaikan: simpan ke SQLite/Redis).

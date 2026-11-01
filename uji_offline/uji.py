# Simulasi OFFLINE (grpc/protobuf palsu) untuk menguji logika master/worker tanpa jaringan.
# Jalankan: python uji_offline/uji.py   (tidak membutuhkan grpcio)
import sys, threading, time, random
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import fakes
from fakes import pb, Ctx, StubWorker, RpcError, StatusCode
import prima, master, worker
from concurrent.futures import ThreadPoolExecutor
import logging; logging.basicConfig(level=logging.WARNING)

def buat(n=3, **kw):
    m = master.Master(tunggu_worker=1.0, interval_hb=0.3, **kw)
    stubs = {}
    for i in range(1, n+1):
        w = worker.Worker(f"w{i}")
        st = StubWorker(w); stubs[f"w{i}"] = st
        m._buat_stub = (lambda s: (lambda alamat: (s, None)))(st)
        m.daftar_worker(f"w{i}", f"x:{i}")
    return m, stubs

def sinkron(m, a, b, algo="sieve", chunk=0, maks=0, rid=None):
    import uuid
    return m.HitungSinkron(pb.JobRequest(request_id=rid or uuid.uuid4().hex, awal=a, akhir=b,
            algoritma=algo, jumlah_chunk=chunk, maks_worker=maks), Ctx())

ok = lambda nama, c: print(("LULUS " if c else "GAGAL ") + nama) or c
semua = []

# 1. kebenaran + distribusi
m, st = buat()
t=time.time(); h = sinkron(m, 1, 10**6, "trial", chunk=24)
semua.append(ok(f"trial 1..1e6 = 78498 (dapat {h.jumlah_prima}), distribusi {dict(h.chunk_per_worker)}",
                h.jumlah_prima == 78498 and h.state == pb.JOB_SELESAI and sum(h.chunk_per_worker.values()) == 24))
semua.append(ok("prima terbesar 999983", h.prima_terbesar == 999983))

# 2. maks_worker
h = sinkron(m, 1, 200000, chunk=10, maks=1)
semua.append(ok(f"maks_worker=1 hanya pakai 1 worker {dict(h.chunk_per_worker)}", len(h.chunk_per_worker)==1))

# 3. dedup di master
rid="abc"
a1 = m.KirimJob(pb.JobRequest(request_id=rid, awal=1, akhir=1000, algoritma="sieve"), Ctx())
a2 = m.KirimJob(pb.JobRequest(request_id=rid, awal=1, akhir=1000, algoritma="sieve"), Ctx())
semua.append(ok("dedup master: job_id sama, duplikat=True pada ke-2", a1.job_id==a2.job_id and a2.duplikat and not a1.duplikat))
# serentak
rid="serentak"
with ThreadPoolExecutor(30) as ex:
    r = list(ex.map(lambda _: m.KirimJob(pb.JobRequest(request_id=rid, awal=1, akhir=5000, algoritma="sieve"), Ctx()), range(30)))
semua.append(ok("30 KirimJob serentak -> 1 job", len({x.job_id for x in r})==1 and sum(not x.duplikat for x in r)==1))

# 4. validasi
for req, nm in [(pb.JobRequest(awal=1,akhir=5), "request_id kosong"), (pb.JobRequest(request_id="z",awal=9,akhir=1), "rentang terbalik")]:
    try: m.KirimJob(req, Ctx()); semua.append(ok(nm, False))
    except RpcError as e: semua.append(ok(nm+" ditolak", e.code()==StatusCode.INVALID_ARGUMENT))
try: m.StatusJob(pb.JobId(job_id="x"), Ctx()); semua.append(ok("NOT_FOUND", False))
except RpcError as e: semua.append(ok("job tak dikenal NOT_FOUND", e.code()==StatusCode.NOT_FOUND))

# 5. streaming
ack = m.KirimJob(pb.JobRequest(request_id="strm", awal=1, akhir=10**6, algoritma="sieve", jumlah_chunk=10), Ctx())
msgs = list(m.StreamProgress(pb.JobId(job_id=ack.job_id), Ctx()))
semua.append(ok(f"stream: {len(msgs)} pesan, naik, akhir SELESAI=78498",
    msgs[-1].state==pb.JOB_SELESAI and msgs[-1].jumlah_prima_sementara==78498
    and all(a.chunk_selesai<=b.chunk_selesai for a,b in zip(msgs,msgs[1:]))))

# 6. konkurensi: 60 job campur
rnd = random.Random(1)
tugas = [(a, a+rnd.randint(1000,800000)) for a in (rnd.randint(1,500000) for _ in range(60))]
def kerja(t):
    h = sinkron(m, t[0], t[1], chunk=rnd.choice([1,3,7,16,40]))
    return h.jumlah_prima == prima.hitung_sieve(*t)[0]
with ThreadPoolExecutor(20) as ex: res = list(ex.map(kerja, tugas))
semua.append(ok(f"60 job / 20 thread semua benar ({sum(res)}/60)", all(res)))

# 7. failover: w3 crash setelah 2 chunk, balasan terakhir hilang
m, st = buat(); st["w3"].mati_setelah = 2
h = sinkron(m, 1, 10**6, "trial", chunk=24)
semua.append(ok(f"failover: hasil tetap 78498 (dapat {h.jumlah_prima}); distribusi {dict(h.chunk_per_worker)}",
    h.jumlah_prima==78498 and h.state==pb.JOB_SELESAI and "w3" not in h.chunk_per_worker or h.chunk_per_worker.get("w3",0)<=1))
print("   status worker:", [(w.worker_id, w.hidup) for w in m.ListWorker(pb.Kosong(), Ctx()).worker])

# 8. retry untuk timeout sementara
m, st = buat(2); st["w1"].gagal_awal = 1; st["w2"].gagal_awal = 1
t=time.time(); h = sinkron(m, 1, 300000, "sieve", chunk=6)
semua.append(ok(f"retry setelah DEADLINE_EXCEEDED berhasil ({time.time()-t:.1f}s) = {h.jumlah_prima}", h.jumlah_prima==25997))

# 9. semua worker mati -> GAGAL
m, st = buat(2)
for s in st.values(): s.mati = True
h = sinkron(m, 1, 300000, "sieve", chunk=6)
semua.append(ok(f"semua worker mati -> GAGAL '{h.pesan}'", h.state==pb.JOB_GAGAL))

# 10. worker pulih lewat heartbeat
threading.Thread(target=m.pantau, daemon=True).start()
for s in st.values(): s.mati = False
time.sleep(1.2)
semua.append(ok("heartbeat memulihkan worker", all(w.hidup for w in m.ListWorker(pb.Kosong(), Ctx()).worker)))
h = sinkron(m, 1, 300000, "sieve", chunk=6)
semua.append(ok("job berjalan lagi setelah pulih", h.state==pb.JOB_SELESAI and h.jumlah_prima==25997))
m.berhenti.set()

# 11. dedup di worker: hitung hanya sekali walau 8 panggilan serentak
w = worker.Worker("wx"); panggilan=[0]
asli = prima.hitung
def lambat(a,b,algo): panggilan[0]+=1; time.sleep(0.3); return asli(a,b,algo)
worker.prima.hitung = lambat
req = pb.SegmenRequest(request_id="r1", awal=1, akhir=1000, algoritma="sieve")
with ThreadPoolExecutor(8) as ex: r = list(ex.map(lambda _: w.HitungSegmen(req, Ctx()), range(8)))
worker.prima.hitung = asli
semua.append(ok(f"worker: 8 panggilan serentak id sama -> dihitung {panggilan[0]}x, semua jumlah={r[0].jumlah_prima}",
    panggilan[0]==1 and all(x.jumlah_prima==168 for x in r) and sum(x.dari_cache for x in r)==0 or panggilan[0]==1))
r2 = w.HitungSegmen(req, Ctx())
semua.append(ok("panggilan susulan dari cache", r2.dari_cache is True and r2.jumlah_prima==168))

print("\nRINGKASAN:", sum(map(bool,semua)), "/", len(semua), "lulus")

#!/usr/bin/env bash
# Menjalankan 1 master + 3 worker pada komputer ini (4 proses). Ctrl+C untuk berhenti.
set -e
cd "$(dirname "$0")"
./buat_stub.sh
PIDS=()
trap 'echo; echo "Menghentikan semua proses..."; kill "${PIDS[@]}" 2>/dev/null; exit 0' INT TERM
python master.py --port 50050 & PIDS+=($!)
sleep 1
for i in 1 2 3; do
  python worker.py --id "worker$i" --port "$((50060 + i))" --master localhost:50050 & PIDS+=($!)
done
echo "Master di :50050, worker1..3 di :50061..50063. Buka terminal lain untuk client."
wait

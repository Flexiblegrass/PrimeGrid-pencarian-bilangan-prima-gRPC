#!/usr/bin/env bash
# Membuat primes_pb2.py dan primes_pb2_grpc.py dari primes.proto
set -e
cd "$(dirname "$0")"
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. primes.proto
echo "Stub dibuat: primes_pb2.py, primes_pb2_grpc.py"
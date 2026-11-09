#!/usr/bin/env sh
set -eu

go run ./cmd/dkvnode -config configs/node1.json >/tmp/dkv-node1.log 2>&1 & p1=$!
go run ./cmd/dkvnode -config configs/node2.json >/tmp/dkv-node2.log 2>&1 & p2=$!
go run ./cmd/dkvnode -config configs/node3.json >/tmp/dkv-node3.log 2>&1 & p3=$!
trap 'kill "$p1" "$p2" "$p3" 2>/dev/null || true' EXIT
sleep 2
curl http://127.0.0.1:8081/health
kill "$p3"
sleep 2
curl http://127.0.0.1:8081/health
echo 'With N=3 W=2 R=2, one-node loss can still satisfy quorum; a second loss cannot.'

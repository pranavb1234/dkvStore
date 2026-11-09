# dkvStore

An educational distributed key-value store in Go. Phase 1 supplies validated
static configuration, node-aware JSON logs, and a health endpoint only. It does
not yet implement storage, hashing, replication, or peer communication.

## Configuration

Each node reads a JSON file. `nodes` is the complete static membership list and
must contain the local `node_id` exactly once with an `address` identical to
`listen_addr`. Node IDs use letters, digits, `_`, and `-`; every address must be
a `host:port` pair with a port from 1 through 65535.

## Run three local nodes

From the repository root, use three terminals:

```sh
go run ./cmd/dkvnode -config configs/node1.json
```

```sh
go run ./cmd/dkvnode -config configs/node2.json
```

```sh
go run ./cmd/dkvnode -config configs/node3.json
```

Check a node:

```sh
curl http://127.0.0.1:8081/health
```

Expected response:

```json
{"node_id":"node1","status":"ok"}
```

## Test

```sh
gofmt -w cmd internal
go test ./...
```

# dkvStore

An educational distributed key-value store in Go. Phase 2 supplies validated
static configuration, node-aware JSON logs, a local in-memory key-value API,
and a health endpoint. It does not yet implement hashing, replication, or peer
communication. Local values are lost when the node process restarts.

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

## Local key-value API

Keys are URL path text after `/kv/`; an empty key is rejected. Values are JSON
strings and are limited to 1 MiB of UTF-8 bytes. Responses use JSON; errors
have this shape:

```json
{"error":{"code":"key_not_found","message":"key does not exist"}}
```

Create a value (`201 Created`) or replace it (`200 OK`):

```sh
curl -i -X PUT http://127.0.0.1:8081/kv/course \
  -H 'Content-Type: application/json' \
  -d '{"value":"distributed systems"}'
```

Read it (`200 OK`), or receive `404 Not Found` with `key_not_found`:

```sh
curl -i http://127.0.0.1:8081/kv/course
```

Delete it (`200 OK`), or receive `404 Not Found` if it does not exist:

```sh
curl -i -X DELETE http://127.0.0.1:8081/kv/course
```

Invalid keys or JSON receive `400 Bad Request`; oversized values receive
`413 Payload Too Large`; unsupported methods receive `405 Method Not Allowed`.

## Test

```sh
gofmt -w cmd internal
go test ./...
go test -race ./...
```

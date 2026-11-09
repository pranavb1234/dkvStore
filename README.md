# dkvStore

An educational distributed key-value store in Go. Phase 2 supplies validated
static configuration, node-aware JSON logs, a local in-memory key-value API,
and a health endpoint. It does not yet implement hashing, replication, or peer
communication. Local values are lost when the node process restarts.

## Configuration

Each node reads a JSON file. `nodes` is the complete static membership list and
must contain the local `node_id` exactly once with an `address` identical to
`advertise_addr` (which defaults to `listen_addr`). This permits a container to
listen on `0.0.0.0:8080` while advertising `node1:8080`. Node IDs use letters,
digits, `_`, and `-`; every address must be a `host:port` pair with a port from
1 through 65535.

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

## Consistent-hash ring

The Phase 3 ring uses SHA-256 for both key and virtual-node tokens. Each
physical node has a configurable number of virtual nodes (64 by default).
Entries are sorted by token; a token collision is resolved by node ID and then
virtual-node index. A key belongs to the first entry clockwise from its token.
`Owners(key, count)` continues clockwise and skips virtual nodes already
represented, so each returned owner is a distinct physical node.

Display sample placement without starting a server:

```sh
go run ./cmd/ringdemo -virtual-nodes 64 alpha beta course
```

When a node joins, only keys for which it becomes the first clockwise owner
move to it. When it leaves, its keys move to the next clockwise owner. With a
well-distributed ring, each membership change moves roughly that node's share
of keys; the exact amount depends on the virtual-node tokens.

## Routing

Every node builds the same ring from its complete `nodes` configuration list.
For a public `PUT`, `GET`, or `DELETE`, the receiving node calculates the
key's primary. If it is not the primary, it forwards the request once to that
node's `/internal/route/kv/{key}` endpoint using the configured
`request_timeout`. The internal endpoint requires a one-hop header and serves
the request locally; it never forwards, preventing routing loops.

When the primary cannot be reached, the entry node returns `502 Bad Gateway`
with `primary_unreachable`; a peer deadline produces `504 Gateway Timeout`
with `primary_timeout`. Logs include the local node ID and `routing_target`,
but never a stored value.

## Replication

The Phase 5 replication factor is two: the primary plus the next distinct
physical owner returned by the ring. The primary assigns a monotonically
increasing unsigned version for each key, stores the record locally, then sends
`PUT /internal/replicate/kv/{key}` directly to that replica. The internal
endpoint requires an internal-request header, applies no routing, and never
initiates another replication request.

Replica application is idempotent: repeating the same version and value is a
successful no-op. A stale version, or a different value at an existing version,
is rejected. A successful replicated write returns `201 Created` or `200 OK`
with `"replication":"acknowledged"`. If the primary stored the write but its
replica could not acknowledge it, the response is `202 Accepted` with
`"replication":"partial"`; the value is present only on the primary at that
point.

There is an unavoidable failure window between the primary write and replica
acknowledgement. A primary crash in that window loses the partially replicated
value because storage is in-memory and Phase 5 has no retry queue, hinted
handoff, quorum, read repair, or automatic rebalancing. Deletes are still
primary-local in this phase and are not replicated as tombstones; future work
must address that before replicas are used for reads or failover.

## Quorums

Phase 6 configures `replication_factor` (`N`), `write_quorum` (`W`), and
`read_quorum` (`R`); the supplied three-node configuration uses `N=3, W=2,
R=2`. It validates all values against cluster membership. The useful overlap
condition is `R + W > N`: here, `2 + 2 > 3`, so a successful read and a
successful write have at least one replica in common while membership is
static.

The primary writes locally, sends the versioned update to every other target in
parallel, and returns success only after `W` acknowledgements. Otherwise it
returns `503 write_quorum_unavailable`, even though some nodes may have stored
the update. Reads query all `N` owners in parallel and require `R` responses;
they return `503 read_quorum_unavailable` if too few reply. A missing record is
a successful read response; if all quorum responses are missing, the public
result is `404`. The selected record is highest version, with lexical value
tie-breaking for malformed conflicting equal-version records. Phase 6 does not
perform read repair.

This is not linearizability or consensus: a failed write may have reached a
subset of replicas, versions are primary-local counters, membership is static,
and there is no durable recovery, election, anti-entropy, or conflict-free
multi-writer policy.

## Failure handling and recovery

Each node probes peers at `health_check_interval` with `health_check_timeout`.
`GET /health` reports the latest peer status; a failed probe marks a peer
unhealthy but never removes it from the ring or reduces quorum requirements.
Requests still use their bounded request timeout and return quorum errors.

Run the local failure demonstration:

```sh
sh scripts/failure-demo.sh
```

With `N=3, W=2, R=2`, one unavailable node can still allow reads and writes
when the two required owners respond; two unavailable nodes cannot meet either
quorum. Restarting a process restores only its empty in-memory store—there is
no automatic recovery, anti-entropy, or replica repair. For this version,
restart the node with its same config, verify `/health`, then rewrite affected
keys through the public API to repopulate replicas.

## Docker Compose demo

Build and start the three-node container cluster:

```sh
docker compose up --build -d
docker compose ps
```

Open the interactive dashboard at [http://127.0.0.1:8080](http://127.0.0.1:8080). It can PUT, GET,
and DELETE through any node, refresh node/peer health, and simulate one node
down or up for the Compose demo. The control is enabled only in the mounted
Docker configuration; it is a request-availability simulation, not a Docker
container stop. Use `docker compose stop node3` for an actual stopped process.

The containers use stable Compose DNS names (`node1`, `node2`, `node3`) through
the mounted files in `configs/docker/`; peers never use `localhost`. From the
host, verify routing and quorum writes through any exposed node:

```sh
curl -i -X PUT http://127.0.0.1:8082/kv/course \
  -H 'Content-Type: application/json' \
  -d '{"value":"distributed systems"}'
curl http://127.0.0.1:8081/kv/course
curl http://127.0.0.1:8083/health
```

Inspect persistent container logs and structured routing/quorum/health events:

```sh
docker compose logs -f node1 node2 node3
```

Stop the demo (data is intentionally in-memory):

```sh
docker compose down
```

## Test

```sh
gofmt -w cmd internal
go test ./...
go test -race ./...
```

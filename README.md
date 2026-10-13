# raft

A Raft-based distributed key-value store in Go. Implements leader election, log replication, and persistence — the core consensus layer for a linearizable KV store.

## At a glance

|               |                                                                      |
| ------------- | -------------------------------------------------------------------- |
| Language      | Go 1.27                                                              |
| Concurrency   | Single-threaded event loop per node, gRPC for RPC                    |
| Protocol      | gRPC (protobuf) for `RequestVote`, `AppendEntries`, client proposals |
| Persistence   | File-based log with index map, crash-recoverable                     |
| State machine | In-memory KV (SET/DEL) applied from committed log                    |
| Dependencies  | `grpc`, `protobuf`, `viper` (config), `slog` (logging)               |

## What it does

- **Leader election** — randomized election timeouts (350–600ms), `RequestVote` RPC, majority quorum, term-based safety
- **Log replication** — `AppendEntries` with `nextIndex`/`matchIndex` per follower, leader-only commit rule (current term + majority)
- **Persistence** — `currentTerm`, `votedFor`, and log entries written to disk before responding; `BuildIndex` replays on restart
- **Cluster membership** — static 4-node cluster defined in YAML config; each node connects to all peers on startup
- **Client proposals** — `MakeProposal` RPC routes SET/DEL commands to leader; leader replicates, commits, applies, responds
- **Graceful shutdown** — signal handling closes gRPC server, drains connections, persists state

## Usage

```bash
# Build
go build -o raft ./cmd

# Run a node (port must match one in configs/cluster_config.yaml)
./raft 3000
./raft 3001
./raft 3002
./raft 3003
```

Configuration lives in `configs/cluster_config.yaml`; see [Configuration](#configuration).

## Configuration

`configs/cluster_config.yaml` — read at startup by all nodes:

```yaml
seeds:
  - id: "3000"
    addr: "localhost:3000"
    port: "3000"
    filename: "node_3000.log"
  - id: "3001"
    addr: "localhost:3001"
    port: "3001"
    filename: "node_3001.log"
  - id: "3002"
    addr: "localhost:3002"
    port: "3002"
    filename: "node_3002.log"
  - id: "3003"
    addr: "localhost:3003"
    port: "3003"
    filename: "node_3003.log"
```

Each node identifies itself by matching its CLI `port` argument to a seed entry. The `filename` is the per-node log file (created in the working directory).

## Testing

Run unit tests for the log package:

```bash
go test ./internal/log/...
```

Run the integration test (spins up a 3-node cluster over real loopback TCP):

```bash
go test -v ./internal/raft/ -run TestBasicElection
```

The integration suite uses a `TestCluster` harness that:

- Reserves ephemeral ports per node
- Starts each node in its own temp directory with independent log file
- Connects all peers, waits for leader election
- Provides helpers for proposals, commit waits, node kill/restart

## Layout

```
cmd/
  main.go                 # entry point: raft <port>
internal/
  config/                 # YAML config loader (viper)
  db/                     # in-memory KV store (SET/DEL)
  log/                    # persistent log: append, truncate, index, recovery
  raft/
    init.raft.go          # bootstrap, gRPC server, signal handling
    node.raft.go          # Raft FSM: election, replication, proposal handling
    types.raft.go         # roles, events, RPC payload wrappers
    cluster_integration_test.go  # multi-node test harness
  rpc/gen/                # generated gRPC stubs (from proto/)
proto/
  service.proto           # PeerService, ClientService
  entries.proto           # AppendEntries request/response, Entry
  vote.proto              # RequestVote request/response
  propose.proto           # Proposal request/response, EntryType enum
configs/
  cluster_config.yaml     # static cluster definition
```

## License

[MIT](LICENSE)

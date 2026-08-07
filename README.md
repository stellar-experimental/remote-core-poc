# remote-core-poc

A prototype for [stellar-rpc#906](https://github.com/stellar/stellar-rpc/issues/906): can stellar-core run on its **own** hardware and stream raw ledgers to RPC over the network, instead of every RPC instance owning a captive core process?

It is three things:

- **`corestreamd`** — a service that owns the ledger source (captive stellar-core, or a synthetic stand-in) and streams raw ledger XDR to subscribers over WebSocket, with a disk-backed retention ring so a subscriber can replay recent history before joining the live stream.
- **`remoteledger`** — a client that implements `ledgerbackend.LedgerStream`, the exact seam RPC v2 full-history ingest already consumes. Ingest cannot tell it apart from captive core.
- **`benchrunner`** — a harness that measures whether network delivery fits inside the local-pipe envelope. The Phase 3 ingest budget is **80 ms per ledger** for all of ingest, so delivery has to cost a small fraction of that.

Nothing here modifies the `stellar-rpc` repo. The wiring diff for RPC v2 is written down in [`docs/rpc-wiring.md`](docs/rpc-wiring.md).

## Architecture

```
     corestreamd box                              RPC box
 ┌────────────────────────────┐             ┌────────────────────────┐
 │ captive stellar-core       │             │  ingest hot loop       │
 │   │  (or synthetic source) │             │    │                   │
 │   ▼  LedgerStream          │             │    ▼                   │
 │ source loop ── copy ──┐    │             │  remoteledger.Stream   │
 │   │                   ▼    │   WebSocket │    │  (LedgerStream)    │
 │   │            retention   │◄────────────┤    │                   │
 │   ▼            ring (disk) │  ws://…     │    │                   │
 │ broadcaster ──────────────►│─────────────►    │                   │
 │   per-subscriber queue     │  replay,    └────────────────────────┘
 │   /healthz                 │  then live
 └────────────────────────────┘
```

One goroutine pulls the source and nothing a subscriber does can slow it down: each ledger is copied out of the borrowed iterator slice, appended to the retention ring, and pushed to every subscriber's queue. A subscriber whose queue overflows is disconnected instead of being waited for.

A subscription is served **replay first, then live**, with no gap and no duplicate: the live queue is registered before the retained bounds are read, the retained ledgers are sent, and live ledgers already covered by the replay are skipped.

## Wire protocol v1

Connect: `GET ws://host:8462/v1/stream?start=N&end=M`. `start` absent or `0` means the next live ledger; `end` absent means unbounded.

Each ledger is one binary message:

```
[1B version=0x01][1B type=0x01 ledger][4B BE sequence][8B BE emitUnixNano][raw ledger XDR …]
```

`emitUnixNano` is the server's wall clock when the ledger arrived from its source, which is what makes one-way delivery latency measurable (same host, or NTP-synced hosts — the clock-skew caveat is yours to manage). It is **zero for a ledger replayed from the retention ring**: the original arrival time is not persisted, and the time the file was read is not a delivery latency.

Close codes:

| Code | Meaning |
| --- | --- |
| 1000 | The bounded range completed, or an unbounded stream's source ended. |
| 1001 | The source ended while a bounded subscriber was still waiting for ledgers. |
| 4001 | `start` is older than retention. The reason carries `oldest=` and `latest=`; the client surfaces `remoteledger.ErrTooFarBehind`. |
| 4002 | The subscriber fell behind and was dropped; the client surfaces `remoteledger.ErrSlowConsumer`. |

A bounded range that ends short surfaces as `remoteledger.ErrTruncated`, naming the last ledger delivered and the one requested. The client decides that by comparing what arrived against what it asked for, not by trusting the close code — an orderly close from anywhere, including a middlebox, cannot make a short delivery look like a complete one. An unbounded stream has no such expectation, so a clean close simply ends iteration.

The server pings every 15 s. The client accepts a ledger payload of up to 256 MiB, matching the SDK's captive-core frame cap; its WebSocket read limit is that plus the 14-byte header, so the largest ledger is not rejected by its own framing. `remoteledger.WithMaxMessageSize` moves that payload cap.

That 256 MiB is the protocol's ceiling, not a setting: `corestreamd` refuses a `--synthetic-size` above it, and its source loop fails with a clear error rather than publishing a ledger no subscriber's read limit would admit. The last representable ledger sequence (`4294967295`) is likewise not streamable — the protocol has no wrap — so both ends refuse a range that reaches it.

## Run the synthetic demo (no core binary, no network)

The quickest look at the whole path is one command — server, client and measurement in a single process:

```sh
go run ./cmd/benchrunner --mode loopback
```

To drive the real service instead, start it in the **first shell**. It runs in the foreground until you interrupt it:

```sh
go run ./cmd/corestreamd --source synthetic --synthetic-interval 1s --synthetic-size 204800
```

In a **second shell**, check its health and then consume it:

```sh
curl -s localhost:8462/healthz    # {"oldest":1,"latest":12,"subscribers":0}
go run ./cmd/benchrunner --mode remote --url ws://127.0.0.1:8462 --start 1
```

The client streams until you interrupt it, since `--start 1` with no `--end` is an unbounded range. Interrupt the server (Ctrl-C) when you are done; it removes nothing, so `--data-dir` keeps the retained ledgers for the next run.

## Run against real captive core

`corestreamd` builds its captive-core config the same way the RPC v2 daemon builds its live ingestion core: the captive-core toml is the source of truth (`NETWORK_PASSPHRASE` is read back from it), the binary defaults to the one on `PATH`, and only the history-archive URLs come from flags.

```sh
go run ./cmd/corestreamd \
  --source captive \
  --start-ledger 55000000 \
  --core-binary /usr/local/bin/stellar-core \
  --core-config ./captive-core.cfg \
  --history-archive-urls https://history.stellar.org/prd/core-live/core_live_001 \
  --storage-dir /data/captive-core \
  --data-dir /data/corestreamd \
  --retention 10000
```

`--start-ledger` is required in captive mode: resolving "the tip" needs a history-archive round trip this prototype does not do, and a benchmark replays a known range anyway. Use `--network-passphrase` to override what the toml declares (the SDK rejects a disagreement, so a wrong value fails loudly rather than starting core on the wrong network).

## Benchmark

Three modes, all printing count, bytes, throughput, inter-arrival percentiles and — where an emit stamp exists — delivery-latency percentiles. `--csv out.csv` adds per-ledger rows.

```sh
# zero-dependency smoke test and demo: synthetic server + client in one process
go run ./cmd/benchrunner --mode loopback --count 100 --synthetic-interval 10ms --verify

# the measurement that matters: a corestreamd over the network
go run ./cmd/benchrunner --mode remote --url ws://core-box:8462 --start 55000000 --end 55001000 --csv remote.csv

# the baseline it is compared against: captive core in this process, over a pipe
go run ./cmd/benchrunner --mode local --start 55000000 --end 55001000 \
  --core-config ./captive-core.cfg \
  --history-archive-urls https://history.stellar.org/prd/core-live/core_live_001 --csv local.csv
```

`--mode local` reports cadence and throughput but no delivery latency: a local pipe has no emit stamp to measure against, which is the point — it is the envelope, not a delivery path.

Read the two runs together: `local` says how fast ledgers can be produced at all, `remote` says what the network added on top per ledger. Compare the added delivery latency against the 80 ms Phase 3 per-ledger ingest budget.

## Layout

| Path | What it is |
| --- | --- |
| `cmd/corestreamd` | The server binary: flags, source selection, HTTP surface. |
| `cmd/benchrunner` | The benchmark harness: three modes, percentiles, CSV. |
| `remoteledger` | The exported client. Implements `ledgerbackend.LedgerStream`. |
| `internal/server` | Source loop, broadcaster, subscriber lifecycle, captive and synthetic sources. |
| `internal/store` | The disk-backed retention ring. |
| `internal/wire` | Message framing and the close-code vocabulary both sides share. |
| `docs/rpc-wiring.md` | The exact diff to wire this into stellar-rpc v2. |

## Prototype boundaries

Deliberately absent: TLS and auth (plaintext), gRPC (WebSocket was chosen to avoid a protoc/buf toolchain on every box; a gRPC variant is a follow-up if WebSocket framing shows up in the numbers), Prometheus metrics on the server (structured logs and `/healthz` instead), any parsing of ledger bodies (the seam carries opaque bytes end to end), and multi-node retention replication.

Synthetic payloads are **not** valid `LedgerCloseMeta` XDR. Nothing in this prototype decodes a ledger body, so a deterministic blob is enough — and being deterministic is what lets the benchmark verify integrity by regenerating the bytes (`--verify`).

## Tests

```sh
go test ./... -count=1
```

Everything runs with no external dependencies: the end-to-end tests drive a real WebSocket server on `127.0.0.1` with the synthetic source, and the captive-core tests build the real SDK config against a stub binary.

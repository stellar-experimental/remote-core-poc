# remote-core-poc

A prototype for [stellar-rpc#906](https://github.com/stellar/stellar-rpc/issues/906): can stellar-core run on its **own** hardware and stream raw ledgers to RPC over the network, instead of every RPC instance owning a captive core process?

It is three things:

- **`corestreamd`** — a service that owns the ledger source (captive stellar-core, a raw-ledger dump replayed from disk, or a synthetic stand-in) and streams raw ledger XDR to subscribers over WebSocket — chunked and forwarded **as the source emits**, with a disk-backed retention ring so a subscriber can replay recent history before joining the live stream.
- **`remoteledger`** — a client that implements `ledgerbackend.LedgerStream`, the exact seam RPC v2 full-history ingest already consumes. It reassembles the chunk flow into the complete raw meta and verifies its checksum before handing it up, so ingest cannot tell it apart from captive core.
- **`benchrunner`** — a harness that measures delivery latency: how long after the source finishes emitting a ledger it is assembled and verified on the consumer.

Nothing here modifies the `stellar-rpc` repo. The wiring diff for RPC v2 is written down in [`docs/rpc-wiring.md`](docs/rpc-wiring.md).

## Why chunked

This protocol's retired predecessor (the pre-chunking revision in git history, commit `7d74d10`) sent each ledger as ONE WebSocket message. That is store-and-forward: the server holds the complete meta, then serializes it onto the wire, so measured delivery is almost exactly the wire-serialization time of the whole blob — ~19 ms p50 for a 14.5 MiB ledger on a 6-ish Gbps effective NIC, scaling linearly with payload size.

The chunked protocol taps **below the complete-ledger seam**: the server reads each ledger's byte stream incrementally and forwards chunks the moment they exist, never waiting for a full ledger before its first chunk goes out. If the source emits over a window `T_emit` and the wire can move the ledger in `S/R < T_emit`, the transfer hides completely inside emission and what remains after the source finishes is one chunk plus RTT — **~0.5–1 ms p50** instead of the whole blob's wire time. On 12.5 Gbps-baseline NICs (c6id-class), a 14.5 MiB ledger needs ~9.7 ms of wire against core's **measured ~7.5 ms pipe-write burst** (core serializes the whole meta into memory first — ~27 ms invisible to any reader (per-stress-frame: core's metastream.write timer reads ~35 ms undiluted by setup ledgers, minus the 7.5 ms write burst) — then writes it as ~58 × 256 KiB buffered flushes; measured against a real `stellar-core apply-load` at this exact shape), so most but not all of the transfer hides: ~2.2 ms of wire remains after emission at stress size, and delivery lands ~3.5 ms rather than sub-millisecond. On small burst-credit NICs (m6id.2xlarge receivers) the mechanism does not apply, and compression — deliberately not built here, but sized: real SAC-shape meta compresses ~7× at >500 MiB/s with Go zstd (expect materially less on entropy-rich pubnet meta) — is the documented option for both regimes.

The store-and-forward baseline is not kept in the tree: measure it for a comparison row by building `corestreamd` and `benchrunner` at commit `7d74d10` and running them on the same pairing.

## Architecture

```
     corestreamd box                                RPC box
 ┌───────────────────────────────┐             ┌────────────────────────┐
 │ file replayer / captive core  │             │  ingest hot loop       │
 │   │  (or synthetic source)    │             │    │                   │
 │   ▼  incremental emission     │             │    ▼                   │
 │ source loop ──── chunk ──┐    │  WebSocket  │  remoteledger.Stream   │
 │   │      │               ▼    │  ws://…     │    │  (LedgerStream)   │
 │   │      └► complete ► ring   │◄────────────┤    │                   │
 │   ▼           ledgers  (disk) │  catch up,  │  reassemble + verify   │
 │ flow watch ──────────────────►│─────────────►    │                   │
 │  (BEGIN/CHUNK*/END + wakeup)  │  then live  └────────────────────────┘
 │   /healthz                    │
 └───────────────────────────────┘
```

One goroutine reads the source and nothing a subscriber does can slow it down: each chunk is hashed, appended to the in-memory flow and published with a wakeup the moment it exists — no batching, no coalescing timers. The completed ledger is written to the retention ring **after** its END is published, so live delivery never pays for the disk write; the flow stays in memory until the next ledger begins, so the just-completed ledger is served from there and the ring only ever serves ledgers older than the current flow, which are always on disk by then.

Each subscription is a cursor over the retention ring: **catch up from the ring, then follow the live flow**, with no gap and no duplicate. A subscriber behind the flow reads complete ledgers from the ring (delivered as zero-stamped chunk flows); one at the flow writes its chunks as they are published and sleeps until it grows. There is no per-subscriber queue to overflow, so a stalled subscriber is never dropped for lagging — it catches back up from the ring by itself, and only falling out of retention entirely ends the stream (close 4001).

**Slow-subscriber policy:** a subscriber still mid-ledger when the flow moves on to the next ledger has spent its budget (the rest of that ledger's emission). Its live flow is abandoned and the ledger redelivered complete — and stampless — from the ring; the client discards the partial assembly when the fresh BEGIN for the same sequence arrives. The server counts these as `live_abandons` in `/healthz`, the client reports them per ledger as `DiscardedPartials`, and a clean measurement window must show zero.

## Wire protocol

Connect: `GET ws://host:8462/stream?start=N&end=M`. `start` absent or `0` means the next live ledger; `end` absent means unbounded.

Each ledger is a flow of binary messages:

```
BEGIN [1B ver=0x02][1B type=0x10][4B BE seq][8B BE emitStartUnixNano]
CHUNK [1B ver=0x02][1B type=0x11][4B BE seq][4B BE chunkIdx][payload bytes]
END   [1B ver=0x02][1B type=0x12][4B BE seq][4B BE chunkCount]
      [8B BE totalLen][8B BE emitEndUnixNano][8B BE xxhash64-of-raw]
```

- `emitStartUnixNano` is the server clock at the **first** byte of the ledger from its source, `emitEndUnixNano` at the **last** byte. Together they make the headline delivery metric and the source's emission window `T_emit` measurable. Both are **zero for a ledger replayed from the retention ring**: replayed ledgers never contribute delivery samples.
- Chunks default to 256 KiB (`--chunk-size`, 4 KiB–8 MiB): at 12.5 Gbps a chunk is ~0.17 ms of wire time, and a 14.5 MiB ledger is ~58 chunks, so framing overhead stays trivial.
- The checksum is xxhash64 over the raw ledger. The client verifies it on END — along with `totalLen` and `chunkCount` against what actually arrived — before handing the meta up. Violations are protocol errors that end the stream (`remoteledger.ErrProtocol`), never silent drops.
- A fresh BEGIN for the sequence currently being assembled is the one legal mid-flow BEGIN: it announces a ring redelivery, and the client discards its partial.

The 256 MiB per-ledger cap is an **assembly-buffer cap** (`remoteledger.WithMaxMessageSize`), matching the SDK's captive-core frame cap; the per-message WebSocket read limit is just one chunk plus its header (`remoteledger.WithMaxChunkSize`, default 8 MiB — the largest chunk a server flag can configure, so defaults on both ends always interoperate). The server bounds each message write at 10 seconds — a real policy edge: a peer that keeps draining the socket, however slowly, is never dropped, but one frozen outright with a chunk in flight is disconnected after that bound, because a serve loop blocked mid-write cannot fall back to the ring.

TCP/WS hygiene, since this is where the milliseconds hide: TCP_NODELAY is on (Go's default for TCP connections, on both ends), WebSocket permessage-deflate is explicitly disabled on both ends, and every chunk is written the moment it exists. Socket buffer sizing is left to the OS's autotuning, which keeps a few chunks in flight on any modern kernel.

Close codes:

| Code | Meaning |
| --- | --- |
| 1000 | The bounded range completed, or an unbounded stream's source ended. |
| 1001 | The source ended while a bounded subscriber was still waiting for ledgers. |
| 4001 | `start` is older than retention. The reason carries `oldest=` and `latest=`; the client surfaces `remoteledger.ErrTooFarBehind`. |

A bounded range that ends short surfaces as `remoteledger.ErrTruncated`, naming the last ledger delivered and the one requested. The client decides that by comparing what arrived against what it asked for, not by trusting the close code — an orderly close from anywhere, including a middlebox, cannot make a short delivery look like a complete one. An unbounded stream has no such expectation, so a clean close simply ends iteration.

The server sends no WebSocket pings: a stalled subscriber could not answer one, and killing it for that would break the catch-up promise for peers that are merely slow. Dead peers are noticed by the OS's TCP keepalive on the read side, or by the 10-second per-message write timeout once chunks are in flight.

The 256 MiB ledger cap is the protocol's ceiling, not a setting: `corestreamd` refuses a `--synthetic-size` above it, and its source loop fails with a clear error rather than emitting a ledger no subscriber would admit. The last representable ledger sequence (`4294967295`) is likewise not streamable — the protocol has no wrap — so both ends refuse a range that reaches it.

## Measurement: what the numbers mean

`benchrunner` reports, per run:

- **`delivery`** *(the headline)* — client clock at *assembly complete, checksum verified* minus `emitEndUnixNano`: how long after the source finished emitting was the ledger usable. Requires NTP-synced clocks across hosts (chrony, offset verified well under 1 ms — state it in any report).
- **`t_emit`** — `emitEnd − emitStart`, the source's own emission window, measured on the server's clock alone. This is a first-class output: the real unknown of the whole design is captive core's actual emission profile, and this row is where a wired-in core binary would answer it.
- **`pipeline`** — assembled minus `emitStart`: the whole path from the source's first byte.
- **`inter-arrival`**, **`fallbacks`** (partial deliveries discarded — must be zero in a clean window), and the count of ring-replayed ledgers excluded from delivery samples.

The pre-chunking baseline (commit `7d74d10`) reports a single-stamp `delivery` measuring receipt minus *complete arrival at the server* — the store-and-forward number a comparison row is made of.

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

## Replay a dump: the acceptance setup

`--source file` replays a directory of `ledger-<seq>.xdr` files — the exact layout the retention ring writes, so **any previous run's `<data-dir>/ledgers` is a dump** (capture one from real core with `--source captive`, or use the sac-6000 dump from the issue-906 rerun laid out one file per ledger). Records are renumbered from `--start-ledger` and cycled endlessly (`--file-count` bounds the replay), which is what lets a fixed dump stand in for an endless source in a long run.

The replayer emits **incrementally**: each ledger's bytes are paced over `--emit-window` (default 15 ms — the disk-read proxy measured in the issue-906 rerun), one ledger every `--cadence` (default 600 ms). This is what makes the overlap measurable at all; a replayer that reads a whole record and then sends it has finished "emitting" before the first byte moves and can only ever measure store-and-forward.

The acceptance environment is two instances with ≥12.5 Gbps **baseline** NICs (c6id.8xlarge or equivalent — m6id.2xlarge is 3.125 Gbps baseline and must not be the receiver), same AZ, NTP verified on both ends:

```sh
# core box: serve the dump
go run ./cmd/corestreamd --source file --file-dir /data/sac-6000 \
  --emit-window 15ms --cadence 600ms --retention 20000

# RPC box: ≥1,000 live-measured ledgers from the tip, no ring replays in the window
go run ./cmd/benchrunner --mode remote --url ws://core-box:8462 --csv chunked.csv
#   (interrupt after ~1,100 ledgers; or bound the range just ahead of /healthz's latest)

# the store-and-forward comparison row, on the SAME pairing, from git history:
git checkout 7d74d10 && go run ./cmd/corestreamd ... && go run ./cmd/benchrunner ... --csv baseline.csv
```

Gates at emit-window 15 ms: delivery p50 ≤ 1.5 ms, **p99 ≤ 4 ms**, max ≤ 10 ms; zero checksum failures; zero fallbacks. Also report sensitivity rows at `--emit-window 10ms` and `5ms` (no gate — at 5 ms roughly `(9.7−5)+1 ≈ 6 ms` should poke out, which is the row that bounds what core's real emission profile can promise). If a gate misses, decompose the tail from the CSV (`emit_ns`, `delivery_ns`, `pipeline_ns` per ledger) before changing the design.

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

Captive mode ignores `--emit-window` and `--cadence`: core paces itself, and the SDK seam only surfaces **complete** metas, so each one is chunk-forwarded the moment the seam yields it — overlap with core's real emission needs a tap below that seam (the meta pipe), which is future work. When that tap exists, instrument first-byte/last-byte per ledger first: that single `t_emit` distribution bounds everything this design can promise in production.

## Benchmark

Three modes, all printing count, bytes, throughput, inter-arrival percentiles and — where emit stamps exist — the delivery/t_emit/pipeline percentiles defined above. `--csv out.csv` adds per-ledger rows.

```sh
# zero-dependency smoke test and demo: synthetic server + client in one process,
# 14.5 MiB ledgers emitted over 15ms every 600ms — the acceptance shape without hardware
go run ./cmd/benchrunner --mode loopback --count 100 --synthetic-size 15196160 \
  --synthetic-interval 600ms --emit-window 15ms --verify

# the measurement that matters: a corestreamd over the network
go run ./cmd/benchrunner --mode remote --url ws://core-box:8462 --start 55000000 --end 55001000 --csv remote.csv

# the baseline it is compared against: captive core in this process, over a pipe
go run ./cmd/benchrunner --mode local --start 55000000 --end 55001000 \
  --core-config ./captive-core.cfg \
  --history-archive-urls https://history.stellar.org/prd/core-live/core_live_001 --csv local.csv
```

`--mode local` reports cadence and throughput but no delivery latency: a local pipe has no emit stamp to measure against, which is the point — it is the envelope, not a delivery path.

Read the runs together: `local` says how fast ledgers can be produced at all, `remote` says what the network added on top per ledger, and the pre-chunking baseline (commit `7d74d10`) on the same pairing says what chunked overlap bought over store-and-forward.

## Layout

| Path | What it is |
| --- | --- |
| `cmd/corestreamd` | The server binary: flags, source selection, HTTP surface. |
| `cmd/benchrunner` | The benchmark harness: three modes, percentiles, CSV. |
| `remoteledger` | The exported client. Implements `ledgerbackend.LedgerStream`; reassembles and verifies the chunk flow. |
| `internal/server` | Source loop, flow broadcaster, subscriber lifecycle, emission pacing, file/captive/synthetic sources. |
| `internal/store` | The disk-backed retention ring (whose layout doubles as the dump format). |
| `internal/wire` | Message framing and the close-code vocabulary both sides share. |
| `docs/rpc-wiring.md` | The exact diff to wire this into stellar-rpc v2. |

## Prototype boundaries

The retention ring has no run identity: after a restart over a stale `--data-dir`, ledgers the previous run retained are served as history until the new source's first publish resets the ring — a subscriber connecting in that window can receive the old run's bytes spliced with the new run's, checksum-clean on both sides. Clear the data dir (or use a fresh one) when changing sources or start ledgers.

Deliberately absent: **compression** (the documented option for small-NIC receivers, where the wire time exceeds the emission window; its codec adds ~2–6 ms of tail, strictly worse on c6id-class NICs), TLS and auth (plaintext), gRPC (WebSocket was chosen to avoid a protoc/buf toolchain on every box; a gRPC variant is a follow-up if WebSocket framing shows up in the numbers), Prometheus metrics on the server (structured logs and `/healthz` instead), any parsing of ledger bodies (the seam carries opaque bytes end to end), verbatim-frame-reuse coupling with RPC storage, multi-subscriber scale-out beyond keeping the source-loop invariants, and multi-node retention replication.

Synthetic payloads are **not** valid `LedgerCloseMeta` XDR. Nothing in this prototype decodes a ledger body, so a deterministic blob is enough — and being deterministic is what lets the benchmark verify integrity by regenerating the bytes (`--verify`).

## Tests

```sh
go test ./... -count=1
```

Everything runs with no external dependencies: the end-to-end tests drive a real WebSocket server on `127.0.0.1` with the synthetic source, and the captive-core tests build the real SDK config against a stub binary.

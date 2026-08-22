# Two-box acceptance runbook (c6id.8xlarge pairing)

The single-box de-risk (2026-08-19, `user-dev-064a`) measured every layer of
this design except the physical wire: the software stack's own delivery tail
is ~0.3 ms; core's real emission window is **7.5 ms** (not the ~15 ms this
README once assumed); and the true end-to-end cell — a real core tapped via
`-source pipe`, relayed through a userspace 12.5 Gbit + 50 µs link emulation —
reads **delivery p50 3.48 / p99 4.48 ms**, projecting **p99 ≈ 3.5 ms** on real
NICs. The pairing is confirmation, not discovery: the one number it adds is
real ENA/interrupt behavior under the real wire.

## Preconditions (without these the numbers are unreadable)

1. Two c6id.8xlarge in the **same cluster placement group**. This is not only
   about the ~100 µs RTT: EC2 caps a SINGLE TCP flow at ~5 Gbit/s outside a
   placement group and ~10 Gbit/s inside one (measured on these boxes: 4.96
   Gbit/s one flow, 12.26 Gbit/s across four). This transport is one
   WebSocket, so it gets the single-flow number, and a 14.48 MiB ledger needs
   24.3 ms of wire at 5 Gbit/s against a 7.5 ms emission window.
2. **chrony disciplined to the AWS Time Sync PTP hardware clock on BOTH
   boxes** (`/dev/ptp0`, `refclock PHC`): delivery is a cross-machine
   timestamp difference and plain NTP's ±0.5–1 ms error is the same order as
   the signal. Verify `chronyc tracking` shows sub-100 µs before measuring.
3. Quiet boxes: nothing else heavy on either side; ~40 GB free disk on the
   server box for the retention ring at stress size.
4. A `BUILD_TESTS` stellar-core (the apt `~perftests` build) on the server
   box for the real-core cells.

## Cells, in order

Server box runs `corestreamd`, client box runs `benchrunner -mode remote
-url ws://<server>:8462 -csv <cell>.csv`. One cell at a time.

**Read this before P1: the gate is a COMPRESSED measurement.** A single
WebSocket is a single TCP flow, and at ~5 Gbit/s a 14.48 MiB ledger needs
~24 ms of wire against a ~7.5 ms emission window, so uncompressed it cannot
pass at any EC2 flow rate — the two-box run measured p50 16.6 ms. Compressed,
the same ledger is ~2 MB and ~3.2 ms of wire, which fits inside the window.
Single-box readings at honest single-flow rates, for what to expect:

| cell shape | p50 | p99 |
|---|---|---|
| uncompressed, 5 Gbit/s, n=1000 | 18.60 ms | 19.32 ms |
| uncompressed, 10 Gbit/s (placement group) | 6.28 ms | 7.13 ms |
| **compressed, 5 Gbit/s, n=1000** | **1.25 ms** | **2.32 ms** |
| compressed, real core meta via the pipe tap | 2.19 ms | — |

**Cell P1 — the gate cell (compressed, acceptance shape).**
Server: `corestreamd -source synthetic -synthetic-compressible -synthetic-size 15184000
-synthetic-interval 600ms -emit-window 7500us -chunk-size 262144
-synthetic-count 1015`. Client: `-end 1005`.
`--compress` defaults on; `--synthetic-compressible` is REQUIRED — the default
synthetic payload is incompressible noise, so without it the cell compresses
nothing and reports the uncompressed number while looking healthy.
≥1,000 ledgers. Expected: delivery p50 ~1.3 ms / p99 ~2.5 ms.
Gate: **p99 ≤ 4 ms**, zero fallbacks, first streamed ledger excluded (or rely
on n ≥ 1,000 pushing warmup past p99.9). Pace at 7.5 ms — the measured core
burst — NOT the 15 ms this repo once assumed.
If you pass `--verify`, the client must also get `-remote-compressible`, or it
regenerates the wrong payload shape and reports a healthy stream as corrupt.

**Cell P1u — the uncompressed confirmation.**
Same as P1 with `-compress=false` and without `-synthetic-compressible`.
Expected p99 ~19 ms at 5 Gbit/s, ~7 ms inside a placement group. This is the
row that confirms the single-flow limit on real NICs; it is not the gate.

**Cell P2 — true end-to-end, real core.**
Server: `corestreamd -source pipe -pipe-cmd "stellar-core apply-load
--conf apply-load-sac6000-meta.cfg"` (config in the de-risk record; 100 ledgers
at ~2 s apply pace, mean frame 14.48 MiB, compresses 7.08x). Client: unbounded,
stream ends when core exits. Expected ≈ P1 at n=100 (single-box read: p50
2.19 ms; the real wire should land at or under it). Run it with
`-compress=false` too for the paired uncompressed row (17.94 ms p50).

**What P2 does NOT cover.** The target shape is 600 ms close time at sac6000
density, and P2 does not run at it: apply-load produces a ledger every ~2 s on
this hardware, 3.3x slower than the target, so P2 proves the tap and the codec
against real meta at a third of the cadence. Only the synthetic source (P1)
runs at 600 ms. So the transport is measured at the target shape and real core
meta is measured at core's natural pace — the two have never been measured
together, and cannot be until core closes sac6000 ledgers at 600 ms. Read a
P2 pass as "the tap is honest", not as "the target shape is proven".

**Cell P3 (optional) — today's network shape.**
Server: `-source pipe -pipe-cmd "stellar-core catchup <recent>/<count>
--metadata-output-stream fd:3"` on a pubnet config. Real pubnet metas are
~10–20× smaller than stress; expect sub-millisecond delivery throughout —
this cell documents the easy regime, it cannot fail the gate.

**Baseline row (once, for the comparison table).** Build `corestreamd` +
`benchrunner` at the pre-chunking revision `7d74d10` and rerun P1's shape:
expected p50 ~10–12 ms — the store-and-forward number the mechanism is
measured against.

## Reading the results

- `delivery` is the headline (assembled-and-verified − emitEnd); `fallbacks`
  must be 0 in every cell.
- If P1 misses the gate, rule out the two ways compression can be absent
  before blaming the network. Check `/healthz`'s `raw_fallbacks`: non-zero
  means chunks shipped uncompressed because every encoder state was busy —
  raise `-compress-workers`, it is a CPU problem, not a protocol one. Then
  check the bytes actually moved: ~2 MB per ledger means the payload
  compressed, ~14.5 MB means `-synthetic-compressible` was left off and the
  cell measured incompressible noise.
- If P1 lands 4–5 ms with clean fallbacks and ~2 MB on the wire: the
  mechanism is working as measured and the gate as written is the casualty.
  The design-relevant criterion is remote-vs-local delta (local captive pays
  the same ~27 ms serialize + 7.5 ms drain before decoding; remote's true
  penalty is the unhidden wire ≈ 2.2 ms + tail).
- If P1 exceeds ~5.5 ms: something outside the model (irq steering, placement,
  clock) — check `chronyc tracking`, `ethtool -S` coalescing, and rerun.
- Capture `ethtool -S` allowance counters (`bw_in_allowance_exceeded`,
  `pps_allowance_exceeded`, `conntrack_allowance_exceeded`) and `ss -ti`
  retransmits on the ledger socket alongside the percentiles. CloudWatch
  cannot substitute: AWS documents that these counters can show dropped
  packets while the instance metrics show nothing, because the averaging
  window is far coarser than a microburst — and at 600 ms the stream IS a
  microburst, on the wire 0.53% of the time.

## Sizing at the target shape

600 ms close time at sac6000 density is 14.48 MiB of meta every 600 ms:
202 Mbit/s raw, 26.7 Mbit/s compressed, 2.19 TB/day of meta. Two consequences
worth setting before a long run, neither of them about the wire:

- **The ring stores raw**, so `-retention 10000` is ~141 GiB and covers only
  100 minutes at this cadence. Size it in wall-clock: six hours is ~509 GiB
  and 36,000 ledgers, plus ~25 MB/s of sustained ring writes.
- **The subscriber must absorb ~85 GiB/hour** of meta, continuously. That is a
  much tighter constraint than anything measured here.

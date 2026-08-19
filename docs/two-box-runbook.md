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

1. Two c6id.8xlarge in the **same cluster placement group** (same-AZ RTT
   ~100 µs is what the projection assumes).
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

**Cell P1 — acceptance shape, simulated window (the gate cell).**
Server: `corestreamd -source synthetic -synthetic-size 15184000
-synthetic-interval 600ms -emit-window 7500us -chunk-size 262144
-synthetic-count 1015`. Client: `-end 1005`.
≥1,000 ledgers. Expected: delivery p50 ~2.5–3.5 ms / p99 ~3–4.5 ms.
Gate: **p99 ≤ 4 ms**, zero fallbacks, first streamed ledger excluded (or rely
on n ≥ 1,000 pushing warmup past p99.9). Pace at 7.5 ms — the measured core
burst — NOT the 15 ms this repo once assumed.

**Cell P2 — true end-to-end, real core.**
Server: `corestreamd -source pipe -pipe-cmd "stellar-core apply-load --conf
apply-load-sac6000-meta.cfg"` (config in the de-risk record; 100 ledgers at
~2 s apply pace, mean frame 14.48 MiB). Client: unbounded, stream ends when
core exits. Expected ≈ P1 at n=100 (single-box read: p50 3.48 / p99 4.48
through the emulator; the real wire should land at or under it).

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
- If P1 lands 4–5 ms: the mechanism is working as measured; the gate as
  written is the casualty. The design-relevant criterion is remote-vs-local
  delta (local captive pays the same ~23 ms serialize + 7.5 ms drain before
  decoding; remote's true penalty is the unhidden wire ≈ 2.2 ms + tail).
  Decide between re-anchoring the gate and building the sized per-chunk
  compression (~7× at >500 MiB/s, measured on the acceptance corpus itself —
  the same SAC-shape meta the gate runs on).
- If P1 exceeds ~5.5 ms: something outside the model (irq steering, placement,
  clock) — check `chronyc tracking`, `ethtool -S` coalescing, and rerun.

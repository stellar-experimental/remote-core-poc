# remote-core-poc v2: chunked streaming — design brief and acceptance spec

Audience: an implementation agent working in
github.com/stellar-experimental/remote-core-poc. Goal: evolve the PoC from
one-message-per-ledger delivery to chunked relay-as-produced streaming, and
demonstrate **delivery p99 ≤ 4ms** for the heaviest (sac-6000) ledgers on a
c6id-class instance pairing. No compression in this iteration. Nothing in the
stellar-rpc repo changes.

## 1. Why the current PoC measured 19.9ms, and why that was expected

The v1 wire protocol sends each ledger as ONE binary WebSocket message. That
is store-and-forward: the server holds the complete 14.5MiB meta, then
serializes it onto the wire, and the measured delivery time is almost exactly
the wire-serialization time of the whole blob. The issue-906 rerun confirms
this internally: "latency scaled linearly with payload size … this is wire
serialization time, not queueing."

Do the arithmetic against the receiver's NIC (m6id.2xlarge: 3.125 Gbps
baseline, 12.5 Gbps burst — EC2 burst credits explain the spread):

- 14.5 MiB = ~122 Mbit. At ~6.4 Gbps effective (partial burst): 19.0ms — their
  measured p50 was 18.98ms. At ~3.4 Gbps (throttled toward baseline): 35.8ms —
  their measured max was 35.27ms.

Nothing was implemented incorrectly per se; v1 implements a different variant
than the one the 1–4ms estimate describes. The estimate's mechanism is:

**Overlap transfer with the source's own emission.** The source produces the
meta over an emission window T_emit (the file replayer's disk read is ~15ms;
real core serializes over some window — unmeasured, see §6). If chunks are
forwarded the moment they exist, delivery-after-the-source-finishes is

    max(0, S/R − T_emit) + RTT + last_chunk_time

On c6id-class NICs (12.5 Gbps BASELINE, guaranteed, no burst dance):
S/R = 122 Mbit / 12.5 Gbps ≈ 9.7ms < T_emit ≈ 15ms → the transfer hides
completely inside emission, and the tail is one chunk + RTT ≈ **~0.5–1ms p50,
1–4ms p99**. That is the target of this work.

Two corollaries that shape the spec:
- On small-NIC receivers this mechanism is NOT sufficient (S/R at 3.4 Gbps =
  36ms > 15ms; the tail stays wire-bound). The acceptance environment is
  therefore c6id-class BOTH ends. Compression would fix small NICs but is
  deliberately out of scope here (documented option, not built).
- Even v1 unchanged would measure ~10ms on c6id-class. The chunked design must
  demonstrably beat that, or it has not delivered the mechanism.

## 2. The structural change: tap below the complete-ledger seam

v1's server sources ledgers via a LedgerStream-style iterator that only yields
COMPLETE metas — overlap is impossible at that seam, by construction. v2:

- SERVER: read the byte stream of each ledger incrementally from the source
  (file replayer in v2; captive core's meta pipe when available) and forward
  chunks immediately. The server must never wait for a full ledger before its
  first chunk goes out.
- CLIENT: unchanged seam. It reassembles chunks into the complete raw meta and
  hands it to `remoteledger` / `ledgerbackend.LedgerStream` exactly as today.
  RPC ingest remains unable to tell it apart from captive core.

## 3. Wire protocol v2

Binary WebSocket messages, one version byte as today. Three message types:

    BEGIN [1B ver=0x02][1B type=0x10][4B BE seq][8B BE emitStartUnixNano]
    CHUNK [1B ver=0x02][1B type=0x11][4B BE seq][4B BE chunkIdx][payload bytes]
    END   [1B ver=0x02][1B type=0x12][4B BE seq][4B BE chunkCount]
          [8B BE totalLen][8B BE emitEndUnixNano][8B BE xxhash64-of-raw]

- emitStartUnixNano = server clock at the FIRST byte of this ledger from the
  source; emitEndUnixNano = server clock at the LAST byte. These two stamps
  make both the headline metric (§5) and T_emit measurable.
- Chunk size: default 256 KiB, flag-tunable (`--chunk-size`). At 12.5 Gbps a
  chunk is ~0.17ms of wire time; ~58 chunks per stress ledger keeps framing
  overhead trivial.
- Ring-replayed ledgers (catch-up) may be delivered as BEGIN+chunks+END with
  both emit stamps ZERO, preserving v1's rule: replayed ledgers never
  contribute delivery samples.
- Checksum: xxhash64 over the raw ledger; client verifies on END before
  handing the meta up. totalLen must equal assembled length. Violations are
  protocol errors (close), not silent drops.
- Existing close-code semantics (1000/1001/4001, ErrTooFarBehind,
  ErrTruncated-by-comparison) carry over unchanged. The 256 MiB per-LEDGER cap
  becomes an assembly-buffer cap; per-message read limit shrinks to
  chunk-size + header slack.

## 4. Server internals

- The publish path today is "copy → ring append → tip pointer swap". v2 keeps
  the ring storing COMPLETE ledgers (it is the catch-up source and stays
  simple), but live at-tip subscribers additionally receive the in-flight
  chunk flow as it is produced.
- Slow-subscriber policy, kept simple for the PoC: if a live subscriber's
  socket would block past a small budget mid-ledger, abandon its live chunk
  flow, and let it recover that ledger (complete) from the ring via the
  existing cursor machinery; client-side assembly discards any partial on
  seeing a ring-delivered replacement for the same seq. A stalled subscriber
  must never slow the source loop — that invariant is v1's and survives.
- TCP/WS hygiene (these are where milliseconds hide):
  - TCP_NODELAY on the underlying conn (both ends).
  - WebSocket per-message compression (permessage-deflate) OFF.
  - Write the CHUNK the moment it exists; no batching/coalescing timers.
  - Socket write buffer sized for at least a few chunks in flight.
  - The v1 2-minute per-message write timeout becomes per-chunk (seconds).

## 5. Measurement: the metric definition changes — this is essential

v1's delivery metric (send-stamp of the complete blob → client receipt)
measures blob serialization and CANNOT show overlap. v2's headline metric:

    delivery = client_clock(assembly complete, checksum verified)
             − emitEndUnixNano                     (server clock, NTP-synced)

Report p50/p90/p99/max over live-delivered ledgers only (≥1,000 samples),
plus: inter-arrival spread, T_emit per ledger (emitEnd − emitStart) as its own
distribution, and the secondary metric client_assembled − emitStart (the whole
pipeline, for continuity with v1 comparisons). Clock discipline as in the v1
rerun: NTP/chrony both ends, verified offset well under 1ms, stated in the
report.

## 6. The file replayer must emit incrementally — and T_emit is a first-class output

v1's `--source file` reads a whole ledger record, then sends: T_emit is over
before the first byte moves, so it structurally measures store-and-forward.
v2 replayer: emit each ledger's bytes chunk-by-chunk, paced to a configurable
emission window (`--emit-window`, default 15ms per ledger — the disk-read
proxy measured in the issue-906 rerun), inside the 600ms cadence.

Report the acceptance run at emit-window 15ms, AND sensitivity rows at 10ms
and 5ms. Expectation: at 15ms the transfer (9.7ms) hides fully (~1ms tails);
at 5ms roughly (9.7−5)+1 ≈ ~6ms pokes out. This sensitivity row matters
because the REAL unknown of this whole design is captive core's actual
emission profile: nobody has measured how fast core writes a 14.5MiB meta to
its pipe. If/when a core binary is wired in, instrument first-byte/last-byte
per ledger and report the distribution — that single measurement bounds
everything this design can promise in production.

## 7. Acceptance

Environment: two instances, both with ≥12.5 Gbps BASELINE NICs (c6id.8xlarge
or equivalent; note m6id.2xlarge is 3.125 baseline and must not be the
receiver for acceptance), same AZ, NTP verified. Dataset: the same sac-6000
dump (14.49 MiB mean ledgers), 600ms cadence, ≥1,000 live-measured ledgers,
no ring replays inside the measurement window.

Gates:
- delivery (assembled − emitEnd) p50 ≤ 1.5ms, **p99 ≤ 4ms**, max ≤ 10ms at
  emit-window 15ms.
- zero checksum failures, zero ring-fallbacks during the window.
- sensitivity rows (10ms / 5ms emit-window) reported, no gate.
- one v1-vs-v2 comparison row on the SAME pairing, to show mechanism, not
  hardware, delivered the gain.
If a gate misses: report the measured tail decomposition (which stage —
last-chunk encode-side scheduling, wire, or client assembly) before changing
the design; the formula in §1 says where every millisecond must have gone.

## 8. Non-goals for this iteration

- No compression (documented separately as the small-NIC/bandwidth option; it
  adds codec tails of ~2–6ms which are strictly worse on c6id-class).
- No verbatim-frame-reuse coupling with RPC storage.
- No changes to the stellar-rpc repository or the client-facing seam.
- No multi-subscriber scale-out work beyond keeping v1's invariants.

## 9. Context pointers

- stellar-rpc#906 and its comment 5334712523 (the v1 rerun this brief
  reanalyzes; the sac-6000 dump/method there is reusable as-is).
- The v1 README's protocol/close-code/retention semantics (kept except where
  §3 supersedes).
- EC2 NIC facts used throughout: m6id.2xlarge 3.125 Gbps baseline / 12.5
  burst; c6id.8xlarge 12.5 Gbps baseline. Same-AZ RTT ~0.1–0.5ms.

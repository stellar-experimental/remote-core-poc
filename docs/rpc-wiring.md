# Wiring `remoteledger` into stellar-rpc v2

This is the diff that would move RPC v2 full-history ingest off local captive core and onto a `corestreamd` box. It is **documented, not applied** — nothing in this prototype changes the `stellar-rpc` repo.

## Why it is small

RPC v2 ingest consumes exactly one seam. `startup.go` opens a stream and the hot loop iterates it:

```go
// startup.go:105
stream, err := cfg.Core.OpenCore(ctx)

// hotloop.go:185
for raw, verr := range cfg.Stream.RawLedgers(ctx, ledgerbackend.UnboundedRange(cfg.Resume)) {
```

`cfg.Core` is a `CoreOpener` (startup.go:336), whose only method returns a `ledgerbackend.LedgerStream`:

```go
type CoreOpener interface {
	OpenCore(ctx context.Context) (ledgerbackend.LedgerStream, error)
}
```

`remoteledger.Stream` **is** a `ledgerbackend.LedgerStream`. So the whole change is a second opener next to `captiveCoreOpener` (daemon.go:493) and a config switch that picks it. Ingest, the hot loop, resume, retention and the stores are untouched.

## The opener

Add next to `captiveCoreOpener` in `cmd/stellar-rpc/internal/rpcv2/daemon.go`:

```go
// remoteCoreOpener opens the live ingestion stream from a corestreamd that owns
// captive core on another machine. The stream still owns its lifecycle: the
// WebSocket is dialled on the first RawLedgers pull and closed when the loop
// exits, which is the same contract captiveCoreOpener's core process has.
type remoteCoreOpener struct {
	url string
}

// OpenCore returns the live ingestion stream backed by a remote core streamer.
func (o *remoteCoreOpener) OpenCore(context.Context) (ledgerbackend.LedgerStream, error) {
	return remoteledger.New(o.url), nil
}
```

`OpenCore` ignores its context on purpose, exactly as the captive opener only stores it on the config: the context that matters is the one the hot loop passes to `RawLedgers`, and that is what tears the connection down.

## The switch

`resolveCore` (daemon.go) currently picks between an injected opener and the captive pair:

```go
func resolveCore(opts daemonOptions, cfg config.Config, logger *supportlog.Entry) (resolvedCore, error) {
	if opts.Core != nil {
		return resolvedCore{live: opts.Core, backfill: opts.Core}, nil
	}
	return newCaptiveCoreOpeners(cfg.Ingestion, cfg.Storage.DefaultDataDir, logger)
}
```

The remote source replaces the **live** opener only:

```go
func resolveCore(opts daemonOptions, cfg config.Config, logger *supportlog.Entry) (resolvedCore, error) {
	if opts.Core != nil {
		return resolvedCore{live: opts.Core, backfill: opts.Core}, nil
	}
	if url := cfg.Ingestion.RemoteCoreURL; url != "" {
		// Live ingest follows the remote stream; backfill still replays locally,
		// because a backfill chunk is a bounded range of old ledgers and a remote
		// streamer only retains the recent ones.
		core, err := newCaptiveCoreOpeners(cfg.Ingestion, cfg.Storage.DefaultDataDir, logger)
		if err != nil {
			return resolvedCore{}, err
		}
		core.live = &remoteCoreOpener{url: url}
		return core, nil
	}
	return newCaptiveCoreOpeners(cfg.Ingestion, cfg.Storage.DefaultDataDir, logger)
}
```

`RemoteCoreURL` is a new `[ingestion]` key (`remote_core_url`), empty by default, so every existing deployment keeps its current behaviour.

## What the RPC side must accept

- **Retention is the remote's, not RPC's.** A resume ledger older than `corestreamd`'s retention makes the first pull yield `remoteledger.ErrTooFarBehind`, which carries the retained bounds. That is a supervised-restart-and-backfill condition, not a crash loop: ingest should treat it the way it treats a resume gap today.
- **Falling behind disconnects you.** If the ingest loop stalls longer than the server's per-subscriber queue, the server drops the connection with `ErrSlowConsumer` rather than stall its own core. A resume from the last ingested ledger is the recovery, and it succeeds as long as retention still covers it.
- **A clean close ends the loop.** Live ingest pulls an unbounded range, so a `corestreamd` whose source ended closes normally and `RawLedgers` simply stops yielding — the same shape as a captive core process exiting, which the loop already handles by restarting. (Bounded consumers, which ingest is not, get `remoteledger.ErrTruncated` instead of a silent short delivery.)
- **Backfill still needs a local source.** Bounded historical replays (`backfill` opener) stay on captive core or the datastore. Serving them remotely would mean replicating retention, which this prototype deliberately does not do.
- **Core's HTTP query server moves with core.** The captive path enables core's query servers because the serving endpoints query them (`applyHTTPServers`, daemon.go). Those endpoints are on the `corestreamd` box, so a deployment that needs them must reach that box directly. This prototype streams ledgers only.
- **Trust and transport.** The prototype is plaintext with no auth. Anything beyond a benchmark needs `wss://` plus a token, both of which are `remoteledger.WithHTTPClient` and a header away, but neither is implemented here.

## What stays exactly the same

The hot loop, its resume logic, the borrowed-slice discipline (the client's yielded slice is its read buffer, like captive core's), and every store below ingest. The seam did its job.

One seam limitation to know about: `ledgerbackend.StreamOption` — the `WithStreamMetrics` mechanism — mutates an unexported `streamConfig`, so no implementation outside the SDK can read one. `remoteledger` accepts options and ignores them. RPC v2 passes none today (nothing under `cmd/` calls `WithStreamMetrics`), so nothing regresses; a future caller that starts instrumenting `RawLedgers` would silently get no ledger-fetch metrics from a remote stream, and would need to measure on its own side instead.

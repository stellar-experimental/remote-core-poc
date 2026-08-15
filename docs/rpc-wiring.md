# Wiring `remoteledger` into stellar-rpc v2

This is the complete diff that moves RPC v2 **live** ingest off local captive core and onto a `corestreamd` box. It is **documented, not applied** — nothing in this prototype changes the `stellar-rpc` repo.

Every identifier the diff uses is either introduced by it or already exists in stellar-rpc-2 at the file and line cited below. Paths are relative to the `stellar-rpc` repo root, and line numbers are from the checkout at `/Users/marwen/platform/stellar-rpc-2`.

## Why it is small

RPC v2 ingest consumes exactly one seam. `startup.go:105` opens a stream and the hot loop iterates it:

```go
// cmd/stellar-rpc/internal/rpcv2/startup.go:105
stream, err := cfg.Core.OpenCore(ctx)

// cmd/stellar-rpc/internal/rpcv2/hotloop.go:185
for raw, verr := range cfg.Stream.RawLedgers(ctx, ledgerbackend.UnboundedRange(cfg.Resume)) {
```

`cfg.Core` is a `CoreOpener` (`startup.go:335`), whose only method returns a `ledgerbackend.LedgerStream`:

```go
type CoreOpener interface {
	OpenCore(ctx context.Context) (ledgerbackend.LedgerStream, error)
}
```

`remoteledger.Stream` **is** a `ledgerbackend.LedgerStream`. So the change is one new opener next to `captiveCoreOpener` (`daemon.go:493`) and one config key that selects it. Ingest, the hot loop, resume, retention and the stores are untouched.

## Existing identifiers the diff relies on

| Identifier | Where it already is |
| --- | --- |
| `CoreOpener`, `OpenCore` | `cmd/stellar-rpc/internal/rpcv2/startup.go:335` |
| `resolvedCore` (fields `live`, `backfill`, `networkPassphrase`, `binaryPath`) | `cmd/stellar-rpc/internal/rpcv2/daemon.go:277` |
| `resolveCore`, `daemonOptions` | `daemon.go:299` |
| `newCaptiveCoreOpeners` | `daemon.go:508` |
| `captiveCoreOpener` | `daemon.go:493` |
| `config.IngestionConfig`, field style of `StellarCoreBinaryPath` | `cmd/stellar-rpc/internal/rpcv2/config/config.go:312`, `:320-322` |
| `validateIngestion`, the `key*` name constants | `cmd/stellar-rpc/internal/rpcv2/config_validate.go:110`, `:98-103` |
| `ledgerbackend`, `supportlog`, `config` imports in `daemon.go` | `daemon.go:17`, `:18`, `:26` |
| `fmt`, `net/url` imports in `config_validate.go` | `config_validate.go:3-17` |

## 1. `go.mod`

```diff
--- a/go.mod
+++ b/go.mod
@@ -29,6 +29,7 @@ require (
 	github.com/spf13/cobra v1.7.0
 	github.com/spf13/pflag v1.0.5
 	github.com/stellar/go-stellar-sdk v0.7.1
+	github.com/stellar-experimental/remote-core-poc v0.1.0
 	github.com/stellar/streamhash v0.0.0-20260713164615-c72a4e6f578d
 	github.com/stretchr/testify v1.11.1
 )
```

The prototype is not published, so until it is, point the require at a checkout:

```diff
+replace github.com/stellar-experimental/remote-core-poc => ../remote-core-poc
```

`go mod tidy` then adds `github.com/coder/websocket` as an indirect dependency — the only new transitive dependency.

**On the SDK version.** This prototype pins `go-stellar-sdk v0.6.1-0.20260716145807-2bfffb159f36`; stellar-rpc-2 now pins `v0.7.1`. Minimal version selection resolves the pair to v0.7.1, so the prototype compiles against the newer SDK in that build. That is safe: `LedgerStream`, the `Range` accessors and constructors, and `NewCaptiveCoreStream` are unchanged between the two versions, and this module builds and its tests pass with the SDK bumped to v0.7.1 (verified by building it that way, not by reading the diff).

## 2. `cmd/stellar-rpc/internal/rpcv2/config/config.go` — the key

```diff
--- a/cmd/stellar-rpc/internal/rpcv2/config/config.go
+++ b/cmd/stellar-rpc/internal/rpcv2/config/config.go
@@ -320,6 +320,13 @@ type IngestionConfig struct {
 	// StellarCoreBinaryPath is the path to the stellar-core binary. Optional —
 	// defaults to the "stellar-core" found on PATH.
 	StellarCoreBinaryPath string `toml:"stellar_core_binary_path"`
+	// RemoteCoreURL is a corestreamd that owns captive core on another machine
+	// (stellar-rpc#906). Optional — empty means this daemon runs its own live
+	// captive core, which is the default and every existing deployment. When set,
+	// LIVE ingestion follows that stream instead; the keys above still describe
+	// the local core, which backfill's bounded replays continue to use, so they
+	// remain required.
+	RemoteCoreURL string `toml:"remote_core_url"`
 	// CaptiveCoreStoragePath is captive core's BUCKET_DIR_PATH base; optional,
 	// defaults to {default_data_dir}/captive-core.
 	CaptiveCoreStoragePath string `toml:"captive_core_storage_path"`
```

## 3. `cmd/stellar-rpc/internal/rpcv2/config_validate.go` — reject a malformed URL at startup

```diff
--- a/cmd/stellar-rpc/internal/rpcv2/config_validate.go
+++ b/cmd/stellar-rpc/internal/rpcv2/config_validate.go
@@ -99,6 +99,7 @@ const (
 	keyCoreHTTPPort             = "core_http_port"
 	keyCoreHTTPQueryPort        = "core_http_query_port"
 	keyCoreQueryThreadPoolSize  = "core_http_query_thread_pool_size"
 	keyCoreQuerySnapshotLedgers = "core_http_query_snapshot_ledgers"
+	keyRemoteCoreURL            = "remote_core_url"
 )
@@ -133,7 +134,32 @@ func validateIngestion(ing config.IngestionConfig) error {
 	if *ing.CoreRequestTimeout < minConfiguredDuration {
 		return fmt.Errorf("[ingestion].core_request_timeout is %v — durations below 1ms are rejected; "+
 			"a bare TOML integer parses as nanoseconds, write a string like \"2s\"", *ing.CoreRequestTimeout)
 	}
+	if err := validateRemoteCoreURL(ing.RemoteCoreURL); err != nil {
+		return err
+	}
 	return validateCoreURL(ing.CoreURL, *ing.CoreHTTPPort)
 }
+
+// validateRemoteCoreURL form-validates [ingestion].remote_core_url. Empty is the
+// unset sentinel: this daemon runs its own live core.
+//
+// A bad URL here would otherwise surface as a dial failure on the first ledger
+// pull, after the daemon has opened its stores and reported healthy. The easy
+// mistake is omitting the scheme ("core-box:8462"), which url.Parse accepts as a
+// path.
+func validateRemoteCoreURL(remoteURL string) error {
+	if remoteURL == "" {
+		return nil
+	}
+	u, err := url.Parse(remoteURL)
+	if err != nil {
+		return fmt.Errorf("[ingestion].%s is not a URL: %w", keyRemoteCoreURL, err)
+	}
+	switch u.Scheme {
+	case "ws", "wss", "http", "https":
+	default:
+		return fmt.Errorf("[ingestion].%s must use ws, wss, http or https (got %q in %q)",
+			keyRemoteCoreURL, u.Scheme, remoteURL)
+	}
+	if u.Host == "" {
+		return fmt.Errorf("[ingestion].%s has no host (got %q)", keyRemoteCoreURL, remoteURL)
+	}
+	return nil
+}
```

## 4. `cmd/stellar-rpc/internal/rpcv2/daemon.go` — the opener and the switch

```diff
--- a/cmd/stellar-rpc/internal/rpcv2/daemon.go
+++ b/cmd/stellar-rpc/internal/rpcv2/daemon.go
@@ -17,6 +17,7 @@ import (
 	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
 	supportlog "github.com/stellar/go-stellar-sdk/support/log"
 	"github.com/stellar/go-stellar-sdk/support/storage"
+	"github.com/stellar-experimental/remote-core-poc/remoteledger"
 
 	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
 	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
@@ -296,9 +297,31 @@ type resolvedCore struct {
 // resolveCore returns the injected opener (tests use it for both roles) or the
 // production pair built from [ingestion].
 func resolveCore(opts daemonOptions, cfg config.Config, logger *supportlog.Entry) (resolvedCore, error) {
 	if opts.Core != nil {
 		return resolvedCore{live: opts.Core, backfill: opts.Core}, nil
 	}
-	return newCaptiveCoreOpeners(cfg.Ingestion, cfg.Storage.DefaultDataDir, logger)
+	core, err := newCaptiveCoreOpeners(cfg.Ingestion, cfg.Storage.DefaultDataDir, logger)
+	if err != nil {
+		return resolvedCore{}, err
+	}
+	if url := cfg.Ingestion.RemoteCoreURL; url != "" {
+		// Live ingest follows the remote stream; backfill keeps the local opener,
+		// because a backfill chunk is a bounded range of old ledgers and a remote
+		// streamer only retains the recent ones.
+		logger.WithField("remote_core_url", url).Info("live ingestion will follow a remote core stream")
+		core.live = &remoteCoreOpener{url: url}
+	}
+	return core, nil
 }
+
+// remoteCoreOpener opens the live ingestion stream from a corestreamd that owns
+// captive core on another machine (stellar-rpc#906).
+type remoteCoreOpener struct {
+	url string
+}
+
+// OpenCore returns the live ingestion stream backed by the remote core streamer.
+//
+// It ignores ctx exactly as captiveCoreOpener only stores it on the config: the
+// stream owns its own lifecycle, dialling on the first RawLedgers pull and
+// closing when the hot loop's iteration ends, so the context that matters is the
+// one that loop passes to RawLedgers.
+func (o *remoteCoreOpener) OpenCore(context.Context) (ledgerbackend.LedgerStream, error) {
+	return remoteledger.New(o.url), nil
+}
```

`resolvedCore.networkPassphrase` and `.binaryPath` keep the values `newCaptiveCoreOpeners` resolved, so `corestate` still reports a core version and the datastore's wrong-network check still runs — both read the local core's config, which a remote live stream does not replace.

## 5. Deployment config

```toml
[ingestion]
captive_core_config = "/etc/stellar/captive-core.cfg"   # still required: backfill uses it
history_archive_urls = ["https://history.stellar.org/prd/core-live/core_live_001"]
remote_core_url = "ws://core-box.internal:8462"          # new: live ingest follows this
```

## What the RPC side must accept

- **Retention is the remote's, not RPC's.** A resume ledger older than `corestreamd`'s retention makes the first pull yield `remoteledger.ErrTooFarBehind`, which carries the retained bounds. That is a backfill-and-resume condition, not a crash loop: ingest should treat it the way it treats a resume gap today.
- **Falling behind is survivable up to retention.** An ingest loop that stalls is not disconnected: the server serves the missed ledgers back out of its retention ring when the loop resumes. Only a stall long enough to fall out of retention ends the stream, with `remoteledger.ErrTooFarBehind` carrying the retained bounds — the same backfill-and-resume condition as a stale resume ledger.
- **A clean close ends the loop.** Live ingest pulls an unbounded range, so a `corestreamd` whose source ended closes normally and `RawLedgers` simply stops yielding — the same shape as a captive core process exiting, which the loop already handles by restarting. (Bounded consumers, which ingest is not, get `remoteledger.ErrTruncated` instead of a silent short delivery.)
- **Backfill still needs a local source.** Bounded historical replays stay on captive core or the datastore. Serving them remotely would mean replicating retention, which this prototype deliberately does not do.
- **Core's HTTP query server moves with core.** The captive path enables core's query servers because serving endpoints query them (`applyHTTPServers`, `daemon.go:650`). Those endpoints are on the `corestreamd` box, so a deployment that needs them must reach that box directly — `[ingestion].core_url` and the query port describe a core this daemon no longer runs. Resolving that split is out of scope here; this prototype streams ledgers only.
- **Trust and transport.** The prototype is plaintext with no auth. Anything beyond a benchmark needs `wss://` plus a token, which are `remoteledger.WithHTTPClient` and a header away, but neither is implemented here.

## What stays exactly the same

The hot loop, its resume logic, the borrowed-slice discipline (the client's yielded slice is its read buffer, like captive core's), and every store below ingest. The seam did its job.

One seam limitation to know about: `ledgerbackend.StreamOption` — the `WithStreamMetrics` mechanism — mutates an unexported `streamConfig`, so no implementation outside the SDK can read one. `remoteledger` accepts options and ignores them. RPC v2 passes none today (nothing under `cmd/` calls `WithStreamMetrics`), so nothing regresses; a future caller that starts instrumenting `RawLedgers` would silently get no ledger-fetch metrics from a remote stream and would have to measure on its own side.

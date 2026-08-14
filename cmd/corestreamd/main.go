// Command corestreamd owns a ledger source on one machine — captive
// stellar-core, or a synthetic stand-in — and streams its raw ledger XDR to
// remote subscribers over WebSocket.
//
//	# synthetic, no core binary and no network needed:
//	corestreamd --source synthetic --synthetic-interval 1s
//
//	# real captive core replaying pubnet from a known ledger:
//	corestreamd --source captive --start-ledger 55000000 \
//	  --core-binary /usr/bin/stellar-core --core-config ./captive-core.cfg \
//	  --history-archive-urls https://history.stellar.org/prd/core-live/core_live_001 \
//	  --storage-dir /data/core
//
// Subscribers connect with github.com/stellar-experimental/remote-core-poc/remoteledger.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar-experimental/remote-core-poc/internal/server"
	"github.com/stellar-experimental/remote-core-poc/internal/store"
	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

func main() {
	if err := run(); err != nil {
		slog.Error("corestreamd failed", "error", err)
		os.Exit(1)
	}
}

type options struct {
	listen      string
	source      string
	retention   int
	startLedger uint
	buffer      int
	dataDir     string
	logLevel    string

	coreBinary        string
	coreConfig        string
	historyArchiveURL string
	storageDir        string
	networkPassphrase string

	syntheticSize     int
	syntheticInterval time.Duration
	syntheticCount    uint
}

// parseFlags reads args, writing usage and flag errors to out.
func parseFlags(out io.Writer, args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("corestreamd", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&o.listen, "listen", ":8462", "address to serve the stream and /healthz on")
	fs.StringVar(&o.source, "source", "synthetic", "ledger source: captive|synthetic")
	fs.IntVar(&o.retention, "retention", 10_000, "ledgers to keep on disk for replay")
	fs.UintVar(&o.startLedger, "start-ledger", 0, "first ledger to stream (required for captive; defaults to 1 for synthetic)")
	fs.IntVar(&o.buffer, "buffer", server.DefaultSubscriberBuffer, "per-subscriber queue depth before it is disconnected as too slow")
	fs.StringVar(&o.dataDir, "data-dir", "corestreamd-data", "directory for the retention ring")
	fs.StringVar(&o.logLevel, "log-level", "info", "log level: debug|info|warn|error")

	fs.StringVar(&o.coreBinary, "core-binary", "", "stellar-core binary (default: found on PATH)")
	fs.StringVar(&o.coreConfig, "core-config", "", "captive-core toml path")
	fs.StringVar(&o.historyArchiveURL, "history-archive-urls", "", "comma-separated history archive URLs")
	fs.StringVar(&o.storageDir, "storage-dir", "", "captive core storage path (default: <data-dir>/captive-core)")
	fs.StringVar(&o.networkPassphrase, "network-passphrase", "", "override the NETWORK_PASSPHRASE in the captive-core toml")

	fs.IntVar(&o.syntheticSize, "synthetic-size", server.DefaultSyntheticSize, "synthetic ledger payload bytes")
	fs.DurationVar(&o.syntheticInterval, "synthetic-interval", time.Second, "synthetic ledger close interval")
	fs.UintVar(&o.syntheticCount, "synthetic-count", 0, "synthetic ledgers to emit (0 = endless)")

	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.source != "captive" && o.source != "synthetic" {
		return o, fmt.Errorf("--source must be captive or synthetic, got %q", o.source)
	}
	if uint64(o.startLedger) > math.MaxUint32 {
		return o, fmt.Errorf("--start-ledger %d does not fit in a ledger sequence", o.startLedger)
	}
	if uint64(o.syntheticCount) > math.MaxUint32 {
		return o, fmt.Errorf("--synthetic-count %d is too large", o.syntheticCount)
	}
	// The protocol has no sequence wrap, so the last representable ledger is not
	// streamable. Refusing a range that reaches it here keeps the retention ring
	// from ever holding a ledger no subscriber could ask to continue from.
	if last := lastSequence(o); last >= math.MaxUint32 {
		return o, fmt.Errorf(
			"the requested range reaches ledger %d, the last representable sequence, which is not streamable; "+
				"lower --start-ledger or --synthetic-count", uint32(math.MaxUint32))
	}
	// 256 MiB is the protocol's payload cap, matching the SDK's captive-core frame
	// cap. It is not operator-tunable, so an oversized synthetic ledger is a flag
	// error rather than a knob.
	if int64(o.syntheticSize) > wire.DefaultMaxPayloadSize {
		return o, fmt.Errorf("--synthetic-size %d exceeds the %d-byte protocol payload cap",
			o.syntheticSize, wire.DefaultMaxPayloadSize)
	}
	if o.source == "captive" {
		if o.startLedger == 0 {
			// Resolving "tip" needs a history archive round-trip this prototype
			// does not do; a benchmark replays a known range anyway.
			return o, errors.New("--start-ledger is required with --source captive")
		}
		if o.coreConfig == "" {
			return o, errors.New("--core-config is required with --source captive")
		}
		if o.historyArchiveURL == "" {
			return o, errors.New("--history-archive-urls is required with --source captive")
		}
	}
	return o, nil
}

// lastSequence is the highest ledger the configured range is known to reach,
// computed in uint64 so the arithmetic itself cannot wrap.
//
// An endless synthetic source and a captive source are both unbounded, so their
// highest sequence is unknown here and only the start is reported. Such a run
// would have to stream billions of ledgers to reach the ceiling, and if it ever
// did, the retention ring refuses the wrapped sequence and the source loop fails
// loudly rather than serving nonsense.
func lastSequence(o options) uint64 {
	start := uint64(o.startLedger)
	if start == 0 {
		start = 1
	}
	if o.source == "synthetic" && o.syntheticCount > 0 {
		return start + uint64(o.syntheticCount) - 1
	}
	return start
}

func run() error {
	o, err := parseFlags(os.Stderr, os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	logger := newLogger(o.logLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	start := uint32(o.startLedger)
	if start == 0 {
		start = 1
	}

	var (
		source ledgerbackend.LedgerStream
		rng    ledgerbackend.Range
	)
	switch o.source {
	case "synthetic":
		source = server.NewSyntheticStream(server.SyntheticConfig{
			Size:     o.syntheticSize,
			Interval: o.syntheticInterval,
		})
		rng = server.SyntheticRange(start, uint32(o.syntheticCount))
	case "captive":
		storageDir := o.storageDir
		if storageDir == "" {
			storageDir = filepath.Join(o.dataDir, "captive-core")
		}
		source, err = server.NewCaptiveSource(ctx, server.CaptiveConfig{
			BinaryPath:         o.coreBinary,
			ConfigPath:         o.coreConfig,
			StoragePath:        storageDir,
			HistoryArchiveURLs: splitURLs(o.historyArchiveURL),
			NetworkPassphrase:  o.networkPassphrase,
		}, supportlog.New())
		if err != nil {
			return err
		}
		rng = ledgerbackend.UnboundedRange(start)
	}

	ring, err := store.Open(filepath.Join(o.dataDir, "ledgers"), o.retention)
	if err != nil {
		return err
	}

	srv, err := server.New(server.Config{
		Source:           source,
		Range:            rng,
		Store:            ring,
		SubscriberBuffer: o.buffer,
		Logger:           logger,
	})
	if err != nil {
		return err
	}

	httpSrv := &http.Server{
		Handler: srv.Handler(),
		// A subscription is a long-lived stream, so no read/write timeouts here;
		// the stream handler bounds its own writes and pings its peer.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", o.listen, err)
	}
	logger.Info("corestreamd listening", "addr", ln.Addr().String(), "source", o.source,
		"start_ledger", start, "retention", o.retention)

	return runServices(ctx, srv, httpSrv, ln, logger)
}

// shutdownTimeout bounds the graceful HTTP shutdown once the process is on its
// way out.
const shutdownTimeout = 5 * time.Second

// runServices runs the source loop and the HTTP server until the first of them
// returns, then stops the other and waits for it.
//
// Neither component is useful alone: a source loop with no listener streams to
// nobody, and a listener with no source hands every subscriber an empty stream.
// So the first one to return — whether it failed or simply finished — ends the
// process, and its error (if any) is what the caller reports.
func runServices(
	ctx context.Context, srv *server.Server, httpSrv *http.Server, ln net.Listener, logger *slog.Logger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Cancelling on the way out is what makes the other component stop.
		defer cancel()
		errs <- srv.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		err := httpSrv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil // our own shutdown, not a failure
		}
		if err != nil {
			err = fmt.Errorf("http server: %w", err)
		}
		errs <- err
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("http shutdown did not finish cleanly", "error", err)
		}
	}()

	wg.Wait()
	close(errs)
	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	if joined != nil {
		logger.Error("corestreamd stopping on failure", "error", joined)
	} else {
		logger.Info("corestreamd stopped")
	}
	return joined
}

func splitURLs(csv string) []string {
	var urls []string
	for _, u := range strings.Split(csv, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	return urls
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

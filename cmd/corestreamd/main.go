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
// Subscribers connect with github.com/stellar/remote-core-poc/remoteledger.
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
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/remote-core-poc/internal/server"
	"github.com/stellar/remote-core-poc/internal/store"
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

	serveErr := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	// The source loop decides the process lifetime: when the source ends, the
	// service has nothing left to serve.
	runErr := srv.Run(ctx)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown did not finish cleanly", "error", err)
	}
	return errors.Join(runErr, <-serveErr)
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

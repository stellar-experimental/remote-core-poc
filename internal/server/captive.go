package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// CaptiveConfig describes the captive stellar-core this server owns.
type CaptiveConfig struct {
	// BinaryPath is the stellar-core binary. Empty looks it up on PATH, the way
	// the RPC daemon does.
	BinaryPath string

	// ConfigPath is the captive-core toml. Required.
	ConfigPath string

	// StoragePath is core's bucket/database directory. Required.
	StoragePath string

	// HistoryArchiveURLs are the archives core and the SDK read. Required.
	HistoryArchiveURLs []string

	// NetworkPassphrase overrides the NETWORK_PASSPHRASE the toml declares.
	// Empty reads it from the toml.
	NetworkPassphrase string

	// UserAgent is sent on history archive requests. Empty uses
	// DefaultCaptiveUserAgent.
	UserAgent string
}

// DefaultCaptiveUserAgent identifies this prototype to history archives.
const DefaultCaptiveUserAgent = "remote-core-poc"

// NewCaptiveSource builds the captive-core LedgerStream the server streams from.
//
// The config is assembled exactly the way stellar-rpc v2's daemon assembles its
// live ingestion core (newCaptiveCoreOpener): the toml comes from the file's
// bytes through NewCaptiveCoreTomlFromData with Strict params, and the same
// passphrase is used for the params, the cross-check and CaptiveCoreConfig. The
// stream starts core on the first pull and shuts it down when iteration ends.
func NewCaptiveSource(ctx context.Context, cfg CaptiveConfig, logger *supportlog.Entry) (ledgerbackend.LedgerStream, error) {
	if cfg.ConfigPath == "" {
		return nil, errors.New("captive: config path is required")
	}
	if cfg.StoragePath == "" {
		return nil, errors.New("captive: storage path is required")
	}
	if len(cfg.HistoryArchiveURLs) == 0 {
		return nil, errors.New("captive: at least one history archive URL is required")
	}
	if logger == nil {
		logger = supportlog.New()
	}

	data, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("captive: read config %q: %w", cfg.ConfigPath, err)
	}

	passphrase := cfg.NetworkPassphrase
	if passphrase == "" {
		passphrase, err = peekNetworkPassphrase(data)
		if err != nil {
			return nil, fmt.Errorf("captive: config %q: %w", cfg.ConfigPath, err)
		}
	}

	binaryPath := cfg.BinaryPath
	if binaryPath == "" {
		found, lerr := exec.LookPath("stellar-core")
		if lerr != nil {
			return nil, fmt.Errorf("captive: no --core-binary given and stellar-core is not on PATH: %w", lerr)
		}
		binaryPath = found
	}

	if err := os.MkdirAll(cfg.StoragePath, 0o755); err != nil {
		return nil, fmt.Errorf("captive: create storage path %q: %w", cfg.StoragePath, err)
	}
	storagePath, err := filepath.Abs(cfg.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("captive: resolve storage path %q: %w", cfg.StoragePath, err)
	}

	userAgent := cfg.UserAgent
	if userAgent == "" {
		userAgent = DefaultCaptiveUserAgent
	}

	// The params are a cross-check, not an override: the SDK rejects a file that
	// disagrees with them. HTTP-server params are left out on purpose, so core
	// binds no ports and the SDK's query-server mismatch branches stay
	// unreachable (they format their error with fields this service never sets).
	params := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:                  passphrase,
		HistoryArchiveURLs:                 cfg.HistoryArchiveURLs,
		Strict:                             true,
		EnforceSorobanDiagnosticEvents:     true,
		EnforceSorobanTransactionMetaExtV1: true,
		EmitUnifiedEvents:                  true,
		CoreBinaryPath:                     binaryPath,
	}
	coreToml, err := ledgerbackend.NewCaptiveCoreTomlFromData(data, params)
	if err != nil {
		return nil, fmt.Errorf("captive: invalid config %q: %w", cfg.ConfigPath, err)
	}

	coreLog := logger.WithField("subservice", "stellar-core")
	return ledgerbackend.NewCaptiveCoreStream(ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         binaryPath,
		StoragePath:        storagePath,
		NetworkPassphrase:  passphrase,
		HistoryArchiveURLs: cfg.HistoryArchiveURLs,
		Log:                coreLog,
		Toml:               coreToml,
		UserAgent:          userAgent,
		Context:            ctx,
	}, coreLog), nil
}

// peekNetworkPassphrase reads NETWORK_PASSPHRASE out of a captive-core toml.
//
// It scans the top-level keys itself rather than pulling in a toml parser: the
// value is needed BEFORE the SDK parses the file, because the SDK takes the
// passphrase as a parameter and rejects a file that disagrees with it. That
// rejection is also the safety net here — a value this scan got wrong surfaces
// as the SDK's mismatch error, never as a core started on the wrong network.
//
// It understands the form captive-core configs use, a quoted string on one line
// before the first [section]. Anything else it cannot read, the operator passes
// with --network-passphrase.
func peekNetworkPassphrase(data []byte) (string, error) {
	const key = "NETWORK_PASSPHRASE"
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			break // top-level keys end at the first table
		}
		name, value, found := strings.Cut(line, "=")
		if !found || strings.TrimSpace(name) != key {
			continue
		}
		passphrase, ok := unquoteTomlString(strings.TrimSpace(value))
		if !ok {
			return "", fmt.Errorf("%s is not a quoted string; pass --network-passphrase instead", key)
		}
		return passphrase, nil
	}
	return "", fmt.Errorf("%s is not defined; define it or pass --network-passphrase", key)
}

// unquoteTomlString takes the quoted string at the start of s, ignoring any
// trailing comment. Escapes are not interpreted: a network passphrase has none.
func unquoteTomlString(s string) (string, bool) {
	if len(s) < 2 {
		return "", false
	}
	quote := s[0]
	if quote != '"' && quote != '\'' {
		return "", false
	}
	end := strings.IndexByte(s[1:], quote)
	if end < 0 {
		return "", false
	}
	return s[1 : 1+end], true
}

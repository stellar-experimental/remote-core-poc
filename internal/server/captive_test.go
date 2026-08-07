package server

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

const testPassphrase = "Test SDF Network ; September 2015"

// testCoreConfig writes a captive-core toml the SDK accepts in Strict mode.
func testCoreConfig(t *testing.T, passphraseLine string) string {
	t.Helper()
	body := passphraseLine + `
DATABASE="sqlite3://stellar.db"

[[HOME_DOMAINS]]
HOME_DOMAIN="testnet.stellar.org"
QUALITY="HIGH"

[[VALIDATORS]]
NAME="sdf_testnet_1"
HOME_DOMAIN="testnet.stellar.org"
PUBLIC_KEY="GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y"
ADDRESS="core-testnet1.stellar.org"
`
	path := filepath.Join(t.TempDir(), "captive-core.cfg")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// fakeCoreBinary stands in for stellar-core. The SDK introspects the binary's
// version while building the toml, so the path has to be executable and print
// something version-shaped.
func fakeCoreBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stellar-core")
	script := "#!/bin/sh\necho \"stellar-core 22.1.0 (fake)\"\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake core: %v", err)
	}
	return path
}

func quietLogger(t *testing.T) *supportlog.Entry {
	t.Helper()
	logger := supportlog.New()
	logger.SetOutput(io.Discard)
	return logger
}

func TestNewCaptiveSourceBuildsStream(t *testing.T) {
	cfg := CaptiveConfig{
		BinaryPath:         fakeCoreBinary(t),
		ConfigPath:         testCoreConfig(t, `NETWORK_PASSPHRASE="`+testPassphrase+`"`),
		StoragePath:        filepath.Join(t.TempDir(), "core"),
		HistoryArchiveURLs: []string{"https://history.stellar.org/prd/core-testnet/core_testnet_001"},
	}
	source, err := NewCaptiveSource(t.Context(), cfg, quietLogger(t))
	if err != nil {
		t.Fatalf("NewCaptiveSource: %v", err)
	}
	if source == nil {
		t.Fatal("NewCaptiveSource returned no stream")
	}
	// The storage path is created up front, the way core expects to find it.
	if _, err := os.Stat(cfg.StoragePath); err != nil {
		t.Errorf("storage path was not created: %v", err)
	}
}

func TestNewCaptiveSourceRejectsPassphraseMismatch(t *testing.T) {
	_, err := NewCaptiveSource(t.Context(), CaptiveConfig{
		BinaryPath:         fakeCoreBinary(t),
		ConfigPath:         testCoreConfig(t, `NETWORK_PASSPHRASE="`+testPassphrase+`"`),
		StoragePath:        filepath.Join(t.TempDir(), "core"),
		HistoryArchiveURLs: []string{"https://history.stellar.org/prd/core-testnet/core_testnet_001"},
		NetworkPassphrase:  "Public Global Stellar Network ; September 2015",
	}, quietLogger(t))
	if err == nil {
		t.Fatal("a passphrase that disagrees with the config was accepted")
	}
	if !strings.Contains(err.Error(), "NETWORK_PASSPHRASE") {
		t.Errorf("error = %v, want it to name the mismatched NETWORK_PASSPHRASE", err)
	}
}

func TestNewCaptiveSourceValidation(t *testing.T) {
	good := CaptiveConfig{
		BinaryPath:         fakeCoreBinary(t),
		ConfigPath:         testCoreConfig(t, `NETWORK_PASSPHRASE="`+testPassphrase+`"`),
		StoragePath:        filepath.Join(t.TempDir(), "core"),
		HistoryArchiveURLs: []string{"https://history.stellar.org/prd/core-testnet/core_testnet_001"},
	}
	tests := map[string]func(c *CaptiveConfig){
		"no config path":  func(c *CaptiveConfig) { c.ConfigPath = "" },
		"no storage path": func(c *CaptiveConfig) { c.StoragePath = "" },
		"no archives":     func(c *CaptiveConfig) { c.HistoryArchiveURLs = nil },
		"missing config":  func(c *CaptiveConfig) { c.ConfigPath = filepath.Join(t.TempDir(), "absent.cfg") },
		"no passphrase":   func(c *CaptiveConfig) { c.ConfigPath = testCoreConfig(t, "# no passphrase here") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := good
			mutate(&cfg)
			if _, err := NewCaptiveSource(t.Context(), cfg, quietLogger(t)); err == nil {
				t.Error("NewCaptiveSource succeeded, want an error")
			}
		})
	}
}

func TestPeekNetworkPassphrase(t *testing.T) {
	tests := []struct {
		name    string
		toml    string
		want    string
		wantErr bool
	}{
		{"plain", `NETWORK_PASSPHRASE="` + testPassphrase + `"`, testPassphrase, false},
		{"spaces around =", `NETWORK_PASSPHRASE = "x"`, "x", false},
		{"single quotes", `NETWORK_PASSPHRASE='x'`, "x", false},
		{"trailing comment", `NETWORK_PASSPHRASE="x" # the network`, "x", false},
		{"indented", "  NETWORK_PASSPHRASE=\"x\"\n", "x", false},
		{"other keys first", "HTTP_PORT=11626\nNETWORK_PASSPHRASE=\"x\"\n", "x", false},
		{"commented out", "#NETWORK_PASSPHRASE=\"x\"\n", "", true},
		{"absent", "HTTP_PORT=11626\n", "", true},
		{"inside a table", "[[VALIDATORS]]\nNETWORK_PASSPHRASE=\"x\"\n", "", true},
		{"unquoted", "NETWORK_PASSPHRASE=x\n", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := peekNetworkPassphrase([]byte(tt.toml))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("peekNetworkPassphrase(%q) = %q, want an error", tt.toml, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("peekNetworkPassphrase(%q): %v", tt.toml, err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

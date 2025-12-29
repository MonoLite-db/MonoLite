// Package testkit provides testing utilities for MonoLite.
package testkit

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/monolite/monodb/engine"
	"github.com/monolite/monodb/protocol"
)

// MonoLiteHandle provides access to a test MonoLite instance.
type MonoLiteHandle struct {
	Addr     string
	DB       *engine.Database
	Srv      *protocol.Server
	DataPath string
	t        *testing.T
}

// MonoLiteOptions configures the test server.
type MonoLiteOptions struct {
	// MaxCacheSize limits the pager cache (for testing cache eviction)
	MaxCacheSize int
	// WALEnabled controls whether WAL is enabled
	WALEnabled bool
	// DataPath overrides the temp directory (if empty, uses t.TempDir())
	DataPath string
}

// DefaultOptions returns sensible defaults for testing.
func DefaultOptions() MonoLiteOptions {
	return MonoLiteOptions{
		MaxCacheSize: 0, // use default
		WALEnabled:   true,
		DataPath:     "",
	}
}

// StartMonoLite starts a test MonoLite server with default options.
func StartMonoLite(t *testing.T) *MonoLiteHandle {
	return StartMonoLiteWithOptions(t, DefaultOptions())
}

// StartMonoLiteWithOptions starts a test MonoLite server with custom options.
func StartMonoLiteWithOptions(t *testing.T, opts MonoLiteOptions) *MonoLiteHandle {
	t.Helper()

	// Create temp directory for data
	dataPath := opts.DataPath
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	dbPath := filepath.Join(dataPath, "test.db")

	// Open database
	db, err := engine.OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Find free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	// Start server
	srv := protocol.NewServer(addr, db)
	if err := srv.Start(); err != nil {
		db.Close()
		t.Fatalf("failed to start server: %v", err)
	}

	// Wait for server to be ready
	waitForServer(t, addr, 5*time.Second)

	return &MonoLiteHandle{
		Addr:     addr,
		DB:       db,
		Srv:      srv,
		DataPath: dataPath,
		t:        t,
	}
}

// Close gracefully shuts down the MonoLite instance.
func (h *MonoLiteHandle) Close() {
	if h.Srv != nil {
		if err := h.Srv.Stop(); err != nil {
			h.t.Logf("warning: error stopping server: %v", err)
		}
	}
	if h.DB != nil {
		if err := h.DB.Close(); err != nil {
			h.t.Logf("warning: error closing database: %v", err)
		}
	}
}

// Restart simulates a crash and recovery by closing and reopening.
func (h *MonoLiteHandle) Restart() error {
	// Stop server (simulate crash - don't flush)
	if h.Srv != nil {
		h.Srv.Stop()
		h.Srv = nil
	}
	if h.DB != nil {
		h.DB.Close()
		h.DB = nil
	}

	// Reopen database (recovery will happen)
	dbPath := filepath.Join(h.DataPath, "test.db")
	db, err := engine.OpenDatabase(dbPath)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	h.DB = db

	// Restart server
	srv := protocol.NewServer(h.Addr, db)
	if err := srv.Start(); err != nil {
		db.Close()
		return fmt.Errorf("failed to restart server: %w", err)
	}
	h.Srv = srv

	// Wait for server
	waitForServer(h.t, h.Addr, 5*time.Second)

	return nil
}

// DBPath returns the path to the database file.
func (h *MonoLiteHandle) DBPath() string {
	return filepath.Join(h.DataPath, "test.db")
}

// waitForServer waits for the server to be accepting connections.
func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s did not become ready within %v", addr, timeout)
}

// OpenDatabaseOnly opens a database without starting the server.
// Useful for lower-level testing (pager, btree, etc.).
func OpenDatabaseOnly(t *testing.T) (*engine.Database, string) {
	t.Helper()
	dataPath := t.TempDir()
	dbPath := filepath.Join(dataPath, "test.db")

	db, err := engine.OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db, dataPath
}

// CreateTempDBFile creates a temporary database file path without opening it.
func CreateTempDBFile(t *testing.T) string {
	t.Helper()
	dataPath := t.TempDir()
	return filepath.Join(dataPath, "test.db")
}

// FileExists checks if a file exists.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}


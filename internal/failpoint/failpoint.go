//go:build !failpoint

// Package failpoint provides fault injection for testing.
// This is the default (no-op) implementation used in production builds.
// Build with -tags=failpoint to enable the injectable version.
package failpoint

import "errors"

// ErrInjected is returned when a failpoint is hit and configured to fail.
var ErrInjected = errors.New("failpoint: injected error")

// Hit checks if a failpoint is enabled and returns an error if so.
// In the default build, this always returns nil (no-op).
func Hit(name string) error {
	return nil
}

// HitN checks if the Nth invocation of a failpoint should fail.
// In the default build, this always returns nil (no-op).
func HitN(name string, n int) error {
	return nil
}

// HitWithValue checks a failpoint and returns a configured value.
// In the default build, this returns nil, nil.
func HitWithValue(name string) (interface{}, error) {
	return nil, nil
}

// Enable configures a failpoint to be active.
// In the default build, this is a no-op.
func Enable(name string, cfg interface{}) {
	// no-op in production
}

// EnableWithValue configures a failpoint with a return value.
// In the default build, this is a no-op.
func EnableWithValue(name string, value interface{}) {
	// no-op in production
}

// Disable turns off a failpoint.
// In the default build, this is a no-op.
func Disable(name string) {
	// no-op in production
}

// DisableAll turns off all failpoints.
// In the default build, this is a no-op.
func DisableAll() {
	// no-op in production
}

// IsEnabled returns whether a failpoint is active.
// In the default build, this always returns false.
func IsEnabled(name string) bool {
	return false
}

// List returns all enabled failpoints.
// In the default build, this returns an empty slice.
func List() []string {
	return nil
}

// Failpoint configuration types
const (
	// ConfigAlwaysFail makes the failpoint always return an error
	ConfigAlwaysFail = "always"
	// ConfigFailOnce makes the failpoint fail once then disable
	ConfigFailOnce = "once"
	// ConfigFailN makes the failpoint fail N times then disable
	ConfigFailN = "n"
	// ConfigFailAfterN makes the failpoint pass N times then fail
	ConfigFailAfterN = "after_n"
)

// Config holds failpoint configuration.
type Config struct {
	Type  string      // ConfigAlwaysFail, ConfigFailOnce, ConfigFailN, ConfigFailAfterN
	N     int         // For ConfigFailN and ConfigFailAfterN
	Value interface{} // Optional return value
}


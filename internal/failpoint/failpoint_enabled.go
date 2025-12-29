//go:build failpoint

// Package failpoint provides fault injection for testing.
// This is the enabled implementation used when built with -tags=failpoint.
package failpoint

import (
	"errors"
	"sync"
	"sync/atomic"
)

// ErrInjected is returned when a failpoint is hit and configured to fail.
var ErrInjected = errors.New("failpoint: injected error")

// failpointState holds the state for a single failpoint.
type failpointState struct {
	enabled bool
	config  Config
	counter int64 // atomic counter for hit tracking
}

var (
	mu         sync.RWMutex
	failpoints = make(map[string]*failpointState)
)

// Hit checks if a failpoint is enabled and returns an error if so.
func Hit(name string) error {
	mu.RLock()
	fp, ok := failpoints[name]
	mu.RUnlock()

	if !ok || !fp.enabled {
		return nil
	}

	return processHit(name, fp)
}

// HitN checks if the Nth invocation of a failpoint should fail.
func HitN(name string, n int) error {
	mu.RLock()
	fp, ok := failpoints[name]
	mu.RUnlock()

	if !ok || !fp.enabled {
		return nil
	}

	count := atomic.LoadInt64(&fp.counter)
	if int(count) != n {
		return nil
	}

	return processHit(name, fp)
}

// HitWithValue checks a failpoint and returns a configured value.
func HitWithValue(name string) (interface{}, error) {
	mu.RLock()
	fp, ok := failpoints[name]
	mu.RUnlock()

	if !ok || !fp.enabled {
		return nil, nil
	}

	err := processHit(name, fp)
	if err != nil {
		return fp.config.Value, err
	}
	return nil, nil
}

// processHit handles the logic for a failpoint hit.
func processHit(name string, fp *failpointState) error {
	count := atomic.AddInt64(&fp.counter, 1)

	switch fp.config.Type {
	case ConfigAlwaysFail:
		return ErrInjected

	case ConfigFailOnce:
		if count == 1 {
			// Disable after first hit
			mu.Lock()
			fp.enabled = false
			mu.Unlock()
			return ErrInjected
		}
		return nil

	case ConfigFailN:
		if int(count) <= fp.config.N {
			if int(count) == fp.config.N {
				// Disable after N hits
				mu.Lock()
				fp.enabled = false
				mu.Unlock()
			}
			return ErrInjected
		}
		return nil

	case ConfigFailAfterN:
		if int(count) > fp.config.N {
			return ErrInjected
		}
		return nil

	default:
		// Unknown config type - treat as always fail
		return ErrInjected
	}
}

// Enable configures a failpoint to be active.
func Enable(name string, cfg interface{}) {
	mu.Lock()
	defer mu.Unlock()

	config := Config{Type: ConfigAlwaysFail}

	switch c := cfg.(type) {
	case Config:
		config = c
	case string:
		config.Type = c
	case nil:
		// Use default (always fail)
	}

	failpoints[name] = &failpointState{
		enabled: true,
		config:  config,
		counter: 0,
	}
}

// EnableWithValue configures a failpoint with a return value.
func EnableWithValue(name string, value interface{}) {
	mu.Lock()
	defer mu.Unlock()

	failpoints[name] = &failpointState{
		enabled: true,
		config: Config{
			Type:  ConfigAlwaysFail,
			Value: value,
		},
		counter: 0,
	}
}

// Disable turns off a failpoint.
func Disable(name string) {
	mu.Lock()
	defer mu.Unlock()

	if fp, ok := failpoints[name]; ok {
		fp.enabled = false
	}
}

// DisableAll turns off all failpoints.
func DisableAll() {
	mu.Lock()
	defer mu.Unlock()

	for _, fp := range failpoints {
		fp.enabled = false
	}
}

// IsEnabled returns whether a failpoint is active.
func IsEnabled(name string) bool {
	mu.RLock()
	defer mu.RUnlock()

	fp, ok := failpoints[name]
	return ok && fp.enabled
}

// List returns all enabled failpoints.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	var result []string
	for name, fp := range failpoints {
		if fp.enabled {
			result = append(result, name)
		}
	}
	return result
}

// Reset resets a failpoint's counter without disabling it.
func Reset(name string) {
	mu.Lock()
	defer mu.Unlock()

	if fp, ok := failpoints[name]; ok {
		atomic.StoreInt64(&fp.counter, 0)
	}
}

// GetHitCount returns how many times a failpoint has been hit.
func GetHitCount(name string) int64 {
	mu.RLock()
	defer mu.RUnlock()

	if fp, ok := failpoints[name]; ok {
		return atomic.LoadInt64(&fp.counter)
	}
	return 0
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

// Helper functions for common configurations

// AlwaysError is a convenience config that always fails.
var AlwaysError = Config{Type: ConfigAlwaysFail}

// FailOnce is a convenience config that fails once.
var FailOnce = Config{Type: ConfigFailOnce}

// FailTimes returns a config that fails N times.
func FailTimes(n int) Config {
	return Config{Type: ConfigFailN, N: n}
}

// FailAfter returns a config that passes N times then fails.
func FailAfter(n int) Config {
	return Config{Type: ConfigFailAfterN, N: n}
}


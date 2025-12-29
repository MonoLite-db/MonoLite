// Package goldstandard contains B1-class gold standard tests.
// These tests verify MongoDB semantic compatibility without requiring a MongoDB instance.
package goldstandard

import (
	"testing"

	"github.com/monolite/monodb/engine"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestCompareBSON_Int64Beyond2Pow53 tests B-GOLD-NUM-001:
// CompareBSON must correctly compare int64 values beyond 2^53 (float64 precision limit).
func TestCompareBSON_Int64Beyond2Pow53(t *testing.T) {
	// 2^53 = 9007199254740992 - this is the max integer exactly representable in float64
	pow53 := int64(9007199254740992)
	pow53Plus1 := int64(9007199254740993)

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int // -1, 0, or 1
	}{
		{
			name:     "2^53 vs 2^53+1 (a < b)",
			a:        pow53,
			b:        pow53Plus1,
			expected: -1,
		},
		{
			name:     "2^53+1 vs 2^53 (a > b)",
			a:        pow53Plus1,
			b:        pow53,
			expected: 1,
		},
		{
			name:     "2^53 vs 2^53 (equal)",
			a:        pow53,
			b:        pow53,
			expected: 0,
		},
		{
			name:     "2^53+1 vs 2^53+1 (equal)",
			a:        pow53Plus1,
			b:        pow53Plus1,
			expected: 0,
		},
		{
			name:     "negative large: -2^53 vs -2^53-1",
			a:        -pow53,
			b:        -pow53 - 1,
			expected: 1, // -2^53 > -2^53-1
		},
		{
			name:     "negative large: -2^53-1 vs -2^53",
			a:        -pow53 - 1,
			b:        -pow53,
			expected: -1,
		},
		{
			name:     "very large int64",
			a:        int64(9007199254740999),
			b:        int64(9007199254741000),
			expected: -1,
		},
		{
			name:     "max int64 boundaries",
			a:        int64(1<<62 - 1),
			b:        int64(1 << 62),
			expected: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := engine.CompareBSON(tc.a, tc.b)

			// Normalize result to -1, 0, 1
			normalized := 0
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			}

			if normalized != tc.expected {
				t.Errorf("CompareBSON(%v, %v) = %d, expected %d",
					tc.a, tc.b, normalized, tc.expected)
			}
		})
	}
}

// TestCompareBSON_Int64VsFloat64 tests comparison between int64 and float64.
func TestCompareBSON_Int64VsFloat64(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "int64(10) vs float64(10.0) should be equal",
			a:        int64(10),
			b:        float64(10.0),
			expected: 0,
		},
		{
			name:     "int64(10) vs float64(10.5) should be less",
			a:        int64(10),
			b:        float64(10.5),
			expected: -1,
		},
		{
			name:     "float64(10.5) vs int64(10) should be greater",
			a:        float64(10.5),
			b:        int64(10),
			expected: 1,
		},
		{
			name:     "int32(10) vs int64(10) should be equal",
			a:        int32(10),
			b:        int64(10),
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := engine.CompareBSON(tc.a, tc.b)

			normalized := 0
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			}

			if normalized != tc.expected {
				t.Errorf("CompareBSON(%v, %v) = %d, expected %d",
					tc.a, tc.b, normalized, tc.expected)
			}
		})
	}
}

// TestCompareBSON_Decimal128 tests B-GOLD-DEC-001:
// Decimal128 comparison must preserve scale and precision.
func TestCompareBSON_Decimal128(t *testing.T) {
	// Helper to create Decimal128
	mustDecimal := func(s string) primitive.Decimal128 {
		d, err := primitive.ParseDecimal128(s)
		if err != nil {
			t.Fatalf("failed to parse decimal %s: %v", s, err)
		}
		return d
	}

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "1.10 vs 1.2 (1.10 < 1.2)",
			a:        mustDecimal("1.10"),
			b:        mustDecimal("1.2"),
			expected: -1,
		},
		{
			name:     "1.2 vs 1.10 (1.2 > 1.10)",
			a:        mustDecimal("1.2"),
			b:        mustDecimal("1.10"),
			expected: 1,
		},
		{
			name:     "0.1 vs 0.10 should be equal",
			a:        mustDecimal("0.1"),
			b:        mustDecimal("0.10"),
			expected: 0,
		},
		{
			name:     "large decimal precision",
			a:        mustDecimal("1234567890123456.789"),
			b:        mustDecimal("1234567890123456.788"),
			expected: 1,
		},
		{
			name:     "negative decimals",
			a:        mustDecimal("-1.5"),
			b:        mustDecimal("-1.4"),
			expected: -1,
		},
		{
			name:     "decimal vs int64",
			a:        mustDecimal("100.0"),
			b:        int64(100),
			expected: 0,
		},
		{
			name:     "decimal vs float64",
			a:        mustDecimal("100.5"),
			b:        float64(100.5),
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := engine.CompareBSON(tc.a, tc.b)

			normalized := 0
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			}

			if normalized != tc.expected {
				t.Errorf("CompareBSON(%v, %v) = %d, expected %d",
					tc.a, tc.b, normalized, tc.expected)
			}
		})
	}
}

// TestCompareBSON_TypeOrdering tests BSON type ordering.
func TestCompareBSON_TypeOrdering(t *testing.T) {
	// MongoDB type ordering (from docs):
	// MinKey < Null < Number < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegEx < MaxKey

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "null vs number",
			a:        nil,
			b:        int64(1),
			expected: -1,
		},
		{
			name:     "number vs string",
			a:        int64(1),
			b:        "hello",
			expected: -1,
		},
		{
			name:     "string vs bool",
			a:        "hello",
			b:        true,
			expected: -1,
		},
		{
			name:     "MinKey vs everything",
			a:        primitive.MinKey{},
			b:        nil,
			expected: -1,
		},
		{
			name:     "MaxKey vs everything",
			a:        primitive.MaxKey{},
			b:        "any string",
			expected: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := engine.CompareBSON(tc.a, tc.b)

			normalized := 0
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			}

			if normalized != tc.expected {
				t.Errorf("CompareBSON(%v, %v) = %d, expected %d",
					tc.a, tc.b, normalized, tc.expected)
			}
		})
	}
}


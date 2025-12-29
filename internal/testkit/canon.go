// Package testkit provides testing utilities for MonoLite.
package testkit

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CanonValue represents a canonicalized BSON value with explicit type information.
type CanonValue struct {
	Type  string      // BSON type name
	Value interface{} // Canonical representation
}

// CanonDoc represents a canonicalized document.
type CanonDoc struct {
	Fields []CanonField
}

// CanonField represents a field in a canonical document.
type CanonField struct {
	Key   string
	Value CanonValue
}

// CanonicalizeDoc converts a bson.Raw or bson.D to canonical form.
func CanonicalizeDoc(doc interface{}) (CanonDoc, error) {
	var d bson.D
	switch v := doc.(type) {
	case bson.D:
		d = v
	case bson.Raw:
		if err := bson.Unmarshal(v, &d); err != nil {
			return CanonDoc{}, fmt.Errorf("failed to unmarshal bson.Raw: %w", err)
		}
	case []byte:
		if err := bson.Unmarshal(v, &d); err != nil {
			return CanonDoc{}, fmt.Errorf("failed to unmarshal bytes: %w", err)
		}
	default:
		return CanonDoc{}, fmt.Errorf("unsupported type: %T", doc)
	}

	canon := CanonDoc{
		Fields: make([]CanonField, 0, len(d)),
	}

	for _, elem := range d {
		// Skip MongoDB-specific fields
		if isMongoInternalField(elem.Key) {
			continue
		}

		canonValue := canonicalizeValue(elem.Value)
		canon.Fields = append(canon.Fields, CanonField{
			Key:   elem.Key,
			Value: canonValue,
		})
	}

	return canon, nil
}

// CanonicalizeDocs converts a slice of documents and sorts by _id.
func CanonicalizeDocs(docs []interface{}) ([]CanonDoc, error) {
	result := make([]CanonDoc, 0, len(docs))

	for _, doc := range docs {
		canon, err := CanonicalizeDoc(doc)
		if err != nil {
			return nil, err
		}
		result = append(result, canon)
	}

	// Sort by _id for stable comparison
	sort.Slice(result, func(i, j int) bool {
		idI := getCanonFieldValue(result[i], "_id")
		idJ := getCanonFieldValue(result[j], "_id")
		return compareCanonValues(idI, idJ) < 0
	})

	return result, nil
}

// canonicalizeValue converts a value to canonical form with type tag.
func canonicalizeValue(v interface{}) CanonValue {
	if v == nil {
		return CanonValue{Type: "null", Value: nil}
	}

	switch val := v.(type) {
	case int32:
		return CanonValue{Type: "int32", Value: int64(val)}
	case int64:
		return CanonValue{Type: "int64", Value: val}
	case int:
		return CanonValue{Type: "int64", Value: int64(val)}
	case float64:
		return CanonValue{Type: "double", Value: val}
	case float32:
		return CanonValue{Type: "double", Value: float64(val)}
	case string:
		return CanonValue{Type: "string", Value: val}
	case bool:
		return CanonValue{Type: "bool", Value: val}
	case primitive.ObjectID:
		return CanonValue{Type: "objectId", Value: val.Hex()}
	case primitive.DateTime:
		return CanonValue{Type: "date", Value: val.Time().UnixMilli()}
	case primitive.Timestamp:
		return CanonValue{Type: "timestamp", Value: fmt.Sprintf("%d:%d", val.T, val.I)}
	case primitive.Decimal128:
		// Preserve Decimal128 as string for exact comparison
		return CanonValue{Type: "decimal128", Value: val.String()}
	case primitive.Binary:
		return CanonValue{Type: "binData", Value: val.Data}
	case primitive.Regex:
		return CanonValue{Type: "regex", Value: fmt.Sprintf("/%s/%s", val.Pattern, val.Options)}
	case primitive.MinKey:
		return CanonValue{Type: "minKey", Value: nil}
	case primitive.MaxKey:
		return CanonValue{Type: "maxKey", Value: nil}
	case bson.D:
		subdoc, _ := CanonicalizeDoc(val)
		return CanonValue{Type: "document", Value: subdoc}
	case bson.A:
		arr := make([]CanonValue, len(val))
		for i, elem := range val {
			arr[i] = canonicalizeValue(elem)
		}
		return CanonValue{Type: "array", Value: arr}
	case []interface{}:
		arr := make([]CanonValue, len(val))
		for i, elem := range val {
			arr[i] = canonicalizeValue(elem)
		}
		return CanonValue{Type: "array", Value: arr}
	default:
		// Unknown type - convert to string representation
		return CanonValue{Type: "unknown", Value: fmt.Sprintf("%v", val)}
	}
}

// isMongoInternalField returns true for MongoDB-specific fields to skip.
func isMongoInternalField(key string) bool {
	switch key {
	case "$clusterTime", "operationTime", "electionId", "setVersion", "setName":
		return true
	}
	return false
}

// getCanonFieldValue gets a field value from a canonical document.
func getCanonFieldValue(doc CanonDoc, key string) CanonValue {
	for _, field := range doc.Fields {
		if field.Key == key {
			return field.Value
		}
	}
	return CanonValue{Type: "null", Value: nil}
}

// compareCanonValues compares two canonical values for sorting.
func compareCanonValues(a, b CanonValue) int {
	// First compare by type
	if a.Type != b.Type {
		return typeOrder(a.Type) - typeOrder(b.Type)
	}

	// Then compare by value
	switch a.Type {
	case "null", "minKey", "maxKey":
		return 0
	case "int32", "int64":
		aVal := a.Value.(int64)
		bVal := b.Value.(int64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case "double":
		aVal := a.Value.(float64)
		bVal := b.Value.(float64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case "string", "objectId", "decimal128":
		aVal := a.Value.(string)
		bVal := b.Value.(string)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case "bool":
		aVal := a.Value.(bool)
		bVal := b.Value.(bool)
		if !aVal && bVal {
			return -1
		} else if aVal && !bVal {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// typeOrder returns the comparison order for BSON types (MongoDB ordering).
func typeOrder(t string) int {
	order := map[string]int{
		"minKey":     1,
		"null":       2,
		"int32":      3,
		"int64":      3,
		"double":     3,
		"decimal128": 3,
		"string":     4,
		"document":   5,
		"array":      6,
		"binData":    7,
		"objectId":   8,
		"bool":       9,
		"date":       10,
		"timestamp":  11,
		"regex":      12,
		"maxKey":     13,
	}
	if o, ok := order[t]; ok {
		return o
	}
	return 99
}

// Equal compares two canonical documents for equality.
func (d CanonDoc) Equal(other CanonDoc) bool {
	if len(d.Fields) != len(other.Fields) {
		return false
	}

	for i, field := range d.Fields {
		if field.Key != other.Fields[i].Key {
			return false
		}
		if !canonValuesEqual(field.Value, other.Fields[i].Value) {
			return false
		}
	}

	return true
}

// canonValuesEqual compares two canonical values for equality.
func canonValuesEqual(a, b CanonValue) bool {
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case "null", "minKey", "maxKey":
		return true
	case "document":
		return a.Value.(CanonDoc).Equal(b.Value.(CanonDoc))
	case "array":
		aArr := a.Value.([]CanonValue)
		bArr := b.Value.([]CanonValue)
		if len(aArr) != len(bArr) {
			return false
		}
		for i := range aArr {
			if !canonValuesEqual(aArr[i], bArr[i]) {
				return false
			}
		}
		return true
	case "binData":
		return bytes.Equal(a.Value.([]byte), b.Value.([]byte))
	default:
		return a.Value == b.Value
	}
}

// String returns a string representation of the canonical document.
func (d CanonDoc) String() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	for i, field := range d.Fields {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(field.Key)
		buf.WriteString(": ")
		buf.WriteString(field.Value.String())
	}
	buf.WriteString("}")
	return buf.String()
}

// String returns a string representation of the canonical value.
func (v CanonValue) String() string {
	switch v.Type {
	case "null":
		return "null"
	case "minKey":
		return "MinKey"
	case "maxKey":
		return "MaxKey"
	case "document":
		return v.Value.(CanonDoc).String()
	case "array":
		arr := v.Value.([]CanonValue)
		var buf bytes.Buffer
		buf.WriteString("[")
		for i, elem := range arr {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(elem.String())
		}
		buf.WriteString("]")
		return buf.String()
	default:
		return fmt.Sprintf("%s(%v)", v.Type, v.Value)
	}
}

// CompareBigInt compares two int64 values that may be > 2^53.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareBigInt(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// Int64ToDecimal converts an int64 to a big.Int for precise comparisons.
func Int64ToDecimal(v int64) *big.Int {
	return big.NewInt(v)
}

// Float64ToInt64Safe converts a float64 to int64 if it's safe (within 2^53 range).
// Returns the value and whether the conversion is safe.
func Float64ToInt64Safe(f float64) (int64, bool) {
	const maxSafeInt = 1 << 53
	if f < -float64(maxSafeInt) || f > float64(maxSafeInt) {
		return 0, false
	}
	return int64(f), true
}


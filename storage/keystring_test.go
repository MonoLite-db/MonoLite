// Created by Yanjunhui

package storage

import (
	"bytes"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestKeyStringNumberOrdering(t *testing.T) {
	numbers := []float64{
		-1000000,
		-100,
		-1,
		-0.5,
		0,
		0.5,
		1,
		100,
		1000000,
	}

	var keys [][]byte
	builder := NewKeyStringBuilder()

	for _, n := range numbers {
		builder.Reset()
		builder.AppendValue(n)
		// 复制字节切片
		// EN: Copy the byte slice.
		keyCopy := make([]byte, len(builder.Bytes()))
		copy(keyCopy, builder.Bytes())
		keys = append(keys, keyCopy)
	}

	// 验证顺序
	// EN: Verify ordering.
	for i := 1; i < len(keys); i++ {
		if CompareKeyStrings(keys[i-1], keys[i]) >= 0 {
			t.Errorf("Number ordering failed: %f should be < %f", numbers[i-1], numbers[i])
		}
	}
}

func TestKeyStringStringOrdering(t *testing.T) {
	strings := []string{
		"",
		"a",
		"aa",
		"ab",
		"b",
		"hello",
		"world",
		"中文",
	}

	var keys [][]byte
	builder := NewKeyStringBuilder()

	for _, s := range strings {
		builder.Reset()
		builder.AppendValue(s)
		keyCopy := make([]byte, len(builder.Bytes()))
		copy(keyCopy, builder.Bytes())
		keys = append(keys, keyCopy)
	}

	// 验证顺序
	// EN: Verify ordering.
	for i := 1; i < len(keys); i++ {
		if CompareKeyStrings(keys[i-1], keys[i]) >= 0 {
			t.Errorf("String ordering failed: %s should be < %s", strings[i-1], strings[i])
		}
	}
}

func TestKeyStringTypeOrdering(t *testing.T) {
	values := []interface{}{
		primitive.MinKey{},
		nil,
		int32(-1),
		int32(0),
		int32(1),
		"a",
		"z",
		bson.D{},
		bson.A{},
		[]byte{0x00},
		primitive.NewObjectID(),
		false,
		true,
		time.Now(),
		primitive.Timestamp{T: 1, I: 1},
		primitive.MaxKey{},
	}

	var keys [][]byte
	builder := NewKeyStringBuilder()

	for _, v := range values {
		builder.Reset()
		builder.AppendValue(v)
		keyCopy := make([]byte, len(builder.Bytes()))
		copy(keyCopy, builder.Bytes())
		keys = append(keys, keyCopy)
	}

	// 验证类型优先级顺序
	// EN: Verify type precedence ordering.
	for i := 1; i < len(keys); i++ {
		if CompareKeyStrings(keys[i-1], keys[i]) >= 0 {
			t.Errorf("Type ordering failed at index %d", i)
		}
	}
}

func TestKeyStringDescending(t *testing.T) {
	numbers := []int32{1, 2, 3, 4, 5}

	var ascending [][]byte
	var descending [][]byte
	builder := NewKeyStringBuilder()

	for _, n := range numbers {
		builder.Reset()
		builder.AppendValueWithDirection(n, 1)
		ascending = append(ascending, append([]byte{}, builder.Bytes()...))

		builder.Reset()
		builder.AppendValueWithDirection(n, -1)
		descending = append(descending, append([]byte{}, builder.Bytes()...))
	}

	// 升序应该保持原顺序
	// EN: Ascending should keep the original order.
	for i := 1; i < len(ascending); i++ {
		if CompareKeyStrings(ascending[i-1], ascending[i]) >= 0 {
			t.Errorf("Ascending order failed at index %d", i)
		}
	}

	// 降序应该是反向顺序
	// EN: Descending should be the reverse order.
	for i := 1; i < len(descending); i++ {
		if CompareKeyStrings(descending[i-1], descending[i]) <= 0 {
			t.Errorf("Descending order failed at index %d", i)
		}
	}
}

func TestKeyStringCompoundKey(t *testing.T) {
	// 复合索引：{name: 1, age: -1}
	// EN: Compound index: {name: 1, age: -1}.
	keys := bson.D{
		{Key: "name", Value: 1},
		{Key: "age", Value: -1},
	}

	docs := []bson.D{
		{{Key: "name", Value: "alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "alice"}, {Key: "age", Value: int32(25)}},
		{{Key: "name", Value: "alice"}, {Key: "age", Value: int32(20)}},
		{{Key: "name", Value: "bob"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "bob"}, {Key: "age", Value: int32(25)}},
	}

	var encodedKeys [][]byte
	for _, doc := range docs {
		encodedKeys = append(encodedKeys, EncodeIndexKey(keys, doc))
	}

	// 预期顺序（name 升序，相同 name 时 age 降序）
	// EN: Expected order (name ascending; for equal name, age descending).
	// alice(30) < alice(25) < alice(20) < bob(30) < bob(25)
	for i := 1; i < len(encodedKeys); i++ {
		if CompareKeyStrings(encodedKeys[i-1], encodedKeys[i]) >= 0 {
			t.Errorf("Compound key ordering failed at index %d", i)
		}
	}
}

func TestKeyStringNestedField(t *testing.T) {
	keys := bson.D{{Key: "address.city", Value: 1}}

	docs := []bson.D{
		{{Key: "name", Value: "a"}, {Key: "address", Value: bson.D{{Key: "city", Value: "beijing"}}}},
		{{Key: "name", Value: "b"}, {Key: "address", Value: bson.D{{Key: "city", Value: "shanghai"}}}},
		{{Key: "name", Value: "c"}, {Key: "address", Value: bson.D{{Key: "city", Value: "tokyo"}}}},
	}

	var encodedKeys [][]byte
	for _, doc := range docs {
		encodedKeys = append(encodedKeys, EncodeIndexKey(keys, doc))
	}

	// 验证顺序
	// EN: Verify ordering.
	for i := 1; i < len(encodedKeys); i++ {
		if CompareKeyStrings(encodedKeys[i-1], encodedKeys[i]) >= 0 {
			t.Errorf("Nested field ordering failed at index %d", i)
		}
	}
}

func TestKeyStringEquality(t *testing.T) {
	builder := NewKeyStringBuilder()

	// 相同值应该产生相同的 KeyString
	// EN: The same value should produce the same KeyString.
	builder.AppendValue("test")
	key1 := append([]byte{}, builder.Bytes()...)

	builder.Reset()
	builder.AppendValue("test")
	key2 := append([]byte{}, builder.Bytes()...)

	if !KeyStringEqual(key1, key2) {
		t.Error("Same values should produce equal KeyStrings")
	}

	// 不同值应该产生不同的 KeyString
	// EN: Different values should produce different KeyStrings.
	builder.Reset()
	builder.AppendValue("other")
	key3 := builder.Bytes()

	if KeyStringEqual(key1, key3) {
		t.Error("Different values should produce different KeyStrings")
	}
}

func TestKeyStringNullHandling(t *testing.T) {
	builder := NewKeyStringBuilder()

	// 字符串中包含 null 字节
	// EN: Strings containing null bytes.
	builder.AppendValue("hello\x00world")
	key1 := append([]byte{}, builder.Bytes()...)

	builder.Reset()
	builder.AppendValue("hello\x00world")
	key2 := append([]byte{}, builder.Bytes()...)

	if !KeyStringEqual(key1, key2) {
		t.Error("Strings with null bytes should be handled correctly")
	}

	// 不同的 null 位置应该产生不同的键
	// EN: Different null byte positions should produce different keys.
	builder.Reset()
	builder.AppendValue("hello\x00\x00world")
	key3 := builder.Bytes()

	if KeyStringEqual(key1, key3) {
		t.Error("Strings with different null patterns should be different")
	}
}

func TestKeyStringObjectID(t *testing.T) {
	oid1, _ := primitive.ObjectIDFromHex("000000000000000000000001")
	oid2, _ := primitive.ObjectIDFromHex("000000000000000000000002")
	oid3, _ := primitive.ObjectIDFromHex("ffffffffffffffffffffffff")

	builder := NewKeyStringBuilder()

	builder.AppendValue(oid1)
	key1 := append([]byte{}, builder.Bytes()...)

	builder.Reset()
	builder.AppendValue(oid2)
	key2 := append([]byte{}, builder.Bytes()...)

	builder.Reset()
	builder.AppendValue(oid3)
	key3 := append([]byte{}, builder.Bytes()...)

	if CompareKeyStrings(key1, key2) >= 0 {
		t.Error("oid1 should be < oid2")
	}
	if CompareKeyStrings(key2, key3) >= 0 {
		t.Error("oid2 should be < oid3")
	}
}

func BenchmarkKeyStringEncode(b *testing.B) {
	doc := bson.D{
		{Key: "name", Value: "benchmark test"},
		{Key: "age", Value: int32(25)},
		{Key: "score", Value: 95.5},
	}
	keys := bson.D{
		{Key: "name", Value: 1},
		{Key: "age", Value: -1},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeIndexKey(keys, doc)
	}
}

func BenchmarkKeyStringCompare(b *testing.B) {
	builder := NewKeyStringBuilder()
	builder.AppendValue("test string")
	builder.AppendValue(int32(12345))
	key1 := builder.Bytes()

	builder.Reset()
	builder.AppendValue("test string")
	builder.AppendValue(int32(12346))
	key2 := builder.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompareKeyStrings(key1, key2)
	}
}

func TestEncodeIndexKeyMissingField(t *testing.T) {
	keys := bson.D{{Key: "missing", Value: 1}}
	doc := bson.D{{Key: "name", Value: "test"}}

	// 缺失字段应该编码为 null
	// EN: Missing fields should be encoded as null.
	encoded := EncodeIndexKey(keys, doc)
	if len(encoded) == 0 {
		t.Error("Missing field should produce a valid KeyString")
	}

	// 比较两个缺失相同字段的文档
	// EN: Compare two documents missing the same field.
	doc2 := bson.D{{Key: "other", Value: "test"}}
	encoded2 := EncodeIndexKey(keys, doc2)

	if !bytes.Equal(encoded, encoded2) {
		t.Error("Documents with same missing field should produce equal KeyStrings")
	}
}

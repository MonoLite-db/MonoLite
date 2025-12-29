// Created by Yanjunhui

package storage

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// KeyString 类型标记（按 MongoDB 排序优先级）
const (
	KSTypeMinKey    byte = 0x00
	KSTypeNull      byte = 0x05
	KSTypeNumber    byte = 0x10
	KSTypeBigInt    byte = 0x11 // 【BUG-011 修复】大整数类型，用于超出 float64 精度范围的 int64
	KSTypeString    byte = 0x14
	KSTypeObject    byte = 0x18
	KSTypeArray     byte = 0x1C
	KSTypeBinData   byte = 0x20
	KSTypeObjectID  byte = 0x24
	KSTypeBool      byte = 0x28
	KSTypeDate      byte = 0x2C
	KSTypeTimestamp byte = 0x30
	KSTypeRegex     byte = 0x34
	KSTypeMaxKey    byte = 0xFF

	// 特殊标记
	KSEnd byte = 0x04 // 字段结束

	// 【BUG-011 修复】float64 精确表示整数的范围边界
	maxSafeInt64 = int64(1) << 53
	minSafeInt64 = -(int64(1) << 53)
)

// KeyStringBuilder 构建 KeyString
type KeyStringBuilder struct {
	buf bytes.Buffer
}

// NewKeyStringBuilder 创建构建器
func NewKeyStringBuilder() *KeyStringBuilder {
	return &KeyStringBuilder{}
}

// AppendValue 追加一个值（默认升序）
func (b *KeyStringBuilder) AppendValue(v interface{}) {
	b.AppendValueWithDirection(v, 1)
}

// AppendValueWithDirection 追加一个值，支持方向（1=升序, -1=降序）
func (b *KeyStringBuilder) AppendValueWithDirection(v interface{}, direction int) {
	var valueBuf bytes.Buffer
	encodeValue(&valueBuf, v)

	if direction >= 0 {
		b.buf.Write(valueBuf.Bytes())
	} else {
		// 降序：反转所有字节
		data := valueBuf.Bytes()
		for _, by := range data {
			b.buf.WriteByte(^by)
		}
	}

	// 写入字段分隔符
	if direction >= 0 {
		b.buf.WriteByte(KSEnd)
	} else {
		b.buf.WriteByte(^KSEnd)
	}
}

// Bytes 获取最终的字节序列
func (b *KeyStringBuilder) Bytes() []byte {
	return b.buf.Bytes()
}

// Reset 重置构建器
func (b *KeyStringBuilder) Reset() {
	b.buf.Reset()
}

// encodeValue 编码单个值
func encodeValue(buf *bytes.Buffer, v interface{}) {
	if v == nil {
		buf.WriteByte(KSTypeNull)
		return
	}

	switch val := v.(type) {
	case primitive.MinKey:
		buf.WriteByte(KSTypeMinKey)

	case primitive.MaxKey:
		buf.WriteByte(KSTypeMaxKey)

	case int:
		buf.WriteByte(KSTypeNumber)
		encodeNumber(buf, float64(val))

	case int32:
		buf.WriteByte(KSTypeNumber)
		encodeNumber(buf, float64(val))

	case int64:
		// 【BUG-011 修复】检查是否超出 float64 精确范围
		if val > maxSafeInt64 || val < minSafeInt64 {
			buf.WriteByte(KSTypeBigInt)
			encodeBigInt(buf, val)
		} else {
			buf.WriteByte(KSTypeNumber)
			encodeNumber(buf, float64(val))
		}

	case float32:
		buf.WriteByte(KSTypeNumber)
		encodeNumber(buf, float64(val))

	case float64:
		buf.WriteByte(KSTypeNumber)
		encodeNumber(buf, val)

	case string:
		buf.WriteByte(KSTypeString)
		encodeString(buf, val)

	case primitive.Symbol:
		buf.WriteByte(KSTypeString)
		encodeString(buf, string(val))

	case bson.D:
		buf.WriteByte(KSTypeObject)
		encodeObject(buf, val)

	case bson.A:
		buf.WriteByte(KSTypeArray)
		encodeArray(buf, val)

	case []interface{}:
		buf.WriteByte(KSTypeArray)
		encodeArray(buf, bson.A(val))

	case []byte:
		buf.WriteByte(KSTypeBinData)
		encodeBinary(buf, val)

	case primitive.Binary:
		buf.WriteByte(KSTypeBinData)
		encodeBinary(buf, val.Data)

	case primitive.ObjectID:
		buf.WriteByte(KSTypeObjectID)
		buf.Write(val[:])

	case bool:
		buf.WriteByte(KSTypeBool)
		if val {
			buf.WriteByte(0x02)
		} else {
			buf.WriteByte(0x01)
		}

	case time.Time:
		buf.WriteByte(KSTypeDate)
		encodeDate(buf, val)

	case primitive.DateTime:
		buf.WriteByte(KSTypeDate)
		encodeDate(buf, val.Time())

	case primitive.Timestamp:
		buf.WriteByte(KSTypeTimestamp)
		encodeTimestamp(buf, val)

	case primitive.Regex:
		buf.WriteByte(KSTypeRegex)
		encodeRegex(buf, val)

	default:
		// 未知类型，使用 null
		buf.WriteByte(KSTypeNull)
	}
}

// encodeNumber 编码数字（可比较的格式）
// 使用 IEEE 754 浮点数的可比较编码
func encodeNumber(buf *bytes.Buffer, f float64) {
	bits := math.Float64bits(f)

	// 处理 NaN
	if math.IsNaN(f) {
		bits = math.Float64bits(math.NaN())
	}

	// 转换为可比较格式：
	// 正数：翻转符号位
	// 负数：翻转所有位
	if bits&(1<<63) != 0 {
		// 负数：翻转所有位
		bits = ^bits
	} else {
		// 正数/零：翻转符号位
		bits ^= 1 << 63
	}

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	buf.Write(b[:])
}

// encodeBigInt 编码大整数（超出 float64 精度范围的 int64）
// 【BUG-011 修复】新增此函数，使用 8 字节大端序编码，保持排序特性
func encodeBigInt(buf *bytes.Buffer, val int64) {
	// 为了保持正确的排序顺序，需要特殊处理符号
	// 使用"翻转符号位"的技巧：
	// - 将 int64 转为 uint64
	// - 翻转符号位，使负数在正数之前（按字典序）
	var encoded uint64
	if val >= 0 {
		// 正数：翻转符号位使其大于负数
		encoded = uint64(val) ^ (1 << 63)
	} else {
		// 负数：同样翻转符号位
		// 对于 int64，负数的二进制表示的最高位是 1
		// 转为 uint64 后，翻转符号位使其变为 0，排在正数之前
		encoded = uint64(val) ^ (1 << 63)
	}

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], encoded)
	buf.Write(b[:])
}

// encodeString 编码字符串（null-terminated 但转义内部 null）
func encodeString(buf *bytes.Buffer, s string) {
	for _, b := range []byte(s) {
		if b == 0x00 {
			// 转义 null 字节
			buf.WriteByte(0x00)
			buf.WriteByte(0xFF)
		} else if b == 0xFF {
			// 转义 0xFF
			buf.WriteByte(0xFF)
			buf.WriteByte(0x00)
		} else {
			buf.WriteByte(b)
		}
	}
	// 字符串结束标记
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
}

// encodeObject 编码对象
func encodeObject(buf *bytes.Buffer, doc bson.D) {
	for _, elem := range doc {
		// 字段名
		encodeString(buf, elem.Key)
		// 字段值
		encodeValue(buf, elem.Value)
	}
	// 对象结束标记
	buf.WriteByte(0x00)
}

// encodeArray 编码数组
func encodeArray(buf *bytes.Buffer, arr bson.A) {
	for _, elem := range arr {
		encodeValue(buf, elem)
	}
	// 数组结束标记
	buf.WriteByte(0x00)
}

// encodeBinary 编码二进制数据
func encodeBinary(buf *bytes.Buffer, data []byte) {
	// 长度前缀（4 字节大端）
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	buf.Write(lenBuf[:])
	buf.Write(data)
}

// encodeDate 编码日期
func encodeDate(buf *bytes.Buffer, t time.Time) {
	millis := t.UnixMilli()

	// 转换为可比较格式（类似数字）
	bits := uint64(millis)
	bits ^= 1 << 63 // 翻转符号位处理负数

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	buf.Write(b[:])
}

// encodeTimestamp 编码时间戳
func encodeTimestamp(buf *bytes.Buffer, ts primitive.Timestamp) {
	var b [8]byte
	binary.BigEndian.PutUint32(b[0:4], ts.T)
	binary.BigEndian.PutUint32(b[4:8], ts.I)
	buf.Write(b[:])
}

// encodeRegex 编码正则表达式
func encodeRegex(buf *bytes.Buffer, re primitive.Regex) {
	encodeString(buf, re.Pattern)
	encodeString(buf, re.Options)
}

// EncodeIndexKey 从文档字段编码索引键
// keys: 索引定义，如 bson.D{{Key: "name", Value: 1}, {Key: "age", Value: -1}}
// doc: 文档
func EncodeIndexKey(keys bson.D, doc bson.D) []byte {
	builder := NewKeyStringBuilder()

	for _, keySpec := range keys {
		field := keySpec.Key
		direction := 1

		// 获取方向
		switch v := keySpec.Value.(type) {
		case int:
			direction = v
		case int32:
			direction = int(v)
		case int64:
			direction = int(v)
		case float64:
			direction = int(v)
		}

		// 获取字段值
		value := getFieldValue(doc, field)
		builder.AppendValueWithDirection(value, direction)
	}

	return builder.Bytes()
}

// getFieldValue 从文档获取字段值（支持点号路径）
func getFieldValue(doc bson.D, field string) interface{} {
	parts := splitPath(field)
	current := interface{}(doc)

	for _, part := range parts {
		switch v := current.(type) {
		case bson.D:
			found := false
			for _, elem := range v {
				if elem.Key == part {
					current = elem.Value
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		case bson.A:
			// 数组索引访问
			idx := 0
			for i, c := range part {
				if c < '0' || c > '9' {
					return nil
				}
				idx = idx*10 + int(c-'0')
				if i > 5 { // 防止溢出
					return nil
				}
			}
			if idx >= len(v) {
				return nil
			}
			current = v[idx]
		default:
			return nil
		}
	}

	return current
}

// splitPath 分割点号路径
func splitPath(path string) []string {
	var parts []string
	start := 0
	for i, c := range path {
		if c == '.' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}

// CompareKeyStrings 比较两个 KeyString
// 返回：-1 (a < b), 0 (a == b), 1 (a > b)
func CompareKeyStrings(a, b []byte) int {
	return bytes.Compare(a, b)
}

// KeyStringEqual 检查两个 KeyString 是否相等
func KeyStringEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}


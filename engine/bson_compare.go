// Created by Yanjunhui

package engine

import (
	"bytes"
	"math/big"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoDB BSON 类型优先级（用于排序）
// EN: MongoDB BSON type comparison order (used for sorting).
//
// 参考：https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
// EN: Reference: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
const (
	TypeOrderMinKey = iota
	TypeOrderNull
	TypeOrderNumber // int, long, double, decimal
	// TypeOrderSymbol 已弃用但需要支持
	// EN: TypeOrderSymbol is deprecated but still needs to be supported.
	TypeOrderSymbol
	TypeOrderString
	TypeOrderObject
	TypeOrderArray
	TypeOrderBinData
	TypeOrderObjectID
	TypeOrderBoolean
	TypeOrderDate
	TypeOrderTimestamp
	TypeOrderRegex
	TypeOrderMaxKey
	TypeOrderUnknown = 100
)

// getTypeOrder 获取 BSON 值的类型排序优先级
// EN: getTypeOrder returns the BSON type order for comparison/sorting.
func getTypeOrder(v interface{}) int {
	if v == nil {
		return TypeOrderNull
	}

	switch v.(type) {
	case primitive.MinKey:
		return TypeOrderMinKey
	case int, int32, int64, float32, float64, primitive.Decimal128:
		return TypeOrderNumber
	case primitive.Symbol:
		return TypeOrderSymbol
	case string:
		return TypeOrderString
	case bson.D, bson.M, map[string]interface{}:
		return TypeOrderObject
	case bson.A, []interface{}:
		return TypeOrderArray
	case primitive.Binary, []byte:
		return TypeOrderBinData
	case primitive.ObjectID:
		return TypeOrderObjectID
	case bool:
		return TypeOrderBoolean
	case time.Time, primitive.DateTime:
		return TypeOrderDate
	case primitive.Timestamp:
		return TypeOrderTimestamp
	case primitive.Regex:
		return TypeOrderRegex
	case primitive.MaxKey:
		return TypeOrderMaxKey
	default:
		return TypeOrderUnknown
	}
}

// CompareBSON 比较两个 BSON 值
// EN: CompareBSON compares two BSON values.
//
// 返回：-1 (a < b), 0 (a == b), 1 (a > b)
// EN: Returns: -1 (a < b), 0 (a == b), 1 (a > b).
func CompareBSON(a, b interface{}) int {
	// 获取类型顺序（注意：MinKey 比 Null 还小）
	// EN: Get type order (note: MinKey is smaller than Null).
	orderA := getTypeOrder(a)
	orderB := getTypeOrder(b)

	// 先按类型优先级比较
	// EN: Compare by type order first.
	if orderA < orderB {
		return -1
	}
	if orderA > orderB {
		return 1
	}

	// 同类型比较
	// EN: Compare within the same type.
	return compareByType(a, b, orderA)
}

// compareByType 按类型比较
// EN: compareByType compares values of the same type order.
func compareByType(a, b interface{}, typeOrder int) int {
	switch typeOrder {
	case TypeOrderMinKey:
		// MinKey 都相等
		// EN: MinKey values are equal.
		return 0
	case TypeOrderNull:
		// null 都相等
		// EN: Null values are equal.
		return 0
	case TypeOrderNumber:
		return compareNumericValues(a, b)
	case TypeOrderString, TypeOrderSymbol:
		return compareStrings(a, b)
	case TypeOrderObject:
		return compareObjects(a, b)
	case TypeOrderArray:
		return compareArrays(a, b)
	case TypeOrderBinData:
		return compareBinary(a, b)
	case TypeOrderObjectID:
		return compareObjectIDs(a, b)
	case TypeOrderBoolean:
		return compareBooleans(a, b)
	case TypeOrderDate:
		return compareDates(a, b)
	case TypeOrderTimestamp:
		return compareTimestamps(a, b)
	case TypeOrderRegex:
		return compareRegex(a, b)
	case TypeOrderMaxKey:
		// MaxKey 都相等
		// EN: MaxKey values are equal.
		return 0
	default:
		return 0
	}
}

// compareNumericValues 比较数字
// EN: compareNumericValues compares numeric values.
//
// 【P0 修复】重写数值比较逻辑，避免大整数精度丢失
// EN: [P0 fix] Rework numeric comparison to avoid precision loss for large integers.
//
// MongoDB 数值比较规则：
// EN: MongoDB numeric comparison rules:
// - 同类型直接比较
// EN: - Compare directly when types are the same.
// - int32/int64/double 之间按数值大小比较
// EN: - Compare int32/int64/double by numeric value.
// - Decimal128 需要特殊处理
// EN: - Decimal128 requires special handling.
const (
	// maxSafeFloat64Int float64 能精确表示的最大整数
	// EN: maxSafeFloat64Int is the largest integer that float64 can represent exactly.
	maxSafeFloat64Int = 1 << 53
)

func compareNumericValues(a, b interface{}) int {
	// 【P0 修复】特殊处理 Decimal128 vs Decimal128
	// EN: [P0 fix] Special-case Decimal128 vs Decimal128.
	aDecimal, aIsDecimal := a.(primitive.Decimal128)
	bDecimal, bIsDecimal := b.(primitive.Decimal128)
	if aIsDecimal && bIsDecimal {
		return compareDecimal128(aDecimal, bDecimal)
	}

	// 获取数值的标准化表示
	// EN: Get normalized numeric representations.
	aInt, aFloat, aIsInt := normalizeNumeric(a)
	bInt, bFloat, bIsInt := normalizeNumeric(b)

	// 两者都是整数，直接用 int64 比较
	// EN: If both are integers, compare as int64.
	if aIsInt && bIsInt {
		if aInt < bInt {
			return -1
		}
		if aInt > bInt {
			return 1
		}
		return 0
	}

	// 至少一个是浮点数
	// EN: At least one is floating point.
	// 如果整数超出 float64 精度范围，需要特殊处理
	// EN: If an integer is outside float64 exact range, handle specially.
	if aIsInt {
		if aInt > maxSafeFloat64Int || aInt < -maxSafeFloat64Int {
			// 大整数与浮点数比较
			// EN: Compare large integer with float.
			// 将浮点数转换为整数部分比较
			// EN: Compare using the integer part of the float.
			bIntPart := int64(bFloat)
			if aInt < bIntPart {
				return -1
			}
			if aInt > bIntPart {
				return 1
			}
			// 整数部分相等，检查浮点数的小数部分
			// EN: Integer parts equal; compare the fractional part.
			if bFloat > float64(bIntPart) {
				// 浮点数有正小数部分，所以大于整数
				// EN: Float has a positive fractional part, so it is greater than the integer.
				return -1
			}
			if bFloat < float64(bIntPart) {
				return 1
			}
			return 0
		}
		aFloat = float64(aInt)
	}

	if bIsInt {
		if bInt > maxSafeFloat64Int || bInt < -maxSafeFloat64Int {
			// 大整数与浮点数比较
			// EN: Compare large integer with float.
			aIntPart := int64(aFloat)
			if aIntPart < bInt {
				return -1
			}
			if aIntPart > bInt {
				return 1
			}
			if aFloat > float64(aIntPart) {
				// 浮点数有正小数部分，所以大于整数
				// EN: Float has a positive fractional part, so it is greater than the integer.
				return 1
			}
			if aFloat < float64(aIntPart) {
				return -1
			}
			return 0
		}
		bFloat = float64(bInt)
	}

	// 都在安全范围内，使用 float64 比较
	// EN: Within safe range; compare as float64.
	if aFloat < bFloat {
		return -1
	}
	if aFloat > bFloat {
		return 1
	}
	return 0
}

// normalizeNumeric 将数值标准化为 int64 或 float64
// EN: normalizeNumeric normalizes a numeric value to int64 or float64.
//
// 返回 (int64值, float64值, 是否为整数类型)
// EN: Returns (int64 value, float64 value, isInteger).
func normalizeNumeric(v interface{}) (int64, float64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), 0, true
	case int32:
		return int64(val), 0, true
	case int64:
		return val, 0, true
	case float32:
		return 0, float64(val), false
	case float64:
		return 0, val, false
	case primitive.Decimal128:
		// 【P0 修复】正确处理 Decimal128
		// EN: [P0 fix] Correctly handle Decimal128.
		return compareDecimal128ToNumeric(val)
	default:
		return 0, 0, true
	}
}

// compareDecimal128ToNumeric 处理 Decimal128 的数值表示
// EN: compareDecimal128ToNumeric converts Decimal128 into an int64/float64 representation.
func compareDecimal128ToNumeric(d primitive.Decimal128) (int64, float64, bool) {
	bigInt, exp, err := d.BigInt()
	if err != nil || bigInt == nil {
		return 0, 0, true
	}

	// 如果指数为 0 且值在 int64 范围内，返回整数
	// EN: If exponent is 0 and value fits in int64, return integer.
	if exp == 0 && bigInt.IsInt64() {
		return bigInt.Int64(), 0, true
	}

	// 否则尝试转换为 float64
	// EN: Otherwise, try converting to float64.
	// 注意：这可能会丢失精度，但对于小数这是合理的
	// EN: Note: this may lose precision, but is acceptable for fractional values here.
	f, _ := bigInt.Float64()

	// 应用指数
	// EN: Apply exponent.
	if exp > 0 {
		for i := 0; i < exp; i++ {
			f *= 10
		}
	} else if exp < 0 {
		for i := 0; i > exp; i-- {
			f /= 10
		}
	}

	return 0, f, false
}

// compareDecimal128 精确比较两个 Decimal128 值
// EN: compareDecimal128 compares two Decimal128 values precisely.
//
// 使用 big.Int 和指数进行精确比较，避免 float64 精度丢失
// EN: Uses big.Int and exponent alignment to avoid float64 precision loss.
func compareDecimal128(a, b primitive.Decimal128) int {
	aBigInt, aExp, aErr := a.BigInt()
	bBigInt, bExp, bErr := b.BigInt()

	// 处理错误情况
	// EN: Handle error cases.
	if aErr != nil && bErr != nil {
		return 0
	}
	if aErr != nil {
		// 无效值视为较小
		// EN: Treat invalid value as smaller.
		return -1
	}
	if bErr != nil {
		return 1
	}

	// 检查符号
	// EN: Check sign.
	aSign := aBigInt.Sign()
	bSign := bBigInt.Sign()
	if aSign < bSign {
		return -1
	}
	if aSign > bSign {
		return 1
	}
	if aSign == 0 && bSign == 0 {
		// 都是零
		// EN: Both are zero.
		return 0
	}

	// 将两个数调整到相同的指数进行比较
	// EN: Align the exponents before comparison.
	// 为了避免精度丢失，我们将指数较大的数乘以 10 的幂
	// EN: To avoid precision loss, multiply the number with larger exponent by a power of 10.
	aNorm := new(big.Int).Set(aBigInt)
	bNorm := new(big.Int).Set(bBigInt)

	if aExp > bExp {
		// a 的指数更大，需要将 a 乘以 10^(aExp-bExp)
		// EN: a has larger exponent; multiply a by 10^(aExp-bExp).
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(aExp-bExp)), nil)
		aNorm.Mul(aNorm, multiplier)
	} else if bExp > aExp {
		// b 的指数更大，需要将 b 乘以 10^(bExp-aExp)
		// EN: b has larger exponent; multiply b by 10^(bExp-aExp).
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(bExp-aExp)), nil)
		bNorm.Mul(bNorm, multiplier)
	}

	return aNorm.Cmp(bNorm)
}

// toFloat64Value 转换为 float64（保留用于其他用途）
// EN: toFloat64Value converts a numeric value to float64 (kept for other uses).
func toFloat64Value(v interface{}) float64 {
	intVal, floatVal, isInt := normalizeNumeric(v)
	if isInt {
		return float64(intVal)
	}
	return floatVal
}

// compareStrings 比较字符串
// EN: compareStrings compares strings (including Symbol).
func compareStrings(a, b interface{}) int {
	sa := toString(a)
	sb := toString(b)

	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

// toString 转换为字符串
// EN: toString converts a value to string (supports string and Symbol).
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case primitive.Symbol:
		return string(val)
	default:
		return ""
	}
}

// compareObjects 比较对象（按字段顺序递归比较）
// EN: compareObjects compares objects recursively by field order.
func compareObjects(a, b interface{}) int {
	docA := toBsonD(a)
	docB := toBsonD(b)

	// 先比较字段数量
	// EN: Compare field count first.
	if len(docA) < len(docB) {
		return -1
	}
	if len(docA) > len(docB) {
		return 1
	}

	// 按顺序比较每个字段
	// EN: Compare each field in order.
	for i := 0; i < len(docA); i++ {
		// 比较字段名
		// EN: Compare field name.
		if docA[i].Key < docB[i].Key {
			return -1
		}
		if docA[i].Key > docB[i].Key {
			return 1
		}

		// 比较字段值
		// EN: Compare field value.
		cmp := CompareBSON(docA[i].Value, docB[i].Value)
		if cmp != 0 {
			return cmp
		}
	}

	return 0
}

// toBsonD 转换为 bson.D
// EN: toBsonD converts a value to bson.D.
func toBsonD(v interface{}) bson.D {
	switch val := v.(type) {
	case bson.D:
		return val
	case bson.M:
		result := make(bson.D, 0, len(val))
		for k, v := range val {
			result = append(result, bson.E{Key: k, Value: v})
		}
		return result
	case map[string]interface{}:
		result := make(bson.D, 0, len(val))
		for k, v := range val {
			result = append(result, bson.E{Key: k, Value: v})
		}
		return result
	default:
		return nil
	}
}

// compareArrays 比较数组
// EN: compareArrays compares arrays.
func compareArrays(a, b interface{}) int {
	arrA := toBsonA(a)
	arrB := toBsonA(b)

	// 按元素顺序比较
	// EN: Compare by element order.
	minLen := len(arrA)
	if len(arrB) < minLen {
		minLen = len(arrB)
	}

	for i := 0; i < minLen; i++ {
		cmp := CompareBSON(arrA[i], arrB[i])
		if cmp != 0 {
			return cmp
		}
	}

	// 长度不同时，较短的数组较小
	// EN: If lengths differ, the shorter array is smaller.
	if len(arrA) < len(arrB) {
		return -1
	}
	if len(arrA) > len(arrB) {
		return 1
	}

	return 0
}

// toBsonA 转换为 bson.A
// EN: toBsonA converts a value to bson.A.
func toBsonA(v interface{}) bson.A {
	switch val := v.(type) {
	case bson.A:
		return val
	case []interface{}:
		return bson.A(val)
	default:
		return nil
	}
}

// compareBinary 比较二进制数据
// EN: compareBinary compares binary data.
func compareBinary(a, b interface{}) int {
	bytesA := toBytes(a)
	bytesB := toBytes(b)

	return bytes.Compare(bytesA, bytesB)
}

// toBytes 转换为字节数组
// EN: toBytes converts a value to a byte slice.
func toBytes(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case primitive.Binary:
		return val.Data
	default:
		return nil
	}
}

// compareObjectIDs 比较 ObjectID
// EN: compareObjectIDs compares ObjectIDs.
func compareObjectIDs(a, b interface{}) int {
	oidA, okA := a.(primitive.ObjectID)
	oidB, okB := b.(primitive.ObjectID)

	if !okA || !okB {
		return 0
	}

	return bytes.Compare(oidA[:], oidB[:])
}

// compareBooleans 比较布尔值
// EN: compareBooleans compares booleans.
func compareBooleans(a, b interface{}) int {
	boolA, okA := a.(bool)
	boolB, okB := b.(bool)

	if !okA || !okB {
		return 0
	}

	// false < true
	if !boolA && boolB {
		return -1
	}
	if boolA && !boolB {
		return 1
	}
	return 0
}

// compareDates 比较日期
// EN: compareDates compares dates.
func compareDates(a, b interface{}) int {
	timeA := toTime(a)
	timeB := toTime(b)

	if timeA.Before(timeB) {
		return -1
	}
	if timeA.After(timeB) {
		return 1
	}
	return 0
}

// toTime 转换为 time.Time
// EN: toTime converts a value to time.Time.
func toTime(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case primitive.DateTime:
		return val.Time()
	default:
		return time.Time{}
	}
}

// compareTimestamps 比较时间戳
// EN: compareTimestamps compares timestamps.
func compareTimestamps(a, b interface{}) int {
	tsA, okA := a.(primitive.Timestamp)
	tsB, okB := b.(primitive.Timestamp)

	if !okA || !okB {
		return 0
	}

	// 先比较 T（秒），再比较 I（序号）
	// EN: Compare T (seconds) first, then I (increment).
	if tsA.T < tsB.T {
		return -1
	}
	if tsA.T > tsB.T {
		return 1
	}
	if tsA.I < tsB.I {
		return -1
	}
	if tsA.I > tsB.I {
		return 1
	}
	return 0
}

// compareRegex 比较正则表达式
// EN: compareRegex compares regex values.
func compareRegex(a, b interface{}) int {
	regexA, okA := a.(primitive.Regex)
	regexB, okB := b.(primitive.Regex)

	if !okA || !okB {
		return 0
	}

	// 先比较 pattern，再比较 options
	// EN: Compare pattern first, then options.
	if regexA.Pattern < regexB.Pattern {
		return -1
	}
	if regexA.Pattern > regexB.Pattern {
		return 1
	}
	if regexA.Options < regexB.Options {
		return -1
	}
	if regexA.Options > regexB.Options {
		return 1
	}
	return 0
}

// CompareBSONValues 比较两个值是否相等（用于查询匹配）
// EN: CompareBSONValues checks equality of two values (for query matching).
func CompareBSONValues(a, b interface{}) bool {
	return CompareBSON(a, b) == 0
}

// CompareForSort 用于排序比较（支持升序/降序）
// EN: CompareForSort compares values for sorting (supports ascending/descending).
//
// direction: 1 为升序, -1 为降序
// EN: direction: 1 for ascending, -1 for descending.
func CompareForSort(a, b interface{}, direction int) int {
	result := CompareBSON(a, b)
	if direction < 0 {
		return -result
	}
	return result
}

// SortDocuments 对文档列表按指定字段排序
// EN: SortDocuments sorts documents by the specified fields.
func SortDocuments(docs []bson.D, sortSpec bson.D) {
	if len(sortSpec) == 0 || len(docs) <= 1 {
		return
	}

	// 使用快速排序
	// EN: Use quicksort.
	quickSortDocs(docs, 0, len(docs)-1, sortSpec)
}

// quickSortDocs 快速排序文档
// EN: quickSortDocs performs quicksort on documents.
func quickSortDocs(docs []bson.D, low, high int, sortSpec bson.D) {
	if low < high {
		p := partitionDocs(docs, low, high, sortSpec)
		quickSortDocs(docs, low, p-1, sortSpec)
		quickSortDocs(docs, p+1, high, sortSpec)
	}
}

// partitionDocs 分区操作
// EN: partitionDocs performs a partition step for quicksort.
func partitionDocs(docs []bson.D, low, high int, sortSpec bson.D) int {
	pivot := docs[high]
	i := low - 1

	for j := low; j < high; j++ {
		if compareDocsBySort(docs[j], pivot, sortSpec) <= 0 {
			i++
			docs[i], docs[j] = docs[j], docs[i]
		}
	}

	docs[i+1], docs[high] = docs[high], docs[i+1]
	return i + 1
}

// compareDocsBySort 按排序规则比较两个文档
// EN: compareDocsBySort compares two documents according to sort spec.
func compareDocsBySort(a, b bson.D, sortSpec bson.D) int {
	for _, spec := range sortSpec {
		field := spec.Key
		direction := 1

		switch v := spec.Value.(type) {
		case int:
			direction = v
		case int32:
			direction = int(v)
		case int64:
			direction = int(v)
		case float64:
			direction = int(v)
		}

		valA := getDocFieldValue(a, field)
		valB := getDocFieldValue(b, field)

		cmp := CompareForSort(valA, valB, direction)
		if cmp != 0 {
			return cmp
		}
	}

	return 0
}

// getDocFieldValue 获取文档字段值（支持点号路径）
// EN: getDocFieldValue gets a document field value (supports dotted paths).
func getDocFieldValue(doc bson.D, field string) interface{} {
	// 复用 index.go 中的 getDocField 函数
	// EN: Reuse getDocField from index.go.
	return getDocField(doc, field)
}

// IsEqual 检查两个值是否相等（深度比较）
// EN: IsEqual checks whether two values are equal (deep comparison).
func IsEqual(a, b interface{}) bool {
	return reflect.DeepEqual(a, b) || CompareBSON(a, b) == 0
}

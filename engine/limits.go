// Created by Yanjunhui

package engine

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson"
)

// 资源限制常量（对齐 MongoDB 7.0）
// EN: Resource limit constants (aligned with MongoDB 7.0 where applicable).
const (
	// 文档大小限制
	// EN: Document size limits.
	MaxBSONDocumentSize = 16 * 1024 * 1024 // 16 MB

	// 嵌套深度限制
	// EN: Nesting depth limits.
	MaxDocumentNestingDepth = 100

	// 字段名长度限制
	// EN: Field name length limits.
	MaxFieldNameLength = 1024

	// 命名空间限制
	// EN: Namespace limits.
	// MaxNamespaceLength 数据库名 + 集合名
	// EN: MaxNamespaceLength is the max length of database name + collection name.
	MaxNamespaceLength      = 255
	MaxDatabaseNameLength   = 64
	MaxCollectionNameLength = 255

	// 批量操作限制
	// EN: Batch operation limits.
	MaxWriteBatchSize = 100000
	MaxBulkOpsCount   = 100000

	// 索引限制
	// EN: Index limits.
	MaxIndexesPerCollection = 64
	// MaxIndexKeyLength 索引键总长度
	// EN: MaxIndexKeyLength is the total byte length limit of index keys.
	MaxIndexKeyLength = 1024
	// MaxCompoundIndexFields 复合索引最大字段数
	// EN: MaxCompoundIndexFields is the max number of fields in a compound index.
	MaxCompoundIndexFields = 32

	// 数组/对象元素限制
	// EN: Array/object element limits.
	// MaxArrayLength 数组最大元素数
	// EN: MaxArrayLength is the max number of elements in an array.
	MaxArrayLength = 1000000
	// MaxObjectFields 对象最大字段数
	// EN: MaxObjectFields is the max number of fields in an object.
	MaxObjectFields = 100000

	// 字符串限制
	// EN: String limits.
	MaxStringLength = 16 * 1024 * 1024 // 16 MB

	// 查询限制
	// EN: Query limits.
	MaxSortPatternLength = 32
	MaxProjectionFields  = 100
)

// Limits 可配置的资源限制
// EN: Limits contains configurable resource limits.
type Limits struct {
	MaxDocumentSize         int
	MaxNestingDepth         int
	MaxWriteBatchSize       int
	MaxNamespaceLength      int
	MaxIndexesPerCollection int
}

// DefaultLimits 返回默认限制配置
// EN: DefaultLimits returns the default limit configuration.
func DefaultLimits() *Limits {
	return &Limits{
		MaxDocumentSize:         MaxBSONDocumentSize,
		MaxNestingDepth:         MaxDocumentNestingDepth,
		MaxWriteBatchSize:       MaxWriteBatchSize,
		MaxNamespaceLength:      MaxNamespaceLength,
		MaxIndexesPerCollection: MaxIndexesPerCollection,
	}
}

// ValidateDocument 验证 BSON 文档
// EN: ValidateDocument validates a BSON document against resource limits.
func ValidateDocument(doc bson.D) error {
	return validateDocumentRecursive(doc, 0)
}

// validateDocumentRecursive 递归验证文档
// EN: validateDocumentRecursive recursively validates a document.
func validateDocumentRecursive(doc bson.D, depth int) error {
	if depth > MaxDocumentNestingDepth {
		return ErrBadValue(fmt.Sprintf("document exceeds maximum nesting depth of %d", MaxDocumentNestingDepth))
	}

	if len(doc) > MaxObjectFields {
		return ErrBadValue(fmt.Sprintf("document exceeds maximum field count of %d", MaxObjectFields))
	}

	for _, elem := range doc {
		// 验证字段名
		// EN: Validate field name.
		if err := validateFieldName(elem.Key, depth == 0); err != nil {
			return err
		}

		// 递归验证嵌套文档和数组
		// EN: Recursively validate nested documents and arrays.
		if err := validateValue(elem.Value, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// validateValue 验证 BSON 值
// EN: validateValue validates a BSON value.
func validateValue(v interface{}, depth int) error {
	if depth > MaxDocumentNestingDepth {
		return ErrBadValue(fmt.Sprintf("document exceeds maximum nesting depth of %d", MaxDocumentNestingDepth))
	}

	switch val := v.(type) {
	case bson.D:
		return validateDocumentRecursive(val, depth)

	case bson.A:
		if len(val) > MaxArrayLength {
			return ErrBadValue(fmt.Sprintf("array exceeds maximum length of %d", MaxArrayLength))
		}
		for _, elem := range val {
			if err := validateValue(elem, depth+1); err != nil {
				return err
			}
		}

	case string:
		if len(val) > MaxStringLength {
			return ErrBadValue(fmt.Sprintf("string exceeds maximum length of %d bytes", MaxStringLength))
		}
		// 检查是否包含空字符
		// EN: Disallow null bytes.
		if strings.Contains(val, "\x00") {
			return ErrBadValue("string values cannot contain null bytes")
		}

	case []byte:
		if len(val) > MaxBSONDocumentSize {
			return ErrBadValue(fmt.Sprintf("binary data exceeds maximum size of %d bytes", MaxBSONDocumentSize))
		}
	}

	return nil
}

// validateFieldName 验证字段名（内部使用）
// EN: validateFieldName validates a field name (internal helper).
//
// isTopLevel: 是否是顶级字段
// EN: isTopLevel indicates whether the field is top-level.
//
// forStorage: 是否用于存储（true=文档字段, false=表达式/操作符）
// EN: forStorage indicates whether the name is used for stored documents vs expressions/operators.
func validateFieldName(name string, isTopLevel bool) error {
	return validateDocumentFieldName(name, isTopLevel)
}

// ValidateDocumentFieldName 验证文档字段名（用于存储）
// EN: ValidateDocumentFieldName validates stored document field names.
//
// 文档顶级字段不能以 $ 开头（这是 MongoDB 的规则）
// EN: Top-level stored document fields cannot start with '$' (MongoDB rule).
func ValidateDocumentFieldName(name string, isTopLevel bool) error {
	return validateDocumentFieldName(name, isTopLevel)
}

func validateDocumentFieldName(name string, isTopLevel bool) error {
	if name == "" {
		return NewMongoError(ErrorCodeEmptyFieldName, "field name cannot be empty")
	}

	if len(name) > MaxFieldNameLength {
		return ErrBadValue(fmt.Sprintf("field name exceeds maximum length of %d", MaxFieldNameLength))
	}

	// 检查空字符
	// EN: Check for null bytes.
	if strings.Contains(name, "\x00") {
		return ErrBadValue("field names cannot contain null bytes")
	}

	// 检查有效 UTF-8
	// EN: Check valid UTF-8.
	if !utf8.ValidString(name) {
		return ErrBadValue("field names must be valid UTF-8")
	}

	// 文档的顶级字段绝对不能以 $ 开头（MongoDB 规则）
	// EN: Top-level stored document fields must not start with '$' (MongoDB rule).
	// 这与表达式/filter 不同，表达式中可以使用 $eq/$gt 等操作符
	// EN: This differs from expressions/filters where $eq/$gt operators are allowed.
	if isTopLevel && len(name) > 0 && name[0] == '$' {
		return NewMongoError(ErrorCodeDollarPrefixedFieldName,
			fmt.Sprintf("field name '%s' cannot start with '$' in stored documents", name))
	}

	return nil
}

// ValidateExpressionFieldName 验证表达式字段名（用于 filter/update/pipeline）
// EN: ValidateExpressionFieldName validates expression field names (filter/update/pipeline).
//
// 允许 $ 开头的操作符（如 $eq, $set, $match）
// EN: Allows operator names starting with '$' (e.g., $eq, $set, $match).
func ValidateExpressionFieldName(name string) error {
	if name == "" {
		return NewMongoError(ErrorCodeEmptyFieldName, "field name cannot be empty")
	}

	if len(name) > MaxFieldNameLength {
		return ErrBadValue(fmt.Sprintf("field name exceeds maximum length of %d", MaxFieldNameLength))
	}

	// 检查空字符
	// EN: Check for null bytes.
	if strings.Contains(name, "\x00") {
		return ErrBadValue("field names cannot contain null bytes")
	}

	// 检查有效 UTF-8
	// EN: Check valid UTF-8.
	if !utf8.ValidString(name) {
		return ErrBadValue("field names must be valid UTF-8")
	}

	// 表达式中，$ 开头的必须是已知操作符
	// EN: In expressions, names starting with '$' must be known operators.
	if len(name) > 0 && name[0] == '$' {
		if !isKnownOperator(name) {
			return NewMongoError(ErrorCodeDollarPrefixedFieldName,
				fmt.Sprintf("unknown operator '%s'", name))
		}
	}

	return nil
}

// isKnownOperator 检查是否是已知的操作符
// EN: isKnownOperator reports whether name is a known operator.
func isKnownOperator(name string) bool {
	knownOperators := map[string]bool{
		// 查询操作符
		// EN: Query operators.
		"$eq": true, "$ne": true, "$gt": true, "$gte": true,
		"$lt": true, "$lte": true, "$in": true, "$nin": true,
		"$and": true, "$or": true, "$not": true, "$nor": true,
		"$exists": true, "$type": true, "$regex": true, "$options": true,
		"$elemMatch": true, "$size": true, "$all": true,
		"$mod": true, "$text": true, "$where": true,

		// 更新操作符
		// EN: Update operators.
		"$set": true, "$unset": true, "$inc": true, "$dec": true,
		"$mul": true, "$min": true, "$max": true, "$rename": true,
		"$push": true, "$pull": true, "$pop": true, "$addToSet": true,
		"$each": true, "$slice": true, "$sort": true, "$position": true,
		"$bit": true, "$currentDate": true, "$setOnInsert": true,

		// 聚合操作符（注：$sort 已在更新操作符中定义）
		// EN: Aggregation operators (note: $sort is already defined above).
		"$match": true, "$project": true, "$group": true,
		"$limit": true, "$skip": true, "$unwind": true, "$lookup": true,
		"$count": true, "$facet": true, "$bucket": true, "$sample": true,
		"$sum": true, "$avg": true, "$first": true, "$last": true,
		"$concat": true, "$substr": true, "$toLower": true, "$toUpper": true,
		"$cond": true, "$ifNull": true, "$switch": true,

		// 其他
		// EN: Others.
		"$db": true, "$readPreference": true, "$clusterTime": true,
	}
	return knownOperators[name]
}

// ValidateDatabaseName 验证数据库名
// EN: ValidateDatabaseName validates a database name.
func ValidateDatabaseName(name string) error {
	if name == "" {
		return ErrInvalidNamespace("database name cannot be empty")
	}

	if len(name) > MaxDatabaseNameLength {
		return ErrInvalidNamespace(fmt.Sprintf("database name exceeds maximum length of %d", MaxDatabaseNameLength))
	}

	// 检查非法字符
	// EN: Check invalid characters.
	invalidChars := []string{"/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?", "\x00"}
	for _, ch := range invalidChars {
		if strings.Contains(name, ch) {
			return ErrInvalidNamespace(fmt.Sprintf("database name cannot contain '%s'", ch))
		}
	}

	// 不能是空白字符串
	// EN: Must not be whitespace-only.
	if strings.TrimSpace(name) == "" {
		return ErrInvalidNamespace("database name cannot be empty or whitespace only")
	}

	return nil
}

// ValidateCollectionName 验证集合名
// EN: ValidateCollectionName validates a collection name.
func ValidateCollectionName(name string) error {
	if name == "" {
		return ErrInvalidNamespace("collection name cannot be empty")
	}

	if len(name) > MaxCollectionNameLength {
		return ErrInvalidNamespace(fmt.Sprintf("collection name exceeds maximum length of %d", MaxCollectionNameLength))
	}

	// 不能以 system. 开头（系统保留）
	// EN: Must not start with system. (reserved).
	if strings.HasPrefix(name, "system.") {
		return ErrInvalidNamespace("collection name cannot start with 'system.'")
	}

	// 检查非法字符
	// EN: Check invalid characters.
	if strings.Contains(name, "$") {
		return ErrInvalidNamespace("collection name cannot contain '$'")
	}

	if strings.Contains(name, "\x00") {
		return ErrInvalidNamespace("collection name cannot contain null bytes")
	}

	// 不能是空白字符串
	// EN: Must not be whitespace-only.
	if strings.TrimSpace(name) == "" {
		return ErrInvalidNamespace("collection name cannot be empty or whitespace only")
	}

	return nil
}

// ValidateNamespace 验证命名空间（db.collection）
// EN: ValidateNamespace validates a namespace in the form "db.collection".
func ValidateNamespace(namespace string) error {
	if len(namespace) > MaxNamespaceLength {
		return ErrInvalidNamespace(fmt.Sprintf("namespace exceeds maximum length of %d", MaxNamespaceLength))
	}

	parts := strings.SplitN(namespace, ".", 2)
	if len(parts) != 2 {
		return ErrInvalidNamespace("namespace must be in format 'database.collection'")
	}

	if err := ValidateDatabaseName(parts[0]); err != nil {
		return err
	}

	return ValidateCollectionName(parts[1])
}

// ValidateBatchSize 验证批量大小
// EN: ValidateBatchSize validates a write batch size.
func ValidateBatchSize(size int) error {
	if size < 0 {
		return ErrBadValue("batch size cannot be negative")
	}
	if size > MaxWriteBatchSize {
		return ErrBadValue(fmt.Sprintf("batch size exceeds maximum of %d", MaxWriteBatchSize))
	}
	return nil
}

// ValidateIndexKeys 验证索引键
// EN: ValidateIndexKeys validates index key specification.
func ValidateIndexKeys(keys bson.D) error {
	if len(keys) == 0 {
		return ErrCannotCreateIndex("index keys cannot be empty")
	}

	if len(keys) > MaxCompoundIndexFields {
		return ErrCannotCreateIndex(fmt.Sprintf("compound index cannot have more than %d fields", MaxCompoundIndexFields))
	}

	seenFields := make(map[string]bool)
	totalKeyLength := 0

	for _, key := range keys {
		if key.Key == "" {
			return ErrCannotCreateIndex("index key field cannot be empty")
		}

		if seenFields[key.Key] {
			return ErrCannotCreateIndex(fmt.Sprintf("duplicate field in index: %s", key.Key))
		}
		seenFields[key.Key] = true

		totalKeyLength += len(key.Key)
		if totalKeyLength > MaxIndexKeyLength {
			return ErrCannotCreateIndex(fmt.Sprintf("index key total length exceeds maximum of %d", MaxIndexKeyLength))
		}

		// 验证索引方向
		// EN: Validate index direction/type.
		switch v := key.Value.(type) {
		case int32:
			if v != 1 && v != -1 {
				return ErrCannotCreateIndex(fmt.Sprintf("invalid index direction: %d, must be 1 or -1", v))
			}
		case int64:
			if v != 1 && v != -1 {
				return ErrCannotCreateIndex(fmt.Sprintf("invalid index direction: %d, must be 1 or -1", v))
			}
		case int:
			if v != 1 && v != -1 {
				return ErrCannotCreateIndex(fmt.Sprintf("invalid index direction: %d, must be 1 or -1", v))
			}
		case string:
			// 支持特殊索引类型如 "text", "2dsphere" 等
			// EN: Support special index types like "text", "2dsphere", etc.
			validTypes := map[string]bool{"text": true, "2d": true, "2dsphere": true, "hashed": true}
			if !validTypes[v] {
				return ErrCannotCreateIndex(fmt.Sprintf("invalid index type: %s", v))
			}
		default:
			return ErrCannotCreateIndex("index direction must be a number (1 or -1) or valid index type")
		}
	}

	return nil
}

// CheckDocumentSize 检查序列化后的文档大小
// EN: CheckDocumentSize validates the serialized document size.
func CheckDocumentSize(data []byte) error {
	if len(data) > MaxBSONDocumentSize {
		return ErrDocumentTooLarge(len(data), MaxBSONDocumentSize)
	}
	return nil
}

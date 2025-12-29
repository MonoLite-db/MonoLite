// Created by Yanjunhui

package engine

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson"
)

// 资源限制常量（对齐 MongoDB 7.0）
const (
	// 文档大小限制
	MaxBSONDocumentSize = 16 * 1024 * 1024 // 16 MB
	
	// 嵌套深度限制
	MaxDocumentNestingDepth = 100
	
	// 字段名长度限制
	MaxFieldNameLength = 1024
	
	// 命名空间限制
	MaxNamespaceLength     = 255  // 数据库名 + 集合名
	MaxDatabaseNameLength  = 64
	MaxCollectionNameLength = 255
	
	// 批量操作限制
	MaxWriteBatchSize = 100000
	MaxBulkOpsCount   = 100000
	
	// 索引限制
	MaxIndexesPerCollection = 64
	MaxIndexKeyLength       = 1024 // 索引键总长度
	MaxCompoundIndexFields  = 32   // 复合索引最大字段数
	
	// 数组/对象元素限制
	MaxArrayLength   = 1000000 // 数组最大元素数
	MaxObjectFields  = 100000  // 对象最大字段数
	
	// 字符串限制
	MaxStringLength = 16 * 1024 * 1024 // 16 MB
	
	// 查询限制
	MaxSortPatternLength = 32
	MaxProjectionFields  = 100
)

// Limits 可配置的资源限制
type Limits struct {
	MaxDocumentSize       int
	MaxNestingDepth       int
	MaxWriteBatchSize     int
	MaxNamespaceLength    int
	MaxIndexesPerCollection int
}

// DefaultLimits 返回默认限制配置
func DefaultLimits() *Limits {
	return &Limits{
		MaxDocumentSize:       MaxBSONDocumentSize,
		MaxNestingDepth:       MaxDocumentNestingDepth,
		MaxWriteBatchSize:     MaxWriteBatchSize,
		MaxNamespaceLength:    MaxNamespaceLength,
		MaxIndexesPerCollection: MaxIndexesPerCollection,
	}
}

// ValidateDocument 验证 BSON 文档
func ValidateDocument(doc bson.D) error {
	return validateDocumentRecursive(doc, 0)
}

// validateDocumentRecursive 递归验证文档
func validateDocumentRecursive(doc bson.D, depth int) error {
	if depth > MaxDocumentNestingDepth {
		return ErrBadValue(fmt.Sprintf("document exceeds maximum nesting depth of %d", MaxDocumentNestingDepth))
	}
	
	if len(doc) > MaxObjectFields {
		return ErrBadValue(fmt.Sprintf("document exceeds maximum field count of %d", MaxObjectFields))
	}
	
	for _, elem := range doc {
		// 验证字段名
		if err := validateFieldName(elem.Key, depth == 0); err != nil {
			return err
		}
		
		// 递归验证嵌套文档和数组
		if err := validateValue(elem.Value, depth+1); err != nil {
			return err
		}
	}
	
	return nil
}

// validateValue 验证 BSON 值
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
// isTopLevel: 是否是顶级字段
// forStorage: 是否用于存储（true=文档字段, false=表达式/操作符）
func validateFieldName(name string, isTopLevel bool) error {
	return validateDocumentFieldName(name, isTopLevel)
}

// ValidateDocumentFieldName 验证文档字段名（用于存储）
// 文档顶级字段不能以 $ 开头（这是 MongoDB 的规则）
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
	if strings.Contains(name, "\x00") {
		return ErrBadValue("field names cannot contain null bytes")
	}
	
	// 检查有效 UTF-8
	if !utf8.ValidString(name) {
		return ErrBadValue("field names must be valid UTF-8")
	}
	
	// 文档的顶级字段绝对不能以 $ 开头（MongoDB 规则）
	// 这与表达式/filter 不同，表达式中可以使用 $eq/$gt 等操作符
	if isTopLevel && len(name) > 0 && name[0] == '$' {
		return NewMongoError(ErrorCodeDollarPrefixedFieldName, 
			fmt.Sprintf("field name '%s' cannot start with '$' in stored documents", name))
	}
	
	return nil
}

// ValidateExpressionFieldName 验证表达式字段名（用于 filter/update/pipeline）
// 允许 $ 开头的操作符（如 $eq, $set, $match）
func ValidateExpressionFieldName(name string) error {
	if name == "" {
		return NewMongoError(ErrorCodeEmptyFieldName, "field name cannot be empty")
	}
	
	if len(name) > MaxFieldNameLength {
		return ErrBadValue(fmt.Sprintf("field name exceeds maximum length of %d", MaxFieldNameLength))
	}
	
	// 检查空字符
	if strings.Contains(name, "\x00") {
		return ErrBadValue("field names cannot contain null bytes")
	}
	
	// 检查有效 UTF-8
	if !utf8.ValidString(name) {
		return ErrBadValue("field names must be valid UTF-8")
	}
	
	// 表达式中，$ 开头的必须是已知操作符
	if len(name) > 0 && name[0] == '$' {
		if !isKnownOperator(name) {
			return NewMongoError(ErrorCodeDollarPrefixedFieldName, 
				fmt.Sprintf("unknown operator '%s'", name))
		}
	}
	
	return nil
}

// isKnownOperator 检查是否是已知的操作符
func isKnownOperator(name string) bool {
	knownOperators := map[string]bool{
		// 查询操作符
		"$eq": true, "$ne": true, "$gt": true, "$gte": true,
		"$lt": true, "$lte": true, "$in": true, "$nin": true,
		"$and": true, "$or": true, "$not": true, "$nor": true,
		"$exists": true, "$type": true, "$regex": true, "$options": true,
		"$elemMatch": true, "$size": true, "$all": true,
		"$mod": true, "$text": true, "$where": true,
		
		// 更新操作符
		"$set": true, "$unset": true, "$inc": true, "$dec": true,
		"$mul": true, "$min": true, "$max": true, "$rename": true,
		"$push": true, "$pull": true, "$pop": true, "$addToSet": true,
		"$each": true, "$slice": true, "$sort": true, "$position": true,
		"$bit": true, "$currentDate": true, "$setOnInsert": true,
		
		// 聚合操作符（注：$sort 已在更新操作符中定义）
		"$match": true, "$project": true, "$group": true,
		"$limit": true, "$skip": true, "$unwind": true, "$lookup": true,
		"$count": true, "$facet": true, "$bucket": true, "$sample": true,
		"$sum": true, "$avg": true, "$first": true, "$last": true,
		"$concat": true, "$substr": true, "$toLower": true, "$toUpper": true,
		"$cond": true, "$ifNull": true, "$switch": true,
		
		// 其他
		"$db": true, "$readPreference": true, "$clusterTime": true,
	}
	return knownOperators[name]
}

// ValidateDatabaseName 验证数据库名
func ValidateDatabaseName(name string) error {
	if name == "" {
		return ErrInvalidNamespace("database name cannot be empty")
	}
	
	if len(name) > MaxDatabaseNameLength {
		return ErrInvalidNamespace(fmt.Sprintf("database name exceeds maximum length of %d", MaxDatabaseNameLength))
	}
	
	// 检查非法字符
	invalidChars := []string{"/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?", "\x00"}
	for _, ch := range invalidChars {
		if strings.Contains(name, ch) {
			return ErrInvalidNamespace(fmt.Sprintf("database name cannot contain '%s'", ch))
		}
	}
	
	// 不能是空白字符串
	if strings.TrimSpace(name) == "" {
		return ErrInvalidNamespace("database name cannot be empty or whitespace only")
	}
	
	return nil
}

// ValidateCollectionName 验证集合名
func ValidateCollectionName(name string) error {
	if name == "" {
		return ErrInvalidNamespace("collection name cannot be empty")
	}
	
	if len(name) > MaxCollectionNameLength {
		return ErrInvalidNamespace(fmt.Sprintf("collection name exceeds maximum length of %d", MaxCollectionNameLength))
	}
	
	// 不能以 system. 开头（系统保留）
	if strings.HasPrefix(name, "system.") {
		return ErrInvalidNamespace("collection name cannot start with 'system.'")
	}
	
	// 检查非法字符
	if strings.Contains(name, "$") {
		return ErrInvalidNamespace("collection name cannot contain '$'")
	}
	
	if strings.Contains(name, "\x00") {
		return ErrInvalidNamespace("collection name cannot contain null bytes")
	}
	
	// 不能是空白字符串
	if strings.TrimSpace(name) == "" {
		return ErrInvalidNamespace("collection name cannot be empty or whitespace only")
	}
	
	return nil
}

// ValidateNamespace 验证命名空间（db.collection）
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
func CheckDocumentSize(data []byte) error {
	if len(data) > MaxBSONDocumentSize {
		return ErrDocumentTooLarge(len(data), MaxBSONDocumentSize)
	}
	return nil
}


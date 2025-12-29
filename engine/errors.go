// Created by Yanjunhui

package engine

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// MongoDB 兼容错误码定义
// 参考：https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
const (
	// 通用错误 (1-99)
	ErrorCodeOK                 = 0
	ErrorCodeInternalError      = 1
	ErrorCodeBadValue           = 2
	ErrorCodeNoSuchKey          = 4
	ErrorCodeGraphContainsCycle = 5
	ErrorCodeHostUnreachable    = 6
	ErrorCodeHostNotFound       = 7
	ErrorCodeUnknownError       = 8
	ErrorCodeFailedToParse      = 9
	ErrorCodeCannotMutateObject = 10
	ErrorCodeUserNotFound       = 11
	ErrorCodeUnsupportedFormat  = 12
	ErrorCodeUnauthorized       = 13
	ErrorCodeTypeMismatch       = 14
	ErrorCodeOverflow           = 15
	ErrorCodeInvalidLength      = 16
	ErrorCodeProtocolError      = 17
	ErrorCodeAuthenticationFailed = 18
	ErrorCodeCannotReuseObject    = 19
	ErrorCodeIllegalOperation     = 20
	ErrorCodeEmptyArrayOperation  = 21
	ErrorCodeInvalidBSON          = 22

	// 命令错误 (26-50)
	ErrorCodeNamespaceNotFound    = 26
	ErrorCodeIndexNotFound        = 27
	ErrorCodePathNotViable        = 28
	ErrorCodeNonExistentPath      = 29
	ErrorCodeInvalidPath          = 30
	ErrorCodeRoleNotFound         = 31
	ErrorCodeRolesNotRelated      = 32
	ErrorCodePrivilegeNotFound    = 33
	ErrorCodeCannotBackfillArray  = 34
	ErrorCodeUserModificationFailed = 35
	ErrorCodeRemoteChangeDetected   = 36
	ErrorCodeFileRenameFailed       = 37
	ErrorCodeFileNotOpen            = 38
	ErrorCodeFileStreamFailed       = 39
	ErrorCodeConflictingUpdateOperators = 40
	ErrorCodeFileAlreadyOpen           = 41
	ErrorCodeLogWriteFailed            = 42
	ErrorCodeCursorNotFound            = 43

	// 查询和操作错误 (48-70)
	ErrorCodeDollarPrefixedFieldName = 52
	ErrorCodeInvalidIdField          = 53
	ErrorCodeNotSingleValueField     = 54
	ErrorCodeInvalidDBRef            = 55
	ErrorCodeEmptyFieldName          = 56
	ErrorCodeDottedFieldName         = 57
	ErrorCodeRoleDataInconsistent    = 58
	// ErrorCodeCommandNotFound = 59 定义在下方命令相关部分
	ErrorCodeNoProgressMade          = 60
	ErrorCodeRemoteResultsUnavailable = 61
	ErrorCodeIndexOptionsConflict     = 85
	ErrorCodeIndexKeySpecsConflict    = 86

	// 写错误 (61-80)
	ErrorCodeDuplicateKey            = 11000
	ErrorCodeCannotCreateIndex       = 67
	ErrorCodeIndexBuildAborted       = 71
	ErrorCodeWriteConcernFailed      = 64
	ErrorCodeMultipleErrorsOccurred  = 65

	// 事务/MVCC 错误
	ErrorCodeNoSuchTransaction       = 251
	ErrorCodeTransactionCommitted    = 256
	ErrorCodeTransactionAborted      = 263
	ErrorCodeNoSuchSession           = 206
	ErrorCodeTransactionTooOld       = 225

	// 命令相关
	ErrorCodeCommandNotFound         = 59
	ErrorCodeInvalidOptions          = 72
	ErrorCodeInvalidNamespace        = 73
	ErrorCodeNotWritablePrimary      = 10107
	ErrorCodeNetworkTimeout          = 89
	ErrorCodeOperationFailed         = 96

	// 文档相关
	ErrorCodeDocumentTooLarge        = 17419
	ErrorCodeDocumentValidationFailure = 121
)

// 错误码名称映射
var errorCodeNames = map[int]string{
	ErrorCodeOK:                  "OK",
	ErrorCodeInternalError:       "InternalError",
	ErrorCodeBadValue:            "BadValue",
	ErrorCodeNoSuchKey:           "NoSuchKey",
	ErrorCodeHostUnreachable:     "HostUnreachable",
	ErrorCodeHostNotFound:        "HostNotFound",
	ErrorCodeUnknownError:        "UnknownError",
	ErrorCodeFailedToParse:       "FailedToParse",
	ErrorCodeCannotMutateObject:  "CannotMutateObject",
	ErrorCodeUserNotFound:        "UserNotFound",
	ErrorCodeUnsupportedFormat:   "UnsupportedFormat",
	ErrorCodeUnauthorized:        "Unauthorized",
	ErrorCodeTypeMismatch:        "TypeMismatch",
	ErrorCodeOverflow:            "Overflow",
	ErrorCodeInvalidLength:       "InvalidLength",
	ErrorCodeProtocolError:       "ProtocolError",
	ErrorCodeAuthenticationFailed: "AuthenticationFailed",
	ErrorCodeIllegalOperation:    "IllegalOperation",
	ErrorCodeEmptyArrayOperation: "EmptyArrayOperation",
	ErrorCodeInvalidBSON:         "InvalidBSON",
	
	ErrorCodeNamespaceNotFound:   "NamespaceNotFound",
	ErrorCodeIndexNotFound:       "IndexNotFound",
	ErrorCodePathNotViable:       "PathNotViable",
	ErrorCodeNonExistentPath:     "NonExistentPath",
	ErrorCodeInvalidPath:         "InvalidPath",
	ErrorCodeCursorNotFound:      "CursorNotFound",
	
	ErrorCodeDollarPrefixedFieldName: "DollarPrefixedFieldName",
	ErrorCodeInvalidIdField:          "InvalidIdField",
	ErrorCodeEmptyFieldName:          "EmptyFieldName",
	ErrorCodeDottedFieldName:         "DottedFieldName",
	ErrorCodeIndexOptionsConflict:    "IndexOptionsConflict",
	ErrorCodeIndexKeySpecsConflict:   "IndexKeySpecsConflict",
	
	ErrorCodeDuplicateKey:           "DuplicateKey",
	ErrorCodeCannotCreateIndex:      "CannotCreateIndex",
	ErrorCodeWriteConcernFailed:     "WriteConcernFailed",
	ErrorCodeMultipleErrorsOccurred: "MultipleErrorsOccurred",
	
	ErrorCodeNoSuchTransaction:   "NoSuchTransaction",
	ErrorCodeTransactionCommitted: "TransactionCommitted",
	ErrorCodeTransactionAborted:  "TransactionAborted",
	ErrorCodeNoSuchSession:       "NoSuchSession",
	ErrorCodeTransactionTooOld:   "TransactionTooOld",
	
	ErrorCodeCommandNotFound:     "CommandNotFound",
	ErrorCodeInvalidOptions:      "InvalidOptions",
	ErrorCodeInvalidNamespace:    "InvalidNamespace",
	ErrorCodeNotWritablePrimary:  "NotWritablePrimary",
	ErrorCodeNetworkTimeout:      "NetworkTimeout",
	ErrorCodeOperationFailed:     "OperationFailed",
	
	ErrorCodeDocumentTooLarge:         "DocumentTooLarge",
	ErrorCodeDocumentValidationFailure: "DocumentValidationFailure",
}

// MongoError MongoDB 兼容的错误类型
type MongoError struct {
	Code     int    // 错误码
	CodeName string // 错误码名称
	Message  string // 错误消息
}

// Error 实现 error 接口
func (e *MongoError) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.CodeName, e.Code, e.Message)
}

// ErrorCode 返回错误码（实现 protocol.StructuredError 接口）
func (e *MongoError) ErrorCode() int {
	return e.Code
}

// ErrorCodeName 返回错误码名称（实现 protocol.StructuredError 接口）
func (e *MongoError) ErrorCodeName() string {
	return e.CodeName
}

// ToBSON 转换为 BSON 格式（用于响应）
func (e *MongoError) ToBSON() bson.D {
	return bson.D{
		{Key: "ok", Value: int32(0)},
		{Key: "errmsg", Value: e.Message},
		{Key: "code", Value: int32(e.Code)},
		{Key: "codeName", Value: e.CodeName},
	}
}

// NewMongoError 创建新的 MongoDB 错误
func NewMongoError(code int, message string) *MongoError {
	codeName, ok := errorCodeNames[code]
	if !ok {
		codeName = "UnknownError"
	}
	return &MongoError{
		Code:     code,
		CodeName: codeName,
		Message:  message,
	}
}

// 常用错误构造函数

// ErrInternalError 内部错误
func ErrInternalError(msg string) *MongoError {
	return NewMongoError(ErrorCodeInternalError, msg)
}

// ErrBadValue 参数值错误
func ErrBadValue(msg string) *MongoError {
	return NewMongoError(ErrorCodeBadValue, msg)
}

// ErrTypeMismatch 类型不匹配
func ErrTypeMismatch(msg string) *MongoError {
	return NewMongoError(ErrorCodeTypeMismatch, msg)
}

// ErrNamespaceNotFound 命名空间不存在
func ErrNamespaceNotFound(namespace string) *MongoError {
	return NewMongoError(ErrorCodeNamespaceNotFound, fmt.Sprintf("ns not found: %s", namespace))
}

// ErrCursorNotFound 游标不存在
func ErrCursorNotFound(cursorId int64) *MongoError {
	return NewMongoError(ErrorCodeCursorNotFound, fmt.Sprintf("cursor id %d not found", cursorId))
}

// ErrDuplicateKey 重复键错误
func ErrDuplicateKey(keyPattern string, keyValue interface{}) *MongoError {
	return NewMongoError(ErrorCodeDuplicateKey, fmt.Sprintf(
		"E11000 duplicate key error collection: %s dup key: %v", keyPattern, keyValue))
}

// ErrIndexNotFound 索引不存在
func ErrIndexNotFound(indexName string) *MongoError {
	return NewMongoError(ErrorCodeIndexNotFound, fmt.Sprintf("index not found with name [%s]", indexName))
}

// ErrCommandNotFound 命令不存在
func ErrCommandNotFound(cmdName string) *MongoError {
	return NewMongoError(ErrorCodeCommandNotFound, fmt.Sprintf("no such command: '%s'", cmdName))
}

// ErrInvalidNamespace 无效命名空间
func ErrInvalidNamespace(namespace string) *MongoError {
	return NewMongoError(ErrorCodeInvalidNamespace, fmt.Sprintf("Invalid namespace specified '%s'", namespace))
}

// ErrDocumentTooLarge 文档过大
func ErrDocumentTooLarge(size int, maxSize int) *MongoError {
	return NewMongoError(ErrorCodeDocumentTooLarge, fmt.Sprintf(
		"document is too large: %d bytes, max size is %d bytes", size, maxSize))
}

// ErrFailedToParse 解析失败
func ErrFailedToParse(msg string) *MongoError {
	return NewMongoError(ErrorCodeFailedToParse, msg)
}

// ErrInvalidOptions 无效选项
func ErrInvalidOptions(msg string) *MongoError {
	return NewMongoError(ErrorCodeInvalidOptions, msg)
}

// ErrIllegalOperation 非法操作
func ErrIllegalOperation(msg string) *MongoError {
	return NewMongoError(ErrorCodeIllegalOperation, msg)
}

// ErrInvalidIdField 无效的 _id 字段
func ErrInvalidIdField(msg string) *MongoError {
	return NewMongoError(ErrorCodeInvalidIdField, msg)
}

// ErrCannotCreateIndex 无法创建索引
func ErrCannotCreateIndex(msg string) *MongoError {
	return NewMongoError(ErrorCodeCannotCreateIndex, msg)
}

// IsMongoError 检查是否为 MongoError
func IsMongoError(err error) bool {
	_, ok := err.(*MongoError)
	return ok
}

// AsMongoError 将 error 转换为 MongoError（如果不是则包装为内部错误）
func AsMongoError(err error) *MongoError {
	if err == nil {
		return nil
	}
	if me, ok := err.(*MongoError); ok {
		return me
	}
	return ErrInternalError(err.Error())
}

// ErrorResponse 构建错误响应 BSON
func ErrorResponse(err error) bson.D {
	me := AsMongoError(err)
	return me.ToBSON()
}

// SuccessResponse 构建成功响应 BSON
func SuccessResponse(data bson.D) bson.D {
	// 确保 ok: 1 在响应中
	hasOk := false
	for _, elem := range data {
		if elem.Key == "ok" {
			hasOk = true
			break
		}
	}
	if !hasOk {
		data = append(bson.D{{Key: "ok", Value: int32(1)}}, data...)
	}
	return data
}

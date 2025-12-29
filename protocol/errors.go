// Created by Yanjunhui

package protocol

import "fmt"

// ProtocolError 表示协议层错误
// EN: ProtocolError represents a wire-protocol layer error.
type ProtocolError struct {
	Code    int32
	Message string
}

// 协议错误码（对齐 MongoDB 错误码）
// EN: Protocol error codes (aligned with MongoDB where possible).
const (
	ErrorCodeProtocolError  int32 = 2   // 通用协议错误 (EN: generic protocol error)
	ErrorCodeBadValue       int32 = 2   // 坏值 (EN: bad value)
	ErrorCodeChecksumFailed int32 = 300 // 校验和失败（自定义，MongoDB 没有专门的错误码） (EN: checksum failed; custom since MongoDB has no dedicated code)
)

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("ProtocolError(%d): %s", e.Code, e.Message)
}

// NewProtocolError 创建新的协议错误
// EN: NewProtocolError creates a new protocol error.
func NewProtocolError(message string) *ProtocolError {
	return &ProtocolError{
		Code:    ErrorCodeProtocolError,
		Message: message,
	}
}

// NewChecksumError 创建校验和失败错误
// EN: NewChecksumError creates a checksum-failed protocol error.
func NewChecksumError(message string) *ProtocolError {
	return &ProtocolError{
		Code:    ErrorCodeChecksumFailed,
		Message: message,
	}
}

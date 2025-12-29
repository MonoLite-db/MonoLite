// Created by Yanjunhui

package protocol

import "fmt"

// ProtocolError 表示协议层错误
type ProtocolError struct {
	Code    int32
	Message string
}

// 协议错误码（对齐 MongoDB 错误码）
const (
	ErrorCodeProtocolError   int32 = 2   // 通用协议错误
	ErrorCodeBadValue        int32 = 2   // 坏值
	ErrorCodeChecksumFailed  int32 = 300 // 校验和失败（自定义，MongoDB 没有专门的错误码）
)

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("ProtocolError(%d): %s", e.Code, e.Message)
}

// NewProtocolError 创建新的协议错误
func NewProtocolError(message string) *ProtocolError {
	return &ProtocolError{
		Code:    ErrorCodeProtocolError,
		Message: message,
	}
}

// NewChecksumError 创建校验和失败错误
func NewChecksumError(message string) *ProtocolError {
	return &ProtocolError{
		Code:    ErrorCodeChecksumFailed,
		Message: message,
	}
}


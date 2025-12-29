// Created by Yanjunhui

package protocol

import (
	"encoding/binary"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// OpQueryMessage 表示旧版 OP_QUERY 消息（已废弃但仍用于握手）
type OpQueryMessage struct {
	Flags              int32
	FullCollectionName string
	NumberToSkip       int32
	NumberToReturn     int32
	Query              bson.D
	ReturnFieldsSelector bson.D // 可选
}

// ParseOpQuery 解析 OP_QUERY 消息体
func ParseOpQuery(body []byte) (*OpQueryMessage, error) {
	if len(body) < 12 {
		return nil, fmt.Errorf("OP_QUERY body too short: %d", len(body))
	}

	q := &OpQueryMessage{
		Flags: int32(binary.LittleEndian.Uint32(body[0:4])),
	}

	pos := 4

	// 读取 fullCollectionName（C 字符串）
	nameEnd := pos
	for nameEnd < len(body) && body[nameEnd] != 0 {
		nameEnd++
	}
	if nameEnd >= len(body) {
		return nil, fmt.Errorf("OP_QUERY collection name not terminated")
	}
	q.FullCollectionName = string(body[pos:nameEnd])
	pos = nameEnd + 1

	// 读取 numberToSkip 和 numberToReturn
	if pos+8 > len(body) {
		return nil, fmt.Errorf("OP_QUERY missing skip/return fields")
	}
	q.NumberToSkip = int32(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4
	q.NumberToReturn = int32(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4

	// 读取查询文档
	if pos+4 > len(body) {
		return nil, fmt.Errorf("OP_QUERY missing query document")
	}
	docLen := int(binary.LittleEndian.Uint32(body[pos:]))
	if pos+docLen > len(body) {
		return nil, fmt.Errorf("OP_QUERY query document extends beyond message")
	}

	if err := bson.Unmarshal(body[pos:pos+docLen], &q.Query); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query: %w", err)
	}
	pos += docLen

	// 可选：读取 returnFieldsSelector
	if pos < len(body) && pos+4 <= len(body) {
		selectorLen := int(binary.LittleEndian.Uint32(body[pos:]))
		if pos+selectorLen <= len(body) {
			if err := bson.Unmarshal(body[pos:pos+selectorLen], &q.ReturnFieldsSelector); err == nil {
				// 忽略解析错误
			}
		}
	}

	return q, nil
}

// OpReplyMsg 表示 OP_REPLY 响应消息
type OpReplyMsg struct {
	ResponseFlags  int32
	CursorID       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      []bson.D
}

// BuildOpReply 构建 OP_REPLY 响应
func BuildOpReply(requestID int32, docs []bson.D) (*Message, error) {
	// 序列化所有文档
	var docBytes []byte
	for _, doc := range docs {
		b, err := bson.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}
		docBytes = append(docBytes, b...)
	}

	// OP_REPLY 结构：
	// responseFlags (4) + cursorID (8) + startingFrom (4) + numberReturned (4) + documents
	bodyLen := 4 + 8 + 4 + 4 + len(docBytes)
	body := make([]byte, bodyLen)

	pos := 0
	// responseFlags = 0
	binary.LittleEndian.PutUint32(body[pos:], 0)
	pos += 4

	// cursorID = 0
	binary.LittleEndian.PutUint64(body[pos:], 0)
	pos += 8

	// startingFrom = 0
	binary.LittleEndian.PutUint32(body[pos:], 0)
	pos += 4

	// numberReturned
	binary.LittleEndian.PutUint32(body[pos:], uint32(len(docs)))
	pos += 4

	// documents
	copy(body[pos:], docBytes)

	// 构建消息头
	msgLen := int32(HeaderSize + bodyLen)
	header := &MsgHeader{
		MessageLength: msgLen,
		RequestID:     nextRequestID(),
		ResponseTo:    requestID,
		OpCode:        OpReply,
	}

	return &Message{
		Header: header,
		Body:   body,
	}, nil
}

// Created by Yanjunhui

package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
)

// CRC32C (Castagnoli) 多项式，MongoDB Wire Protocol 使用此算法校验 OP_MSG
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// OP_MSG 标志位
const (
	MsgFlagChecksumPresent uint32 = 1 << 0  // 消息末尾有 CRC32 校验和
	MsgFlagMoreToCome      uint32 = 1 << 1  // 还有更多消息
	MsgFlagExhaustAllowed  uint32 = 1 << 16 // 允许 exhaust 模式

	// 【P2 修复】Required bits 掩码（bits 0-15）
	// MongoDB Wire Protocol: unknown required bits MUST cause an error
	MsgFlagKnownRequiredBits uint32 = MsgFlagChecksumPresent | MsgFlagMoreToCome
	MsgFlagRequiredBitsMask  uint32 = 0xFFFF // bits 0-15
)

// Section 类型
const (
	SectionTypeBody         byte = 0 // 单个 BSON 文档
	SectionTypeDocSequence  byte = 1 // 文档序列
)

// OpMsgMessage 表示 OP_MSG 消息
type OpMsgMessage struct {
	Flags    uint32
	Sections []Section
	Checksum uint32 // 仅当 ChecksumPresent 标志设置时有效
}

// Section 表示 OP_MSG 中的一个 section
type Section interface {
	Type() byte
}

// BodySection 表示 Kind 0 的 section（单个 BSON 文档）
type BodySection struct {
	Document bson.D
}

func (s *BodySection) Type() byte {
	return SectionTypeBody
}

// DocumentSequenceSection 表示 Kind 1 的 section（文档序列）
type DocumentSequenceSection struct {
	Identifier string   // 序列标识符
	Documents  []bson.D // 文档列表
}

func (s *DocumentSequenceSection) Type() byte {
	return SectionTypeDocSequence
}

// VerifyOpMsgChecksum 验证 OP_MSG 的 CRC32C 校验和
// fullMessage 是完整的消息（Header + Body），包含 checksum
// 返回 true 表示校验通过，false 表示校验失败
func VerifyOpMsgChecksum(fullMessage []byte) bool {
	if len(fullMessage) < HeaderSize+9 { // Header + flags(4) + kind(1) + checksum(4)
		return false
	}
	// checksum 覆盖范围：整个消息除最后 4 字节
	dataToCheck := fullMessage[:len(fullMessage)-4]
	expectedChecksum := binary.LittleEndian.Uint32(fullMessage[len(fullMessage)-4:])
	actualChecksum := crc32.Checksum(dataToCheck, crc32cTable)
	return actualChecksum == expectedChecksum
}

// ParseOpMsg 解析 OP_MSG 消息体
// fullMessage 参数可选，如果提供则用于验证 checksum
func ParseOpMsg(body []byte, fullMessage ...[]byte) (*OpMsgMessage, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("OP_MSG body too short: %d", len(body))
	}

	msg := &OpMsgMessage{
		Flags:    binary.LittleEndian.Uint32(body[0:4]),
		Sections: make([]Section, 0),
	}

	// 【P2 修复】检查 required bits（bits 0-15）
	// MongoDB Wire Protocol: 如果设置了任何未知的 required bit，必须返回错误
	requiredBits := msg.Flags & MsgFlagRequiredBitsMask
	unknownRequiredBits := requiredBits &^ MsgFlagKnownRequiredBits
	if unknownRequiredBits != 0 {
		return nil, NewProtocolError(fmt.Sprintf("OP_MSG contains unknown required flag bits: 0x%x", unknownRequiredBits))
	}

	pos := 4
	hasChecksum := msg.Flags&MsgFlagChecksumPresent != 0

	// 如果有校验和，消息体末尾 4 字节是校验和
	endPos := len(body)
	if hasChecksum {
		if len(body) < 9 {
			return nil, fmt.Errorf("OP_MSG with checksum too short")
		}
		msg.Checksum = binary.LittleEndian.Uint32(body[len(body)-4:])
		endPos = len(body) - 4

		// 如果提供了完整消息，验证 checksum
		if len(fullMessage) > 0 && fullMessage[0] != nil {
			if !VerifyOpMsgChecksum(fullMessage[0]) {
				return nil, NewProtocolError("OP_MSG checksum verification failed")
			}
		}
	}

	// 解析 sections
	for pos < endPos {
		kind := body[pos]
		pos++

		switch kind {
		case SectionTypeBody:
			// Kind 0: 单个 BSON 文档
			if pos+4 > endPos {
				return nil, fmt.Errorf("OP_MSG section body too short")
			}
			docLen := int(binary.LittleEndian.Uint32(body[pos:]))
			if pos+docLen > endPos {
				return nil, fmt.Errorf("OP_MSG document extends beyond message")
			}

			var doc bson.D
			if err := bson.Unmarshal(body[pos:pos+docLen], &doc); err != nil {
				return nil, fmt.Errorf("failed to unmarshal document: %w", err)
			}

			msg.Sections = append(msg.Sections, &BodySection{Document: doc})
			pos += docLen

		case SectionTypeDocSequence:
			// Kind 1: 文档序列
			if pos+4 > endPos {
				return nil, fmt.Errorf("OP_MSG document sequence too short")
			}
			seqLen := int(binary.LittleEndian.Uint32(body[pos:]))
			if pos+seqLen > endPos {
				return nil, fmt.Errorf("OP_MSG document sequence extends beyond message")
			}

			seqEnd := pos + seqLen
			pos += 4 // 跳过长度

			// 读取标识符（C 字符串）
			identEnd := pos
			for identEnd < seqEnd && body[identEnd] != 0 {
				identEnd++
			}
			if identEnd >= seqEnd {
				return nil, fmt.Errorf("OP_MSG document sequence identifier not terminated")
			}
			identifier := string(body[pos:identEnd])
			pos = identEnd + 1 // 跳过 null 终结符

			// 【P2 修复】严格解析文档序列
			docs := make([]bson.D, 0)
			for pos < seqEnd {
				if pos+4 > seqEnd {
					// 【严格解析】剩余字节不足以读取文档长度
					return nil, NewProtocolError(fmt.Sprintf(
						"OP_MSG document sequence truncated: need 4 bytes for doc length, have %d",
						seqEnd-pos))
				}
				docLen := int(binary.LittleEndian.Uint32(body[pos:]))
				if docLen < 5 { // BSON 文档最小长度是 5 字节
					return nil, NewProtocolError(fmt.Sprintf(
						"OP_MSG document sequence contains invalid document length: %d",
						docLen))
				}
				if pos+docLen > seqEnd {
					// 【严格解析】文档长度超出序列边界
					return nil, NewProtocolError(fmt.Sprintf(
						"OP_MSG document extends beyond sequence boundary: docLen=%d, remaining=%d",
						docLen, seqEnd-pos))
				}

				var doc bson.D
				if err := bson.Unmarshal(body[pos:pos+docLen], &doc); err != nil {
					return nil, fmt.Errorf("failed to unmarshal sequence document: %w", err)
				}
				docs = append(docs, doc)
				pos += docLen
			}

			// 【严格解析】确保精确消费到 seqEnd
			if pos != seqEnd {
				return nil, NewProtocolError(fmt.Sprintf(
					"OP_MSG document sequence did not consume all bytes: pos=%d, seqEnd=%d",
					pos, seqEnd))
			}

			msg.Sections = append(msg.Sections, &DocumentSequenceSection{
				Identifier: identifier,
				Documents:  docs,
			})

		default:
			return nil, fmt.Errorf("unknown OP_MSG section kind: %d", kind)
		}
	}

	return msg, nil
}

// GetCommand 从 OP_MSG 中提取命令文档
func (m *OpMsgMessage) GetCommand() (bson.D, error) {
	for _, section := range m.Sections {
		if body, ok := section.(*BodySection); ok {
			return body.Document, nil
		}
	}
	return nil, fmt.Errorf("no command document found")
}

// GetDocuments 获取文档序列（用于 insert 等批量操作）
func (m *OpMsgMessage) GetDocuments(identifier string) []bson.D {
	for _, section := range m.Sections {
		if seq, ok := section.(*DocumentSequenceSection); ok {
			if seq.Identifier == identifier {
				return seq.Documents
			}
		}
	}
	return nil
}

// BuildOpMsgReply 构建 OP_MSG 响应
func BuildOpMsgReply(requestID int32, responseDoc bson.D) (*Message, error) {
	// 序列化响应文档
	docBytes, err := bson.Marshal(responseDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	// 构建消息体：flags (4) + kind (1) + document
	bodyLen := 4 + 1 + len(docBytes)
	body := make([]byte, bodyLen)

	// Flags = 0
	binary.LittleEndian.PutUint32(body[0:4], 0)

	// Kind 0 section
	body[4] = SectionTypeBody

	// Document
	copy(body[5:], docBytes)

	// 构建消息头
	msgLen := int32(HeaderSize + bodyLen)
	header := &MsgHeader{
		MessageLength: msgLen,
		RequestID:     nextRequestID(),
		ResponseTo:    requestID,
		OpCode:        OpMsg,
	}

	return &Message{
		Header: header,
		Body:   body,
	}, nil
}

// 请求 ID 计数器（使用 atomic 避免并发 data race）
var requestIDCounter int32 = 0

func nextRequestID() int32 {
	return atomic.AddInt32(&requestIDCounter, 1)
}

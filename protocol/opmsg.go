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
// EN: CRC32C (Castagnoli) table; MongoDB Wire Protocol uses CRC32C for OP_MSG checksums.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// OP_MSG 标志位
// EN: OP_MSG flags.
const (
	MsgFlagChecksumPresent uint32 = 1 << 0  // 消息末尾有 CRC32 校验和 (EN: message ends with CRC32 checksum)
	MsgFlagMoreToCome      uint32 = 1 << 1  // 还有更多消息 (EN: more messages to come)
	MsgFlagExhaustAllowed  uint32 = 1 << 16 // 允许 exhaust 模式 (EN: exhaust allowed)

	// 【P2 修复】Required bits 掩码（bits 0-15）
	// EN: [P2 fix] Required-bits mask (bits 0-15).
	// MongoDB Wire Protocol: unknown required bits MUST cause an error
	// EN: MongoDB Wire Protocol requires erroring on unknown required bits.
	MsgFlagKnownRequiredBits uint32 = MsgFlagChecksumPresent | MsgFlagMoreToCome
	MsgFlagRequiredBitsMask  uint32 = 0xFFFF // bits 0-15
)

// Section 类型
// EN: OP_MSG section types.
const (
	SectionTypeBody        byte = 0 // 单个 BSON 文档 (EN: single BSON document)
	SectionTypeDocSequence byte = 1 // 文档序列 (EN: document sequence)
)

// OpMsgMessage 表示 OP_MSG 消息
// EN: OpMsgMessage represents an OP_MSG message.
type OpMsgMessage struct {
	Flags    uint32
	Sections []Section
	Checksum uint32 // 仅当 ChecksumPresent 标志设置时有效
	// EN: Valid only when the ChecksumPresent flag is set.
}

// Section 表示 OP_MSG 中的一个 section
// EN: Section represents a section within OP_MSG.
type Section interface {
	Type() byte
}

// BodySection 表示 Kind 0 的 section（单个 BSON 文档）
// EN: BodySection is a kind-0 section (a single BSON document).
type BodySection struct {
	Document bson.D
}

func (s *BodySection) Type() byte {
	return SectionTypeBody
}

// DocumentSequenceSection 表示 Kind 1 的 section（文档序列）
// EN: DocumentSequenceSection is a kind-1 section (a document sequence).
type DocumentSequenceSection struct {
	Identifier string // 序列标识符
	// EN: Sequence identifier.
	Documents []bson.D // 文档列表
	// EN: Document list.
}

func (s *DocumentSequenceSection) Type() byte {
	return SectionTypeDocSequence
}

// VerifyOpMsgChecksum 验证 OP_MSG 的 CRC32C 校验和
// EN: VerifyOpMsgChecksum validates the OP_MSG CRC32C checksum.
//
// fullMessage 是完整的消息（Header + Body），包含 checksum
// EN: fullMessage is the full wire message (Header + Body), including the checksum.
//
// 返回 true 表示校验通过，false 表示校验失败
// EN: Returns true if checksum matches; false otherwise.
func VerifyOpMsgChecksum(fullMessage []byte) bool {
	if len(fullMessage) < HeaderSize+9 { // Header + flags(4) + kind(1) + checksum(4)
		return false
	}
	// checksum 覆盖范围：整个消息除最后 4 字节
	// EN: Checksum covers the entire message excluding the last 4 bytes.
	dataToCheck := fullMessage[:len(fullMessage)-4]
	expectedChecksum := binary.LittleEndian.Uint32(fullMessage[len(fullMessage)-4:])
	actualChecksum := crc32.Checksum(dataToCheck, crc32cTable)
	return actualChecksum == expectedChecksum
}

// ParseOpMsg 解析 OP_MSG 消息体
// EN: ParseOpMsg parses an OP_MSG body.
//
// fullMessage 参数可选，如果提供则用于验证 checksum
// EN: fullMessage is optional; if provided, it is used to verify checksum.
func ParseOpMsg(body []byte, fullMessage ...[]byte) (*OpMsgMessage, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("OP_MSG body too short: %d", len(body))
	}

	msg := &OpMsgMessage{
		Flags:    binary.LittleEndian.Uint32(body[0:4]),
		Sections: make([]Section, 0),
	}

	// 【P2 修复】检查 required bits（bits 0-15）
	// EN: [P2 fix] Validate required bits (bits 0-15).
	// MongoDB Wire Protocol: 如果设置了任何未知的 required bit，必须返回错误
	// EN: MongoDB Wire Protocol requires returning an error if any unknown required bit is set.
	requiredBits := msg.Flags & MsgFlagRequiredBitsMask
	unknownRequiredBits := requiredBits &^ MsgFlagKnownRequiredBits
	if unknownRequiredBits != 0 {
		return nil, NewProtocolError(fmt.Sprintf("OP_MSG contains unknown required flag bits: 0x%x", unknownRequiredBits))
	}

	pos := 4
	hasChecksum := msg.Flags&MsgFlagChecksumPresent != 0

	// 如果有校验和，消息体末尾 4 字节是校验和
	// EN: If checksum is present, the last 4 bytes of the body are the checksum.
	endPos := len(body)
	if hasChecksum {
		if len(body) < 9 {
			return nil, fmt.Errorf("OP_MSG with checksum too short")
		}
		msg.Checksum = binary.LittleEndian.Uint32(body[len(body)-4:])
		endPos = len(body) - 4

		// 如果提供了完整消息，验证 checksum
		// EN: If the full message is provided, verify checksum.
		if len(fullMessage) > 0 && fullMessage[0] != nil {
			if !VerifyOpMsgChecksum(fullMessage[0]) {
				return nil, NewProtocolError("OP_MSG checksum verification failed")
			}
		}
	}

	// 解析 sections
	// EN: Parse sections.
	for pos < endPos {
		kind := body[pos]
		pos++

		switch kind {
		case SectionTypeBody:
			// Kind 0: 单个 BSON 文档
			// EN: Kind 0: single BSON document.
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
			// EN: Kind 1: document sequence.
			if pos+4 > endPos {
				return nil, fmt.Errorf("OP_MSG document sequence too short")
			}
			seqLen := int(binary.LittleEndian.Uint32(body[pos:]))
			if pos+seqLen > endPos {
				return nil, fmt.Errorf("OP_MSG document sequence extends beyond message")
			}

			seqEnd := pos + seqLen
			pos += 4 // 跳过长度 (EN: skip length)

			// 读取标识符（C 字符串）
			// EN: Read identifier (C-string).
			identEnd := pos
			for identEnd < seqEnd && body[identEnd] != 0 {
				identEnd++
			}
			if identEnd >= seqEnd {
				return nil, fmt.Errorf("OP_MSG document sequence identifier not terminated")
			}
			identifier := string(body[pos:identEnd])
			pos = identEnd + 1 // 跳过 null 终结符
			// EN: Skip null terminator.

			// 【P2 修复】严格解析文档序列
			// EN: [P2 fix] Strictly parse document sequence.
			docs := make([]bson.D, 0)
			for pos < seqEnd {
				if pos+4 > seqEnd {
					// 【严格解析】剩余字节不足以读取文档长度
					// EN: [Strict parsing] Not enough bytes to read document length.
					return nil, NewProtocolError(fmt.Sprintf(
						"OP_MSG document sequence truncated: need 4 bytes for doc length, have %d",
						seqEnd-pos))
				}
				docLen := int(binary.LittleEndian.Uint32(body[pos:]))
				if docLen < 5 { // BSON 文档最小长度是 5 字节 (EN: minimum BSON document length is 5 bytes)
					return nil, NewProtocolError(fmt.Sprintf(
						"OP_MSG document sequence contains invalid document length: %d",
						docLen))
				}
				if pos+docLen > seqEnd {
					// 【严格解析】文档长度超出序列边界
					// EN: [Strict parsing] Document length exceeds sequence boundary.
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
			// EN: [Strict parsing] Ensure we consumed exactly up to seqEnd.
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
// EN: GetCommand extracts the command document from OP_MSG.
func (m *OpMsgMessage) GetCommand() (bson.D, error) {
	for _, section := range m.Sections {
		if body, ok := section.(*BodySection); ok {
			return body.Document, nil
		}
	}
	return nil, fmt.Errorf("no command document found")
}

// GetDocuments 获取文档序列（用于 insert 等批量操作）
// EN: GetDocuments returns a document sequence by identifier (used for insert/update/delete batches).
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
// EN: BuildOpMsgReply builds an OP_MSG reply message.
func BuildOpMsgReply(requestID int32, responseDoc bson.D) (*Message, error) {
	// 序列化响应文档
	// EN: Marshal response document.
	docBytes, err := bson.Marshal(responseDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	// 构建消息体：flags (4) + kind (1) + document
	// EN: Build body: flags (4) + kind (1) + document.
	bodyLen := 4 + 1 + len(docBytes)
	body := make([]byte, bodyLen)

	// Flags = 0
	binary.LittleEndian.PutUint32(body[0:4], 0)

	// Kind 0 section
	body[4] = SectionTypeBody

	// Document
	copy(body[5:], docBytes)

	// 构建消息头
	// EN: Build message header.
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
// EN: Request ID counter (use atomic to avoid concurrent data races).
var requestIDCounter int32 = 0

func nextRequestID() int32 {
	return atomic.AddInt32(&requestIDCounter, 1)
}

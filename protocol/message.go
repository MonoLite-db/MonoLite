// Created by Yanjunhui

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MongoDB Wire Protocol 操作码
// EN: MongoDB Wire Protocol opcodes.
const (
	OpReply       int32 = 1    // 已废弃，但需要兼容旧客户端 (EN: deprecated; kept for legacy compatibility)
	OpUpdate      int32 = 2001 // 已废弃 (EN: deprecated)
	OpInsert      int32 = 2002 // 已废弃 (EN: deprecated)
	OpQuery       int32 = 2004 // 已废弃，但 hello 握手仍可能使用 (EN: deprecated; still used by some clients for handshake)
	OpGetMore     int32 = 2005 // 已废弃 (EN: deprecated)
	OpDelete      int32 = 2006 // 已废弃 (EN: deprecated)
	OpKillCursors int32 = 2007 // 已废弃 (EN: deprecated)
	OpCompressed  int32 = 2012 // 压缩消息 (EN: compressed message)
	OpMsg         int32 = 2013 // MongoDB 3.6+ 主要消息格式 (EN: primary message format since MongoDB 3.6)
)

// MsgHeader 消息头（16 字节）
// EN: MsgHeader is the 16-byte wire message header.
type MsgHeader struct {
	MessageLength int32 // 消息总长度（包括头部） (EN: total message length including header)
	RequestID     int32 // 请求 ID (EN: request ID)
	ResponseTo    int32 // 响应的请求 ID (EN: request ID this is responding to)
	OpCode        int32 // 操作码 (EN: opcode)
}

// HeaderSize 消息头大小
// EN: HeaderSize is the wire header size in bytes.
const HeaderSize = 16

// ReadHeader 从 Reader 读取消息头
// EN: ReadHeader reads a message header from an io.Reader.
func ReadHeader(r io.Reader) (*MsgHeader, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return &MsgHeader{
		MessageLength: int32(binary.LittleEndian.Uint32(buf[0:4])),
		RequestID:     int32(binary.LittleEndian.Uint32(buf[4:8])),
		ResponseTo:    int32(binary.LittleEndian.Uint32(buf[8:12])),
		OpCode:        int32(binary.LittleEndian.Uint32(buf[12:16])),
	}, nil
}

// Write 将消息头写入 Writer
// EN: Write writes the message header to an io.Writer.
func (h *MsgHeader) Write(w io.Writer) error {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.MessageLength))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(h.RequestID))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.ResponseTo))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.OpCode))

	_, err := w.Write(buf)
	return err
}

// Bytes 返回消息头的字节表示
// EN: Bytes returns the byte representation of the header.
func (h *MsgHeader) Bytes() []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.MessageLength))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(h.RequestID))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.ResponseTo))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.OpCode))
	return buf
}

// Message 表示一个 MongoDB Wire Protocol 消息
// EN: Message represents a MongoDB Wire Protocol message.
type Message struct {
	Header *MsgHeader
	Body   []byte
}

// ReadMessage 从 Reader 读取完整消息
// EN: ReadMessage reads a full message (header + body) from an io.Reader.
func ReadMessage(r io.Reader) (*Message, error) {
	header, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}

	// 验证消息长度
	// EN: Validate message length.
	if header.MessageLength < HeaderSize {
		return nil, fmt.Errorf("invalid message length: %d", header.MessageLength)
	}

	// 限制最大消息大小（48MB）
	// EN: Enforce maximum message size (48MB).
	if header.MessageLength > 48*1024*1024 {
		return nil, fmt.Errorf("message too large: %d", header.MessageLength)
	}

	// 读取消息体
	// EN: Read message body.
	bodyLen := header.MessageLength - HeaderSize
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	return &Message{
		Header: header,
		Body:   body,
	}, nil
}

// Write 将消息写入 Writer
// EN: Write writes the full message to an io.Writer.
func (m *Message) Write(w io.Writer) error {
	if err := m.Header.Write(w); err != nil {
		return err
	}
	_, err := w.Write(m.Body)
	return err
}

// Bytes 返回完整消息的字节表示
// EN: Bytes returns the byte representation of the full message.
func (m *Message) Bytes() []byte {
	result := make([]byte, m.Header.MessageLength)
	copy(result[0:HeaderSize], m.Header.Bytes())
	copy(result[HeaderSize:], m.Body)
	return result
}

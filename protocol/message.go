// Created by Yanjunhui

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MongoDB Wire Protocol 操作码
const (
	OpReply       int32 = 1    // 已废弃，但需要兼容旧客户端
	OpUpdate      int32 = 2001 // 已废弃
	OpInsert      int32 = 2002 // 已废弃
	OpQuery       int32 = 2004 // 已废弃，但 hello 握手仍可能使用
	OpGetMore     int32 = 2005 // 已废弃
	OpDelete      int32 = 2006 // 已废弃
	OpKillCursors int32 = 2007 // 已废弃
	OpCompressed  int32 = 2012 // 压缩消息
	OpMsg         int32 = 2013 // MongoDB 3.6+ 主要消息格式
)

// MsgHeader 消息头（16 字节）
type MsgHeader struct {
	MessageLength int32 // 消息总长度（包括头部）
	RequestID     int32 // 请求 ID
	ResponseTo    int32 // 响应的请求 ID
	OpCode        int32 // 操作码
}

// HeaderSize 消息头大小
const HeaderSize = 16

// ReadHeader 从 Reader 读取消息头
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
func (h *MsgHeader) Bytes() []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.MessageLength))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(h.RequestID))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.ResponseTo))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.OpCode))
	return buf
}

// Message 表示一个 MongoDB Wire Protocol 消息
type Message struct {
	Header *MsgHeader
	Body   []byte
}

// ReadMessage 从 Reader 读取完整消息
func ReadMessage(r io.Reader) (*Message, error) {
	header, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}

	// 验证消息长度
	if header.MessageLength < HeaderSize {
		return nil, fmt.Errorf("invalid message length: %d", header.MessageLength)
	}

	// 限制最大消息大小（48MB）
	if header.MessageLength > 48*1024*1024 {
		return nil, fmt.Errorf("message too large: %d", header.MessageLength)
	}

	// 读取消息体
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
func (m *Message) Write(w io.Writer) error {
	if err := m.Header.Write(w); err != nil {
		return err
	}
	_, err := w.Write(m.Body)
	return err
}

// Bytes 返回完整消息的字节表示
func (m *Message) Bytes() []byte {
	result := make([]byte, m.Header.MessageLength)
	copy(result[0:HeaderSize], m.Header.Bytes())
	copy(result[HeaderSize:], m.Body)
	return result
}

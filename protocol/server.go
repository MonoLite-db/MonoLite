// Created by Yanjunhui

package protocol

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/monolite/monodb/engine"
)

// Server 表示 MongoDB Wire Protocol 服务器
// EN: Server is a MongoDB Wire Protocol server.
type Server struct {
	addr     string
	db       *engine.Database
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewServer 创建一个新的服务器
// EN: NewServer creates a new server.
func NewServer(addr string, db *engine.Database) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		db:     db,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动服务器
// EN: Start starts listening and accepting connections.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = listener

	log.Printf("MonoDB server listening on %s", s.addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop 停止服务器
// EN: Stop stops the server and waits for all connections to finish.
func (s *Server) Stop() error {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// acceptLoop 接受新连接
// EN: acceptLoop accepts new connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个连接
// EN: handleConnection processes a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", clientAddr)

	handler := &ConnectionHandler{
		conn:   conn,
		db:     s.db,
		ctx:    s.ctx,
		dbName: "test", // 默认数据库名 (EN: default database name)
	}

	if err := handler.Run(); err != nil {
		if err != io.EOF && !strings.Contains(err.Error(), "use of closed") {
			log.Printf("Connection %s error: %v", clientAddr, err)
		}
	}

	log.Printf("Connection %s closed", clientAddr)
}

// Addr 返回服务器监听地址
// EN: Addr returns the server listening address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// ConnectionHandler 处理单个连接的消息
// EN: ConnectionHandler handles messages on a single connection.
type ConnectionHandler struct {
	conn   net.Conn
	db     *engine.Database
	ctx    context.Context
	dbName string
}

// Run 运行连接处理循环
// EN: Run executes the connection read/dispatch loop.
func (h *ConnectionHandler) Run() error {
	for {
		select {
		case <-h.ctx.Done():
			return nil
		default:
		}

		// 设置读取超时
		// EN: Set read deadline.
		h.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		// 读取消息
		// EN: Read a message.
		msg, err := ReadMessage(h.conn)
		if err != nil {
			return err
		}

		// 处理消息
		// EN: Handle message.
		response, err := h.handleMessage(msg)
		if err != nil {
			log.Printf("Error handling message: %v", err)
			// 尝试发送错误响应
			// EN: Attempt to send an error response.
			response = h.buildErrorResponse(msg.Header.RequestID, err)
		}

		if response != nil {
			if err := response.Write(h.conn); err != nil {
				return fmt.Errorf("failed to write response: %w", err)
			}
		}
	}
}

// handleMessage 根据操作码处理消息
// EN: handleMessage dispatches by opcode.
func (h *ConnectionHandler) handleMessage(msg *Message) (*Message, error) {
	switch msg.Header.OpCode {
	case OpMsg:
		return h.handleOpMsg(msg)
	case OpQuery:
		return h.handleOpQuery(msg)
	case OpCompressed:
		// OP_COMPRESSED 不支持：返回明确的协议错误
		// EN: OP_COMPRESSED is not supported; return a clear protocol error.
		// 驱动在握手时已被告知不支持 compression（hello 不宣称 compression）
		// EN: OP_COMPRESSED is not supported; return a protocol error.
		// EN: The driver is informed during handshake (hello does not advertise compression).
		return buildErrorResponse(msg.Header.RequestID, ErrorCodeProtocolError, "ProtocolError",
			"OP_COMPRESSED is not supported. Server does not support compression.")
	default:
		return nil, fmt.Errorf("unsupported opcode: %d", msg.Header.OpCode)
	}
}

// handleOpMsg 处理 OP_MSG 消息
// EN: handleOpMsg handles OP_MSG messages.
func (h *ConnectionHandler) handleOpMsg(msg *Message) (*Message, error) {
	// 传入完整消息以便进行 checksum 校验（如果设置了 ChecksumPresent 标志）
	// EN: Pass the full message bytes to validate checksum if ChecksumPresent is set.
	opMsg, err := ParseOpMsg(msg.Body, msg.Bytes())
	if err != nil {
		// 如果是协议错误（如 checksum 校验失败），返回结构化错误
		// EN: For protocol errors (e.g., checksum failure), return a structured error response.
		if protocolErr, ok := err.(*ProtocolError); ok {
			return buildErrorResponse(msg.Header.RequestID, protocolErr.Code, "ProtocolError", protocolErr.Message)
		}
		return nil, err
	}

	cmd, err := opMsg.GetCommand()
	if err != nil {
		return nil, err
	}

	// 检查是否有文档序列（用于 insert/update/delete 等操作）
	// EN: Attach document sequences for insert/update/delete commands.
	// insert 使用 "documents" 序列
	// EN: insert uses the "documents" sequence.
	if docs := opMsg.GetDocuments("documents"); docs != nil {
		cmd = append(cmd, bson.E{Key: "documents", Value: docsToArray(docs)})
	}
	// update 使用 "updates" 序列
	// EN: update uses the "updates" sequence.
	if docs := opMsg.GetDocuments("updates"); docs != nil {
		cmd = append(cmd, bson.E{Key: "updates", Value: docsToArray(docs)})
	}
	// delete 使用 "deletes" 序列
	// EN: delete uses the "deletes" sequence.
	if docs := opMsg.GetDocuments("deletes"); docs != nil {
		cmd = append(cmd, bson.E{Key: "deletes", Value: docsToArray(docs)})
	}

	// 提取数据库名
	// EN: Extract database name from $db.
	if dbVal := getFieldValue(cmd, "$db"); dbVal != nil {
		if dbName, ok := dbVal.(string); ok {
			h.dbName = dbName
		}
	}

	// 执行命令
	// EN: Execute command.
	response, err := h.db.RunCommand(cmd)
	if err != nil {
		response = buildStructuredErrorResponse(err)
	}

	return BuildOpMsgReply(msg.Header.RequestID, response)
}

// handleOpQuery 处理 OP_QUERY 消息（主要用于握手）
// EN: handleOpQuery handles legacy OP_QUERY messages (primarily for handshake).
func (h *ConnectionHandler) handleOpQuery(msg *Message) (*Message, error) {
	query, err := ParseOpQuery(msg.Body)
	if err != nil {
		return nil, err
	}

	// 检查是否是 admin.$cmd 查询（通常是 isMaster/hello）
	// EN: Only admin.$cmd is supported here (typically isMaster/hello).
	if strings.HasSuffix(query.FullCollectionName, ".$cmd") {
		response, err := h.db.RunCommand(query.Query)
		if err != nil {
			response = buildStructuredErrorResponse(err)
		}
		return BuildOpReply(msg.Header.RequestID, []bson.D{response})
	}

	// 其他 OP_QUERY 不再支持
	// EN: Other OP_QUERY queries are not supported.
	return BuildOpReply(msg.Header.RequestID, []bson.D{{
		{Key: "ok", Value: int32(0)},
		{Key: "errmsg", Value: "OP_QUERY is deprecated, use OP_MSG"},
	}})
}

// StructuredError 接口用于检测结构化错误（来自 engine.MongoError 等）
// EN: StructuredError is implemented by engine errors that carry MongoDB code/codeName.
type StructuredError interface {
	error
	ErrorCode() int
	ErrorCodeName() string
}

// buildStructuredErrorResponse 构建结构化错误响应
// EN: buildStructuredErrorResponse builds a structured error response.
//
// 如果 err 实现了 StructuredError 接口，则包含 code/codeName
// EN: If err implements StructuredError, include code/codeName.
func buildStructuredErrorResponse(err error) bson.D {
	if se, ok := err.(StructuredError); ok {
		return bson.D{
			{Key: "ok", Value: int32(0)},
			{Key: "errmsg", Value: err.Error()},
			{Key: "code", Value: int32(se.ErrorCode())},
			{Key: "codeName", Value: se.ErrorCodeName()},
		}
	}
	// 普通错误，至少包含 errmsg
	// EN: Generic error: at least include errmsg.
	return bson.D{
		{Key: "ok", Value: int32(0)},
		{Key: "errmsg", Value: err.Error()},
		{Key: "code", Value: int32(1)}, // 通用错误码 (EN: generic error code)
		{Key: "codeName", Value: "InternalError"},
	}
}

// buildErrorResponse 构建错误响应（方法版本，向后兼容）
// EN: buildErrorResponse builds an error response (method form, for backward compatibility).
func (h *ConnectionHandler) buildErrorResponse(requestID int32, err error) *Message {
	response := buildStructuredErrorResponse(err)
	msg, _ := BuildOpMsgReply(requestID, response)
	return msg
}

// buildErrorResponse 构建带有 code/codeName 的结构化错误响应
// EN: buildErrorResponse builds a structured error response containing code/codeName.
func buildErrorResponse(requestID int32, code int32, codeName string, message string) (*Message, error) {
	response := bson.D{
		{Key: "ok", Value: int32(0)},
		{Key: "errmsg", Value: message},
		{Key: "code", Value: code},
		{Key: "codeName", Value: codeName},
	}
	return BuildOpMsgReply(requestID, response)
}

// getFieldValue 从 bson.D 中获取字段值
// EN: getFieldValue returns the value of key in a bson.D (or nil if missing).
func getFieldValue(doc bson.D, key string) interface{} {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value
		}
	}
	return nil
}

// docsToArray 将 []bson.D 转换为 bson.A
// EN: docsToArray converts []bson.D to bson.A.
func docsToArray(docs []bson.D) bson.A {
	arr := make(bson.A, len(docs))
	for i, doc := range docs {
		arr[i] = doc
	}
	return arr
}

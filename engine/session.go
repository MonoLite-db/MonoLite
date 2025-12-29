// Created by Yanjunhui

package engine

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SessionState 会话状态
// EN: SessionState represents the session state.
const (
	SessionStateActive = iota
	SessionStateEnded
)

// Session MongoDB 逻辑会话
// EN: Session represents a MongoDB logical session.
//
// 参考：https://www.mongodb.com/docs/manual/reference/server-sessions/
// EN: Reference: https://www.mongodb.com/docs/manual/reference/server-sessions/
type Session struct {
	ID         primitive.Binary // lsid.id (UUID)
	LastUsed   time.Time
	State      int
	CurrentTxn *SessionTransaction
	// TxnNumberUsed 最近使用的 txnNumber
	// EN: TxnNumberUsed is the most recently used txnNumber.
	TxnNumberUsed int64
	mu            sync.Mutex
}

// SessionTransaction 会话内的事务
// EN: SessionTransaction represents a transaction within a session.
type SessionTransaction struct {
	TxnNumber  int64
	State      int // TxnStateActive/TxnStateCommitted/TxnStateAborted
	StartTime  time.Time
	AutoCommit bool
	// Transaction 关联的底层事务
	// EN: Transaction is the underlying engine transaction.
	Transaction *Transaction
	// Operations 事务内操作计数
	// EN: Operations is the operation count within the transaction.
	Operations   int
	ReadConcern  string
	WriteConcern string
}

// SessionManager 会话管理器
// EN: SessionManager manages sessions.
type SessionManager struct {
	sessions      map[string]*Session // lsid.id hex -> Session
	mu            sync.RWMutex
	db            *Database
	sessionTTL    time.Duration
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// SessionOptions 会话选项
// EN: SessionOptions contains session options.
type SessionOptions struct {
	DefaultTimeout    time.Duration
	CausalConsistency bool
}

// NewSessionManager 创建会话管理器
// EN: NewSessionManager creates a SessionManager.
func NewSessionManager(db *Database) *SessionManager {
	sm := &SessionManager{
		sessions:   make(map[string]*Session),
		db:         db,
		sessionTTL: 30 * time.Minute, // MongoDB 默认 30 分钟会话超时
		// EN: MongoDB default session timeout is 30 minutes.
		stopCleanup: make(chan struct{}),
	}

	// 启动过期会话清理协程
	// EN: Start background cleanup for expired sessions.
	sm.cleanupTicker = time.NewTicker(5 * time.Minute)
	go sm.cleanupExpiredSessions()

	return sm
}

// GetOrCreateSession 获取或创建会话
// EN: GetOrCreateSession gets or creates a session.
func (sm *SessionManager) GetOrCreateSession(lsid bson.D) (*Session, error) {
	// 从 lsid 提取 id
	// EN: Extract id from lsid.
	var sessionID primitive.Binary
	for _, elem := range lsid {
		if elem.Key == "id" {
			if id, ok := elem.Value.(primitive.Binary); ok {
				sessionID = id
			}
		}
	}

	if sessionID.Data == nil {
		return nil, NewMongoError(ErrorCodeBadValue, "lsid.id is required")
	}

	key := sessionIDToKey(sessionID)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if session, exists := sm.sessions[key]; exists {
		session.mu.Lock()
		session.LastUsed = time.Now()
		session.mu.Unlock()
		return session, nil
	}

	// 创建新会话
	// EN: Create a new session.
	session := &Session{
		ID:            sessionID,
		LastUsed:      time.Now(),
		State:         SessionStateActive,
		TxnNumberUsed: -1, // 表示还未使用过事务
		// EN: -1 means no transaction number has been used yet.
	}
	sm.sessions[key] = session

	return session, nil
}

// EndSession 结束会话
// EN: EndSession ends a session.
func (sm *SessionManager) EndSession(lsid bson.D) error {
	var sessionID primitive.Binary
	for _, elem := range lsid {
		if elem.Key == "id" {
			if id, ok := elem.Value.(primitive.Binary); ok {
				sessionID = id
			}
		}
	}

	if sessionID.Data == nil {
		return NewMongoError(ErrorCodeBadValue, "lsid.id is required")
	}

	key := sessionIDToKey(sessionID)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if session, exists := sm.sessions[key]; exists {
		session.mu.Lock()
		// 如果有活跃事务，中止它
		// EN: Abort active transaction if present.
		if session.CurrentTxn != nil && session.CurrentTxn.State == TxnStateActive {
			if session.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
				_ = sm.db.txnManager.Abort(session.CurrentTxn.Transaction)
			}
			session.CurrentTxn.State = TxnStateAborted
		}
		session.State = SessionStateEnded
		session.mu.Unlock()

		delete(sm.sessions, key)
	}

	return nil
}

// RefreshSession 刷新会话（延长超时）
// EN: RefreshSession refreshes a session (extends timeout).
func (sm *SessionManager) RefreshSession(lsid bson.D) error {
	var sessionID primitive.Binary
	for _, elem := range lsid {
		if elem.Key == "id" {
			if id, ok := elem.Value.(primitive.Binary); ok {
				sessionID = id
			}
		}
	}

	key := sessionIDToKey(sessionID)

	sm.mu.RLock()
	session, exists := sm.sessions[key]
	sm.mu.RUnlock()

	if !exists {
		return NewMongoError(ErrorCodeNoSuchSession, "session not found")
	}

	session.mu.Lock()
	session.LastUsed = time.Now()
	session.mu.Unlock()

	return nil
}

// StartTransaction 在会话中启动事务
// EN: StartTransaction starts a transaction within a session.
func (sm *SessionManager) StartTransaction(session *Session, txnNumber int64, readConcern, writeConcern string) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.State != SessionStateActive {
		return NewMongoError(ErrorCodeNoSuchSession, "session has ended")
	}

	// 检查 txnNumber
	// EN: Validate txnNumber.
	if txnNumber <= session.TxnNumberUsed {
		return NewMongoError(ErrorCodeTransactionTooOld, "txnNumber is too old")
	}

	// 如果有正在进行的事务，先中止它
	// EN: Abort any in-progress transaction first.
	if session.CurrentTxn != nil && session.CurrentTxn.State == TxnStateActive {
		// 隐式中止前一个事务
		// EN: Implicitly abort the previous transaction.
		if session.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
			sm.db.txnManager.Abort(session.CurrentTxn.Transaction)
		}
	}

	// 创建底层事务
	// EN: Create underlying engine transaction.
	var txn *Transaction
	if sm.db.txnManager != nil {
		txn = sm.db.txnManager.Begin()
	}

	session.CurrentTxn = &SessionTransaction{
		TxnNumber:    txnNumber,
		State:        TxnStateActive,
		StartTime:    time.Now(),
		AutoCommit:   false,
		Transaction:  txn,
		ReadConcern:  readConcern,
		WriteConcern: writeConcern,
	}
	session.TxnNumberUsed = txnNumber
	session.LastUsed = time.Now()

	return nil
}

// CommitTransaction 提交会话中的事务（MongoDB 标准：必须匹配 txnNumber）
// EN: CommitTransaction commits the session transaction (MongoDB requires txnNumber to match).
func (sm *SessionManager) CommitTransaction(session *Session, txnNumber int64) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.CurrentTxn == nil {
		return NewMongoError(ErrorCodeNoSuchTransaction, "no transaction in progress")
	}

	// 必须匹配 txnNumber（避免提交到错误的事务）
	// EN: txnNumber must match to avoid committing the wrong transaction.
	if session.CurrentTxn.TxnNumber != txnNumber {
		return NewMongoError(ErrorCodeNoSuchTransaction, "transaction number mismatch")
	}

	if session.CurrentTxn.State != TxnStateActive {
		if session.CurrentTxn.State == TxnStateCommitted {
			// 重复提交是允许的（幂等性）
			// EN: Repeated commit is allowed (idempotent).
			return nil
		}
		return NewMongoError(ErrorCodeTransactionAborted, "transaction has been aborted")
	}

	// 提交底层事务
	// EN: Commit underlying engine transaction.
	if session.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
		if err := sm.db.txnManager.Commit(session.CurrentTxn.Transaction); err != nil {
			return err
		}
	}

	session.CurrentTxn.State = TxnStateCommitted
	session.LastUsed = time.Now()

	return nil
}

// AbortTransaction 中止会话中的事务（MongoDB 标准：必须匹配 txnNumber）
// EN: AbortTransaction aborts the session transaction (MongoDB requires txnNumber to match).
func (sm *SessionManager) AbortTransaction(session *Session, txnNumber int64) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.CurrentTxn == nil {
		return NewMongoError(ErrorCodeNoSuchTransaction, "no transaction in progress")
	}

	// 必须匹配 txnNumber（避免中止到错误的事务）
	// EN: txnNumber must match to avoid aborting the wrong transaction.
	if session.CurrentTxn.TxnNumber != txnNumber {
		return NewMongoError(ErrorCodeNoSuchTransaction, "transaction number mismatch")
	}

	if session.CurrentTxn.State != TxnStateActive {
		if session.CurrentTxn.State == TxnStateAborted {
			// 重复中止是允许的（幂等性）
			// EN: Repeated abort is allowed (idempotent).
			return nil
		}
		return NewMongoError(ErrorCodeTransactionCommitted, "transaction has been committed")
	}

	// 中止底层事务
	// EN: Abort underlying engine transaction.
	if session.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
		if err := sm.db.txnManager.Abort(session.CurrentTxn.Transaction); err != nil {
			return err
		}
	}

	session.CurrentTxn.State = TxnStateAborted
	session.LastUsed = time.Now()

	return nil
}

// GetActiveTransaction 获取会话的活跃事务
// EN: GetActiveTransaction returns the active transaction for a session.
func (sm *SessionManager) GetActiveTransaction(session *Session, txnNumber int64) (*SessionTransaction, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.CurrentTxn == nil {
		return nil, NewMongoError(ErrorCodeNoSuchTransaction, "no transaction in progress")
	}

	if session.CurrentTxn.TxnNumber != txnNumber {
		return nil, NewMongoError(ErrorCodeNoSuchTransaction, "transaction number mismatch")
	}

	if session.CurrentTxn.State != TxnStateActive {
		if session.CurrentTxn.State == TxnStateCommitted {
			return nil, NewMongoError(ErrorCodeTransactionCommitted, "transaction has been committed")
		}
		return nil, NewMongoError(ErrorCodeTransactionAborted, "transaction has been aborted")
	}

	return session.CurrentTxn, nil
}

// cleanupExpiredSessions 清理过期会话
// EN: cleanupExpiredSessions cleans up expired sessions.
func (sm *SessionManager) cleanupExpiredSessions() {
	for {
		select {
		case <-sm.cleanupTicker.C:
			sm.doCleanup()
		case <-sm.stopCleanup:
			return
		}
	}
}

// doCleanup 执行清理
// EN: doCleanup performs cleanup.
func (sm *SessionManager) doCleanup() {
	now := time.Now()
	expiredKeys := make([]string, 0)

	// 先在 SessionManager 读锁下拷贝会话快照，避免锁顺序反转导致潜在死锁：
	// EN: Copy a snapshot under SessionManager read lock first to avoid lock-order inversion deadlocks.
	// - 其他路径常见顺序：sm.mu -> session.mu
	// EN: - Common lock order elsewhere: sm.mu -> session.mu
	// - 若这里持 sm.mu.RLock 再拿 session.mu，容易与“先拿 session.mu 再等待 sm.mu.Lock”形成死锁环
	// EN: - Holding sm.mu.RLock then taking session.mu can deadlock with paths that take session.mu first and then wait for sm.mu.Lock.
	type sessionItem struct {
		key     string
		session *Session
	}
	items := make([]sessionItem, 0)
	sm.mu.RLock()
	for key, s := range sm.sessions {
		items = append(items, sessionItem{key: key, session: s})
	}
	sm.mu.RUnlock()

	for _, it := range items {
		s := it.session
		s.mu.Lock()
		if now.Sub(s.LastUsed) > sm.sessionTTL {
			// 中止活跃事务
			// EN: Abort active transaction.
			if s.CurrentTxn != nil && s.CurrentTxn.State == TxnStateActive {
				if s.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
					_ = sm.db.txnManager.Abort(s.CurrentTxn.Transaction)
				}
				s.CurrentTxn.State = TxnStateAborted
			}
			expiredKeys = append(expiredKeys, it.key)
		}
		s.mu.Unlock()
	}

	if len(expiredKeys) > 0 {
		sm.mu.Lock()
		for _, key := range expiredKeys {
			delete(sm.sessions, key)
		}
		sm.mu.Unlock()

		LogInfo("cleaned up expired sessions", map[string]interface{}{
			"count": len(expiredKeys),
		})
	}
}

// Close 关闭会话管理器
// EN: Close stops the session manager.
func (sm *SessionManager) Close() {
	if sm.cleanupTicker != nil {
		sm.cleanupTicker.Stop()
	}
	// 防止重复关闭导致 panic
	// EN: Prevent double-close panics.
	select {
	case <-sm.stopCleanup:
		// 已经关闭
		// EN: Already closed.
		return
	default:
		close(sm.stopCleanup)
	}

	// 中止所有活跃事务
	// EN: Abort all active transactions.
	sm.mu.Lock()
	for _, session := range sm.sessions {
		session.mu.Lock()
		if session.CurrentTxn != nil && session.CurrentTxn.State == TxnStateActive {
			if session.CurrentTxn.Transaction != nil && sm.db.txnManager != nil {
				_ = sm.db.txnManager.Abort(session.CurrentTxn.Transaction)
			}
			session.CurrentTxn.State = TxnStateAborted
		}
		session.mu.Unlock()
	}
	sm.sessions = make(map[string]*Session)
	sm.mu.Unlock()
}

// GetActiveSessionCount 获取活跃会话数
// EN: GetActiveSessionCount returns the number of active sessions.
func (sm *SessionManager) GetActiveSessionCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

// sessionIDToKey 将会话 ID 转为 map key
// EN: sessionIDToKey converts session ID to a map key.
func sessionIDToKey(id primitive.Binary) string {
	return string(id.Data)
}

// CommandContext 命令上下文（包含会话和事务信息）
// EN: CommandContext contains session and transaction context extracted from a command.
type CommandContext struct {
	Session    *Session
	SessionTxn *SessionTransaction
	TxnNumber  int64
	// AutoCommit nil 表示未指定
	// EN: AutoCommit is nil if not specified.
	AutoCommit   *bool
	StartTxn     bool
	ReadConcern  string
	WriteConcern string
}

// ExtractCommandContext 从命令中提取会话/事务上下文
// EN: ExtractCommandContext extracts session/transaction context from a command.
func (sm *SessionManager) ExtractCommandContext(cmd bson.D) (*CommandContext, error) {
	ctx := &CommandContext{}

	var lsid bson.D
	var hasTxnNumber bool

	for _, elem := range cmd {
		switch elem.Key {
		case "lsid":
			if l, ok := elem.Value.(bson.D); ok {
				lsid = l
			}
		case "txnNumber":
			switch v := elem.Value.(type) {
			case int32:
				ctx.TxnNumber = int64(v)
				hasTxnNumber = true
			case int64:
				ctx.TxnNumber = v
				hasTxnNumber = true
			case int:
				ctx.TxnNumber = int64(v)
				hasTxnNumber = true
			}
		case "autocommit":
			if ac, ok := elem.Value.(bool); ok {
				ctx.AutoCommit = &ac
			}
		case "startTransaction":
			if st, ok := elem.Value.(bool); ok {
				ctx.StartTxn = st
			}
		case "readConcern":
			if rc, ok := elem.Value.(bson.D); ok {
				for _, rcElem := range rc {
					if rcElem.Key == "level" {
						if level, ok := rcElem.Value.(string); ok {
							ctx.ReadConcern = level
						}
					}
				}
			}
		case "writeConcern":
			if wc, ok := elem.Value.(bson.D); ok {
				for _, wcElem := range wc {
					if wcElem.Key == "w" {
						switch v := wcElem.Value.(type) {
						case string:
							ctx.WriteConcern = v
						case int32:
							// 简化处理
							// EN: Simplified handling.
							ctx.WriteConcern = "w"
						}
					}
				}
			}
		}
	}

	// 如果有 lsid，获取或创建会话
	// EN: If lsid is present, get or create session.
	if lsid != nil {
		session, err := sm.GetOrCreateSession(lsid)
		if err != nil {
			return nil, err
		}
		ctx.Session = session

		// 处理事务
		// EN: Handle transaction fields.
		if hasTxnNumber {
			if ctx.StartTxn {
				// 启动新事务
				// EN: Start a new transaction.
				if ctx.AutoCommit == nil || *ctx.AutoCommit {
					return nil, NewMongoError(ErrorCodeBadValue, "autocommit must be false for multi-document transactions")
				}
				if err := sm.StartTransaction(session, ctx.TxnNumber, ctx.ReadConcern, ctx.WriteConcern); err != nil {
					return nil, err
				}
			}

			// 获取当前事务
			// EN: Get current transaction.
			if ctx.AutoCommit != nil && !*ctx.AutoCommit {
				sessionTxn, err := sm.GetActiveTransaction(session, ctx.TxnNumber)
				if err != nil {
					return nil, err
				}
				ctx.SessionTxn = sessionTxn
			}
		}
	}

	return ctx, nil
}

// RecordOperationInTxn 在事务中记录操作
// EN: RecordOperationInTxn records an operation in the current transaction.
func (ctx *CommandContext) RecordOperationInTxn(op, collection string, docID interface{}, oldDoc bson.D) {
	if ctx.SessionTxn != nil && ctx.SessionTxn.Transaction != nil {
		ctx.SessionTxn.Transaction.AddUndoRecord(op, collection, docID, oldDoc)
		ctx.SessionTxn.Operations++
	}
}

// IsInTransaction 检查是否在事务中
// EN: IsInTransaction reports whether this command is in an active transaction.
func (ctx *CommandContext) IsInTransaction() bool {
	return ctx.SessionTxn != nil && ctx.SessionTxn.State == TxnStateActive
}

// GetTransaction 获取底层事务
// EN: GetTransaction returns the underlying engine transaction.
func (ctx *CommandContext) GetTransaction() *Transaction {
	if ctx.SessionTxn != nil {
		return ctx.SessionTxn.Transaction
	}
	return nil
}

// Created by Yanjunhui

package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// 事务状态
const (
	TxnStateActive    = iota // 活跃
	TxnStateCommitted        // 已提交
	TxnStateAborted          // 已中止
)

// 隔离级别
const (
	IsolationReadCommitted  = iota // 读已提交（默认）
	IsolationRepeatableRead        // 可重复读
	IsolationSnapshot              // 快照隔离
)

// 锁类型
const (
	LockTypeRead  = iota // 共享锁（读锁）
	LockTypeWrite        // 排他锁（写锁）
)

// 锁等待超时
const (
	DefaultLockTimeout = 30 * time.Second
	MaxLockWaitTime    = 60 * time.Second
)

// TxnID 事务 ID
type TxnID uint64

// Transaction 事务
type Transaction struct {
	ID             TxnID
	State          int
	IsolationLevel int
	StartTime      time.Time
	Timeout        time.Duration
	
	// 事务持有的锁
	locks          map[string]*Lock
	locksMu        sync.Mutex
	
	// 事务操作日志（用于回滚）
	undoLog        []UndoRecord
	undoMu         sync.Mutex
	
	// 关联的数据库
	db             *Database
}

// Lock 锁
type Lock struct {
	Resource string    // 资源标识（如 "collection:users" 或 "doc:users:123"）
	Type     int       // 锁类型
	TxnID    TxnID     // 持有者事务 ID
	HeldAt   time.Time // 获取时间
}

// UndoRecord 回滚记录
type UndoRecord struct {
	Operation  string      // insert, update, delete
	Collection string      // 集合名
	DocID      interface{} // 文档 _id
	OldDoc     bson.D      // 原始文档（用于回滚）
}

// LockManager 锁管理器
type LockManager struct {
	locks     map[string]*lockEntry  // 资源 -> 锁条目
	mu        sync.Mutex
	waitGraph map[TxnID][]TxnID      // 等待图（用于死锁检测）
}

// lockEntry 锁条目（支持共享锁）
type lockEntry struct {
	resource  string           // 【BUG-001 修复】资源标识，用于追踪和唤醒等待者
	exclusive *Lock            // 排他锁持有者
	shared    map[TxnID]*Lock  // 共享锁持有者列表
	waitQueue []*lockRequest   // 等待队列
	mu        sync.Mutex
}

// lockRequest 锁请求
type lockRequest struct {
	txnID    TxnID
	lockType int
	done     chan error
}

// TransactionManager 事务管理器
type TransactionManager struct {
	nextTxnID   uint64
	activeTxns  map[TxnID]*Transaction
	lockManager *LockManager
	mu          sync.RWMutex
	db          *Database
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(db *Database) *TransactionManager {
	return &TransactionManager{
		nextTxnID:  1,
		activeTxns: make(map[TxnID]*Transaction),
		lockManager: &LockManager{
			locks:     make(map[string]*lockEntry),
			waitGraph: make(map[TxnID][]TxnID),
		},
		db: db,
	}
}

// Begin 开始新事务
func (tm *TransactionManager) Begin() *Transaction {
	return tm.BeginWithOptions(IsolationReadCommitted, DefaultLockTimeout)
}

// BeginWithOptions 带选项开始事务
func (tm *TransactionManager) BeginWithOptions(isolation int, timeout time.Duration) *Transaction {
	txnID := TxnID(atomic.AddUint64(&tm.nextTxnID, 1))
	
	txn := &Transaction{
		ID:             txnID,
		State:          TxnStateActive,
		IsolationLevel: isolation,
		StartTime:      time.Now(),
		Timeout:        timeout,
		locks:          make(map[string]*Lock),
		undoLog:        make([]UndoRecord, 0),
		db:             tm.db,
	}
	
	tm.mu.Lock()
	tm.activeTxns[txnID] = txn
	tm.mu.Unlock()
	
	return txn
}

// Commit 提交事务
func (tm *TransactionManager) Commit(txn *Transaction) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeTransactionCommitted, "transaction is not active")
	}
	
	// 标记为已提交
	txn.State = TxnStateCommitted
	
	// 释放所有锁
	tm.releaseAllLocks(txn)
	
	// 清理 undo 日志
	txn.undoMu.Lock()
	txn.undoLog = nil
	txn.undoMu.Unlock()
	
	// 从活跃事务列表移除
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
	
	// 刷新数据到磁盘
	if tm.db != nil {
		return tm.db.Flush()
	}
	
	return nil
}

// Abort 中止事务
func (tm *TransactionManager) Abort(txn *Transaction) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeTransactionAborted, "transaction is not active")
	}
	
	// 执行回滚
	if err := tm.rollback(txn); err != nil {
		// 回滚失败，记录错误但继续清理
		LogError("rollback failed", map[string]interface{}{
			"txnID": txn.ID,
			"error": err.Error(),
		})
	}
	
	// 标记为已中止
	txn.State = TxnStateAborted
	
	// 释放所有锁
	tm.releaseAllLocks(txn)
	
	// 从活跃事务列表移除
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
	
	return nil
}

// rollback 回滚事务
func (tm *TransactionManager) rollback(txn *Transaction) error {
	txn.undoMu.Lock()
	defer txn.undoMu.Unlock()
	
	// 逆序执行回滚操作
	for i := len(txn.undoLog) - 1; i >= 0; i-- {
		record := txn.undoLog[i]
		
		col := tm.db.GetCollection(record.Collection)
		if col == nil {
			continue
		}
		
		switch record.Operation {
		case "insert":
			// 回滚插入：删除文档
			col.DeleteOne(bson.D{{Key: "_id", Value: record.DocID}})
			
		case "update":
			// 回滚更新：恢复原文档
			if record.OldDoc != nil {
				col.ReplaceOne(
					bson.D{{Key: "_id", Value: record.DocID}},
					record.OldDoc,
				)
			}
			
		case "delete":
			// 回滚删除：重新插入文档
			if record.OldDoc != nil {
				col.Insert(record.OldDoc)
			}
		}
	}
	
	return nil
}

// releaseAllLocks 释放事务持有的所有锁
func (tm *TransactionManager) releaseAllLocks(txn *Transaction) {
	txn.locksMu.Lock()
	locks := make([]*Lock, 0, len(txn.locks))
	for _, lock := range txn.locks {
		locks = append(locks, lock)
	}
	txn.locks = make(map[string]*Lock)
	txn.locksMu.Unlock()
	
	for _, lock := range locks {
		tm.lockManager.Release(lock.Resource, txn.ID)
	}
}

// AcquireLock 获取锁
// 【BUG-006 修复】实现完整的锁升级逻辑
func (tm *TransactionManager) AcquireLock(txn *Transaction, resource string, lockType int) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeNoSuchTransaction, "transaction is not active")
	}

	// 检查是否已持有锁
	txn.locksMu.Lock()
	existingLock, hasLock := txn.locks[resource]
	txn.locksMu.Unlock()

	if hasLock {
		// 如果已持有写锁，或者请求的是读锁（等于或更弱），直接返回
		if existingLock.Type == LockTypeWrite || lockType == LockTypeRead {
			return nil
		}
		// 需要锁升级：读锁 -> 写锁
		return tm.upgradeLock(txn, resource, existingLock)
	}

	// 正常获取锁
	lock, err := tm.lockManager.Acquire(resource, txn.ID, lockType, txn.Timeout)
	if err != nil {
		return err
	}

	// 记录锁
	txn.locksMu.Lock()
	txn.locks[resource] = lock
	txn.locksMu.Unlock()

	return nil
}

// upgradeLock 锁升级：从读锁升级到写锁
// 【BUG-006 修复】新增此函数
func (tm *TransactionManager) upgradeLock(txn *Transaction, resource string, existingLock *Lock) error {
	// Step 1: 先释放读锁
	tm.lockManager.Release(resource, txn.ID)

	// Step 2: 获取写锁（可能需要等待其他读锁释放）
	newLock, err := tm.lockManager.Acquire(resource, txn.ID, LockTypeWrite, txn.Timeout)
	if err != nil {
		// 升级失败，尝试重新获取读锁以恢复原状态
		if _, rerr := tm.lockManager.Acquire(resource, txn.ID, LockTypeRead, txn.Timeout); rerr != nil {
			LogError("failed to restore read lock during upgrade failure", map[string]interface{}{
				"txnID":    txn.ID,
				"resource": resource,
				"error":    rerr.Error(),
			})
		} else {
			// 恢复读锁成功，更新事务锁记录
			txn.locksMu.Lock()
			txn.locks[resource] = existingLock
			txn.locksMu.Unlock()
		}
		return fmt.Errorf("lock upgrade failed: %w", err)
	}

	// Step 3: 更新事务锁记录
	txn.locksMu.Lock()
	txn.locks[resource] = newLock
	txn.locksMu.Unlock()

	return nil
}

// AddUndoRecord 添加回滚记录
func (txn *Transaction) AddUndoRecord(op, collection string, docID interface{}, oldDoc bson.D) {
	txn.undoMu.Lock()
	defer txn.undoMu.Unlock()
	
	txn.undoLog = append(txn.undoLog, UndoRecord{
		Operation:  op,
		Collection: collection,
		DocID:      docID,
		OldDoc:     oldDoc,
	})
}

// LockManager 方法

// Acquire 获取锁
// 【BUG-001 修复】统一锁顺序：先获取 lm.mu 创建/获取 entry，然后释放 lm.mu，再获取 entry.mu
// 这避免了原来的 AB-BA 死锁模式
func (lm *LockManager) Acquire(resource string, txnID TxnID, lockType int, timeout time.Duration) (*Lock, error) {
	// 【关键修复】Step 1: 获取全局锁，创建或获取 entry
	lm.mu.Lock()
	entry, exists := lm.locks[resource]
	if !exists {
		entry = &lockEntry{
			resource:  resource, // 【BUG-001 修复】记录资源标识
			shared:    make(map[TxnID]*Lock),
			waitQueue: make([]*lockRequest, 0),
		}
		lm.locks[resource] = entry
	}
	lm.mu.Unlock() // 【关键修复】立即释放全局锁

	// 【关键修复】Step 2: 获取局部锁
	entry.mu.Lock()

	// 尝试立即获取锁
	if canAcquire(entry, txnID, lockType) {
		lock := &Lock{
			Resource: resource,
			Type:     lockType,
			TxnID:    txnID,
			HeldAt:   time.Now(),
		}

		if lockType == LockTypeWrite {
			entry.exclusive = lock
		} else {
			entry.shared[txnID] = lock
		}

		entry.mu.Unlock()
		return lock, nil
	}

	// 需要等待 - 收集等待信息后释放 entry.mu
	request := &lockRequest{
		txnID:    txnID,
		lockType: lockType,
		done:     make(chan error, 1),
	}
	entry.waitQueue = append(entry.waitQueue, request)

	// 【关键修复】收集当前锁持有者信息（在 entry.mu 保护下）
	waitForTxns := make([]TxnID, 0)
	if entry.exclusive != nil && entry.exclusive.TxnID != txnID {
		waitForTxns = append(waitForTxns, entry.exclusive.TxnID)
	}
	for tid := range entry.shared {
		if tid != txnID {
			waitForTxns = append(waitForTxns, tid)
		}
	}

	entry.mu.Unlock() // 【关键修复】先释放 entry.mu

	// 【关键修复】Step 3: 再获取 lm.mu 更新等待图
	lm.mu.Lock()
	lm.waitGraph[txnID] = waitForTxns

	// 检测死锁（在 lm.mu 保护下）
	deadlockDetected := lm.detectDeadlockLocked(txnID)
	lm.mu.Unlock()

	if deadlockDetected {
		// 从等待队列移除
		entry.mu.Lock()
		for i, req := range entry.waitQueue {
			if req == request {
				entry.waitQueue = append(entry.waitQueue[:i], entry.waitQueue[i+1:]...)
				break
			}
		}
		entry.mu.Unlock()

		lm.mu.Lock()
		delete(lm.waitGraph, txnID)
		lm.mu.Unlock()

		return nil, NewMongoError(ErrorCodeTransactionAborted, "deadlock detected")
	}

	// 等待锁
	select {
	case err := <-request.done:
		lm.mu.Lock()
		delete(lm.waitGraph, txnID)
		lm.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return &Lock{
			Resource: resource,
			Type:     lockType,
			TxnID:    txnID,
			HeldAt:   time.Now(),
		}, nil

	case <-time.After(timeout):
		// 超时处理
		entry.mu.Lock()
		for i, req := range entry.waitQueue {
			if req == request {
				entry.waitQueue = append(entry.waitQueue[:i], entry.waitQueue[i+1:]...)
				break
			}
		}
		entry.mu.Unlock()

		lm.mu.Lock()
		delete(lm.waitGraph, txnID)
		lm.mu.Unlock()

		return nil, NewMongoError(ErrorCodeOperationFailed, "lock acquisition timeout")
	}
}

// canAcquire 检查是否可以获取锁
func canAcquire(entry *lockEntry, txnID TxnID, lockType int) bool {
	if lockType == LockTypeRead {
		// 读锁：没有排他锁或排他锁是自己的
		return entry.exclusive == nil || entry.exclusive.TxnID == txnID
	}
	
	// 写锁：没有其他锁
	if entry.exclusive != nil && entry.exclusive.TxnID != txnID {
		return false
	}
	
	// 检查是否有其他事务的共享锁
	for tid := range entry.shared {
		if tid != txnID {
			return false
		}
	}
	
	return true
}

// Release 释放锁
// 【BUG-013 修复】清理空的 lockEntry 以避免内存泄漏
func (lm *LockManager) Release(resource string, txnID TxnID) {
	lm.mu.Lock()
	entry, exists := lm.locks[resource]
	lm.mu.Unlock()

	if !exists {
		return
	}

	entry.mu.Lock()

	// 释放锁
	if entry.exclusive != nil && entry.exclusive.TxnID == txnID {
		entry.exclusive = nil
	}
	delete(entry.shared, txnID)

	// 唤醒等待者
	lm.wakeWaiters(entry)

	// 【BUG-013 修复】检查是否可以清理 entry
	isEmpty := entry.exclusive == nil &&
		len(entry.shared) == 0 &&
		len(entry.waitQueue) == 0

	entry.mu.Unlock()

	// 【BUG-013 修复】清理空的 entry
	if isEmpty {
		lm.mu.Lock()
		// 双重检查（entry 可能在我们释放锁后被重新使用）
		entry.mu.Lock()
		stillEmpty := entry.exclusive == nil &&
			len(entry.shared) == 0 &&
			len(entry.waitQueue) == 0
		entry.mu.Unlock()

		if stillEmpty {
			delete(lm.locks, resource)
		}
		lm.mu.Unlock()
	}
}

// wakeWaiters 唤醒等待的事务
// 【BUG-009 修复】使用 entry.resource 填充 Lock.Resource 字段
func (lm *LockManager) wakeWaiters(entry *lockEntry) {
	newQueue := make([]*lockRequest, 0)

	for _, req := range entry.waitQueue {
		if canAcquire(entry, req.txnID, req.lockType) {
			// 授予锁
			lock := &Lock{
				Resource: entry.resource, // 【BUG-009 修复】使用正确的资源标识
				Type:     req.lockType,
				TxnID:    req.txnID,
				HeldAt:   time.Now(),
			}
			if req.lockType == LockTypeWrite {
				entry.exclusive = lock
			} else {
				entry.shared[req.txnID] = lock
			}
			req.done <- nil
		} else {
			newQueue = append(newQueue, req)
		}
	}

	entry.waitQueue = newQueue
}

// updateWaitGraph 更新等待图
func (lm *LockManager) updateWaitGraph(txnID TxnID, entry *lockEntry) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	waitFor := make([]TxnID, 0)
	
	if entry.exclusive != nil && entry.exclusive.TxnID != txnID {
		waitFor = append(waitFor, entry.exclusive.TxnID)
	}
	
	for tid := range entry.shared {
		if tid != txnID {
			waitFor = append(waitFor, tid)
		}
	}
	
	lm.waitGraph[txnID] = waitFor
}

// removeFromWaitGraph 从等待图移除
func (lm *LockManager) removeFromWaitGraph(txnID TxnID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.waitGraph, txnID)
}

// detectDeadlock 检测死锁（DFS）- 会获取 lm.mu
func (lm *LockManager) detectDeadlock(startTxnID TxnID) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.detectDeadlockLocked(startTxnID)
}

// detectDeadlockLocked 检测死锁（DFS）- 调用者需持有 lm.mu
// 【BUG-001 修复】新增此函数，避免在 Acquire 中重复获取 lm.mu
func (lm *LockManager) detectDeadlockLocked(startTxnID TxnID) bool {
	visited := make(map[TxnID]bool)
	inStack := make(map[TxnID]bool)

	var dfs func(txnID TxnID) bool
	dfs = func(txnID TxnID) bool {
		if inStack[txnID] {
			return true // 发现环
		}
		if visited[txnID] {
			return false
		}

		visited[txnID] = true
		inStack[txnID] = true

		for _, waitFor := range lm.waitGraph[txnID] {
			if dfs(waitFor) {
				return true
			}
		}

		inStack[txnID] = false
		return false
	}

	return dfs(startTxnID)
}

// GetActiveTransactionCount 获取活跃事务数
func (tm *TransactionManager) GetActiveTransactionCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.activeTxns)
}

// GetTransaction 获取事务
func (tm *TransactionManager) GetTransaction(txnID TxnID) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTxns[txnID]
}

// CollectionLockResource 生成集合级锁资源标识
func CollectionLockResource(collection string) string {
	return fmt.Sprintf("col:%s", collection)
}

// DocumentLockResource 生成文档级锁资源标识
func DocumentLockResource(collection string, docID interface{}) string {
	return fmt.Sprintf("doc:%s:%v", collection, docID)
}


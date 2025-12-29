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
// EN: Transaction states.
const (
	// TxnStateActive 活跃
	// EN: Active.
	TxnStateActive = iota
	// TxnStateCommitted 已提交
	// EN: Committed.
	TxnStateCommitted
	// TxnStateAborted 已中止
	// EN: Aborted.
	TxnStateAborted
)

// 隔离级别
// EN: Isolation levels.
const (
	// IsolationReadCommitted 读已提交（默认）
	// EN: Read committed (default).
	IsolationReadCommitted = iota
	// IsolationRepeatableRead 可重复读
	// EN: Repeatable read.
	IsolationRepeatableRead
	// IsolationSnapshot 快照隔离
	// EN: Snapshot isolation.
	IsolationSnapshot
)

// 锁类型
// EN: Lock types.
const (
	// LockTypeRead 共享锁（读锁）
	// EN: Shared lock (read lock).
	LockTypeRead = iota
	// LockTypeWrite 排他锁（写锁）
	// EN: Exclusive lock (write lock).
	LockTypeWrite
)

// 锁等待超时
// EN: Lock wait timeouts.
const (
	DefaultLockTimeout = 30 * time.Second
	MaxLockWaitTime    = 60 * time.Second
)

// TxnID 事务 ID
// EN: TxnID is a transaction ID.
type TxnID uint64

// Transaction 事务
// EN: Transaction represents a transaction.
type Transaction struct {
	ID             TxnID
	State          int
	IsolationLevel int
	StartTime      time.Time
	Timeout        time.Duration

	// 事务持有的锁
	// EN: Locks held by the transaction.
	locks   map[string]*Lock
	locksMu sync.Mutex

	// 事务操作日志（用于回滚）
	// EN: Undo log (for rollback).
	undoLog []UndoRecord
	undoMu  sync.Mutex

	// 关联的数据库
	// EN: Associated database.
	db *Database
}

// Lock 锁
// EN: Lock represents a lock on a resource.
type Lock struct {
	// Resource 资源标识（如 "collection:users" 或 "doc:users:123"）
	// EN: Resource identifier (e.g., \"collection:users\" or \"doc:users:123\").
	Resource string
	// Type 锁类型
	// EN: Lock type.
	Type int
	// TxnID 持有者事务 ID
	// EN: Owner transaction ID.
	TxnID TxnID
	// HeldAt 获取时间
	// EN: Time when the lock was acquired.
	HeldAt time.Time
}

// UndoRecord 回滚记录
// EN: UndoRecord represents an undo log record.
type UndoRecord struct {
	Operation string // insert, update, delete
	// Collection 集合名
	// EN: Collection name.
	Collection string
	// DocID 文档 _id
	// EN: Document _id.
	DocID interface{}
	// OldDoc 原始文档（用于回滚）
	// EN: Original document (for rollback).
	OldDoc bson.D
}

// LockManager 锁管理器
// EN: LockManager manages locks.
type LockManager struct {
	// locks 资源 -> 锁条目
	// EN: locks maps resource -> lock entry.
	locks map[string]*lockEntry
	mu    sync.Mutex
	// waitGraph 等待图（用于死锁检测）
	// EN: waitGraph is the wait-for graph used for deadlock detection.
	waitGraph map[TxnID][]TxnID
}

// lockEntry 锁条目（支持共享锁）
// EN: lockEntry is a lock entry (supports shared locks).
type lockEntry struct {
	// resource 【BUG-001 修复】资源标识，用于追踪和唤醒等待者
	// EN: resource identifier (used to track and wake waiters).
	resource string
	// exclusive 排他锁持有者
	// EN: Exclusive lock holder.
	exclusive *Lock
	// shared 共享锁持有者列表
	// EN: Shared lock holders.
	shared map[TxnID]*Lock
	// waitQueue 等待队列
	// EN: Waiting queue.
	waitQueue []*lockRequest
	mu        sync.Mutex
}

// lockRequest 锁请求
// EN: lockRequest represents a lock request.
type lockRequest struct {
	txnID    TxnID
	lockType int
	done     chan error
}

// TransactionManager 事务管理器
// EN: TransactionManager manages transactions.
type TransactionManager struct {
	nextTxnID   uint64
	activeTxns  map[TxnID]*Transaction
	lockManager *LockManager
	mu          sync.RWMutex
	db          *Database
}

// NewTransactionManager 创建事务管理器
// EN: NewTransactionManager creates a TransactionManager.
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
// EN: Begin starts a new transaction.
func (tm *TransactionManager) Begin() *Transaction {
	return tm.BeginWithOptions(IsolationReadCommitted, DefaultLockTimeout)
}

// BeginWithOptions 带选项开始事务
// EN: BeginWithOptions starts a transaction with options.
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
// EN: Commit commits a transaction.
func (tm *TransactionManager) Commit(txn *Transaction) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeTransactionCommitted, "transaction is not active")
	}

	// 标记为已提交
	// EN: Mark as committed.
	txn.State = TxnStateCommitted

	// 释放所有锁
	// EN: Release all locks.
	tm.releaseAllLocks(txn)

	// 清理 undo 日志
	// EN: Clear undo log.
	txn.undoMu.Lock()
	txn.undoLog = nil
	txn.undoMu.Unlock()

	// 从活跃事务列表移除
	// EN: Remove from active transaction list.
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	// 刷新数据到磁盘
	// EN: Flush data to disk.
	if tm.db != nil {
		return tm.db.Flush()
	}

	return nil
}

// Abort 中止事务
// EN: Abort aborts a transaction.
func (tm *TransactionManager) Abort(txn *Transaction) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeTransactionAborted, "transaction is not active")
	}

	// 执行回滚
	// EN: Perform rollback.
	if err := tm.rollback(txn); err != nil {
		// 回滚失败，记录错误但继续清理
		// EN: Rollback failed; log error but continue cleanup.
		LogError("rollback failed", map[string]interface{}{
			"txnID": txn.ID,
			"error": err.Error(),
		})
	}

	// 标记为已中止
	// EN: Mark as aborted.
	txn.State = TxnStateAborted

	// 释放所有锁
	// EN: Release all locks.
	tm.releaseAllLocks(txn)

	// 从活跃事务列表移除
	// EN: Remove from active transaction list.
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	return nil
}

// rollback 回滚事务
// EN: rollback rolls back a transaction.
func (tm *TransactionManager) rollback(txn *Transaction) error {
	txn.undoMu.Lock()
	defer txn.undoMu.Unlock()

	// 逆序执行回滚操作
	// EN: Apply undo operations in reverse order.
	for i := len(txn.undoLog) - 1; i >= 0; i-- {
		record := txn.undoLog[i]

		col := tm.db.GetCollection(record.Collection)
		if col == nil {
			continue
		}

		switch record.Operation {
		case "insert":
			// 回滚插入：删除文档
			// EN: Roll back insert: delete document.
			col.DeleteOne(bson.D{{Key: "_id", Value: record.DocID}})

		case "update":
			// 回滚更新：恢复原文档
			// EN: Roll back update: restore old document.
			if record.OldDoc != nil {
				col.ReplaceOne(
					bson.D{{Key: "_id", Value: record.DocID}},
					record.OldDoc,
				)
			}

		case "delete":
			// 回滚删除：重新插入文档
			// EN: Roll back delete: re-insert document.
			if record.OldDoc != nil {
				col.Insert(record.OldDoc)
			}
		}
	}

	return nil
}

// releaseAllLocks 释放事务持有的所有锁
// EN: releaseAllLocks releases all locks held by a transaction.
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
// EN: AcquireLock acquires a lock for a transaction.
//
// 【BUG-006 修复】实现完整的锁升级逻辑
// EN: [BUG-006 fix] Implement full lock upgrade logic.
func (tm *TransactionManager) AcquireLock(txn *Transaction, resource string, lockType int) error {
	if txn.State != TxnStateActive {
		return NewMongoError(ErrorCodeNoSuchTransaction, "transaction is not active")
	}

	// 检查是否已持有锁
	// EN: Check whether lock is already held.
	txn.locksMu.Lock()
	existingLock, hasLock := txn.locks[resource]
	txn.locksMu.Unlock()

	if hasLock {
		// 如果已持有写锁，或者请求的是读锁（等于或更弱），直接返回
		// EN: If already holds write lock, or requesting read lock (same/weaker), return directly.
		if existingLock.Type == LockTypeWrite || lockType == LockTypeRead {
			return nil
		}
		// 需要锁升级：读锁 -> 写锁
		// EN: Need lock upgrade: read -> write.
		return tm.upgradeLock(txn, resource, existingLock)
	}

	// 正常获取锁
	// EN: Acquire normally.
	lock, err := tm.lockManager.Acquire(resource, txn.ID, lockType, txn.Timeout)
	if err != nil {
		return err
	}

	// 记录锁
	// EN: Record lock in transaction.
	txn.locksMu.Lock()
	txn.locks[resource] = lock
	txn.locksMu.Unlock()

	return nil
}

// upgradeLock 锁升级：从读锁升级到写锁
// EN: upgradeLock upgrades a lock from read to write.
//
// 【BUG-006 修复】新增此函数
// EN: [BUG-006 fix] Added this function.
func (tm *TransactionManager) upgradeLock(txn *Transaction, resource string, existingLock *Lock) error {
	// Step 1: 先释放读锁
	// EN: Step 1: release read lock first.
	tm.lockManager.Release(resource, txn.ID)

	// Step 2: 获取写锁（可能需要等待其他读锁释放）
	// EN: Step 2: acquire write lock (may wait for other readers).
	newLock, err := tm.lockManager.Acquire(resource, txn.ID, LockTypeWrite, txn.Timeout)
	if err != nil {
		// 升级失败，尝试重新获取读锁以恢复原状态
		// EN: Upgrade failed; try to re-acquire read lock to restore prior state.
		if _, rerr := tm.lockManager.Acquire(resource, txn.ID, LockTypeRead, txn.Timeout); rerr != nil {
			LogError("failed to restore read lock during upgrade failure", map[string]interface{}{
				"txnID":    txn.ID,
				"resource": resource,
				"error":    rerr.Error(),
			})
		} else {
			// 恢复读锁成功，更新事务锁记录
			// EN: Read lock restored; update transaction lock record.
			txn.locksMu.Lock()
			txn.locks[resource] = existingLock
			txn.locksMu.Unlock()
		}
		return fmt.Errorf("lock upgrade failed: %w", err)
	}

	// Step 3: 更新事务锁记录
	// EN: Step 3: update transaction lock record.
	txn.locksMu.Lock()
	txn.locks[resource] = newLock
	txn.locksMu.Unlock()

	return nil
}

// AddUndoRecord 添加回滚记录
// EN: AddUndoRecord appends an undo record to the transaction.
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
// EN: LockManager methods.

// Acquire 获取锁
// EN: Acquire acquires a lock.
//
// 【BUG-001 修复】统一锁顺序：先获取 lm.mu 创建/获取 entry，然后释放 lm.mu，再获取 entry.mu
// EN: [BUG-001 fix] Unify lock order: take lm.mu to create/get entry, release lm.mu, then take entry.mu.
// 这避免了原来的 AB-BA 死锁模式
// EN: This avoids the previous AB-BA deadlock pattern.
func (lm *LockManager) Acquire(resource string, txnID TxnID, lockType int, timeout time.Duration) (*Lock, error) {
	// 【关键修复】Step 1: 获取全局锁，创建或获取 entry
	// EN: [Key fix] Step 1: take global lock and create/get entry.
	lm.mu.Lock()
	entry, exists := lm.locks[resource]
	if !exists {
		entry = &lockEntry{
			// 【BUG-001 修复】记录资源标识
			// EN: [BUG-001 fix] Record resource identifier.
			resource:  resource,
			shared:    make(map[TxnID]*Lock),
			waitQueue: make([]*lockRequest, 0),
		}
		lm.locks[resource] = entry
	}
	// 【关键修复】立即释放全局锁
	// EN: [Key fix] Release global lock immediately.
	lm.mu.Unlock()

	// 【关键修复】Step 2: 获取局部锁
	// EN: [Key fix] Step 2: take local (entry) lock.
	entry.mu.Lock()

	// 尝试立即获取锁
	// EN: Try to acquire lock immediately.
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
	// EN: Need to wait: collect waiter info then release entry.mu.
	request := &lockRequest{
		txnID:    txnID,
		lockType: lockType,
		done:     make(chan error, 1),
	}
	entry.waitQueue = append(entry.waitQueue, request)

	// 【关键修复】收集当前锁持有者信息（在 entry.mu 保护下）
	// EN: [Key fix] Collect current lock holders (protected by entry.mu).
	waitForTxns := make([]TxnID, 0)
	if entry.exclusive != nil && entry.exclusive.TxnID != txnID {
		waitForTxns = append(waitForTxns, entry.exclusive.TxnID)
	}
	for tid := range entry.shared {
		if tid != txnID {
			waitForTxns = append(waitForTxns, tid)
		}
	}

	// 【关键修复】先释放 entry.mu
	// EN: [Key fix] Release entry.mu first.
	entry.mu.Unlock()

	// 【关键修复】Step 3: 再获取 lm.mu 更新等待图
	// EN: [Key fix] Step 3: take lm.mu to update wait graph.
	lm.mu.Lock()
	lm.waitGraph[txnID] = waitForTxns

	// 检测死锁（在 lm.mu 保护下）
	// EN: Detect deadlock (protected by lm.mu).
	deadlockDetected := lm.detectDeadlockLocked(txnID)
	lm.mu.Unlock()

	if deadlockDetected {
		// 从等待队列移除
		// EN: Remove from wait queue.
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
	// EN: Wait for lock.
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
		// EN: Timeout handling.
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
// EN: canAcquire checks whether the lock can be acquired.
func canAcquire(entry *lockEntry, txnID TxnID, lockType int) bool {
	if lockType == LockTypeRead {
		// 读锁：没有排他锁或排他锁是自己的
		// EN: Read lock: no exclusive lock or exclusive lock belongs to self.
		return entry.exclusive == nil || entry.exclusive.TxnID == txnID
	}

	// 写锁：没有其他锁
	// EN: Write lock: no other locks.
	if entry.exclusive != nil && entry.exclusive.TxnID != txnID {
		return false
	}

	// 检查是否有其他事务的共享锁
	// EN: Check whether other transactions hold shared locks.
	for tid := range entry.shared {
		if tid != txnID {
			return false
		}
	}

	return true
}

// Release 释放锁
// EN: Release releases a lock.
//
// 【BUG-013 修复】清理空的 lockEntry 以避免内存泄漏
// EN: [BUG-013 fix] Clean up empty lockEntry to avoid memory leaks.
func (lm *LockManager) Release(resource string, txnID TxnID) {
	lm.mu.Lock()
	entry, exists := lm.locks[resource]
	lm.mu.Unlock()

	if !exists {
		return
	}

	entry.mu.Lock()

	// 释放锁
	// EN: Release lock.
	if entry.exclusive != nil && entry.exclusive.TxnID == txnID {
		entry.exclusive = nil
	}
	delete(entry.shared, txnID)

	// 唤醒等待者
	// EN: Wake up waiters.
	lm.wakeWaiters(entry)

	// 【BUG-013 修复】检查是否可以清理 entry
	// EN: [BUG-013 fix] Check whether entry can be cleaned up.
	isEmpty := entry.exclusive == nil &&
		len(entry.shared) == 0 &&
		len(entry.waitQueue) == 0

	entry.mu.Unlock()

	// 【BUG-013 修复】清理空的 entry
	// EN: [BUG-013 fix] Clean up empty entry.
	if isEmpty {
		lm.mu.Lock()
		// 双重检查（entry 可能在我们释放锁后被重新使用）
		// EN: Double-check (entry may be reused after we released the lock).
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
// EN: wakeWaiters wakes waiting transactions.
//
// 【BUG-009 修复】使用 entry.resource 填充 Lock.Resource 字段
// EN: [BUG-009 fix] Use entry.resource to populate Lock.Resource.
func (lm *LockManager) wakeWaiters(entry *lockEntry) {
	newQueue := make([]*lockRequest, 0)

	for _, req := range entry.waitQueue {
		if canAcquire(entry, req.txnID, req.lockType) {
			// 授予锁
			// EN: Grant lock.
			lock := &Lock{
				// 【BUG-009 修复】使用正确的资源标识
				// EN: [BUG-009 fix] Use correct resource identifier.
				Resource: entry.resource,
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
// EN: updateWaitGraph updates the wait-for graph.
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
// EN: removeFromWaitGraph removes a transaction from the wait-for graph.
func (lm *LockManager) removeFromWaitGraph(txnID TxnID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.waitGraph, txnID)
}

// detectDeadlock 检测死锁（DFS）- 会获取 lm.mu
// EN: detectDeadlock detects deadlocks (DFS) and acquires lm.mu.
func (lm *LockManager) detectDeadlock(startTxnID TxnID) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.detectDeadlockLocked(startTxnID)
}

// detectDeadlockLocked 检测死锁（DFS）- 调用者需持有 lm.mu
// EN: detectDeadlockLocked detects deadlocks (DFS); caller must hold lm.mu.
//
// 【BUG-001 修复】新增此函数，避免在 Acquire 中重复获取 lm.mu
// EN: [BUG-001 fix] Added to avoid re-acquiring lm.mu within Acquire.
func (lm *LockManager) detectDeadlockLocked(startTxnID TxnID) bool {
	visited := make(map[TxnID]bool)
	inStack := make(map[TxnID]bool)

	var dfs func(txnID TxnID) bool
	dfs = func(txnID TxnID) bool {
		if inStack[txnID] {
			// 发现环
			// EN: Cycle detected.
			return true
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
// EN: GetActiveTransactionCount returns the number of active transactions.
func (tm *TransactionManager) GetActiveTransactionCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.activeTxns)
}

// GetTransaction 获取事务
// EN: GetTransaction returns the transaction by ID.
func (tm *TransactionManager) GetTransaction(txnID TxnID) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTxns[txnID]
}

// CollectionLockResource 生成集合级锁资源标识
// EN: CollectionLockResource builds a collection-level lock resource identifier.
func CollectionLockResource(collection string) string {
	return fmt.Sprintf("col:%s", collection)
}

// DocumentLockResource 生成文档级锁资源标识
// EN: DocumentLockResource builds a document-level lock resource identifier.
func DocumentLockResource(collection string, docID interface{}) string {
	return fmt.Sprintf("doc:%s:%v", collection, docID)
}

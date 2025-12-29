// Created by Yanjunhui

package engine

import (
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// CursorManager 游标管理器
// EN: CursorManager manages query cursors.
//
// 实现 MongoDB 兼容的 cursor 生命周期管理
// EN: It implements MongoDB-compatible cursor lifecycle management.
//
// MongoDB Cursor 行为规范：
// EN: MongoDB cursor semantics:
// - cursorId=0 表示无剩余数据或查询一次性完成
// EN: - cursorId=0 means the query is fully exhausted (no remaining results).
// - 非零 cursorId 表示有更多数据，需要通过 getMore 获取
// EN: - A non-zero cursorId means more results exist and must be fetched via getMore.
// - cursor 在以下情况被自动关闭：
// EN: - A cursor is closed automatically when:
//  1. getMore 返回所有剩余数据后
//
// EN:   1) getMore returns all remaining results
//  2. 客户端调用 killCursors
//
// EN:   2) the client calls killCursors
//  3. cursor 超时（10 分钟无访问）
//
// EN:   3) the cursor times out (10 minutes idle)
// - 对已关闭/不存在的 cursor 调用 getMore 返回 CursorNotFound 错误
// EN: - Calling getMore on a closed/missing cursor returns CursorNotFound.
type CursorManager struct {
	cursors    map[int64]*Cursor
	mu         sync.RWMutex
	gcInterval time.Duration
	stopGC     chan struct{}
}

// Cursor 表示一个查询游标
// EN: Cursor represents a query cursor.
//
// MongoDB 规范：cursor 包含未返回的查询结果
// EN: Per MongoDB semantics, a cursor holds query results that have not been returned yet.
type Cursor struct {
	ID int64
	Ns string // namespace (db.collection)

	// Docs 剩余文档
	// EN: Docs contains remaining documents.
	Docs []bson.D

	// BatchSize 每批返回数量（默认 101）
	// EN: BatchSize is the batch size (default 101).
	BatchSize int64

	// CreatedAt 创建时间
	// EN: CreatedAt is the creation time.
	CreatedAt time.Time

	// LastAccess 最后访问时间
	// EN: LastAccess is the last access time.
	LastAccess time.Time
}

// DefaultBatchSize 默认批量大小
// EN: DefaultBatchSize is the default batch size.
//
// MongoDB 默认值为 101，首批返回 101 条文档
// EN: MongoDB defaults to 101 documents in the first batch.
const DefaultBatchSize = 101

// CursorTimeout 游标超时时间（10分钟）
// EN: CursorTimeout is the cursor idle timeout (10 minutes).
//
// MongoDB 默认超时时间为 10 分钟无活动
// EN: MongoDB closes cursors after 10 minutes of inactivity by default.
const CursorTimeout = 10 * time.Minute

// NewCursorManager 创建游标管理器
// EN: NewCursorManager creates a CursorManager.
func NewCursorManager() *CursorManager {
	cm := &CursorManager{
		cursors:    make(map[int64]*Cursor),
		gcInterval: time.Minute,
		stopGC:     make(chan struct{}),
	}
	go cm.gcLoop()
	return cm
}

// gcLoop 定期清理过期游标
// EN: gcLoop periodically cleans up expired cursors.
func (cm *CursorManager) gcLoop() {
	ticker := time.NewTicker(cm.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.cleanupExpired()
		case <-cm.stopGC:
			return
		}
	}
}

// cleanupExpired 清理过期游标
// EN: cleanupExpired removes cursors that have been idle longer than CursorTimeout.
func (cm *CursorManager) cleanupExpired() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	now := time.Now()
	for id, cursor := range cm.cursors {
		if now.Sub(cursor.LastAccess) > CursorTimeout {
			delete(cm.cursors, id)
		}
	}
}

// Stop 停止游标管理器
// EN: Stop stops the cursor GC loop.
func (cm *CursorManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 防止重复关闭
	// EN: Prevent double-close.
	select {
	case <-cm.stopGC:
		// 已经关闭
		// EN: Already closed.
		return
	default:
		close(cm.stopGC)
	}
}

// CreateCursor 创建新游标
// EN: CreateCursor creates a new cursor for the remaining documents.
func (cm *CursorManager) CreateCursor(ns string, docs []bson.D, batchSize int64) *Cursor {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 生成唯一的游标 ID
	// EN: Generate a unique cursor ID.
	id := cm.generateCursorID()
	now := time.Now()

	cursor := &Cursor{
		ID:         id,
		Ns:         ns,
		Docs:       docs,
		BatchSize:  batchSize,
		CreatedAt:  now,
		LastAccess: now,
	}

	cm.cursors[id] = cursor
	return cursor
}

// generateCursorID 生成唯一的游标 ID
// EN: generateCursorID generates a unique, non-zero cursor ID.
func (cm *CursorManager) generateCursorID() int64 {
	for {
		// 生成随机 ID（避免使用 0，0 表示无游标）
		// EN: Generate a random ID; avoid 0 (0 means "no cursor").
		id := rand.Int63()
		if id == 0 {
			continue
		}
		if _, exists := cm.cursors[id]; !exists {
			return id
		}
	}
}

// GetCursor 获取游标
// EN: GetCursor returns the cursor by ID (or nil if not found).
func (cm *CursorManager) GetCursor(id int64) *Cursor {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cursors[id]
}

// GetMore 获取下一批文档
// EN: GetMore fetches the next batch of documents for a cursor.
//
// GetMore 获取游标的下一批文档（原子操作：同时检查存在性和获取文档）
// EN: It is an atomic operation: checks existence and consumes documents under the same lock.
//
// 返回值：
// EN: Returns:
// - docs: 下一批文档
// EN: - docs: the next batch of documents
// - hasMore: 是否还有更多文档
// EN: - hasMore: whether more documents remain
// - error: 如果游标不存在返回 CursorNotFound 错误
// EN: - error: CursorNotFound if the cursor is missing/expired
func (cm *CursorManager) GetMore(cursorID int64, batchSize int64) ([]bson.D, bool, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cursor, exists := cm.cursors[cursorID]
	if !exists {
		// 游标不存在或已过期，返回 CursorNotFound 错误
		// EN: Cursor does not exist or has expired; return CursorNotFound.
		return nil, false, ErrCursorNotFound(cursorID)
	}

	cursor.LastAccess = time.Now()

	if batchSize <= 0 {
		batchSize = cursor.BatchSize
	}

	// 获取下一批文档
	// EN: Get next batch of documents.
	remaining := len(cursor.Docs)
	if remaining == 0 {
		// 没有更多文档，删除游标
		// EN: No remaining documents; delete the cursor.
		delete(cm.cursors, cursorID)
		return []bson.D{}, false, nil
	}

	count := int(batchSize)
	if count > remaining {
		count = remaining
	}

	batch := cursor.Docs[:count]
	cursor.Docs = cursor.Docs[count:]

	// 检查是否还有更多文档
	// EN: Check whether more documents remain.
	hasMore := len(cursor.Docs) > 0
	if !hasMore {
		// 没有更多文档，删除游标
		// EN: Cursor exhausted; delete it.
		delete(cm.cursors, cursorID)
	}

	return batch, hasMore, nil
}

// KillCursor 关闭指定游标
// EN: KillCursor closes (deletes) the specified cursor.
func (cm *CursorManager) KillCursor(id int64) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.cursors[id]; exists {
		delete(cm.cursors, id)
		return true
	}
	return false
}

// KillCursors 关闭多个游标
// EN: KillCursors closes multiple cursors and returns the IDs that were actually killed.
func (cm *CursorManager) KillCursors(ids []int64) []int64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	killed := make([]int64, 0)
	for _, id := range ids {
		if _, exists := cm.cursors[id]; exists {
			delete(cm.cursors, id)
			killed = append(killed, id)
		}
	}
	return killed
}

// Count 返回活跃游标数量
// EN: Count returns the number of active cursors.
func (cm *CursorManager) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.cursors)
}

// GetFirstBatch 从文档列表中获取第一批，返回首批文档和游标ID
// EN: GetFirstBatch returns the first batch and a cursor ID for any remaining documents.
//
// 如果所有文档都在首批中返回，cursorID 为 0
// EN: If all documents fit into the first batch, cursorID is 0.
func (cm *CursorManager) GetFirstBatch(ns string, docs []bson.D, batchSize int64) (firstBatch []bson.D, cursorID int64) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	total := len(docs)
	count := int(batchSize)
	if count > total {
		count = total
	}

	firstBatch = docs[:count]
	remaining := docs[count:]

	// 如果还有剩余文档，创建游标
	// EN: Create a cursor only if documents remain.
	if len(remaining) > 0 {
		cursor := cm.CreateCursor(ns, remaining, batchSize)
		cursorID = cursor.ID
	}

	return firstBatch, cursorID
}

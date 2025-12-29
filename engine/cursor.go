// Created by Yanjunhui

package engine

import (
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// CursorManager 游标管理器
// 实现 MongoDB 兼容的 cursor 生命周期管理
//
// MongoDB Cursor 行为规范：
// - cursorId=0 表示无剩余数据或查询一次性完成
// - 非零 cursorId 表示有更多数据，需要通过 getMore 获取
// - cursor 在以下情况被自动关闭：
//   1. getMore 返回所有剩余数据后
//   2. 客户端调用 killCursors
//   3. cursor 超时（10 分钟无访问）
// - 对已关闭/不存在的 cursor 调用 getMore 返回 CursorNotFound 错误
type CursorManager struct {
	cursors    map[int64]*Cursor
	mu         sync.RWMutex
	gcInterval time.Duration
	stopGC     chan struct{}
}

// Cursor 表示一个查询游标
// MongoDB 规范：cursor 包含未返回的查询结果
type Cursor struct {
	ID         int64
	Ns         string    // namespace (db.collection)
	Docs       []bson.D  // 剩余文档
	BatchSize  int64     // 每批返回数量（默认 101）
	CreatedAt  time.Time // 创建时间
	LastAccess time.Time // 最后访问时间
}

// DefaultBatchSize 默认批量大小
// MongoDB 默认值为 101，首批返回 101 条文档
const DefaultBatchSize = 101

// CursorTimeout 游标超时时间（10分钟）
// MongoDB 默认超时时间为 10 分钟无活动
const CursorTimeout = 10 * time.Minute

// NewCursorManager 创建游标管理器
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
func (cm *CursorManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	// 防止重复关闭
	select {
	case <-cm.stopGC:
		// 已经关闭
		return
	default:
		close(cm.stopGC)
	}
}

// CreateCursor 创建新游标
func (cm *CursorManager) CreateCursor(ns string, docs []bson.D, batchSize int64) *Cursor {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 生成唯一的游标 ID
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
func (cm *CursorManager) generateCursorID() int64 {
	for {
		// 生成随机 ID（避免使用 0，0 表示无游标）
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
func (cm *CursorManager) GetCursor(id int64) *Cursor {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cursors[id]
}

// GetMore 获取下一批文档
// GetMore 获取游标的下一批文档（原子操作：同时检查存在性和获取文档）
//
// 返回值：
// - docs: 下一批文档
// - hasMore: 是否还有更多文档
// - error: 如果游标不存在返回 CursorNotFound 错误
func (cm *CursorManager) GetMore(cursorID int64, batchSize int64) ([]bson.D, bool, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cursor, exists := cm.cursors[cursorID]
	if !exists {
		// 游标不存在或已过期，返回 CursorNotFound 错误
		return nil, false, ErrCursorNotFound(cursorID)
	}

	cursor.LastAccess = time.Now()

	if batchSize <= 0 {
		batchSize = cursor.BatchSize
	}

	// 获取下一批文档
	remaining := len(cursor.Docs)
	if remaining == 0 {
		// 没有更多文档，删除游标
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
	hasMore := len(cursor.Docs) > 0
	if !hasMore {
		// 没有更多文档，删除游标
		delete(cm.cursors, cursorID)
	}

	return batch, hasMore, nil
}

// KillCursor 关闭指定游标
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
func (cm *CursorManager) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.cursors)
}

// GetFirstBatch 从文档列表中获取第一批，返回首批文档和游标ID
// 如果所有文档都在首批中返回，cursorID 为 0
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
	if len(remaining) > 0 {
		cursor := cm.CreateCursor(ns, remaining, batchSize)
		cursorID = cursor.ID
	}

	return firstBatch, cursorID
}

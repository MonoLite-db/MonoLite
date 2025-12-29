// Created by Yanjunhui

package engine

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/monolite/monodb/storage"
)

// CollectionInfo 存储 Collection 元信息
type CollectionInfo struct {
	Name          string
	FirstPageId   storage.PageId // 数据页链表头
	LastPageId    storage.PageId // 数据页链表尾
	DocumentCount int64          // 文档数量
	IndexPageId   storage.PageId // 索引目录页（已废弃，索引信息通过 Indexes 字段持久化）
	Indexes       []IndexMeta    // 索引元数据列表
}

// IndexMeta 索引元数据（用于持久化）
type IndexMeta struct {
	Name       string         // 索引名称
	Keys       bson.D         // 索引键
	Unique     bool           // 是否唯一索引
	RootPageId storage.PageId // B+Tree 根页面 ID
}

// Collection 表示一个文档集合
type Collection struct {
	info         *CollectionInfo
	db           *Database
	mu           sync.RWMutex
	indexManager *IndexManager
}

// Name 返回集合名称
func (c *Collection) Name() string {
	return c.info.Name
}

// Count 返回文档数量
func (c *Collection) Count() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.info.DocumentCount
}

// insertedRecord 记录已插入的文档信息（用于回滚）
// 【BUG-002 修复】新增此结构体
type insertedRecord struct {
	pageId    storage.PageId
	slotIndex int
	id        interface{}
}

// Insert 插入一个或多个文档
// 返回插入文档的 _id 列表（支持任意 BSON 类型）
// 【BUG-002 修复】实现完整的回滚机制
func (c *Collection) Insert(docs ...bson.D) ([]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 验证批量大小
	if len(docs) > MaxWriteBatchSize {
		return nil, ErrBadValue(fmt.Sprintf("insert batch size exceeds maximum of %d", MaxWriteBatchSize))
	}

	ids := make([]interface{}, 0, len(docs))
	var insertedRecords []insertedRecord // 【BUG-002 修复】记录已插入的文档

	for i := range docs {
		// 验证文档结构
		if err := ValidateDocument(docs[i]); err != nil {
			// 【BUG-002 修复】回滚已插入的文档
			c.rollbackInsertedRecords(insertedRecords)
			return nil, err
		}

		// 确保 _id 字段存在
		id, err := c.ensureId(&docs[i])
		if err != nil {
			c.rollbackInsertedRecords(insertedRecords)
			return nil, err
		}

		// 序列化文档
		data, err := bson.Marshal(docs[i])
		if err != nil {
			c.rollbackInsertedRecords(insertedRecords)
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}

		// 检查文档大小
		if err := CheckDocumentSize(data); err != nil {
			c.rollbackInsertedRecords(insertedRecords)
			return nil, err
		}

		// 【强一致性】Step 1: 先检查 unique 索引约束
		// 在写入数据前检查，避免写入后索引失败导致数据不一致
		if c.indexManager != nil {
			if err := c.indexManager.CheckUniqueConstraints(docs[i]); err != nil {
				c.rollbackInsertedRecords(insertedRecords)
				return nil, NewMongoError(ErrorCodeDuplicateKey, err.Error())
			}
		}

		// 【强一致性】Step 2: 写入数据页（此时已确认不会违反 unique 约束）
		// 【BUG-002 修复】记录写入位置以便回滚
		pageId, slotIndex, err := c.writeDocumentWithLocation(id, data)
		if err != nil {
			c.rollbackInsertedRecords(insertedRecords)
			return nil, err
		}

		// 记录已插入的文档（用于可能的回滚）
		insertedRecords = append(insertedRecords, insertedRecord{
			pageId:    pageId,
			slotIndex: slotIndex,
			id:        id,
		})

		// 【强一致性】Step 3: 更新索引
		if c.indexManager != nil {
			if err := c.indexManager.InsertDocument(docs[i]); err != nil {
				// 【BUG-002 修复】索引更新失败，回滚当前文档和所有已插入的文档
				c.rollbackInsertedRecords(insertedRecords)
				return nil, NewMongoError(ErrorCodeInternalError,
					fmt.Sprintf("index update failed, rolled back: %v", err))
			}
		}

		ids = append(ids, id)
		c.info.DocumentCount++
	}

	// 保存集合元信息
	if err := c.db.saveCatalog(); err != nil {
		return nil, err
	}

	return ids, nil
}

// writeDocumentWithLocation 将文档写入数据页并返回位置
// 【BUG-002 修复】新增此函数，返回写入位置以便回滚
func (c *Collection) writeDocumentWithLocation(id interface{}, data []byte) (storage.PageId, int, error) {
	pager := c.db.pager

	// 记录格式：直接使用 raw BSON 文档
	record := data

	// 尝试在现有页面中插入
	if c.info.LastPageId != 0 {
		page, err := pager.ReadPage(c.info.LastPageId)
		if err == nil {
			sp := storage.WrapSlottedPage(page)
			slotIndex, err := sp.InsertRecord(record)
			if err == nil {
				pager.MarkDirty(page.ID())
				return page.ID(), slotIndex, nil
			}
		}
	}

	// 需要分配新页面
	page, err := pager.AllocatePage(storage.PageTypeData)
	if err != nil {
		return 0, -1, err
	}

	sp := storage.WrapSlottedPage(page)
	slotIndex, err := sp.InsertRecord(record)
	if err != nil {
		return 0, -1, err
	}

	// 更新链表
	if c.info.FirstPageId == 0 {
		c.info.FirstPageId = page.ID()
	} else {
		// 链接到上一页
		lastPage, err := pager.ReadPage(c.info.LastPageId)
		if err != nil {
			return 0, -1, err
		}
		lastPage.SetNextPageId(page.ID())
		page.SetPrevPageId(c.info.LastPageId)
		pager.MarkDirty(c.info.LastPageId)
	}
	c.info.LastPageId = page.ID()
	pager.MarkDirty(page.ID())

	return page.ID(), slotIndex, nil
}

// rollbackInsertedRecords 回滚已插入的文档
// 【BUG-002 修复】新增此函数
func (c *Collection) rollbackInsertedRecords(records []insertedRecord) {
	for _, rec := range records {
		// 回滚数据
		if err := c.rollbackDocument(rec.pageId, rec.slotIndex); err != nil {
			// 记录回滚失败的错误（不能丢弃，这是严重问题）
			LogError("CRITICAL: rollback failed", map[string]interface{}{
				"pageId":    rec.pageId,
				"slotIndex": rec.slotIndex,
				"docId":     rec.id,
				"error":     err.Error(),
			})
		}
		// 同时需要回滚已更新的索引
		if c.indexManager != nil {
			c.indexManager.RollbackDocumentById(rec.id)
		}
		// 回滚 DocumentCount
		c.info.DocumentCount--
	}
}

// rollbackDocument 回滚单个文档
// 【BUG-002 修复】新增此函数
func (c *Collection) rollbackDocument(pageId storage.PageId, slotIndex int) error {
	page, err := c.db.pager.ReadPage(pageId)
	if err != nil {
		return fmt.Errorf("rollback failed: cannot get page %d: %v", pageId, err)
	}

	slottedPage := storage.WrapSlottedPage(page)
	if err := slottedPage.DeleteRecord(slotIndex); err != nil {
		return fmt.Errorf("rollback failed: cannot delete slot %d: %v", slotIndex, err)
	}

	c.db.pager.MarkDirty(pageId)
	return nil
}

// ensureId 确保文档有 _id 字段
// 支持多种 BSON 类型作为 _id：ObjectID、string、int32、int64、float64、binary 等
// 如果文档没有 _id 字段，自动生成一个 ObjectID
func (c *Collection) ensureId(doc *bson.D) (interface{}, error) {
	for _, elem := range *doc {
		if elem.Key == "_id" {
			// 验证 _id 类型是否为 MongoDB 支持的类型
			if err := validateIdType(elem.Value); err != nil {
				return nil, err
			}
			return elem.Value, nil
		}
	}

	// 生成新 ObjectID 并插入到文档最前面
	id := primitive.NewObjectID()
	*doc = append(bson.D{{Key: "_id", Value: id}}, *doc...)
	return id, nil
}

// validateIdType 验证 _id 字段类型是否有效
// MongoDB 允许除了 Array 和 Regex 之外的所有 BSON 类型作为 _id
func validateIdType(value interface{}) error {
	switch value.(type) {
	case primitive.ObjectID, string, int32, int64, float64, bool,
		primitive.Binary, primitive.DateTime, primitive.Timestamp,
		primitive.Decimal128, bson.D, primitive.M:
		return nil
	case bson.A:
		return fmt.Errorf("_id cannot be an array")
	case primitive.Regex:
		return fmt.Errorf("_id cannot be a regex")
	case nil:
		return fmt.Errorf("_id cannot be null")
	default:
		// 其他类型也允许（如自定义类型）
		return nil
	}
}

// writeDocument 将文档写入数据页
// 新记录格式：直接存储 raw BSON 文档（BSON 本身包含长度信息）
// 这样支持任意类型的 _id，不再固定为 ObjectID
// 【BUG-002 修复】现在委托给 writeDocumentWithLocation 实现
func (c *Collection) writeDocument(id interface{}, data []byte) error {
	_, _, err := c.writeDocumentWithLocation(id, data)
	return err
}

// Find 查询文档
func (c *Collection) Find(filter bson.D) ([]bson.D, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.findUnlocked(filter)
}

// findUnlocked 无锁查询（内部使用）
func (c *Collection) findUnlocked(filter bson.D) ([]bson.D, error) {
	results := make([]bson.D, 0)
	pager := c.db.pager

	// 遍历所有数据页
	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return nil, err
		}

		// 读取页面中的所有记录
		docs, err := c.readDocumentsFromPage(page)
		if err != nil {
			return nil, err
		}

		// 应用过滤器
		for _, doc := range docs {
			if matchesFilter(doc, filter) {
				results = append(results, doc)
			}
		}

		currentPageId = page.NextPageId()
	}

	return results, nil
}

// FindWithOptions 带选项的查询
func (c *Collection) FindWithOptions(filter bson.D, opts *QueryOptions) ([]bson.D, error) {
	results, err := c.Find(filter)
	if err != nil {
		return nil, err
	}

	return ApplyOptions(results, opts), nil
}

// FindOne 查询单个文档
func (c *Collection) FindOne(filter bson.D) (bson.D, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pager := c.db.pager

	// 遍历所有数据页
	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return nil, err
		}

		docs, err := c.readDocumentsFromPage(page)
		if err != nil {
			return nil, err
		}

		for _, doc := range docs {
			if matchesFilter(doc, filter) {
				return doc, nil
			}
		}

		currentPageId = page.NextPageId()
	}

	return nil, nil // 未找到
}

// FindById 根据 _id 查询文档
// 支持任意 BSON 类型的 _id（ObjectID、string、int64 等）
func (c *Collection) FindById(id interface{}) (bson.D, error) {
	filter := bson.D{{Key: "_id", Value: id}}
	return c.FindOne(filter)
}

// readDocument 通过 RecordId 读取单个文档
// 用于索引回表校验等场景
func (c *Collection) readDocument(rid storage.RecordId) (bson.D, error) {
	page, err := c.db.pager.ReadPage(rid.PageId)
	if err != nil {
		return nil, fmt.Errorf("cannot read page %d: %w", rid.PageId, err)
	}

	sp := storage.WrapSlottedPage(page)
	record, err := sp.GetRecord(int(rid.SlotIndex))
	if err != nil {
		return nil, fmt.Errorf("cannot read slot %d: %w", rid.SlotIndex, err)
	}

	if record == nil || len(record) < 5 {
		return nil, fmt.Errorf("slot %d is empty or invalid", rid.SlotIndex)
	}

	var doc bson.D
	if err := bson.Unmarshal(record, &doc); err != nil {
		return nil, fmt.Errorf("cannot unmarshal document: %w", err)
	}

	return doc, nil
}

// readDocumentsFromPage 从页面读取所有文档
// 新记录格式：直接存储 raw BSON 文档
func (c *Collection) readDocumentsFromPage(page *storage.Page) ([]bson.D, error) {
	docs := make([]bson.D, 0)
	sp := storage.WrapSlottedPage(page)

	for i := 0; i < int(page.ItemCount()); i++ {
		record, err := sp.GetRecord(i)
		if err != nil {
			continue // 跳过已删除的记录
		}

		if len(record) < 5 { // BSON 最小长度：4 字节长度 + 1 字节终止符
			continue
		}

		// 直接反序列化 BSON 文档
		var doc bson.D
		if err := bson.Unmarshal(record, &doc); err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

// UpdateResult 更新操作的结果
type UpdateResult struct {
	MatchedCount  int64       // 匹配的文档数
	ModifiedCount int64       // 实际修改的文档数
	UpsertedCount int64       // upsert 插入的文档数
	UpsertedID    interface{} // upsert 插入的文档 _id（如果有）
}

// Update 更新文档
// 返回 UpdateResult 包含匹配数、修改数、upsert 信息
func (c *Collection) Update(filter bson.D, update bson.D, upsert bool) (*UpdateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := &UpdateResult{}
	pager := c.db.pager

	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return result, err
		}

		sp := storage.WrapSlottedPage(page)

		for i := 0; i < int(page.ItemCount()); i++ {
			record, err := sp.GetRecord(i)
			if err != nil {
				continue
			}

			if len(record) < 5 {
				continue
			}

			var doc bson.D
			if err := bson.Unmarshal(record, &doc); err != nil {
				continue
			}

			if matchesFilter(doc, filter) {
				result.MatchedCount++

				// 保存原始文档用于索引更新
				originalDoc := copyDoc(doc)
				originalData := record

				// 应用更新操作
				if err := applyUpdate(&doc, update); err != nil {
					return result, err
				}

				// 重新序列化
				newData, err := bson.Marshal(doc)
				if err != nil {
					return result, err
				}

				// 检查数据是否真的改变了
				if !bytesEqual(originalData, newData) {
					// 【强一致性】检查 unique 约束
					if c.indexManager != nil {
						if err := c.indexManager.CheckUniqueConstraints(doc); err != nil {
							return result, NewMongoError(ErrorCodeDuplicateKey, err.Error())
						}
					}

					// 更新记录（新格式直接使用 BSON 数据）
					if err := sp.UpdateRecord(i, newData); err != nil {
						return result, err
					}

					// 更新索引（先删除旧条目，再添加新条目）
					if c.indexManager != nil {
						if err := c.indexManager.DeleteDocument(originalDoc); err != nil {
							return result, fmt.Errorf("failed to delete old index entry: %w", err)
						}
						if err := c.indexManager.InsertDocument(doc); err != nil {
							return result, fmt.Errorf("failed to insert new index entry: %w", err)
						}
					}

					pager.MarkDirty(page.ID())
					result.ModifiedCount++
				}
			}
		}

		currentPageId = page.NextPageId()
	}

	// 处理 upsert
	if result.MatchedCount == 0 && upsert {
		newDoc := bson.D{}
		// 复制 filter 中的非操作符字段
		for _, elem := range filter {
			if len(elem.Key) > 0 && elem.Key[0] != '$' {
				newDoc = append(newDoc, elem)
			}
		}
		// 应用更新
		if err := applyUpdate(&newDoc, update); err != nil {
			return result, err
		}

		id, err := c.insertUnlocked(newDoc)
		if err != nil {
			return result, err
		}
		result.UpsertedCount = 1
		result.UpsertedID = id
		result.MatchedCount = 0 // MongoDB 规范：upsert 时 matchedCount 为 0
	}

	if err := c.db.saveCatalog(); err != nil {
		return result, err
	}

	return result, nil
}

// bytesEqual 比较两个字节切片是否相等
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// insertUnlocked 无锁插入（内部使用）
// 返回插入文档的 _id（支持任意 BSON 类型）
func (c *Collection) insertUnlocked(doc bson.D) (interface{}, error) {
	id, err := c.ensureId(&doc)
	if err != nil {
		return nil, err
	}

	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	// 【强一致性】先检查 unique 索引约束
	if c.indexManager != nil {
		if err := c.indexManager.CheckUniqueConstraints(doc); err != nil {
			return nil, NewMongoError(ErrorCodeDuplicateKey, err.Error())
		}
	}

	if err := c.writeDocument(id, data); err != nil {
		return nil, err
	}

	// 更新索引
	if c.indexManager != nil {
		if err := c.indexManager.InsertDocument(doc); err != nil {
			return nil, fmt.Errorf("failed to update index: %w", err)
		}
	}

	c.info.DocumentCount++
	return id, nil
}

// Delete 删除文档
func (c *Collection) Delete(filter bson.D) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pager := c.db.pager
	var deletedCount int64

	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return deletedCount, err
		}

		sp := storage.WrapSlottedPage(page)

		for i := 0; i < int(page.ItemCount()); i++ {
			record, err := sp.GetRecord(i)
			if err != nil {
				continue
			}

			if len(record) < 5 {
				continue
			}

			var doc bson.D
			if err := bson.Unmarshal(record, &doc); err != nil {
				continue
			}

			if matchesFilter(doc, filter) {
				// 【P0 修复】实现原子删除：先删除索引，失败则恢复索引
				if c.indexManager != nil {
					if err := c.indexManager.DeleteDocument(doc); err != nil {
						// 索引删除失败，不删除记录，返回错误
						// 注意：DeleteDocument 内部已实现回滚机制
						return deletedCount, fmt.Errorf("failed to delete index entry: %w", err)
					}
				}

				// 索引已删除（或无索引），现在删除记录
				if err := sp.DeleteRecord(i); err != nil {
					// 【P0 关键修复】记录删除失败，需要恢复已删除的索引条目
					if c.indexManager != nil {
						// 重新插入索引条目
						if restoreErr := c.indexManager.InsertDocument(doc); restoreErr != nil {
							// 恢复失败，记录严重错误
							LogError("CRITICAL: failed to restore index after record delete failure", map[string]interface{}{
								"collection": c.Name(),
								"docID":      getDocField(doc, "_id"),
								"deleteErr":  err.Error(),
								"restoreErr": restoreErr.Error(),
							})
						}
					}
					return deletedCount, fmt.Errorf("failed to delete record after index removal (index restored): %w", err)
				}

				pager.MarkDirty(page.ID())
				deletedCount++
				c.info.DocumentCount--
			}
		}

		currentPageId = page.NextPageId()
	}

	if err := c.db.saveCatalog(); err != nil {
		return deletedCount, err
	}

	return deletedCount, nil
}

// DeleteOne 删除单个匹配的文档
func (c *Collection) DeleteOne(filter bson.D) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pager := c.db.pager

	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return 0, err
		}

		sp := storage.WrapSlottedPage(page)

		for i := 0; i < int(page.ItemCount()); i++ {
			record, err := sp.GetRecord(i)
			if err != nil {
				continue
			}

			if len(record) < 5 {
				continue
			}

			var doc bson.D
			if err := bson.Unmarshal(record, &doc); err != nil {
				continue
			}

			if matchesFilter(doc, filter) {
				// 【P0 修复】实现原子删除
				if c.indexManager != nil {
					if err := c.indexManager.DeleteDocument(doc); err != nil {
						return 0, fmt.Errorf("failed to delete index entry: %w", err)
					}
				}

				// 删除记录
				if err := sp.DeleteRecord(i); err != nil {
					// 【P0 关键修复】记录删除失败，恢复索引
					if c.indexManager != nil {
						if restoreErr := c.indexManager.InsertDocument(doc); restoreErr != nil {
							LogError("CRITICAL: failed to restore index after record delete failure", map[string]interface{}{
								"collection": c.Name(),
								"docID":      getDocField(doc, "_id"),
								"deleteErr":  err.Error(),
								"restoreErr": restoreErr.Error(),
							})
						}
					}
					return 0, fmt.Errorf("failed to delete record after index removal (index restored): %w", err)
				}

				pager.MarkDirty(page.ID())
				c.info.DocumentCount--

				if err := c.db.saveCatalog(); err != nil {
					return 0, err
				}
				return 1, nil
			}
		}

		currentPageId = page.NextPageId()
	}

	return 0, nil
}

// FindAndModifyOptions findAndModify 的选项
type FindAndModifyOptions struct {
	Query  bson.D // 查询条件
	Update bson.D // 更新操作（与 Remove 互斥）
	Remove bool   // 是否删除
	New    bool   // 返回修改后的文档（默认返回原文档）
	Upsert bool   // 未找到时是否插入
	Sort   bson.D // 排序
}

// FindAndModify 查找并修改单个文档
func (c *Collection) FindAndModify(opts *FindAndModifyOptions) (bson.D, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pager := c.db.pager

	// 先查找所有匹配的文档（需要排序）
	var candidates []struct {
		doc       bson.D
		pageId    storage.PageId
		slotIndex int
	}

	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return nil, err
		}

		sp := storage.WrapSlottedPage(page)

		for i := 0; i < int(page.ItemCount()); i++ {
			record, err := sp.GetRecord(i)
			if err != nil {
				continue
			}

			if len(record) < 5 {
				continue
			}

			var doc bson.D
			if err := bson.Unmarshal(record, &doc); err != nil {
				continue
			}

			if matchesFilter(doc, opts.Query) {
				candidates = append(candidates, struct {
					doc       bson.D
					pageId    storage.PageId
					slotIndex int
				}{doc, page.ID(), i})
			}
		}

		currentPageId = page.NextPageId()
	}

	// 如果有排序，对候选文档排序
	if len(opts.Sort) > 0 && len(candidates) > 1 {
		sortDocs := make([]bson.D, len(candidates))
		for i, c := range candidates {
			sortDocs[i] = c.doc
		}
		sorted := ApplyOptions(sortDocs, &QueryOptions{Sort: opts.Sort})

		// 找到排序后第一个文档对应的原始位置
		if len(sorted) > 0 {
			for i, c := range candidates {
				if docEqual(c.doc, sorted[0]) {
					candidates[0], candidates[i] = candidates[i], candidates[0]
					break
				}
			}
		}
	}

	// 没有找到匹配的文档
	if len(candidates) == 0 {
		if opts.Upsert && opts.Update != nil {
			// 执行 upsert
			newDoc := bson.D{}
			for _, elem := range opts.Query {
				if elem.Key[0] != '$' {
					newDoc = append(newDoc, elem)
				}
			}
			if err := applyUpdate(&newDoc, opts.Update); err != nil {
				return nil, err
			}

			id, err := c.insertUnlocked(newDoc)
			if err != nil {
				return nil, err
			}

			if opts.New {
				// 返回新插入的文档
				// insertUnlocked 内部会 ensureId，但它修改的是其本地 doc 副本；
				// 这里的 newDoc 可能仍然没有 _id。为保证返回文档符合预期，显式补齐/覆盖 _id。
				hasID := false
				for i, elem := range newDoc {
					if elem.Key == "_id" {
						newDoc[i].Value = id
						hasID = true
						break
					}
				}
				if !hasID {
					newDoc = append(bson.D{{Key: "_id", Value: id}}, newDoc...)
				}
				return newDoc, nil
			}
			return nil, nil
		}
		return nil, nil
	}

	// 取第一个匹配的文档
	target := candidates[0]
	originalDoc := target.doc

	if opts.Remove {
		// 先删除文档
		// 强一致性：先删除索引条目，成功后再删除记录
		if c.indexManager != nil {
			if err := c.indexManager.DeleteDocument(originalDoc); err != nil {
				return nil, fmt.Errorf("failed to delete index entry: %w", err)
			}
		}

		page, err := pager.ReadPage(target.pageId)
		if err != nil {
			return nil, err
		}
		sp := storage.WrapSlottedPage(page)
		if err := sp.DeleteRecord(target.slotIndex); err != nil {
			return nil, fmt.Errorf("failed to delete record after index removal: %w", err)
		}

		pager.MarkDirty(page.ID())
		c.info.DocumentCount--

		if err := c.db.saveCatalog(); err != nil {
			return nil, err
		}
		return originalDoc, nil
	}

	// 更新文档
	if opts.Update != nil {
		modifiedDoc := copyDoc(originalDoc)
		if err := applyUpdate(&modifiedDoc, opts.Update); err != nil {
			return nil, err
		}

		// 【强一致性】检查 unique 约束
		if c.indexManager != nil {
			if err := c.indexManager.CheckUniqueConstraints(modifiedDoc); err != nil {
				return nil, NewMongoError(ErrorCodeDuplicateKey, err.Error())
			}
		}

		// 重新序列化（新格式直接使用 BSON 数据）
		newData, err := bson.Marshal(modifiedDoc)
		if err != nil {
			return nil, err
		}

		// 更新记录
		page, err := pager.ReadPage(target.pageId)
		if err != nil {
			return nil, err
		}
		sp := storage.WrapSlottedPage(page)
		if err := sp.UpdateRecord(target.slotIndex, newData); err != nil {
			return nil, err
		}

		// 更新索引（先删除旧条目，再添加新条目）
		if c.indexManager != nil {
			if err := c.indexManager.DeleteDocument(originalDoc); err != nil {
				return nil, fmt.Errorf("failed to delete old index entry: %w", err)
			}
			if err := c.indexManager.InsertDocument(modifiedDoc); err != nil {
				return nil, fmt.Errorf("failed to insert new index entry: %w", err)
			}
		}

		pager.MarkDirty(page.ID())

		if err := c.db.saveCatalog(); err != nil {
			return nil, err
		}

		if opts.New {
			return modifiedDoc, nil
		}
		return originalDoc, nil
	}

	return originalDoc, nil
}

// copyDoc 复制文档
// 【BUG-010 修复】实现深拷贝，避免嵌套文档/数组的引用问题
func copyDoc(doc bson.D) bson.D {
	// 深拷贝：序列化再反序列化
	data, err := bson.Marshal(doc)
	if err != nil {
		// 如果序列化失败，返回浅拷贝（兜底）
		result := make(bson.D, len(doc))
		copy(result, doc)
		return result
	}

	var result bson.D
	if err := bson.Unmarshal(data, &result); err != nil {
		// 如果反序列化失败，返回浅拷贝（兜底）
		result = make(bson.D, len(doc))
		copy(result, doc)
		return result
	}

	return result
}

// docEqual 比较两个文档是否相等（简化比较）
func docEqual(a, b bson.D) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key {
			return false
		}
		if !valuesEqual(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

// ReplaceOne 替换单个文档
func (c *Collection) ReplaceOne(filter bson.D, replacement bson.D) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pager := c.db.pager

	currentPageId := c.info.FirstPageId
	for currentPageId != 0 {
		page, err := pager.ReadPage(currentPageId)
		if err != nil {
			return 0, err
		}

		sp := storage.WrapSlottedPage(page)

		for i := 0; i < int(page.ItemCount()); i++ {
			record, err := sp.GetRecord(i)
			if err != nil {
				continue
			}

			if len(record) < 5 {
				continue
			}

			var doc bson.D
			if err := bson.Unmarshal(record, &doc); err != nil {
				continue
			}

			if matchesFilter(doc, filter) {
				// 获取原文档的 _id
				originalId := getDocField(doc, "_id")

				// 确保 replacement 有正确的 _id（保留原 _id）
				newDoc := make(bson.D, 0, len(replacement)+1)
				hasId := false
				for _, elem := range replacement {
					if elem.Key == "_id" {
						hasId = true
						newDoc = append(newDoc, bson.E{Key: "_id", Value: originalId})
					} else {
						newDoc = append(newDoc, elem)
					}
				}
				if !hasId {
					newDoc = append(bson.D{{Key: "_id", Value: originalId}}, newDoc...)
				}

				// 【强一致性】检查 unique 约束
				if c.indexManager != nil {
					if err := c.indexManager.CheckUniqueConstraints(newDoc); err != nil {
						return 0, NewMongoError(ErrorCodeDuplicateKey, err.Error())
					}
				}

				// 重新序列化（新格式直接使用 BSON 数据）
				newData, err := bson.Marshal(newDoc)
				if err != nil {
					return 0, err
				}

				// 更新记录
				if err := sp.UpdateRecord(i, newData); err != nil {
					return 0, err
				}

				// 更新索引（先删除旧条目，再添加新条目）
				if c.indexManager != nil {
					if err := c.indexManager.DeleteDocument(doc); err != nil {
						return 0, fmt.Errorf("failed to delete old index entry: %w", err)
					}
					if err := c.indexManager.InsertDocument(newDoc); err != nil {
						return 0, fmt.Errorf("failed to insert new index entry: %w", err)
					}
				}

				pager.MarkDirty(page.ID())

				if err := c.db.saveCatalog(); err != nil {
					return 0, err
				}
				return 1, nil
			}
		}

		currentPageId = page.NextPageId()
	}

	return 0, nil
}

// Distinct 返回指定字段的不重复值
func (c *Collection) Distinct(field string, filter bson.D) ([]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	docs, err := c.findUnlocked(filter)
	if err != nil {
		return nil, err
	}

	seen := make(map[interface{}]bool)
	var result []interface{}

	for _, doc := range docs {
		val := getDocField(doc, field)
		if val == nil {
			continue
		}

		// 使用字符串化作为 key（简化去重）
		key := fmt.Sprintf("%T:%v", val, val)
		if !seen[key] {
			seen[key] = true
			result = append(result, val)
		}
	}

	return result, nil
}

// matchesFilter 检查文档是否匹配过滤器
func matchesFilter(doc bson.D, filter bson.D) bool {
	if len(filter) == 0 {
		return true
	}

	matcher := NewFilterMatcher(filter)
	return matcher.Match(doc)
}

// docToMap 将 bson.D 转换为 map
func docToMap(doc bson.D) map[string]interface{} {
	m := make(map[string]interface{})
	for _, elem := range doc {
		m[elem.Key] = elem.Value
	}
	return m
}

// valuesEqual 比较两个值是否相等
// 统一使用 CompareBSON 实现 MongoDB 标准 BSON 类型比较规则
func valuesEqual(a, b interface{}) bool {
	return CompareBSON(a, b) == 0
}

// compareNumbers 比较数字（跨类型）
// 统一使用 CompareBSON 实现
func compareNumbers(a, b interface{}) bool {
	return CompareBSON(a, b) == 0
}

func toFloat64(v interface{}) float64 {
	return toFloat64Value(v) // 复用 bson_compare.go 中的实现
}

// applyUpdate 应用更新操作符
func applyUpdate(doc *bson.D, update bson.D) error {
	for _, elem := range update {
		switch elem.Key {
		case "$set":
			setDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$set value must be a document")
			}
			for _, setElem := range setDoc {
				setField(doc, setElem.Key, setElem.Value)
			}

		case "$unset":
			unsetDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$unset value must be a document")
			}
			for _, unsetElem := range unsetDoc {
				removeField(doc, unsetElem.Key)
			}

		case "$inc":
			incDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$inc value must be a document")
			}
			for _, incElem := range incDoc {
				if err := incrementField(doc, incElem.Key, incElem.Value); err != nil {
					return err
				}
			}

		case "$mul":
			mulDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$mul value must be a document")
			}
			for _, mulElem := range mulDoc {
				if err := multiplyField(doc, mulElem.Key, mulElem.Value); err != nil {
					return err
				}
			}

		case "$min":
			minDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$min value must be a document")
			}
			for _, minElem := range minDoc {
				updateFieldMin(doc, minElem.Key, minElem.Value)
			}

		case "$max":
			maxDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$max value must be a document")
			}
			for _, maxElem := range maxDoc {
				updateFieldMax(doc, maxElem.Key, maxElem.Value)
			}

		case "$rename":
			renameDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$rename value must be a document")
			}
			for _, renameElem := range renameDoc {
				newName, ok := renameElem.Value.(string)
				if !ok {
					return fmt.Errorf("$rename target must be a string")
				}
				renameField(doc, renameElem.Key, newName)
			}

		case "$push":
			pushDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$push value must be a document")
			}
			for _, pushElem := range pushDoc {
				if err := pushToArray(doc, pushElem.Key, pushElem.Value); err != nil {
					return err
				}
			}

		case "$pop":
			popDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$pop value must be a document")
			}
			for _, popElem := range popDoc {
				popFromArray(doc, popElem.Key, popElem.Value)
			}

		case "$pull":
			pullDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$pull value must be a document")
			}
			for _, pullElem := range pullDoc {
				pullFromArray(doc, pullElem.Key, pullElem.Value)
			}

		case "$addToSet":
			addDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$addToSet value must be a document")
			}
			for _, addElem := range addDoc {
				addToSet(doc, addElem.Key, addElem.Value)
			}

		case "$pullAll":
			pullAllDoc, ok := elem.Value.(bson.D)
			if !ok {
				return fmt.Errorf("$pullAll value must be a document")
			}
			for _, pullElem := range pullAllDoc {
				pullAllFromArray(doc, pullElem.Key, pullElem.Value)
			}

		default:
			// 非操作符字段，直接设置（替换模式）
			setField(doc, elem.Key, elem.Value)
		}
	}
	return nil
}

// setField 设置文档字段
func setField(doc *bson.D, key string, value interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			(*doc)[i].Value = value
			return
		}
	}
	*doc = append(*doc, bson.E{Key: key, Value: value})
}

// removeField 移除文档字段
func removeField(doc *bson.D, key string) {
	for i, elem := range *doc {
		if elem.Key == key {
			*doc = append((*doc)[:i], (*doc)[i+1:]...)
			return
		}
	}
}

// incrementField 增加字段值
func incrementField(doc *bson.D, key string, incVal interface{}) error {
	incAmount := toFloat64(incVal)

	for i, elem := range *doc {
		if elem.Key == key {
			currentVal := toFloat64(elem.Value)
			(*doc)[i].Value = currentVal + incAmount
			return nil
		}
	}

	// 字段不存在，直接设置
	*doc = append(*doc, bson.E{Key: key, Value: incVal})
	return nil
}

// multiplyField 乘法更新
func multiplyField(doc *bson.D, key string, mulVal interface{}) error {
	mulAmount := toFloat64(mulVal)

	for i, elem := range *doc {
		if elem.Key == key {
			currentVal := toFloat64(elem.Value)
			(*doc)[i].Value = currentVal * mulAmount
			return nil
		}
	}

	// 字段不存在，设置为 0
	*doc = append(*doc, bson.E{Key: key, Value: float64(0)})
	return nil
}

// updateFieldMin 取最小值更新
func updateFieldMin(doc *bson.D, key string, minVal interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			if compareValues(minVal, elem.Value) < 0 {
				(*doc)[i].Value = minVal
			}
			return
		}
	}
	// 字段不存在，直接设置
	*doc = append(*doc, bson.E{Key: key, Value: minVal})
}

// updateFieldMax 取最大值更新
func updateFieldMax(doc *bson.D, key string, maxVal interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			if compareValues(maxVal, elem.Value) > 0 {
				(*doc)[i].Value = maxVal
			}
			return
		}
	}
	// 字段不存在，直接设置
	*doc = append(*doc, bson.E{Key: key, Value: maxVal})
}

// renameField 重命名字段
func renameField(doc *bson.D, oldName, newName string) {
	for i, elem := range *doc {
		if elem.Key == oldName {
			// 删除旧字段，添加新字段
			value := elem.Value
			*doc = append((*doc)[:i], (*doc)[i+1:]...)
			*doc = append(*doc, bson.E{Key: newName, Value: value})
			return
		}
	}
}

// pushToArray 向数组追加元素
func pushToArray(doc *bson.D, key string, value interface{}) error {
	for i, elem := range *doc {
		if elem.Key == key {
			arr, ok := elem.Value.(bson.A)
			if !ok {
				return fmt.Errorf("field %s is not an array", key)
			}

			// 检查是否有 $each 修饰符
			if valDoc, ok := value.(bson.D); ok {
				for _, ve := range valDoc {
					if ve.Key == "$each" {
						if eachArr, ok := ve.Value.(bson.A); ok {
							arr = append(arr, eachArr...)
							(*doc)[i].Value = arr
							return nil
						}
					}
				}
			}

			arr = append(arr, value)
			(*doc)[i].Value = arr
			return nil
		}
	}

	// 字段不存在，创建新数组
	*doc = append(*doc, bson.E{Key: key, Value: bson.A{value}})
	return nil
}

// popFromArray 从数组头部或尾部移除元素
func popFromArray(doc *bson.D, key string, value interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			arr, ok := elem.Value.(bson.A)
			if !ok || len(arr) == 0 {
				return
			}

			pos := toFloat64(value)
			if pos >= 0 {
				// 移除尾部
				arr = arr[:len(arr)-1]
			} else {
				// 移除头部
				arr = arr[1:]
			}
			(*doc)[i].Value = arr
			return
		}
	}
}

// pullFromArray 从数组移除匹配的元素
func pullFromArray(doc *bson.D, key string, value interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			arr, ok := elem.Value.(bson.A)
			if !ok {
				return
			}

			newArr := bson.A{}
			for _, item := range arr {
				if !valuesEqual(item, value) {
					newArr = append(newArr, item)
				}
			}
			(*doc)[i].Value = newArr
			return
		}
	}
}

// pullAllFromArray 从数组移除所有指定的元素
func pullAllFromArray(doc *bson.D, key string, value interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			arr, ok := elem.Value.(bson.A)
			if !ok {
				return
			}

			valuesToRemove, ok := value.(bson.A)
			if !ok {
				return
			}

			newArr := bson.A{}
			for _, item := range arr {
				shouldKeep := true
				for _, v := range valuesToRemove {
					if valuesEqual(item, v) {
						shouldKeep = false
						break
					}
				}
				if shouldKeep {
					newArr = append(newArr, item)
				}
			}
			(*doc)[i].Value = newArr
			return
		}
	}
}

// addToSet 向数组添加唯一元素
func addToSet(doc *bson.D, key string, value interface{}) {
	for i, elem := range *doc {
		if elem.Key == key {
			arr, ok := elem.Value.(bson.A)
			if !ok {
				return
			}

			// 检查是否有 $each 修饰符
			if valDoc, ok := value.(bson.D); ok {
				for _, ve := range valDoc {
					if ve.Key == "$each" {
						if eachArr, ok := ve.Value.(bson.A); ok {
							for _, v := range eachArr {
								if !arrayContains(arr, v) {
									arr = append(arr, v)
								}
							}
							(*doc)[i].Value = arr
							return
						}
					}
				}
			}

			// 单个值
			if !arrayContains(arr, value) {
				arr = append(arr, value)
				(*doc)[i].Value = arr
			}
			return
		}
	}

	// 字段不存在，创建新数组
	*doc = append(*doc, bson.E{Key: key, Value: bson.A{value}})
}

// arrayContains 检查数组是否包含指定值
func arrayContains(arr bson.A, value interface{}) bool {
	for _, item := range arr {
		if valuesEqual(item, value) {
			return true
		}
	}
	return false
}

// getIndexManager 获取或创建索引管理器
func (c *Collection) getIndexManager() *IndexManager {
	if c.indexManager == nil {
		c.indexManager = NewIndexManager(c)
	}
	return c.indexManager
}

// restoreIndexes 从持久化的索引元数据恢复索引
// 在数据库加载时调用，恢复已创建的索引
func (c *Collection) restoreIndexes() {
	if len(c.info.Indexes) == 0 {
		return
	}

	im := c.getIndexManager()
	for _, meta := range c.info.Indexes {
		// 打开已存在的 B+Tree
		tree := storage.OpenBTree(c.db.pager, meta.RootPageId, meta.Name, meta.Unique)

		idx := &Index{
			info: &IndexInfo{
				Name:       meta.Name,
				Keys:       meta.Keys,
				Unique:     meta.Unique,
				RootPageId: meta.RootPageId,
			},
			tree:  tree,
			pager: c.db.pager,
		}
		im.indexes[meta.Name] = idx
	}
}

// CreateIndex 创建索引
func (c *Collection) CreateIndex(keys bson.D, options bson.D) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.getIndexManager().CreateIndex(keys, options)
}

// DropIndex 删除索引
func (c *Collection) DropIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.getIndexManager().DropIndex(name)
}

// ListIndexes 列出所有索引
func (c *Collection) ListIndexes() bson.A {
	c.mu.RLock()
	defer c.mu.RUnlock()

	indexes := c.getIndexManager().ListIndexes()
	result := bson.A{}
	for _, idx := range indexes {
		result = append(result, idx)
	}
	return result
}

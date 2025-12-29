// Created by Yanjunhui

package engine

import (
	"fmt"

	"github.com/monolite/monodb/storage"
	"go.mongodb.org/mongo-driver/bson"
)

// ValidationResult 校验结果
type ValidationResult struct {
	Valid    bool     // 是否有效
	Errors   []string // 错误列表
	Warnings []string // 警告列表
	Stats    ValidationStats
}

// ValidationStats 校验统计
type ValidationStats struct {
	TotalPages      int // 总页面数
	DataPages       int // 数据页数
	IndexPages      int // 索引页数
	FreePages       int // 空闲页数
	MetaPages       int // 元数据页数
	Collections     int // 集合数
	TotalDocuments  int // 总文档数
	TotalIndexes    int // 总索引数
}

// Validate 校验数据库结构完整性
// 实现 validate 命令，作为崩溃恢复和 fuzz 测试的总闸门
func (db *Database) Validate() *ValidationResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	// 1. 校验 free list
	db.validateFreeList(result)

	// 2. 校验所有页面
	db.validatePages(result)

	// 3. 校验 catalog
	db.validateCatalog(result)

	// 4. 校验各集合
	for name, col := range db.collections {
		db.validateCollection(col, name, result)
	}

	return result
}

// validateFreeList 校验空闲页链表
// 检查：无环、无重复、页类型正确
func (db *Database) validateFreeList(result *ValidationResult) {
	pager := db.pager
	header := pager.Header()

	if header.FreeListHead == 0 {
		return // 空链表，无需校验
	}

	visited := make(map[storage.PageId]bool)
	currentId := header.FreeListHead
	chainLen := 0

	for currentId != 0 {
		// 检查是否有环
		if visited[currentId] {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("free list has cycle at page %d", currentId))
			return
		}
		visited[currentId] = true
		chainLen++

		// 检查页面是否存在且类型正确
		page, err := pager.ReadPage(currentId)
		if err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("cannot read free list page %d: %v", currentId, err))
			return
		}

		if page.Type() != storage.PageTypeFree {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("free list page %d has wrong type: %d (expected %d)", currentId, page.Type(), storage.PageTypeFree))
		}

		currentId = page.NextPageId()
		result.Stats.FreePages++

		// 防止无限循环（最多遍历所有页面）
		if chainLen > int(header.PageCount) {
			result.Valid = false
			result.Errors = append(result.Errors, "free list chain exceeds total page count")
			return
		}
	}
}

// validatePages 校验所有页面
func (db *Database) validatePages(result *ValidationResult) {
	pager := db.pager
	header := pager.Header()

	result.Stats.TotalPages = int(header.PageCount)

	for i := uint32(0); i < header.PageCount; i++ {
		page, err := pager.ReadPage(storage.PageId(i))
		if err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("cannot read page %d: %v", i, err))
			continue
		}

		switch page.Type() {
		case storage.PageTypeData:
			result.Stats.DataPages++
		case storage.PageTypeIndex:
			result.Stats.IndexPages++
		case storage.PageTypeCatalog:
			// catalog pages are tracked via header.CatalogPageId, treat as known type
		case storage.PageTypeFree:
			// free pages counted in validateFreeList
		case storage.PageTypeMeta:
			result.Stats.MetaPages++
		case storage.PageTypeOverflow:
			// overflow pages
		default:
			result.Warnings = append(result.Warnings, fmt.Sprintf("page %d has unknown type: %d", i, page.Type()))
		}
	}
}

// validateCatalog 校验 catalog 结构
// 深度增强：不仅检查页面可读，还尝试解析 catalog 数据验证结构正确性
func (db *Database) validateCatalog(result *ValidationResult) {
	header := db.pager.Header()

	if header.CatalogPageId == 0 {
		return // 空 catalog，可能是新数据库
	}

	// 检查 catalog page 是否可读
	catalogPage, err := db.pager.ReadPage(header.CatalogPageId)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("cannot read catalog page %d: %v", header.CatalogPageId, err))
		return
	}

	// 检查页面类型
	if catalogPage.Type() != storage.PageTypeCatalog {
		result.Warnings = append(result.Warnings, fmt.Sprintf("catalog page %d has unexpected type: %d (expected %d)", header.CatalogPageId, catalogPage.Type(), storage.PageTypeCatalog))
	}

	// 深度校验：尝试解析 catalog 数据
	sp := storage.WrapSlottedPage(catalogPage)
	itemCount := int(catalogPage.ItemCount())
	if itemCount > 0 {
		// 检查每个 slot 是否可解析为有效的 BSON
		for i := 0; i < itemCount; i++ {
			// itemCount 表示槽总数（包含已删除槽）。已删除槽应跳过，而不是判为“读取失败”。
			if sp.IsSlotDeleted(i) {
				continue
			}
			data, err := sp.GetRecord(i)
			if err != nil {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("catalog slot %d: cannot read: %v", i, err))
				continue
			}
			
			// 尝试 BSON 反序列化
			var doc bson.D
			if err := bson.Unmarshal(data, &doc); err != nil {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("catalog slot %d: invalid BSON: %v", i, err))
				continue
			}
			
			// 检查必要字段存在
			hasName := false
			for _, elem := range doc {
				if elem.Key == "name" {
					hasName = true
					break
				}
			}
			if !hasName {
				result.Warnings = append(result.Warnings, fmt.Sprintf("catalog slot %d: missing 'name' field", i))
			}
		}
	}

	result.Stats.Collections = len(db.collections)
}

// validateCollection 校验单个集合
func (db *Database) validateCollection(col *Collection, name string, result *ValidationResult) {
	// 校验数据页链表
	db.validateDataPageChain(col, name, result)

	// 校验索引
	if col.indexManager != nil {
		for idxName, idx := range col.indexManager.indexes {
			db.validateIndex(idx, name, idxName, result)
		}
	}
}

// validateDataPageChain 校验数据页链表
func (db *Database) validateDataPageChain(col *Collection, colName string, result *ValidationResult) {
	if col.info.FirstPageId == 0 {
		return // 空集合
	}

	pager := db.pager
	visited := make(map[storage.PageId]bool)
	currentId := col.info.FirstPageId
	var prevId storage.PageId = 0
	docCount := 0

	for currentId != 0 {
		// 检查是否有环
		if visited[currentId] {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: data page chain has cycle at page %d", colName, currentId))
			return
		}
		visited[currentId] = true

		// 读取页面
		page, err := pager.ReadPage(currentId)
		if err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: cannot read data page %d: %v", colName, currentId, err))
			return
		}

		// 检查页面类型
		if page.Type() != storage.PageTypeData {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: page %d in data chain has wrong type: %d", colName, currentId, page.Type()))
		}

		// 检查 Prev 指针一致性
		if page.PrevPageId() != prevId {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: page %d has wrong PrevPageId: %d (expected %d)", colName, currentId, page.PrevPageId(), prevId))
		}

		// 统计文档数
		sp := storage.WrapSlottedPage(page)
		docCount += sp.LiveCount()

		prevId = currentId
		currentId = page.NextPageId()
	}

	result.Stats.TotalDocuments += docCount

	// 检查文档数是否一致
	if docCount != int(col.info.DocumentCount) {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: document count mismatch: found %d, expected %d", colName, docCount, col.info.DocumentCount))
	}
}

// validateIndex 校验单个索引
// 深度增强：不仅检查 B+Tree 结构，还进行抽样回表校验
func (db *Database) validateIndex(idx *Index, colName, idxName string, result *ValidationResult) {
	if idx.tree == nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s has nil tree", colName, idxName))
		return
	}

	// 使用 B+Tree 的完整性检查
	if err := idx.tree.CheckTreeIntegrity(); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("collection %s: index %s integrity check failed: %v", colName, idxName, err))
	}

	// 检查叶子链表
	if err := idx.tree.CheckLeafChain(); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("collection %s: index %s leaf chain check failed: %v", colName, idxName, err))
	}

	// 深度校验：索引抽样回表校验
	// 随机抽取若干索引条目，验证对应文档存在
	db.validateIndexSampling(idx, colName, idxName, result)

	result.Stats.TotalIndexes++
}

// validateIndexSampling 索引抽样回表校验
// 检查索引条目指向的文档是否真实存在
// 注意：当前索引存储的值是 _id 的 BSON 编码，不是 RecordId
//
// 分段采样策略：从索引头部、尾部、中间各采样若干条，避免抽样偏置
const (
	maxSamplePerSegment = 5 // 每个分段最多抽样 5 条
	totalSegments       = 3 // 分段数：头部、中间、尾部
)

func (db *Database) validateIndexSampling(idx *Index, colName, idxName string, result *ValidationResult) {
	// 获取集合
	col, exists := db.collections[colName]
	if !exists {
		return
	}

	// 获取索引大小，用于计算中间位置
	keyCount, err := idx.tree.Count()
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot get key count: %v", colName, idxName, err))
		return
	}
	if keyCount == 0 {
		return // 空索引，无需校验
	}

	// 分段采样：头部 + 中间 + 尾部
	var allValues [][]byte

	// 1. 头部采样（索引最小端）
	headValues, err := idx.tree.SearchRangeLimit(nil, nil, true, true, maxSamplePerSegment)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot sample head values: %v", colName, idxName, err))
	} else {
		allValues = append(allValues, headValues...)
	}

	// 2. 尾部采样（索引最大端）
	tailValues, err := idx.tree.SearchRangeLimitReverse(maxSamplePerSegment)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot sample tail values: %v", colName, idxName, err))
	} else {
		allValues = append(allValues, tailValues...)
	}

	// 3. 中间采样（跳过一半后取若干条）
	if keyCount > maxSamplePerSegment*2 {
		midSkip := keyCount / 2
		midValues, err := idx.tree.SearchRangeLimitSkip(midSkip, maxSamplePerSegment)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot sample mid values: %v", colName, idxName, err))
		} else {
			allValues = append(allValues, midValues...)
		}
	}

	if len(allValues) == 0 {
		return // 无法采样
	}

	checkedCount := 0
	missingCount := 0

	// 检查所有采样的值
	for i, value := range allValues {
		
		// 索引存储的值是 _id 的 BSON 编码：{_id: <value>}
		// 解析以获取 _id 值
		var idDoc bson.D
		if err := bson.Unmarshal(value, &idDoc); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: entry %d has invalid _id encoding", colName, idxName, i))
			checkedCount++
			continue
		}
		
		// 提取 _id 值
		var idVal interface{}
		for _, elem := range idDoc {
			if elem.Key == "_id" {
				idVal = elem.Value
				break
			}
		}
		
		if idVal == nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: entry %d missing _id", colName, idxName, i))
			checkedCount++
			continue
		}
		
		// 通过 _id 查找文档
		_, err := col.FindById(idVal)
		if err != nil {
			missingCount++
			// 只记录前几个错误，避免日志过多
			if missingCount <= 3 {
				result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: dangling reference for _id=%v", colName, idxName, idVal))
			}
		}
		
		checkedCount++
	}

	if missingCount > 0 {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("collection %s: index %s: %d/%d sampled entries have dangling references", colName, idxName, missingCount, checkedCount))
	}
}

// ValidateCollection 校验指定集合
func (db *Database) ValidateCollection(colName string) *ValidationResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	col, exists := db.collections[colName]
	if !exists {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s does not exist", colName))
		return result
	}

	db.validateCollection(col, colName, result)
	return result
}

// validateCommand 实现 validate 命令
func (db *Database) validateCommand(cmd bson.D) bson.D {
	// 获取集合名（可选）
	var colName string
	for _, elem := range cmd {
		if elem.Key == "validate" {
			if name, ok := elem.Value.(string); ok {
				colName = name
			}
		}
	}

	var result *ValidationResult
	if colName != "" {
		result = db.ValidateCollection(colName)
	} else {
		result = db.Validate()
	}

	// 构建响应
	response := bson.D{
		{Key: "valid", Value: result.Valid},
		{Key: "errors", Value: result.Errors},
		{Key: "warnings", Value: result.Warnings},
		{Key: "nrecords", Value: result.Stats.TotalDocuments},
		{Key: "nIndexes", Value: result.Stats.TotalIndexes},
		{Key: "ok", Value: 1.0},
	}

	return response
}


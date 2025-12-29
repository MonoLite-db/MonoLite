// Created by Yanjunhui

package engine

import (
	"fmt"

	"github.com/monolite/monodb/storage"
	"go.mongodb.org/mongo-driver/bson"
)

// ValidationResult 校验结果
// EN: ValidationResult contains validation results.
type ValidationResult struct {
	// Valid 是否有效
	// EN: Valid indicates whether the database is valid.
	Valid bool
	// Errors 错误列表
	// EN: Errors is the list of validation errors.
	Errors []string
	// Warnings 警告列表
	// EN: Warnings is the list of validation warnings.
	Warnings []string
	Stats    ValidationStats
}

// ValidationStats 校验统计
// EN: ValidationStats contains validation statistics.
type ValidationStats struct {
	// TotalPages 总页面数
	// EN: TotalPages is the total number of pages.
	TotalPages int
	// DataPages 数据页数
	// EN: DataPages is the number of data pages.
	DataPages int
	// IndexPages 索引页数
	// EN: IndexPages is the number of index pages.
	IndexPages int
	// FreePages 空闲页数
	// EN: FreePages is the number of free pages.
	FreePages int
	// MetaPages 元数据页数
	// EN: MetaPages is the number of meta pages.
	MetaPages int
	// Collections 集合数
	// EN: Collections is the number of collections.
	Collections int
	// TotalDocuments 总文档数
	// EN: TotalDocuments is the total number of documents.
	TotalDocuments int
	// TotalIndexes 总索引数
	// EN: TotalIndexes is the total number of indexes.
	TotalIndexes int
}

// Validate 校验数据库结构完整性
// EN: Validate checks database structural integrity.
//
// 实现 validate 命令，作为崩溃恢复和 fuzz 测试的总闸门
// EN: Implements the validate command, serving as a final gate for crash recovery and fuzz testing.
func (db *Database) Validate() *ValidationResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	// 1. 校验 free list
	// EN: 1) Validate free list.
	db.validateFreeList(result)

	// 2. 校验所有页面
	// EN: 2) Validate all pages.
	db.validatePages(result)

	// 3. 校验 catalog
	// EN: 3) Validate catalog.
	db.validateCatalog(result)

	// 4. 校验各集合
	// EN: 4) Validate collections.
	for name, col := range db.collections {
		db.validateCollection(col, name, result)
	}

	return result
}

// validateFreeList 校验空闲页链表
// EN: validateFreeList validates the free list chain.
//
// 检查：无环、无重复、页类型正确
// EN: Checks: no cycles, no duplicates, and correct page types.
func (db *Database) validateFreeList(result *ValidationResult) {
	pager := db.pager
	header := pager.Header()

	if header.FreeListHead == 0 {
		// 空链表，无需校验
		// EN: Empty free list; nothing to validate.
		return
	}

	visited := make(map[storage.PageId]bool)
	currentId := header.FreeListHead
	chainLen := 0

	for currentId != 0 {
		// 检查是否有环
		// EN: Detect cycles.
		if visited[currentId] {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("free list has cycle at page %d", currentId))
			return
		}
		visited[currentId] = true
		chainLen++

		// 检查页面是否存在且类型正确
		// EN: Ensure page exists and has correct type.
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
		// EN: Prevent infinite loops (at most traverse all pages).
		if chainLen > int(header.PageCount) {
			result.Valid = false
			result.Errors = append(result.Errors, "free list chain exceeds total page count")
			return
		}
	}
}

// validatePages 校验所有页面
// EN: validatePages validates all pages.
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
// EN: validateCatalog validates the catalog structure.
//
// 深度增强：不仅检查页面可读，还尝试解析 catalog 数据验证结构正确性
// EN: Deep validation: not only checks readability but also parses catalog data to validate structural correctness.
func (db *Database) validateCatalog(result *ValidationResult) {
	header := db.pager.Header()

	if header.CatalogPageId == 0 {
		// 空 catalog，可能是新数据库
		// EN: Empty catalog; this may be a new database.
		return
	}

	// 检查 catalog page 是否可读
	// EN: Check whether the catalog page is readable.
	catalogPage, err := db.pager.ReadPage(header.CatalogPageId)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("cannot read catalog page %d: %v", header.CatalogPageId, err))
		return
	}

	// 检查页面类型
	// EN: Check page type.
	if catalogPage.Type() != storage.PageTypeCatalog {
		result.Warnings = append(result.Warnings, fmt.Sprintf("catalog page %d has unexpected type: %d (expected %d)", header.CatalogPageId, catalogPage.Type(), storage.PageTypeCatalog))
	}

	// 深度校验：尝试解析 catalog 数据
	// EN: Deep validation: attempt to parse catalog data.
	sp := storage.WrapSlottedPage(catalogPage)
	itemCount := int(catalogPage.ItemCount())
	if itemCount > 0 {
		// 检查每个 slot 是否可解析为有效的 BSON
		// EN: Ensure each slot can be parsed as valid BSON.
		for i := 0; i < itemCount; i++ {
			// itemCount 表示槽总数（包含已删除槽）。已删除槽应跳过，而不是判为“读取失败”。
			// EN: itemCount includes deleted slots; deleted slots should be skipped rather than treated as read failures.
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
			// EN: Attempt BSON deserialization.
			var doc bson.D
			if err := bson.Unmarshal(data, &doc); err != nil {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("catalog slot %d: invalid BSON: %v", i, err))
				continue
			}

			// 检查必要字段存在
			// EN: Check required fields exist.
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
// EN: validateCollection validates a single collection.
func (db *Database) validateCollection(col *Collection, name string, result *ValidationResult) {
	// 校验数据页链表
	// EN: Validate data page chain.
	db.validateDataPageChain(col, name, result)

	// 校验索引
	// EN: Validate indexes.
	if col.indexManager != nil {
		for idxName, idx := range col.indexManager.indexes {
			db.validateIndex(idx, name, idxName, result)
		}
	}
}

// validateDataPageChain 校验数据页链表
// EN: validateDataPageChain validates the data page linked list for a collection.
func (db *Database) validateDataPageChain(col *Collection, colName string, result *ValidationResult) {
	if col.info.FirstPageId == 0 {
		// 空集合
		// EN: Empty collection.
		return
	}

	pager := db.pager
	visited := make(map[storage.PageId]bool)
	currentId := col.info.FirstPageId
	var prevId storage.PageId = 0
	docCount := 0

	for currentId != 0 {
		// 检查是否有环
		// EN: Detect cycles.
		if visited[currentId] {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: data page chain has cycle at page %d", colName, currentId))
			return
		}
		visited[currentId] = true

		// 读取页面
		// EN: Read page.
		page, err := pager.ReadPage(currentId)
		if err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: cannot read data page %d: %v", colName, currentId, err))
			return
		}

		// 检查页面类型
		// EN: Check page type.
		if page.Type() != storage.PageTypeData {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: page %d in data chain has wrong type: %d", colName, currentId, page.Type()))
		}

		// 检查 Prev 指针一致性
		// EN: Check Prev pointer consistency.
		if page.PrevPageId() != prevId {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("collection %s: page %d has wrong PrevPageId: %d (expected %d)", colName, currentId, page.PrevPageId(), prevId))
		}

		// 统计文档数
		// EN: Count documents.
		sp := storage.WrapSlottedPage(page)
		docCount += sp.LiveCount()

		prevId = currentId
		currentId = page.NextPageId()
	}

	result.Stats.TotalDocuments += docCount

	// 检查文档数是否一致
	// EN: Check whether document count matches metadata.
	if docCount != int(col.info.DocumentCount) {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: document count mismatch: found %d, expected %d", colName, docCount, col.info.DocumentCount))
	}
}

// validateIndex 校验单个索引
// EN: validateIndex validates a single index.
//
// 深度增强：不仅检查 B+Tree 结构，还进行抽样回表校验
// EN: Deep validation: checks B+Tree structure and performs sampled “lookup back” validation.
func (db *Database) validateIndex(idx *Index, colName, idxName string, result *ValidationResult) {
	if idx.tree == nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s has nil tree", colName, idxName))
		return
	}

	// 使用 B+Tree 的完整性检查
	// EN: Run B+Tree integrity check.
	if err := idx.tree.CheckTreeIntegrity(); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("collection %s: index %s integrity check failed: %v", colName, idxName, err))
	}

	// 检查叶子链表
	// EN: Check leaf chain.
	if err := idx.tree.CheckLeafChain(); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("collection %s: index %s leaf chain check failed: %v", colName, idxName, err))
	}

	// 深度校验：索引抽样回表校验
	// EN: Deep validation: index sampled lookup validation.
	// 随机抽取若干索引条目，验证对应文档存在
	// EN: Randomly sample index entries and verify referenced documents exist.
	db.validateIndexSampling(idx, colName, idxName, result)

	result.Stats.TotalIndexes++
}

// validateIndexSampling 索引抽样回表校验
// EN: validateIndexSampling performs sampled “lookup back” validation for an index.
//
// 检查索引条目指向的文档是否真实存在
// EN: Checks whether documents referenced by index entries actually exist.
//
// 注意：当前索引存储的值是 _id 的 BSON 编码，不是 RecordId
// EN: Note: index values are BSON-encoded {_id: <value>}, not RecordId.
//
// 分段采样策略：从索引头部、尾部、中间各采样若干条，避免抽样偏置
// EN: Segmented sampling: sample from head, tail, and middle to reduce bias.
const (
	// maxSamplePerSegment 每个分段最多抽样 5 条
	// EN: maxSamplePerSegment is the maximum samples per segment.
	maxSamplePerSegment = 5
	// totalSegments 分段数：头部、中间、尾部
	// EN: totalSegments is the number of segments: head, middle, tail.
	totalSegments = 3
)

func (db *Database) validateIndexSampling(idx *Index, colName, idxName string, result *ValidationResult) {
	// 获取集合
	// EN: Get collection.
	col, exists := db.collections[colName]
	if !exists {
		return
	}

	// 获取索引大小，用于计算中间位置
	// EN: Get index size to compute middle position.
	keyCount, err := idx.tree.Count()
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot get key count: %v", colName, idxName, err))
		return
	}
	if keyCount == 0 {
		// 空索引，无需校验
		// EN: Empty index; nothing to validate.
		return
	}

	// 分段采样：头部 + 中间 + 尾部
	// EN: Segmented sampling: head + middle + tail.
	var allValues [][]byte

	// 1. 头部采样（索引最小端）
	// EN: 1) Head sampling (smallest keys).
	headValues, err := idx.tree.SearchRangeLimit(nil, nil, true, true, maxSamplePerSegment)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot sample head values: %v", colName, idxName, err))
	} else {
		allValues = append(allValues, headValues...)
	}

	// 2. 尾部采样（索引最大端）
	// EN: 2) Tail sampling (largest keys).
	tailValues, err := idx.tree.SearchRangeLimitReverse(maxSamplePerSegment)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: cannot sample tail values: %v", colName, idxName, err))
	} else {
		allValues = append(allValues, tailValues...)
	}

	// 3. 中间采样（跳过一半后取若干条）
	// EN: 3) Middle sampling (skip half then take some entries).
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
		// 无法采样
		// EN: Unable to sample.
		return
	}

	checkedCount := 0
	missingCount := 0

	// 检查所有采样的值
	// EN: Check all sampled values.
	for i, value := range allValues {

		// 索引存储的值是 _id 的 BSON 编码：{_id: <value>}
		// EN: Index value is BSON-encoded: {_id: <value>}.
		// 解析以获取 _id 值
		// EN: Decode it to get the _id value.
		var idDoc bson.D
		if err := bson.Unmarshal(value, &idDoc); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("collection %s: index %s: entry %d has invalid _id encoding", colName, idxName, i))
			checkedCount++
			continue
		}

		// 提取 _id 值
		// EN: Extract _id value.
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
		// EN: Lookup document by _id.
		_, err := col.FindById(idVal)
		if err != nil {
			missingCount++
			// 只记录前几个错误，避免日志过多
			// EN: Record only the first few errors to avoid excessive logs.
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
// EN: ValidateCollection validates a specific collection.
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
// EN: validateCommand implements the validate command.
func (db *Database) validateCommand(cmd bson.D) bson.D {
	// 获取集合名（可选）
	// EN: Get collection name (optional).
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
	// EN: Build response.
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

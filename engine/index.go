// Created by Yanjunhui

package engine

import (
	"fmt"
	"regexp"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/monolite/monodb/internal/failpoint"
	"github.com/monolite/monodb/storage"
)

// IndexInfo 索引信息
// EN: IndexInfo contains index metadata.
type IndexInfo struct {
	// Name 索引名称
	// EN: Name is the index name.
	Name string
	// Keys 索引键（字段名和排序方向）
	// EN: Keys are the index key fields and sort directions.
	Keys bson.D
	// Unique 是否唯一索引
	// EN: Unique indicates whether this is a unique index.
	Unique bool
	// Background 是否后台创建
	// EN: Background indicates whether the index is built in background.
	Background bool
	// RootPageId B+Tree 根页面
	// EN: RootPageId is the root page ID of the underlying B+Tree.
	RootPageId storage.PageId
}

// IndexManager 索引管理器
// EN: IndexManager manages indexes for a collection.
type IndexManager struct {
	collection *Collection
	indexes    map[string]*Index
}

// Index 表示一个索引
// EN: Index represents an index.
type Index struct {
	info  *IndexInfo
	tree  *storage.BTree
	pager *storage.Pager
}

// NewIndexManager 创建索引管理器
// EN: NewIndexManager creates an IndexManager.
func NewIndexManager(col *Collection) *IndexManager {
	return &IndexManager{
		collection: col,
		indexes:    make(map[string]*Index),
	}
}

// CreateIndex 创建索引
// EN: CreateIndex creates an index.
func (im *IndexManager) CreateIndex(keys bson.D, options bson.D) (string, error) {
	// 生成索引名称
	// EN: Generate index name.
	name := generateIndexName(keys)
	for _, opt := range options {
		if opt.Key == "name" {
			if n, ok := opt.Value.(string); ok {
				name = n
			}
		}
	}

	// 检查索引是否已存在
	// EN: Check whether the index already exists.
	if _, exists := im.indexes[name]; exists {
		// 已存在，直接返回
		// EN: Already exists; return directly.
		return name, nil
	}

	// 解析选项
	// EN: Parse options.
	unique := false
	for _, opt := range options {
		if opt.Key == "unique" {
			if u, ok := opt.Value.(bool); ok {
				unique = u
			}
		}
	}

	// 创建 B+Tree
	// EN: Create underlying B+Tree.
	tree, err := storage.NewBTree(im.collection.db.pager, name, unique)
	if err != nil {
		return "", err
	}

	info := &IndexInfo{
		Name:       name,
		Keys:       keys,
		Unique:     unique,
		RootPageId: tree.RootPage(),
	}

	idx := &Index{
		info:  info,
		tree:  tree,
		pager: im.collection.db.pager,
	}

	im.indexes[name] = idx

	// 为现有文档建立索引
	// EN: Build index for existing documents.
	if err := im.buildIndex(idx); err != nil {
		return "", err
	}

	// 持久化索引元数据到 catalog
	// EN: Persist index metadata to catalog.
	im.collection.info.Indexes = append(im.collection.info.Indexes, IndexMeta{
		Name:       name,
		Keys:       keys,
		Unique:     unique,
		RootPageId: tree.RootPage(),
	})
	if err := im.collection.db.saveCatalog(); err != nil {
		return "", fmt.Errorf("failed to persist index metadata: %w", err)
	}

	return name, nil
}

// buildIndex 为现有文档建立索引
// EN: buildIndex builds an index for existing documents.
//
// 注意：调用此方法时集合锁已被持有，使用 findUnlocked 避免死锁
// EN: Note: collection lock is already held; use findUnlocked to avoid deadlocks.
func (im *IndexManager) buildIndex(idx *Index) error {
	docs, err := im.collection.findUnlocked(nil)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		key := encodeIndexEntryKey(idx, doc)
		if key == nil {
			continue
		}

		// 获取文档 _id
		// EN: Get document _id.
		idVal := getDocField(doc, "_id")
		if idVal == nil {
			continue
		}

		idBytes, err := bson.Marshal(bson.D{{Key: "_id", Value: idVal}})
		if err != nil {
			continue
		}

		if err := idx.tree.Insert(key, idBytes); err != nil {
			if idx.info.Unique {
				return fmt.Errorf("duplicate key for index %s", idx.info.Name)
			}
			return fmt.Errorf("failed to build index %s: %w", idx.info.Name, err)
		}
	}

	return nil
}

// DropIndex 删除索引
// EN: DropIndex drops an index.
func (im *IndexManager) DropIndex(name string) error {
	if name == "_id_" {
		return fmt.Errorf("cannot drop _id index")
	}

	delete(im.indexes, name)

	// 从 CollectionInfo.Indexes 中移除
	// EN: Remove from CollectionInfo.Indexes.
	newIndexes := make([]IndexMeta, 0, len(im.collection.info.Indexes))
	for _, meta := range im.collection.info.Indexes {
		if meta.Name != name {
			newIndexes = append(newIndexes, meta)
		}
	}
	im.collection.info.Indexes = newIndexes

	// 持久化更新
	// EN: Persist updates.
	if err := im.collection.db.saveCatalog(); err != nil {
		return fmt.Errorf("failed to persist index drop: %w", err)
	}

	return nil
}

// ListIndexes 列出所有索引
// EN: ListIndexes lists all indexes.
func (im *IndexManager) ListIndexes() []bson.D {
	result := make([]bson.D, 0)

	// 添加默认的 _id 索引
	// EN: Add the default _id index.
	result = append(result, bson.D{
		{Key: "name", Value: "_id_"},
		{Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "v", Value: int32(2)},
	})

	for _, idx := range im.indexes {
		result = append(result, bson.D{
			{Key: "name", Value: idx.info.Name},
			{Key: "key", Value: idx.info.Keys},
			{Key: "unique", Value: idx.info.Unique},
			{Key: "v", Value: int32(2)},
		})
	}

	return result
}

// CheckUniqueConstraints 预检查 unique 索引约束
// EN: CheckUniqueConstraints pre-checks unique index constraints.
//
// 如果文档会违反任何 unique 约束，返回错误
// EN: Returns an error if the document would violate any unique constraint.
//
// 应在写入数据前调用，实现强一致性
// EN: Should be called before writing data to achieve strong consistency.
func (im *IndexManager) CheckUniqueConstraints(doc bson.D) error {
	for _, idx := range im.indexes {
		if !idx.info.Unique {
			continue
		}

		key := encodeIndexEntryKey(idx, doc)
		if key == nil {
			continue
		}

		// 检查键是否已存在
		// EN: Check whether the key already exists.
		exists, err := idx.tree.Search(key)
		if err != nil {
			return fmt.Errorf("failed to check unique constraint: %w", err)
		}
		if exists != nil {
			return fmt.Errorf("duplicate key error: index '%s'", idx.info.Name)
		}
	}
	return nil
}

// InsertDocument 索引插入文档时更新索引
// EN: InsertDocument updates indexes when inserting a document.
//
// 注意：调用前应先调用 CheckUniqueConstraints 进行预检查
// EN: Note: call CheckUniqueConstraints before invoking this method.
//
// 【P0 修复】增加回滚支持：记录已成功更新的索引，失败时逆序回滚
// EN: [P0 fix] Add rollback support: record successfully updated indexes and rollback in reverse order on failure.
func (im *IndexManager) InsertDocument(doc bson.D) error {
	// 记录已成功更新的索引信息，用于失败时回滚
	// EN: Record successfully-updated index entries for rollback on failure.
	type insertedEntry struct {
		idx *Index
		key []byte
	}
	var insertedEntries []insertedEntry

	for _, idx := range im.indexes {
		// 【FAILPOINT】支持按索引名注入失败
		// EN: [FAILPOINT] Inject failure by index name.
		fpName := "index.insert." + idx.info.Name
		if err := failpoint.Hit(fpName); err != nil {
			// 回滚已成功的插入
			// EN: Roll back successful inserts.
			for i := len(insertedEntries) - 1; i >= 0; i-- {
				entry := insertedEntries[i]
				if delErr := entry.idx.tree.Delete(entry.key); delErr != nil {
					LogError("failed to rollback index entry during failpoint", map[string]interface{}{
						"index": entry.idx.info.Name,
						"error": delErr.Error(),
					})
				}
			}
			return fmt.Errorf("failpoint: %s: %w", fpName, err)
		}

		key := encodeIndexEntryKey(idx, doc)
		if key == nil {
			continue
		}

		idVal := getDocField(doc, "_id")
		if idVal == nil {
			continue
		}

		idBytes, err := bson.Marshal(bson.D{{Key: "_id", Value: idVal}})
		if err != nil {
			continue
		}

		if err := idx.tree.Insert(key, idBytes); err != nil {
			// 【P0 关键修复】索引插入失败，回滚已成功的索引条目
			// EN: [P0 critical fix] Index insert failed; rollback successfully inserted index entries.
			for i := len(insertedEntries) - 1; i >= 0; i-- {
				entry := insertedEntries[i]
				if delErr := entry.idx.tree.Delete(entry.key); delErr != nil {
					LogError("failed to rollback index entry", map[string]interface{}{
						"index": entry.idx.info.Name,
						"error": delErr.Error(),
					})
				}
			}
			return fmt.Errorf("failed to update index '%s': %w", idx.info.Name, err)
		}

		// 记录已成功插入的条目
		// EN: Record successful entry.
		insertedEntries = append(insertedEntries, insertedEntry{idx: idx, key: key})
	}
	return nil
}

// DeleteDocument 删除文档时更新索引
// EN: DeleteDocument updates indexes when deleting a document.
//
// 【P0 修复】增加回滚支持：记录已成功删除的索引，失败时恢复
// EN: [P0 fix] Add rollback support: record successful deletes and restore on failure.
func (im *IndexManager) DeleteDocument(doc bson.D) error {
	// 记录已成功删除的索引信息，用于失败时回滚（重新插入）
	// EN: Record successful index deletes for rollback (re-insert).
	type deletedEntry struct {
		idx     *Index
		key     []byte
		idBytes []byte
	}
	var deletedEntries []deletedEntry

	// 预先获取 _id
	// EN: Pre-fetch _id.
	idVal := getDocField(doc, "_id")
	var idBytes []byte
	if idVal != nil {
		idBytes, _ = bson.Marshal(bson.D{{Key: "_id", Value: idVal}})
	}

	for _, idx := range im.indexes {
		// 【FAILPOINT】支持按索引名注入失败
		// EN: [FAILPOINT] Inject failure by index name.
		fpName := "index.delete." + idx.info.Name
		if err := failpoint.Hit(fpName); err != nil {
			// 回滚已成功的删除（重新插入）
			// EN: Roll back successful deletes (re-insert).
			for i := len(deletedEntries) - 1; i >= 0; i-- {
				entry := deletedEntries[i]
				if insErr := entry.idx.tree.Insert(entry.key, entry.idBytes); insErr != nil {
					LogError("failed to rollback index delete during failpoint", map[string]interface{}{
						"index": entry.idx.info.Name,
						"error": insErr.Error(),
					})
				}
			}
			return fmt.Errorf("failpoint: %s: %w", fpName, err)
		}

		key := encodeIndexEntryKey(idx, doc)
		if key != nil {
			if err := idx.tree.Delete(key); err != nil {
				// 【P0 关键修复】索引删除失败，回滚已成功删除的索引条目（重新插入）
				// EN: [P0 critical fix] Index delete failed; rollback successful deletes (re-insert).
				for i := len(deletedEntries) - 1; i >= 0; i-- {
					entry := deletedEntries[i]
					if insErr := entry.idx.tree.Insert(entry.key, entry.idBytes); insErr != nil {
						LogError("failed to rollback index delete", map[string]interface{}{
							"index": entry.idx.info.Name,
							"error": insErr.Error(),
						})
					}
				}
				return fmt.Errorf("failed to delete index '%s': %w", idx.info.Name, err)
			}
			// 记录已成功删除的条目
			// EN: Record successful delete.
			deletedEntries = append(deletedEntries, deletedEntry{idx: idx, key: key, idBytes: idBytes})
		}
	}
	return nil
}

// RollbackDocumentById 根据 _id 回滚索引条目
// EN: RollbackDocumentById rolls back index entries by _id.
//
// 【BUG-002 修复】新增此函数，用于插入失败时回滚索引
// EN: [BUG-002 fix] Added for rolling back indexes on insert failure.
//
// 注意：这是一个尽力回滚的操作，因为我们可能没有完整的文档信息
// EN: Note: this is best-effort because we may not have the full document.
func (im *IndexManager) RollbackDocumentById(docId interface{}) {
	// 由于我们没有完整的文档，无法生成正确的索引键来删除
	// EN: Without the full document, we cannot compute the exact index key to delete.
	// 这个方法主要用于记录回滚意图，实际的索引清理可能需要通过后台任务完成
	// EN: This primarily records rollback intent; actual cleanup may require a background task.
	// 在当前实现中，由于 Insert 在索引失败时已经回滚了数据，
	// EN: In the current implementation, Insert already rolls back data on index failure,
	// 且索引插入是逐个进行的，失败的那个索引条目本身就没有写入成功
	// EN: and index insert happens one-by-one, so the failed entry was never written successfully,
	// 所以这里实际上不需要做额外操作
	// EN: so no extra action is required here.

	// 但为了完整性，我们记录这个事件
	// EN: But for completeness, record this event.
	LogInfo("index rollback requested", map[string]interface{}{
		"docId": docId,
	})
}

// generateIndexName 生成索引名称
// EN: generateIndexName generates an index name.
func generateIndexName(keys bson.D) string {
	name := ""
	for i, elem := range keys {
		if i > 0 {
			name += "_"
		}
		dir := 1
		if d, ok := elem.Value.(int); ok {
			dir = d
		} else if d, ok := elem.Value.(int32); ok {
			dir = int(d)
		}
		name += fmt.Sprintf("%s_%d", elem.Key, dir)
	}
	return name
}

// extractIndexKey 从文档中提取索引键
// EN: extractIndexKey extracts the index key from a document.
//
// 使用 KeyString 编码而非 BSON 序列化，确保字节序比较 == MongoDB 比较序
// EN: Use KeyString encoding (not BSON) to make byte ordering match MongoDB comparison order.
func extractIndexKey(doc bson.D, keySpec bson.D) []byte {
	// 使用 KeyString 编码：支持正确的跨类型比较、升降序、范围查询
	// EN: KeyString supports cross-type ordering, ascending/descending, and range queries.
	return storage.EncodeIndexKey(keySpec, doc)
}

// encodeIndexEntryKey 为单条索引记录生成 B+Tree key。
// EN: encodeIndexEntryKey builds the B+Tree key for a single index entry.
//
// 关键点：
// EN: Key points:
// - unique 索引：key 仅由索引字段组成（KeyString）
// EN: - Unique index: key is composed only of index fields (KeyString).
// - non-unique 索引：为支持“同键多记录”，并保持 B+Tree 结构严格递增：
// EN: - Non-unique index: to support multiple records for the same key and keep strict ordering:
//
//	key = KeyString(fields) + 0x00 + stable(_id)
//
// EN:   key = KeyString(fields) + 0x00 + stable(_id)
//
// 注意：当前索引维护与删除路径都是“按文档生成 key”，因此追加 _id 后能够精确删除对应条目。
// EN: Note: since maintenance/delete paths generate keys from documents, appending _id enables precise deletion.
func encodeIndexEntryKey(idx *Index, doc bson.D) []byte {
	base := extractIndexKey(doc, idx.info.Keys)
	if base == nil {
		return nil
	}
	if idx.info.Unique {
		return base
	}

	idVal := getDocField(doc, "_id")
	if idVal == nil {
		// 理论上不应发生：文档写入前 ensureId 会补齐
		// EN: Should not happen: ensureId fills _id before writing.
		return base
	}
	idBytes, err := bson.Marshal(bson.D{{Key: "_id", Value: idVal}})
	if err != nil {
		return base
	}

	key := make([]byte, 0, len(base)+1+len(idBytes))
	key = append(key, base...)
	key = append(key, 0x00)
	key = append(key, idBytes...)
	return key
}

// getDocField 从文档中获取字段值（支持点号路径，如 "a.b.c"）
// EN: getDocField gets a document field value (supports dotted paths like \"a.b.c\").
//
// 支持嵌套文档和数组索引访问
// EN: Supports nested documents and array index access.
func getDocField(doc bson.D, path string) interface{} {
	return getNestedValue(doc, path)
}

// getNestedValue 递归获取嵌套字段值
// EN: getNestedValue recursively gets a nested field value.
func getNestedValue(value interface{}, path string) interface{} {
	if path == "" {
		return value
	}

	// 分割路径
	// EN: Split path.
	var key string
	var rest string
	dotIndex := -1
	for i, c := range path {
		if c == '.' {
			dotIndex = i
			break
		}
	}
	if dotIndex == -1 {
		key = path
		rest = ""
	} else {
		key = path[:dotIndex]
		rest = path[dotIndex+1:]
	}

	switch v := value.(type) {
	case bson.D:
		// 从文档中获取字段
		// EN: Get from document.
		for _, elem := range v {
			if elem.Key == key {
				if rest == "" {
					return elem.Value
				}
				return getNestedValue(elem.Value, rest)
			}
		}
		return nil

	case primitive.M:
		// 从 map 中获取字段
		// EN: Get from map.
		if val, ok := v[key]; ok {
			if rest == "" {
				return val
			}
			return getNestedValue(val, rest)
		}
		return nil

	case bson.A:
		// 数组情况：尝试解析数字索引
		// EN: Array: try parsing numeric index.
		index := parseArrayIndex(key)
		if index >= 0 && index < len(v) {
			if rest == "" {
				return v[index]
			}
			return getNestedValue(v[index], rest)
		}
		// 如果不是数字索引，对数组中的每个元素查找（MongoDB 的数组匹配语义）
		// EN: If not a numeric index, search each element (MongoDB array matching semantics).
		// 返回第一个匹配的值
		// EN: Return the first matching value.
		for _, item := range v {
			if itemDoc, ok := item.(bson.D); ok {
				if result := getNestedValue(itemDoc, path); result != nil {
					return result
				}
			}
		}
		return nil

	case []interface{}:
		// 普通切片类型
		// EN: Plain slice type.
		index := parseArrayIndex(key)
		if index >= 0 && index < len(v) {
			if rest == "" {
				return v[index]
			}
			return getNestedValue(v[index], rest)
		}
		return nil

	default:
		return nil
	}
}

// parseArrayIndex 解析数组索引，返回 -1 表示非数字
// EN: parseArrayIndex parses an array index; returns -1 if not numeric.
func parseArrayIndex(s string) int {
	if s == "" {
		return -1
	}
	index := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		index = index*10 + int(c-'0')
	}
	return index
}

// FilterMatcher 过滤器匹配器
// EN: FilterMatcher matches documents against a filter.
type FilterMatcher struct {
	filter bson.D
}

// NewFilterMatcher 创建过滤器匹配器
// EN: NewFilterMatcher creates a FilterMatcher.
func NewFilterMatcher(filter bson.D) *FilterMatcher {
	return &FilterMatcher{filter: filter}
}

// Match 检查文档是否匹配过滤器
// EN: Match checks whether a document matches the filter.
func (fm *FilterMatcher) Match(doc bson.D) bool {
	if len(fm.filter) == 0 {
		return true
	}

	for _, elem := range fm.filter {
		if !fm.matchElement(doc, elem.Key, elem.Value) {
			return false
		}
	}

	return true
}

// matchElement 匹配单个过滤条件
// EN: matchElement matches a single filter element.
func (fm *FilterMatcher) matchElement(doc bson.D, key string, value interface{}) bool {
	// 处理逻辑运算符
	// EN: Handle logical operators.
	switch key {
	case "$and":
		return fm.matchAnd(doc, value)
	case "$or":
		return fm.matchOr(doc, value)
	case "$not":
		return fm.matchNot(doc, value)
	case "$nor":
		return fm.matchNor(doc, value)
	}

	// 获取文档字段值
	// EN: Get document field value.
	docVal := getDocField(doc, key)

	// 如果 value 是 bson.D，可能包含比较运算符
	// EN: If value is a document, it may contain comparison operators.
	if operators, ok := value.(bson.D); ok {
		return fm.matchOperators(docVal, operators)
	}

	// 直接相等比较
	// EN: Direct equality comparison.
	return valuesEqual(docVal, value)
}

// matchOperators 匹配比较运算符
// EN: matchOperators matches a set of comparison operators.
func (fm *FilterMatcher) matchOperators(docVal interface{}, operators bson.D) bool {
	for _, op := range operators {
		if !fm.matchOperator(docVal, op.Key, op.Value) {
			return false
		}
	}
	return true
}

// matchOperator 匹配单个运算符
// EN: matchOperator matches a single operator.
func (fm *FilterMatcher) matchOperator(docVal interface{}, operator string, operand interface{}) bool {
	switch operator {
	case "$eq":
		return valuesEqual(docVal, operand)

	case "$ne":
		return !valuesEqual(docVal, operand)

	case "$gt":
		return compareValues(docVal, operand) > 0

	case "$gte":
		return compareValues(docVal, operand) >= 0

	case "$lt":
		return compareValues(docVal, operand) < 0

	case "$lte":
		return compareValues(docVal, operand) <= 0

	case "$in":
		return fm.matchIn(docVal, operand)

	case "$nin":
		return !fm.matchIn(docVal, operand)

	case "$exists":
		exists := docVal != nil
		if want, ok := operand.(bool); ok {
			return exists == want
		}
		return exists

	case "$type":
		return fm.matchType(docVal, operand)

	case "$regex":
		return fm.matchRegex(docVal, operand)

	case "$size":
		return fm.matchSize(docVal, operand)

	case "$all":
		return fm.matchAll(docVal, operand)

	case "$elemMatch":
		return fm.matchElemMatch(docVal, operand)

	default:
		// 未知运算符，当作字段名处理
		// EN: Unknown operator; treat as field name (unsupported).
		return false
	}
}

// matchAnd 处理 $and
// EN: matchAnd handles $and.
func (fm *FilterMatcher) matchAnd(doc bson.D, value interface{}) bool {
	arr, ok := value.(bson.A)
	if !ok {
		return false
	}

	for _, item := range arr {
		if subFilter, ok := item.(bson.D); ok {
			subMatcher := NewFilterMatcher(subFilter)
			if !subMatcher.Match(doc) {
				return false
			}
		}
	}
	return true
}

// matchOr 处理 $or
// EN: matchOr handles $or.
func (fm *FilterMatcher) matchOr(doc bson.D, value interface{}) bool {
	arr, ok := value.(bson.A)
	if !ok {
		return false
	}

	for _, item := range arr {
		if subFilter, ok := item.(bson.D); ok {
			subMatcher := NewFilterMatcher(subFilter)
			if subMatcher.Match(doc) {
				return true
			}
		}
	}
	return false
}

// matchNot 处理 $not
// EN: matchNot handles $not.
func (fm *FilterMatcher) matchNot(doc bson.D, value interface{}) bool {
	if subFilter, ok := value.(bson.D); ok {
		subMatcher := NewFilterMatcher(subFilter)
		return !subMatcher.Match(doc)
	}
	return true
}

// matchNor 处理 $nor
// EN: matchNor handles $nor.
func (fm *FilterMatcher) matchNor(doc bson.D, value interface{}) bool {
	return !fm.matchOr(doc, value)
}

// matchIn 处理 $in
// EN: matchIn handles $in.
func (fm *FilterMatcher) matchIn(docVal interface{}, operand interface{}) bool {
	arr, ok := operand.(bson.A)
	if !ok {
		return false
	}

	for _, item := range arr {
		if valuesEqual(docVal, item) {
			return true
		}
	}
	return false
}

// matchType 处理 $type
// EN: matchType handles $type.
func (fm *FilterMatcher) matchType(docVal interface{}, operand interface{}) bool {
	var expectedType int
	switch t := operand.(type) {
	case int:
		expectedType = t
	case int32:
		expectedType = int(t)
	case string:
		expectedType = typeNameToNumber(t)
	default:
		return false
	}

	actualType := getBsonType(docVal)
	return actualType == expectedType
}

// matchRegex 处理 $regex
// EN: matchRegex handles $regex.
func (fm *FilterMatcher) matchRegex(docVal interface{}, operand interface{}) bool {
	str, ok := docVal.(string)
	if !ok {
		return false
	}

	pattern, ok := operand.(string)
	if !ok {
		if regex, ok := operand.(primitive.Regex); ok {
			pattern = regex.Pattern
		} else {
			return false
		}
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}

	return re.MatchString(str)
}

// matchSize 处理 $size
// EN: matchSize handles $size.
func (fm *FilterMatcher) matchSize(docVal interface{}, operand interface{}) bool {
	arr, ok := docVal.(bson.A)
	if !ok {
		return false
	}

	var expectedSize int
	switch s := operand.(type) {
	case int:
		expectedSize = s
	case int32:
		expectedSize = int(s)
	case int64:
		expectedSize = int(s)
	default:
		return false
	}

	return len(arr) == expectedSize
}

// matchAll 处理 $all
// EN: matchAll handles $all.
func (fm *FilterMatcher) matchAll(docVal interface{}, operand interface{}) bool {
	arr, ok := docVal.(bson.A)
	if !ok {
		return false
	}

	required, ok := operand.(bson.A)
	if !ok {
		return false
	}

	for _, req := range required {
		found := false
		for _, item := range arr {
			if valuesEqual(item, req) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// matchElemMatch 处理 $elemMatch
// EN: matchElemMatch handles $elemMatch.
func (fm *FilterMatcher) matchElemMatch(docVal interface{}, operand interface{}) bool {
	arr, ok := docVal.(bson.A)
	if !ok {
		return false
	}

	subFilter, ok := operand.(bson.D)
	if !ok {
		return false
	}

	subMatcher := NewFilterMatcher(subFilter)
	for _, item := range arr {
		if itemDoc, ok := item.(bson.D); ok {
			if subMatcher.Match(itemDoc) {
				return true
			}
		}
	}
	return false
}

// compareValues 比较两个值
// EN: compareValues compares two values.
//
// 统一使用 CompareBSON 实现 MongoDB 标准 BSON 类型比较规则
// EN: Always use CompareBSON to follow MongoDB BSON type ordering/comparison rules.
func compareValues(a, b interface{}) int {
	return CompareBSON(a, b)
}

// typeNameToNumber 将类型名转换为 BSON 类型号
// EN: typeNameToNumber converts a type name to a BSON type number.
func typeNameToNumber(name string) int {
	switch name {
	case "double":
		return 1
	case "string":
		return 2
	case "object":
		return 3
	case "array":
		return 4
	case "binData":
		return 5
	case "objectId":
		return 7
	case "bool":
		return 8
	case "date":
		return 9
	case "null":
		return 10
	case "regex":
		return 11
	case "int":
		return 16
	case "long":
		return 18
	default:
		return -1
	}
}

// getBsonType 获取值的 BSON 类型号
// EN: getBsonType returns the BSON type number for a value.
func getBsonType(val interface{}) int {
	switch val.(type) {
	case float64:
		return 1
	case string:
		return 2
	case bson.D:
		return 3
	case bson.A:
		return 4
	case primitive.ObjectID:
		return 7
	case bool:
		return 8
	case primitive.DateTime:
		return 9
	case nil:
		return 10
	case primitive.Regex:
		return 11
	case int32:
		return 16
	case int64:
		return 18
	default:
		return -1
	}
}

// QueryOptions 查询选项
// EN: QueryOptions contains query options.
type QueryOptions struct {
	Sort       bson.D
	Limit      int64
	Skip       int64
	Projection bson.D
}

// ApplyOptions 应用查询选项到结果集
// EN: ApplyOptions applies query options to a result set.
func ApplyOptions(docs []bson.D, opts *QueryOptions) []bson.D {
	if opts == nil {
		return docs
	}

	result := docs

	// 排序
	// EN: Sort.
	if len(opts.Sort) > 0 {
		result = sortDocuments(result, opts.Sort)
	}

	// Skip
	if opts.Skip > 0 {
		if int64(len(result)) <= opts.Skip {
			return []bson.D{}
		}
		result = result[opts.Skip:]
	}

	// Limit
	if opts.Limit > 0 && int64(len(result)) > opts.Limit {
		result = result[:opts.Limit]
	}

	// Projection
	if len(opts.Projection) > 0 {
		result = applyProjection(result, opts.Projection)
	}

	return result
}

// sortDocuments 对文档排序（使用 CompareBSON 实现 MongoDB 标准排序规则）
// EN: sortDocuments sorts documents using MongoDB comparison semantics (CompareBSON).
func sortDocuments(docs []bson.D, sortSpec bson.D) []bson.D {
	result := make([]bson.D, len(docs))
	copy(result, docs)

	sort.Slice(result, func(i, j int) bool {
		for _, spec := range sortSpec {
			field := spec.Key
			direction := 1
			if d, ok := spec.Value.(int); ok {
				direction = d
			} else if d, ok := spec.Value.(int32); ok {
				direction = int(d)
			}

			valI := getDocField(result[i], field)
			valJ := getDocField(result[j], field)

			// 使用 CompareBSON 实现 MongoDB 标准 BSON 类型比较规则
			// EN: Use CompareBSON to follow MongoDB BSON type ordering/comparison rules.
			cmp := CompareBSON(valI, valJ)
			if cmp != 0 {
				if direction < 0 {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return result
}

// applyProjection 应用投影
// EN: applyProjection applies projection to documents.
func applyProjection(docs []bson.D, projection bson.D) []bson.D {
	if len(projection) == 0 {
		return docs
	}

	// 判断是包含还是排除模式
	// EN: Determine include vs exclude mode.
	includeMode := false
	for _, p := range projection {
		if p.Key == "_id" {
			continue
		}
		if v, ok := p.Value.(int); ok && v == 1 {
			includeMode = true
		} else if v, ok := p.Value.(int32); ok && v == 1 {
			includeMode = true
		}
		break
	}

	result := make([]bson.D, len(docs))
	for i, doc := range docs {
		if includeMode {
			// 包含模式：只保留指定字段
			// EN: Include mode: keep only specified fields.
			newDoc := bson.D{}
			// 默认包含 _id
			// EN: Include _id by default.
			includeId := true
			for _, p := range projection {
				if p.Key == "_id" {
					if v, ok := p.Value.(int); ok && v == 0 {
						includeId = false
					} else if v, ok := p.Value.(int32); ok && v == 0 {
						includeId = false
					}
				}
			}
			if includeId {
				if idVal := getDocField(doc, "_id"); idVal != nil {
					newDoc = append(newDoc, bson.E{Key: "_id", Value: idVal})
				}
			}
			for _, p := range projection {
				if p.Key == "_id" {
					continue
				}
				if v, ok := p.Value.(int); ok && v == 1 {
					if val := getDocField(doc, p.Key); val != nil {
						newDoc = append(newDoc, bson.E{Key: p.Key, Value: val})
					}
				} else if v, ok := p.Value.(int32); ok && v == 1 {
					if val := getDocField(doc, p.Key); val != nil {
						newDoc = append(newDoc, bson.E{Key: p.Key, Value: val})
					}
				}
			}
			result[i] = newDoc
		} else {
			// 排除模式：移除指定字段
			// EN: Exclude mode: remove specified fields.
			newDoc := bson.D{}
			excludeFields := make(map[string]bool)
			for _, p := range projection {
				if v, ok := p.Value.(int); ok && v == 0 {
					excludeFields[p.Key] = true
				} else if v, ok := p.Value.(int32); ok && v == 0 {
					excludeFields[p.Key] = true
				}
			}
			for _, elem := range doc {
				if !excludeFields[elem.Key] {
					newDoc = append(newDoc, elem)
				}
			}
			result[i] = newDoc
		}
	}

	return result
}

// Created by Yanjunhui

package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/monolite/monodb/storage"
)

// Database 表示一个数据库实例
type Database struct {
	name           string
	pager          *storage.Pager
	collections    map[string]*Collection
	cursorManager  *CursorManager
	txnManager     *TransactionManager
	sessionManager *SessionManager
	startTime      time.Time
	mu             sync.RWMutex
}

// OpenDatabase 打开或创建数据库
func OpenDatabase(path string) (*Database, error) {
	pager, err := storage.OpenPager(path)
	if err != nil {
		return nil, err
	}

	db := &Database{
		name:          path,
		pager:         pager,
		collections:   make(map[string]*Collection),
		cursorManager: NewCursorManager(),
		startTime:     time.Now(),
	}
	
	// 初始化事务管理器
	db.txnManager = NewTransactionManager(db)
	
	// 初始化会话管理器
	db.sessionManager = NewSessionManager(db)

	// 加载目录
	if err := db.loadCatalog(); err != nil {
		pager.Close()
		return nil, err
	}

	return db, nil
}

// Name 返回数据库名称
func (db *Database) Name() string {
	return db.name
}

// GetCollection 只获取集合，不创建
// 用于读操作（如 find），避免隐式创建集合（MongoDB 行为）
func (db *Database) GetCollection(name string) *Collection {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.collections[name]
}

// Collection 获取或创建集合
func (db *Database) Collection(name string) (*Collection, error) {
	// 验证集合名
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}

	db.mu.RLock()
	if col, ok := db.collections[name]; ok {
		db.mu.RUnlock()
		return col, nil
	}
	db.mu.RUnlock()

	// 创建新集合
	db.mu.Lock()
	defer db.mu.Unlock()

	// 双重检查
	if col, ok := db.collections[name]; ok {
		return col, nil
	}

	info := &CollectionInfo{
		Name:          name,
		FirstPageId:   0,
		LastPageId:    0,
		DocumentCount: 0,
		IndexPageId:   0,
	}

	col := &Collection{
		info: info,
		db:   db,
	}

	db.collections[name] = col

	// 保存目录（已持有 db.mu.Lock()，使用 Locked 版本）
	if err := db.saveCatalogLocked(); err != nil {
		delete(db.collections, name)
		return nil, err
	}

	return col, nil
}

// DropCollection 删除集合
func (db *Database) DropCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	col, ok := db.collections[name]
	if !ok {
		return nil // 集合不存在，视为成功
	}

	// 释放所有数据页
	currentPageId := col.info.FirstPageId
	for currentPageId != 0 {
		page, err := db.pager.ReadPage(currentPageId)
		if err != nil {
			break
		}
		nextId := page.NextPageId()
		db.pager.FreePage(currentPageId)
		currentPageId = nextId
	}

	delete(db.collections, name)

	// 已持有 db.mu.Lock()，使用 Locked 版本
	return db.saveCatalogLocked()
}

// ListCollections 列出所有集合名称
func (db *Database) ListCollections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.collections))
	for name := range db.collections {
		names = append(names, name)
	}
	return names
}

// catalogData catalog 持久化数据结构
type catalogData struct {
	Collections []collectionData `bson:"collections"`
}

// collectionData 集合持久化数据
type collectionData struct {
	Name          string             `bson:"name"`
	FirstPageId   uint32             `bson:"firstPageId"`
	LastPageId    uint32             `bson:"lastPageId"`
	DocumentCount int64              `bson:"documentCount"`
	IndexPageId   uint32             `bson:"indexPageId"`
	Indexes       []indexMetaBSON    `bson:"indexes,omitempty"`
}

// indexMetaBSON 索引元数据 BSON 格式
type indexMetaBSON struct {
	Name       string `bson:"name"`
	Keys       bson.D `bson:"keys"`
	Unique     bool   `bson:"unique"`
	RootPageId uint32 `bson:"rootPageId"`
}

// loadCatalog 加载集合目录
func (db *Database) loadCatalog() error {
	catalogPageId := db.pager.CatalogPageId()
	if catalogPageId == 0 {
		return nil // 空数据库
	}

	page, err := db.pager.ReadPage(catalogPageId)
	if err != nil {
		return err
	}

	data := page.Data()
	
	// 检查数据是否有效（至少有最小长度）
	if len(data) < 5 {
		return nil
	}

	// 检查是否是多页格式（通过 magic number）
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic == catalogMagic {
		// 多页格式
		return db.loadCatalogMultiPage(page, data)
	}

	// 获取 BSON 文档长度（单页格式）
	bsonLen := binary.LittleEndian.Uint32(data[0:4])
	if bsonLen < 5 || int(bsonLen) > len(data) {
		// 尝试使用旧格式加载（向后兼容）
		return db.loadCatalogLegacy(data)
	}

	// 反序列化 BSON 格式的 catalog
	var catalog catalogData
	if err := bson.Unmarshal(data[:bsonLen], &catalog); err != nil {
		// 尝试使用旧格式加载
		return db.loadCatalogLegacy(data)
	}

	return db.restoreCollectionsFromCatalog(&catalog)
}

// loadCatalogMultiPage 加载多页 catalog
func (db *Database) loadCatalogMultiPage(firstPage *storage.Page, firstData []byte) error {
	// 解析头信息
	if len(firstData) < catalogHeaderLen {
		return fmt.Errorf("invalid multi-page catalog header")
	}

	totalLen := binary.LittleEndian.Uint32(firstData[4:8])
	pageCount := binary.LittleEndian.Uint32(firstData[8:12])

	if totalLen == 0 || pageCount == 0 {
		return fmt.Errorf("invalid multi-page catalog: totalLen=%d, pageCount=%d", totalLen, pageCount)
	}

	// 读取所有数据
	bsonData := make([]byte, 0, totalLen)
	
	// 第一页数据
	firstPageDataCap := storage.MaxPageData - catalogHeaderLen
	if int(totalLen) <= firstPageDataCap {
		bsonData = append(bsonData, firstData[catalogHeaderLen:catalogHeaderLen+int(totalLen)]...)
	} else {
		bsonData = append(bsonData, firstData[catalogHeaderLen:]...)
	}

	// 读取后续页面
	currentPageId := firstPage.NextPageId()
	for len(bsonData) < int(totalLen) && currentPageId != 0 {
		page, err := db.pager.ReadPage(currentPageId)
		if err != nil {
			return fmt.Errorf("failed to read catalog page %d: %w", currentPageId, err)
		}

		pageData := page.Data()
		remaining := int(totalLen) - len(bsonData)
		if remaining > len(pageData) {
			remaining = len(pageData)
		}
		bsonData = append(bsonData, pageData[:remaining]...)

		currentPageId = page.NextPageId()
	}

	if len(bsonData) < int(totalLen) {
		return fmt.Errorf("incomplete multi-page catalog: got %d bytes, expected %d", len(bsonData), totalLen)
	}

	// 反序列化
	var catalog catalogData
	if err := bson.Unmarshal(bsonData, &catalog); err != nil {
		return fmt.Errorf("failed to unmarshal multi-page catalog: %w", err)
	}

	return db.restoreCollectionsFromCatalog(&catalog)
}

// restoreCollectionsFromCatalog 从 catalog 数据恢复集合
func (db *Database) restoreCollectionsFromCatalog(catalog *catalogData) error {
	for _, colData := range catalog.Collections {
		info := &CollectionInfo{
			Name:          colData.Name,
			FirstPageId:   storage.PageId(colData.FirstPageId),
			LastPageId:    storage.PageId(colData.LastPageId),
			DocumentCount: colData.DocumentCount,
			IndexPageId:   storage.PageId(colData.IndexPageId),
			Indexes:       make([]IndexMeta, 0, len(colData.Indexes)),
		}

		// 恢复索引元数据
		for _, idxData := range colData.Indexes {
			info.Indexes = append(info.Indexes, IndexMeta{
				Name:       idxData.Name,
				Keys:       idxData.Keys,
				Unique:     idxData.Unique,
				RootPageId: storage.PageId(idxData.RootPageId),
			})
		}

		col := &Collection{
			info: info,
			db:   db,
		}
		db.collections[colData.Name] = col

		// 如果有索引，恢复索引管理器
		if len(info.Indexes) > 0 {
			col.restoreIndexes()
		}
	}

	return nil
}

// loadCatalogLegacy 加载旧格式的 catalog（向后兼容）
func (db *Database) loadCatalogLegacy(data []byte) error {
	pos := 0

	// 读取集合数量
	if len(data) < 4 {
		return nil
	}
	count := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// 校验 count 是否合理（避免损坏数据导致无限循环）
	if count < 0 || count > 10000 {
		return nil
	}

	for i := 0; i < count && pos < len(data); i++ {
		// 读取集合名称长度
		if pos+2 > len(data) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		// 校验名称长度
		if nameLen < 0 || nameLen > 1000 {
			break
		}

		// 读取集合名称
		if pos+nameLen > len(data) {
			break
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// 读取集合元信息 (24 bytes)
		if pos+24 > len(data) {
			break
		}
		info := &CollectionInfo{
			Name:          name,
			FirstPageId:   storage.PageId(binary.LittleEndian.Uint32(data[pos:])),
			LastPageId:    storage.PageId(binary.LittleEndian.Uint32(data[pos+4:])),
			DocumentCount: int64(binary.LittleEndian.Uint64(data[pos+8:])),
			IndexPageId:   storage.PageId(binary.LittleEndian.Uint32(data[pos+16:])),
		}
		pos += 24

		db.collections[name] = &Collection{
			info: info,
			db:   db,
		}
	}

	return nil
}

// saveCatalog 保存集合目录（会获取读锁）
// 用于 Collection 方法调用（它们不持有 db.mu）
func (db *Database) saveCatalog() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.saveCatalogLocked()
}

// catalogMultiPageHeader 多页 catalog 的头信息
// 存储在第一页的开头
const (
	catalogMagic     uint32 = 0x4D504354 // "MPCT" = Multi-Page Catalog
	catalogHeaderLen = 12                // magic(4) + totalLen(4) + pageCount(4)
)

// saveCatalogLocked 保存集合目录（调用者必须持有 db.mu 锁）
// 使用 BSON 格式存储，支持多页存储和索引元数据持久化
func (db *Database) saveCatalogLocked() error {
	// 构建 catalog 数据
	catalog := catalogData{
		Collections: make([]collectionData, 0, len(db.collections)),
	}

	for _, col := range db.collections {
		colData := collectionData{
			Name:          col.info.Name,
			FirstPageId:   uint32(col.info.FirstPageId),
			LastPageId:    uint32(col.info.LastPageId),
			DocumentCount: col.info.DocumentCount,
			IndexPageId:   uint32(col.info.IndexPageId),
			Indexes:       make([]indexMetaBSON, 0),
		}

		// 收集索引元数据
		if col.indexManager != nil {
			for _, idx := range col.indexManager.indexes {
				colData.Indexes = append(colData.Indexes, indexMetaBSON{
					Name:       idx.info.Name,
					Keys:       idx.info.Keys,
					Unique:     idx.info.Unique,
					RootPageId: uint32(idx.info.RootPageId),
				})
			}
		}
		// 也包含 CollectionInfo 中已持久化的索引（尚未加载到 indexManager 的）
		for _, idxMeta := range col.info.Indexes {
			// 检查是否已经在 indexManager 中
			alreadyInManager := false
			if col.indexManager != nil {
				if _, exists := col.indexManager.indexes[idxMeta.Name]; exists {
					alreadyInManager = true
				}
			}
			if !alreadyInManager {
				colData.Indexes = append(colData.Indexes, indexMetaBSON{
					Name:       idxMeta.Name,
					Keys:       idxMeta.Keys,
					Unique:     idxMeta.Unique,
					RootPageId: uint32(idxMeta.RootPageId),
				})
			}
		}

		catalog.Collections = append(catalog.Collections, colData)
	}

	// 序列化为 BSON
	bsonData, err := bson.Marshal(catalog)
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	// 检查是否需要多页存储
	if len(bsonData) <= storage.MaxPageData {
		// 单页存储（向后兼容）
		return db.saveCatalogSinglePage(bsonData)
	}

	// 多页存储
	return db.saveCatalogMultiPage(bsonData)
}

// saveCatalogSinglePage 单页存储 catalog
func (db *Database) saveCatalogSinglePage(data []byte) error {
	catalogPageId := db.pager.CatalogPageId()
	var page *storage.Page
	var err error

	if catalogPageId == 0 {
		page, err = db.pager.AllocatePage(storage.PageTypeCatalog)
		if err != nil {
			return err
		}
		db.pager.SetCatalogPageId(page.ID())
	} else {
		page, err = db.pager.ReadPage(catalogPageId)
		if err != nil {
			return err
		}
		// 清理可能存在的后续页面（从多页切换到单页的情况）
		db.freeCatalogChain(page.NextPageId())
		page.SetNextPageId(0)
	}

	page.SetData(data)
	db.pager.MarkDirty(page.ID())
	return nil
}

// saveCatalogMultiPage 多页存储 catalog
func (db *Database) saveCatalogMultiPage(bsonData []byte) error {
	// 计算需要的页数
	// 第一页：header + 数据
	// 后续页：纯数据
	firstPageDataCap := storage.MaxPageData - catalogHeaderLen
	subsequentPageCap := storage.MaxPageData

	pagesNeeded := 1
	if len(bsonData) > firstPageDataCap {
		remaining := len(bsonData) - firstPageDataCap
		pagesNeeded += (remaining + subsequentPageCap - 1) / subsequentPageCap
	}

	// 准备多页头信息
	header := make([]byte, catalogHeaderLen)
	binary.LittleEndian.PutUint32(header[0:4], catalogMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(bsonData)))
	binary.LittleEndian.PutUint32(header[8:12], uint32(pagesNeeded))

	// 获取或分配第一页
	catalogPageId := db.pager.CatalogPageId()
	var firstPage *storage.Page
	var err error

	if catalogPageId == 0 {
		firstPage, err = db.pager.AllocatePage(storage.PageTypeCatalog)
		if err != nil {
			return err
		}
		db.pager.SetCatalogPageId(firstPage.ID())
	} else {
		firstPage, err = db.pager.ReadPage(catalogPageId)
		if err != nil {
			return err
		}
	}

	// 写入第一页：header + 部分数据
	firstPageData := make([]byte, storage.MaxPageData)
	copy(firstPageData[0:], header)
	dataForFirstPage := bsonData
	if len(dataForFirstPage) > firstPageDataCap {
		dataForFirstPage = bsonData[:firstPageDataCap]
	}
	copy(firstPageData[catalogHeaderLen:], dataForFirstPage)

	firstPage.SetData(firstPageData)
	db.pager.MarkDirty(firstPage.ID())

	// 写入后续页面
	dataOffset := len(dataForFirstPage)
	currentPage := firstPage
	existingNextId := currentPage.NextPageId()

	for dataOffset < len(bsonData) {
		// 获取或分配下一页
		var nextPage *storage.Page
		if existingNextId != 0 {
			nextPage, err = db.pager.ReadPage(existingNextId)
			if err != nil {
				// 分配新页
				nextPage, err = db.pager.AllocatePage(storage.PageTypeCatalog)
				if err != nil {
					return err
				}
			}
			existingNextId = nextPage.NextPageId()
		} else {
			nextPage, err = db.pager.AllocatePage(storage.PageTypeCatalog)
			if err != nil {
				return err
			}
		}

		// 链接页面
		currentPage.SetNextPageId(nextPage.ID())
		db.pager.MarkDirty(currentPage.ID())

		// 写入数据
		endOffset := dataOffset + subsequentPageCap
		if endOffset > len(bsonData) {
			endOffset = len(bsonData)
		}

		pageData := make([]byte, storage.MaxPageData)
		copy(pageData, bsonData[dataOffset:endOffset])
		nextPage.SetData(pageData)
		db.pager.MarkDirty(nextPage.ID())

		dataOffset = endOffset
		currentPage = nextPage
	}

	// 清理多余的旧页面
	currentPage.SetNextPageId(0)
	db.pager.MarkDirty(currentPage.ID())
	db.freeCatalogChain(existingNextId)

	return nil
}

// freeCatalogChain 释放 catalog 页面链
func (db *Database) freeCatalogChain(startPageId storage.PageId) {
	currentId := startPageId
	for currentId != 0 {
		page, err := db.pager.ReadPage(currentId)
		if err != nil {
			break
		}
		nextId := page.NextPageId()
		db.pager.FreePage(currentId)
		currentId = nextId
	}
}

// Flush 将所有更改写入磁盘
func (db *Database) Flush() error {
	return db.pager.Flush()
}

// Close 关闭数据库
func (db *Database) Close() error {
	if db.cursorManager != nil {
		db.cursorManager.Stop()
	}
	if db.sessionManager != nil {
		db.sessionManager.Close()
	}
	return db.pager.Close()
}

// Stats 返回数据库统计信息
func (db *Database) Stats() *DatabaseStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := &DatabaseStats{
		CollectionCount: len(db.collections),
		PageCount:       db.pager.PageCount(),
		Collections:     make(map[string]CollectionStats),
	}

	var totalDocs int64
	for name, col := range db.collections {
		stats.Collections[name] = CollectionStats{
			DocumentCount: col.info.DocumentCount,
		}
		totalDocs += col.info.DocumentCount
	}
	stats.DocumentCount = totalDocs

	return stats
}

// DatabaseStats 数据库统计信息
type DatabaseStats struct {
	CollectionCount int
	DocumentCount   int64
	PageCount       uint32
	Collections     map[string]CollectionStats
}

// CollectionStats 集合统计信息
type CollectionStats struct {
	DocumentCount int64
}

// RunCommand 执行数据库命令（兼容 MongoDB 命令格式）
func (db *Database) RunCommand(cmd bson.D) (bson.D, error) {
	// 解析命令
	for _, elem := range cmd {
		switch elem.Key {
		case "ping":
			return bson.D{{Key: "ok", Value: int32(1)}}, nil

		case "isMaster", "ismaster":
			return db.isMasterCommand(), nil

		case "hello":
			return db.helloCommand(), nil

		case "buildInfo", "buildinfo":
			return db.buildInfoCommand(), nil

		case "listCollections":
			return db.listCollectionsCommand(), nil

		case "insert":
			return db.insertCommand(cmd)

		case "find":
			return db.findCommand(cmd)

		case "update":
			return db.updateCommand(cmd)

		case "delete":
			return db.deleteCommand(cmd)

		case "count":
			return db.countCommand(cmd)

		case "drop":
			return db.dropCommand(cmd)

		case "createIndexes":
			return db.createIndexesCommand(cmd)

		case "listIndexes":
			return db.listIndexesCommand(cmd)

		case "dropIndexes":
			return db.dropIndexesCommand(cmd)

		case "aggregate":
			return db.aggregateCommand(cmd)

		case "getMore":
			return db.getMoreCommand(cmd)

		case "killCursors":
			return db.killCursorsCommand(cmd)

		case "findAndModify":
			return db.findAndModifyCommand(cmd)

		case "distinct":
			return db.distinctCommand(cmd)

		case "dbStats":
			return db.dbStatsCommand(cmd)

		case "collStats":
			return db.collStatsCommand(cmd)

		case "serverStatus":
			return db.serverStatusCommand(), nil

		case "validate":
			return db.validateCommand(cmd), nil

		case "explain":
			return db.explainCommand(cmd)

		case "connectionStatus":
			return db.connectionStatsCommand(), nil

		case "startTransaction":
			return db.startTransactionCommand(cmd)

		case "commitTransaction":
			return db.commitTransactionCommand(cmd)

		case "abortTransaction":
			return db.abortTransactionCommand(cmd)

		case "endSessions":
			return db.endSessionsCommand(cmd)

		case "refreshSessions":
			return db.refreshSessionsCommand(cmd)
		}
	}

	// 获取第一个命令名（用于错误消息）
	cmdName := ""
	if len(cmd) > 0 {
		cmdName = cmd[0].Key
	}
	return nil, ErrCommandNotFound(cmdName)
}

// isMasterCommand 返回服务器信息（MongoDB 兼容）
//
// 能力宣称策略：
// - maxWireVersion=13 (MongoDB 5.0)：保守宣称，不支持 6.0+ 新特性
// - 不宣称 compression（OP_COMPRESSED 未实现）
// - 宣称 logicalSessionTimeoutMinutes（支持会话）
// - 宣称 readOnly=false（支持写入）
func (db *Database) isMasterCommand() bson.D {
	return bson.D{
		{Key: "ismaster", Value: true},
		{Key: "maxBsonObjectSize", Value: int32(16 * 1024 * 1024)},
		{Key: "maxMessageSizeBytes", Value: int32(48 * 1024 * 1024)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "localTime", Value: primitive.NewDateTimeFromTime(time.Now())},
		// Wire Protocol 版本：13 = MongoDB 5.0，保守宣称避免触发不支持的特性
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "maxWireVersion", Value: int32(13)},
		// 会话支持：逻辑会话超时 30 分钟
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		// 不宣称 compression（未实现 OP_COMPRESSED）
		// {Key: "compression", Value: bson.A{}},
		// 单机模式，不宣称副本集相关字段
		{Key: "readOnly", Value: false},
		{Key: "ok", Value: int32(1)},
	}
}

// helloCommand 返回服务器信息（MongoDB 5.0+ 推荐命令）
//
// 与 isMaster 类似，但使用 isWritablePrimary 代替 ismaster
func (db *Database) helloCommand() bson.D {
	return bson.D{
		{Key: "isWritablePrimary", Value: true},
		// 为了兼容旧驱动，同时返回 ismaster
		{Key: "ismaster", Value: true},
		{Key: "maxBsonObjectSize", Value: int32(16 * 1024 * 1024)},
		{Key: "maxMessageSizeBytes", Value: int32(48 * 1024 * 1024)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "localTime", Value: primitive.NewDateTimeFromTime(time.Now())},
		// Wire Protocol 版本
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "maxWireVersion", Value: int32(13)},
		// 会话支持
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		// 不宣称 compression
		{Key: "readOnly", Value: false},
		// hello 命令特有字段
		{Key: "helloOk", Value: true},
		{Key: "ok", Value: int32(1)},
	}
}

func (db *Database) buildInfoCommand() bson.D {
	return bson.D{
		{Key: "version", Value: "0.1.0"},
		{Key: "gitVersion", Value: "monodb"},
		{Key: "modules", Value: bson.A{}},
		{Key: "ok", Value: int32(1)},
	}
}

func (db *Database) listCollectionsCommand() bson.D {
	arr := bson.A{}
	for _, name := range db.ListCollections() {
		arr = append(arr, bson.D{
			{Key: "name", Value: name},
			{Key: "type", Value: "collection"},
		})
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "firstBatch", Value: arr},
		}},
		{Key: "ok", Value: int32(1)},
	}
}

func (db *Database) insertCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var docsArr bson.A

	for _, elem := range cmd {
		switch elem.Key {
		case "insert":
			colName, _ = elem.Value.(string)
		case "documents":
			docsArr, _ = elem.Value.(bson.A)
		}
	}

	if colName == "" {
		return nil, ErrBadValue("insert command requires collection name")
	}

	// 提取会话和事务上下文
	var cmdCtx *CommandContext
	if db.sessionManager != nil {
		var err error
		cmdCtx, err = db.sessionManager.ExtractCommandContext(cmd)
		if err != nil {
			return nil, err
		}
	}

	col, err := db.Collection(colName)
	if err != nil {
		return nil, err
	}

	docs := make([]bson.D, 0, len(docsArr))
	for _, v := range docsArr {
		if doc, ok := v.(bson.D); ok {
			docs = append(docs, doc)
		}
	}

	// 如果在事务中，记录 undo 日志
	insertedIDs, err := col.Insert(docs...)
	if err != nil {
		return ErrorResponse(err), nil
	}

	// 在事务中记录操作（用于回滚）
	if cmdCtx != nil && cmdCtx.IsInTransaction() {
		for i, id := range insertedIDs {
			cmdCtx.RecordOperationInTxn("insert", colName, id, docs[i])
		}
	}

	return bson.D{
		{Key: "n", Value: int32(len(docs))},
		{Key: "ok", Value: int32(1)},
	}, nil
}

func (db *Database) findCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var dbName string
	var filter bson.D
	var opts QueryOptions
	var batchSize int64

	for _, elem := range cmd {
		switch elem.Key {
		case "find":
			colName, _ = elem.Value.(string)
		case "$db":
			dbName, _ = elem.Value.(string)
		case "filter":
			filter, _ = elem.Value.(bson.D)
		case "sort":
			opts.Sort, _ = elem.Value.(bson.D)
		case "limit":
			switch v := elem.Value.(type) {
			case int32:
				opts.Limit = int64(v)
			case int64:
				opts.Limit = v
			case int:
				opts.Limit = int64(v)
			}
		case "skip":
			switch v := elem.Value.(type) {
			case int32:
				opts.Skip = int64(v)
			case int64:
				opts.Skip = v
			case int:
				opts.Skip = int64(v)
			}
		case "projection":
			opts.Projection, _ = elem.Value.(bson.D)
		case "batchSize":
			switch v := elem.Value.(type) {
			case int32:
				batchSize = int64(v)
			case int64:
				batchSize = v
			case int:
				batchSize = int64(v)
			}
		}
	}

	if colName == "" {
		return nil, ErrBadValue("find command requires collection name")
	}

	// 使用 GetCollection 避免隐式创建集合（MongoDB 行为：find 不创建集合）
	col := db.GetCollection(colName)
	var docs []bson.D
	var err error

	// 如果集合不存在，返回空结果
	if col == nil {
		docs = []bson.D{}
	} else if opts.Sort != nil || opts.Limit > 0 || opts.Skip > 0 || opts.Projection != nil {
		docs, err = col.FindWithOptions(filter, &opts)
	} else {
		docs, err = col.Find(filter)
	}
	if err != nil {
		return ErrorResponse(err), nil
	}

	// 构建 namespace
	ns := colName
	if dbName != "" {
		ns = dbName + "." + colName
	}

	// 使用游标管理器处理分批返回
	firstBatch, cursorID := db.cursorManager.GetFirstBatch(ns, docs, batchSize)

	arr := bson.A{}
	for _, doc := range firstBatch {
		arr = append(arr, doc)
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: cursorID},
			{Key: "ns", Value: ns},
			{Key: "firstBatch", Value: arr},
		}},
		{Key: "ok", Value: int32(1)},
	}, nil
}

func (db *Database) updateCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var updates bson.A

	for _, elem := range cmd {
		switch elem.Key {
		case "update":
			colName, _ = elem.Value.(string)
		case "updates":
			updates, _ = elem.Value.(bson.A)
		}
	}

	if colName == "" {
		return nil, ErrBadValue("update command requires collection name")
	}

	// 提取会话和事务上下文
	var cmdCtx *CommandContext
	if db.sessionManager != nil {
		var err error
		cmdCtx, err = db.sessionManager.ExtractCommandContext(cmd)
		if err != nil {
			return nil, err
		}
	}

	// 检查是否有任何 upsert:true
	hasUpsert := false
	for _, updateVal := range updates {
		if updateDoc, ok := updateVal.(bson.D); ok {
			for _, elem := range updateDoc {
				if elem.Key == "upsert" {
					if upsertVal, ok := elem.Value.(bool); ok && upsertVal {
						hasUpsert = true
						break
					}
				}
			}
		}
		if hasUpsert {
			break
		}
	}

	// 根据是否有 upsert 决定获取集合的方式
	var col *Collection
	var err error
	if hasUpsert {
		// 有 upsert 时可能需要创建集合
		col, err = db.Collection(colName)
	} else {
		// 无 upsert 时不应创建集合
		col = db.GetCollection(colName)
		if col == nil {
			// 集合不存在，返回空结果（非 upsert 更新不存在的集合 = 0 条匹配）
			return bson.D{
				{Key: "n", Value: int64(0)},
				{Key: "nModified", Value: int64(0)},
				{Key: "ok", Value: int32(1)},
			}, nil
		}
	}
	if err != nil {
		return nil, AsMongoError(err)
	}

	var totalMatched, totalModified int64
	var upsertedDocs bson.A

	for idx, updateVal := range updates {
		updateDoc, ok := updateVal.(bson.D)
		if !ok {
			continue
		}

		var filter, update bson.D
		var upsert bool

		for _, elem := range updateDoc {
			switch elem.Key {
			case "q":
				filter, _ = elem.Value.(bson.D)
			case "u":
				update, _ = elem.Value.(bson.D)
			case "upsert":
				upsert, _ = elem.Value.(bool)
			}
		}

		// 如果在事务中，先获取原始文档用于 undo
		var oldDocs []bson.D
		if cmdCtx != nil && cmdCtx.IsInTransaction() {
			oldDocs, _ = col.Find(filter)
		}

		result, err := col.Update(filter, update, upsert)
		if err != nil {
			return ErrorResponse(err), nil
		}

		// 在事务中记录 undo（用于回滚）
		if cmdCtx != nil && cmdCtx.IsInTransaction() {
			// 记录被更新的原始文档
			for _, oldDoc := range oldDocs {
				docID := getDocField(oldDoc, "_id")
				if docID != nil {
					cmdCtx.RecordOperationInTxn("update", colName, docID, oldDoc)
				}
			}
			// 如果是 upsert 产生的新文档，记录为 insert（回滚时删除）
			if result.UpsertedCount > 0 && result.UpsertedID != nil {
				cmdCtx.RecordOperationInTxn("insert", colName, result.UpsertedID, nil)
			}
		}

		totalMatched += result.MatchedCount
		totalModified += result.ModifiedCount

		// 收集 upserted 信息
		if result.UpsertedCount > 0 && result.UpsertedID != nil {
			upsertedDocs = append(upsertedDocs, bson.D{
				{Key: "index", Value: int32(idx)},
				{Key: "_id", Value: result.UpsertedID},
			})
			// upsert 时 n 需要计入
			totalMatched += result.UpsertedCount
		}
	}

	response := bson.D{
		{Key: "n", Value: int32(totalMatched)},
		{Key: "nModified", Value: int32(totalModified)},
	}

	// 添加 upserted 数组（如果有）
	if len(upsertedDocs) > 0 {
		response = append(response, bson.E{Key: "upserted", Value: upsertedDocs})
	}

	response = append(response, bson.E{Key: "ok", Value: int32(1)})

	return response, nil
}

func (db *Database) deleteCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var deletes bson.A

	for _, elem := range cmd {
		switch elem.Key {
		case "delete":
			colName, _ = elem.Value.(string)
		case "deletes":
			deletes, _ = elem.Value.(bson.A)
		}
	}

	if colName == "" {
		return nil, ErrBadValue("delete command requires collection name")
	}

	// 提取会话和事务上下文
	var cmdCtx *CommandContext
	if db.sessionManager != nil {
		var err error
		cmdCtx, err = db.sessionManager.ExtractCommandContext(cmd)
		if err != nil {
			return nil, err
		}
	}

	// delete 不应该隐式创建集合
	col := db.GetCollection(colName)
	if col == nil {
		// 集合不存在，返回 0 条删除
		return bson.D{
			{Key: "n", Value: int64(0)},
			{Key: "ok", Value: int32(1)},
		}, nil
	}

	var totalDeleted int64
	for _, deleteVal := range deletes {
		deleteDoc, ok := deleteVal.(bson.D)
		if !ok {
			continue
		}

		var filter bson.D
		var limit int64 // 1=deleteOne, 0=deleteMany (MongoDB delete command semantics)
		for _, elem := range deleteDoc {
			if elem.Key == "q" {
				filter, _ = elem.Value.(bson.D)
			}
			if elem.Key == "limit" {
				switch v := elem.Value.(type) {
				case int:
					limit = int64(v)
				case int32:
					limit = int64(v)
				case int64:
					limit = v
				case float64:
					limit = int64(v)
				default:
					// ignore unsupported types; default 0
				}
			}
		}

		// 如果在事务中，先获取要删除的文档用于 undo
		var docsToDelete []bson.D
		if cmdCtx != nil && cmdCtx.IsInTransaction() {
			if limit == 1 {
				if d, _ := col.FindOne(filter); d != nil {
					docsToDelete = []bson.D{d}
				}
			} else {
				docsToDelete, _ = col.Find(filter)
			}
		}

		var deleted int64
		var err error
		if limit == 1 {
			deleted, err = col.DeleteOne(filter)
		} else {
			deleted, err = col.Delete(filter)
		}
		if err != nil {
			return ErrorResponse(err), nil
		}

		// 在事务中记录 undo（用于回滚）
		if cmdCtx != nil && cmdCtx.IsInTransaction() {
			for _, doc := range docsToDelete {
				docID := getDocField(doc, "_id")
				if docID != nil {
					cmdCtx.RecordOperationInTxn("delete", colName, docID, doc)
				}
			}
		}

		totalDeleted += deleted
	}

	return bson.D{
		{Key: "n", Value: int32(totalDeleted)},
		{Key: "ok", Value: int32(1)},
	}, nil
}

func (db *Database) countCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var query bson.D

	for _, elem := range cmd {
		switch elem.Key {
		case "count":
			colName, _ = elem.Value.(string)
		case "query":
			query, _ = elem.Value.(bson.D)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("count command requires collection name")
	}

	// count 不应该隐式创建集合
	col := db.GetCollection(colName)
	if col == nil {
		// 集合不存在，返回 0
		return bson.D{
			{Key: "n", Value: int64(0)},
			{Key: "ok", Value: int32(1)},
		}, nil
	}

	if len(query) == 0 {
		// 快速路径：返回总数
		return bson.D{
			{Key: "n", Value: col.Count()},
			{Key: "ok", Value: int32(1)},
		}, nil
	}

	// 带过滤器的计数
	docs, err := col.Find(query)
	if err != nil {
		return ErrorResponse(err), nil
	}

	return bson.D{
		{Key: "n", Value: int64(len(docs))},
		{Key: "ok", Value: int32(1)},
	}, nil
}

func (db *Database) dropCommand(cmd bson.D) (bson.D, error) {
	var colName string

	for _, elem := range cmd {
		if elem.Key == "drop" {
			colName, _ = elem.Value.(string)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("drop command requires collection name")
	}

	if err := db.DropCollection(colName); err != nil {
		return ErrorResponse(err), nil
	}

	return bson.D{{Key: "ok", Value: int32(1)}}, nil
}

// Pager 返回底层的 Pager（供 Wire Protocol 使用）
func (db *Database) Pager() *storage.Pager {
	return db.pager
}

// InsertRaw 直接插入原始 BSON 数据（供 Wire Protocol 使用）
// 返回插入文档的 _id 列表（支持任意类型）
func (c *Collection) InsertRaw(docs [][]byte) ([]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]interface{}, 0, len(docs))

	for _, data := range docs {
		var doc bson.D
		if err := bson.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("failed to unmarshal document: %w", err)
		}

		id, err := c.ensureId(&doc)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)

		// 重新序列化（因为可能添加了 _id）
		newData, err := bson.Marshal(doc)
		if err != nil {
			return nil, err
		}

		if err := c.writeDocument(id, newData); err != nil {
			return nil, err
		}

		c.info.DocumentCount++
	}

	if err := c.db.saveCatalog(); err != nil {
		return nil, err
	}

	return ids, nil
}

// createIndexesCommand 创建索引命令
func (db *Database) createIndexesCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var indexes bson.A

	for _, elem := range cmd {
		switch elem.Key {
		case "createIndexes":
			colName, _ = elem.Value.(string)
		case "indexes":
			indexes, _ = elem.Value.(bson.A)
		}
	}

	if colName == "" {
		return nil, ErrBadValue("createIndexes command requires collection name")
	}

	col, err := db.Collection(colName)
	if err != nil {
		return nil, AsMongoError(err)
	}

	createdNames := make([]string, 0)

	for _, idxVal := range indexes {
		idxDoc, ok := idxVal.(bson.D)
		if !ok {
			continue
		}

		var keys bson.D
		var options bson.D

		for _, elem := range idxDoc {
			switch elem.Key {
			case "key":
				keys, _ = elem.Value.(bson.D)
			case "name":
				options = append(options, bson.E{Key: "name", Value: elem.Value})
			case "unique":
				options = append(options, bson.E{Key: "unique", Value: elem.Value})
			case "background":
				options = append(options, bson.E{Key: "background", Value: elem.Value})
			}
		}

		if len(keys) == 0 {
			continue
		}

		// 验证索引键
		if err := ValidateIndexKeys(keys); err != nil {
			return AsMongoError(err).ToBSON(), nil
		}

		name, err := col.CreateIndex(keys, options)
		if err != nil {
			return ErrorResponse(err), nil
		}

		createdNames = append(createdNames, name)
	}

	return bson.D{
		{Key: "createdCollectionAutomatically", Value: false},
		{Key: "numIndexesBefore", Value: int32(1)},
		{Key: "numIndexesAfter", Value: int32(1 + len(createdNames))},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// listIndexesCommand 列出索引命令
func (db *Database) listIndexesCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var dbName string

	for _, elem := range cmd {
		switch elem.Key {
		case "listIndexes":
			colName, _ = elem.Value.(string)
		case "$db":
			dbName, _ = elem.Value.(string)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("listIndexes command requires collection name")
	}

	col := db.GetCollection(colName)
	if col == nil {
		return bson.D{
			{Key: "ok", Value: int32(0)},
			{Key: "errmsg", Value: "ns does not exist"},
			{Key: "code", Value: int32(26)},
		}, nil
	}

	indexes := col.ListIndexes()

	// 构建 namespace
	ns := colName
	if dbName != "" {
		ns = dbName + "." + colName
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns},
			{Key: "firstBatch", Value: indexes},
		}},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// dropIndexesCommand 删除索引命令
func (db *Database) dropIndexesCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var indexName interface{}

	for _, elem := range cmd {
		switch elem.Key {
		case "dropIndexes":
			colName, _ = elem.Value.(string)
		case "index":
			indexName = elem.Value
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("dropIndexes command requires collection name")
	}

	col := db.GetCollection(colName)
	if col == nil {
		return bson.D{
			{Key: "ok", Value: int32(0)},
			{Key: "errmsg", Value: "ns does not exist"},
		}, nil
	}

	// 处理不同类型的 index 参数
	switch idx := indexName.(type) {
	case string:
		if idx == "*" {
			// 删除所有索引（除了 _id）
			indexes := col.ListIndexes()
			for _, idxInfo := range indexes {
				if idxDoc, ok := idxInfo.(bson.D); ok {
					for _, e := range idxDoc {
						if e.Key == "name" {
							if name, ok := e.Value.(string); ok && name != "_id_" {
								col.DropIndex(name)
							}
						}
					}
				}
			}
		} else {
			if err := col.DropIndex(idx); err != nil {
				return ErrorResponse(err), nil
			}
		}
	}

	return bson.D{
		{Key: "nIndexesWas", Value: int32(1)},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// aggregateCommand 聚合命令
func (db *Database) aggregateCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var dbName string
	var pipeline bson.A

	for _, elem := range cmd {
		switch elem.Key {
		case "aggregate":
			colName, _ = elem.Value.(string)
		case "pipeline":
			pipeline, _ = elem.Value.(bson.A)
		case "$db":
			dbName, _ = elem.Value.(string)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("aggregate command requires collection name")
	}

	// 使用 GetCollection 避免隐式创建集合（MongoDB 行为：aggregate 不创建集合）
	col := db.GetCollection(colName)

	// 转换 pipeline
	stages := make([]bson.D, 0, len(pipeline))
	for _, stage := range pipeline {
		if stageDoc, ok := stage.(bson.D); ok {
			stages = append(stages, stageDoc)
		}
	}

	var docs []bson.D
	var err error

	// 如果集合不存在，返回空结果
	if col == nil {
		docs = []bson.D{}
	} else {
		docs, err = col.Aggregate(stages)
	}
	if err != nil {
		return ErrorResponse(err), nil
	}

	// 构建结果
	arr := bson.A{}
	for _, doc := range docs {
		arr = append(arr, doc)
	}

	// 构建 namespace
	ns := colName
	if dbName != "" {
		ns = dbName + "." + colName
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns},
			{Key: "firstBatch", Value: arr},
		}},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// getMoreCommand 获取更多文档
//
// MongoDB 兼容行为：
// - 必须提供 cursorId（非零）
// - 可选提供 batchSize 覆盖默认值
// - 如果 cursor 不存在，返回 CursorNotFound 错误（code: 43）
// - 返回 cursorId=0 表示已返回所有数据
func (db *Database) getMoreCommand(cmd bson.D) (bson.D, error) {
	var cursorID int64
	var colName string
	var batchSize int64

	for _, elem := range cmd {
		switch elem.Key {
		case "getMore":
			cursorID, _ = elem.Value.(int64)
		case "collection":
			colName, _ = elem.Value.(string)
		case "batchSize":
			switch v := elem.Value.(type) {
			case int32:
				batchSize = int64(v)
			case int64:
				batchSize = v
			case int:
				batchSize = int64(v)
			}
		}
	}

	if cursorID == 0 {
		return ErrBadValue("cursor id is required").ToBSON(), nil
	}

	// 原子操作：获取下一批文档（同时检查存在性，避免竞态）
	// 不再单独调用 GetCursor，直接在 GetMore 中检查
	docs, hasMore, err := db.cursorManager.GetMore(cursorID, batchSize)
	if err != nil {
		return AsMongoError(err).ToBSON(), nil
	}

	// 构建结果
	arr := bson.A{}
	for _, doc := range docs {
		arr = append(arr, doc)
	}

	// 如果没有更多文档，返回 cursorID 为 0
	returnCursorID := int64(0)
	if hasMore {
		returnCursorID = cursorID
	}

	// 使用命令中提供的 collection 作为命名空间
	// 如果未提供，使用数据库名
	ns := colName
	if ns == "" {
		ns = db.name
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: returnCursorID},
			{Key: "ns", Value: ns},
			{Key: "nextBatch", Value: arr},
		}},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// killCursorsCommand 关闭游标
//
// MongoDB 兼容行为：
// - 接收 cursor ID 数组
// - 返回 cursorsKilled/cursorsNotFound/cursorsAlive/cursorsUnknown 数组
// - 幂等操作：关闭不存在的 cursor 不报错，仅列入 cursorsNotFound
func (db *Database) killCursorsCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var cursorIDs []int64

	for _, elem := range cmd {
		switch elem.Key {
		case "killCursors":
			colName, _ = elem.Value.(string)
		case "cursors":
			if arr, ok := elem.Value.(bson.A); ok {
				for _, v := range arr {
					if id, ok := v.(int64); ok {
						cursorIDs = append(cursorIDs, id)
					}
				}
			}
		}
	}

	killed := db.cursorManager.KillCursors(cursorIDs)

	// 构建响应
	cursorsKilled := bson.A{}
	cursorsNotFound := bson.A{}
	cursorsAlive := bson.A{}
	cursorsUnknown := bson.A{}

	killedSet := make(map[int64]bool)
	for _, id := range killed {
		killedSet[id] = true
		cursorsKilled = append(cursorsKilled, id)
	}

	for _, id := range cursorIDs {
		if !killedSet[id] {
			cursorsNotFound = append(cursorsNotFound, id)
		}
	}

	_ = colName // 暂未使用

	return bson.D{
		{Key: "cursorsKilled", Value: cursorsKilled},
		{Key: "cursorsNotFound", Value: cursorsNotFound},
		{Key: "cursorsAlive", Value: cursorsAlive},
		{Key: "cursorsUnknown", Value: cursorsUnknown},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// CursorManager 返回游标管理器
func (db *Database) CursorManager() *CursorManager {
	return db.cursorManager
}

// findAndModifyCommand 查找并修改命令
func (db *Database) findAndModifyCommand(cmd bson.D) (bson.D, error) {
	var colName string
	opts := &FindAndModifyOptions{}

	for _, elem := range cmd {
		switch elem.Key {
		case "findAndModify", "findandmodify":
			colName, _ = elem.Value.(string)
		case "query":
			opts.Query, _ = elem.Value.(bson.D)
		case "update":
			opts.Update, _ = elem.Value.(bson.D)
		case "remove":
			opts.Remove, _ = elem.Value.(bool)
		case "new":
			opts.New, _ = elem.Value.(bool)
		case "upsert":
			opts.Upsert, _ = elem.Value.(bool)
		case "sort":
			opts.Sort, _ = elem.Value.(bson.D)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("findAndModify command requires collection name")
	}

	// 根据是否有 upsert 决定获取集合的方式
	var col *Collection
	var err error
	if opts.Upsert {
		// 有 upsert 时可能需要创建集合
		col, err = db.Collection(colName)
		if err != nil {
			return nil, err
		}
	} else {
		// 无 upsert 时不应创建集合
		col = db.GetCollection(colName)
		if col == nil {
			// 集合不存在，返回空结果
			return bson.D{
				{Key: "lastErrorObject", Value: bson.D{
					{Key: "n", Value: int32(0)},
					{Key: "updatedExisting", Value: false},
				}},
				{Key: "value", Value: nil},
				{Key: "ok", Value: int32(1)},
			}, nil
		}
	}

	doc, err := col.FindAndModify(opts)
	if err != nil {
		return ErrorResponse(err), nil
	}

	result := bson.D{
		{Key: "lastErrorObject", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "updatedExisting", Value: doc != nil && !opts.Remove},
		}},
		{Key: "ok", Value: int32(1)},
	}

	if doc != nil {
		result = append(bson.D{{Key: "value", Value: doc}}, result...)
	} else {
		result = append(bson.D{{Key: "value", Value: nil}}, result...)
	}

	return result, nil
}

// distinctCommand distinct 命令
func (db *Database) distinctCommand(cmd bson.D) (bson.D, error) {
	var colName string
	var key string
	var query bson.D

	for _, elem := range cmd {
		switch elem.Key {
		case "distinct":
			colName, _ = elem.Value.(string)
		case "key":
			key, _ = elem.Value.(string)
		case "query":
			query, _ = elem.Value.(bson.D)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("distinct command requires collection name")
	}

	if key == "" {
		return nil, fmt.Errorf("distinct command requires key field")
	}

	col := db.GetCollection(colName)
	if col == nil {
		return bson.D{
			{Key: "values", Value: bson.A{}},
			{Key: "ok", Value: int32(1)},
		}, nil
	}

	values, err := col.Distinct(key, query)
	if err != nil {
		return ErrorResponse(err), nil
	}

	arr := bson.A{}
	for _, v := range values {
		arr = append(arr, v)
	}

	return bson.D{
		{Key: "values", Value: arr},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// dbStatsCommand 数据库统计命令
func (db *Database) dbStatsCommand(cmd bson.D) (bson.D, error) {
	stats := db.Stats()

	return bson.D{
		{Key: "db", Value: db.name},
		{Key: "collections", Value: int32(stats.CollectionCount)},
		{Key: "views", Value: int32(0)},
		{Key: "objects", Value: stats.DocumentCount},
		{Key: "dataSize", Value: int64(stats.PageCount) * 4096},
		{Key: "storageSize", Value: int64(stats.PageCount) * 4096},
		{Key: "indexes", Value: int32(0)},
		{Key: "indexSize", Value: int64(0)},
		{Key: "totalSize", Value: int64(stats.PageCount) * 4096},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// collStatsCommand 集合统计命令
func (db *Database) collStatsCommand(cmd bson.D) (bson.D, error) {
	var colName string

	for _, elem := range cmd {
		switch elem.Key {
		case "collStats", "collstats":
			colName, _ = elem.Value.(string)
		}
	}

	if colName == "" {
		return nil, fmt.Errorf("collStats command requires collection name")
	}

	col := db.GetCollection(colName)
	if col == nil {
		return bson.D{
			{Key: "ok", Value: int32(0)},
			{Key: "errmsg", Value: "ns not found"},
		}, nil
	}

	return bson.D{
		{Key: "ns", Value: colName},
		{Key: "count", Value: col.Count()},
		{Key: "size", Value: int64(0)},
		{Key: "avgObjSize", Value: int64(0)},
		{Key: "storageSize", Value: int64(0)},
		{Key: "nindexes", Value: int32(len(col.ListIndexes()))},
		{Key: "totalIndexSize", Value: int64(0)},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// serverStatusCommand 返回服务器状态信息
func (db *Database) serverStatusCommand() bson.D {
	// 收集统计信息
	var totalDocs int64
	var totalIndexes int
	collections := db.ListCollections()
	
	for _, name := range collections {
		if col := db.GetCollection(name); col != nil {
			totalDocs += col.Count()
			totalIndexes += len(col.ListIndexes())
		}
	}
	
	// 获取存储信息
	var pageCount uint32
	var freePageCount int
	if db.pager != nil {
		pageCount = db.pager.GetPageCount()
		freePageCount = db.pager.GetFreePageCount()
	}
	
	return bson.D{
		{Key: "host", Value: "localhost"},
		{Key: "version", Value: "0.1.0"},
		{Key: "process", Value: "monodb"},
		{Key: "pid", Value: int64(os.Getpid())},
		{Key: "uptime", Value: int64(time.Since(db.startTime).Seconds())},
		{Key: "uptimeMillis", Value: time.Since(db.startTime).Milliseconds()},
		{Key: "collections", Value: int32(len(collections))},
		{Key: "documents", Value: totalDocs},
		{Key: "indexes", Value: int32(totalIndexes)},
		{Key: "storageEngine", Value: bson.D{
			{Key: "name", Value: "monodb"},
			{Key: "pageSize", Value: int32(storage.PageSize)},
			{Key: "pageCount", Value: int32(pageCount)},
			{Key: "freePages", Value: int32(freePageCount)},
		}},
		{Key: "cursors", Value: bson.D{
			{Key: "totalOpen", Value: int32(db.cursorManager.Count())},
		}},
		{Key: "mem", Value: db.getMemoryStats()},
		{Key: "ok", Value: int32(1)},
	}
}

// getMemoryStats 获取内存统计信息
func (db *Database) getMemoryStats() bson.D {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return bson.D{
		{Key: "resident", Value: int64(m.Sys / 1024 / 1024)},       // MB
		{Key: "virtual", Value: int64(m.Sys / 1024 / 1024)},        // MB
		{Key: "heap", Value: int64(m.HeapAlloc / 1024 / 1024)},     // MB
		{Key: "heapInUse", Value: int64(m.HeapInuse / 1024 / 1024)}, // MB
		{Key: "stackInUse", Value: int64(m.StackInuse / 1024 / 1024)}, // MB
		{Key: "gcPauses", Value: int64(m.NumGC)},
	}
}

// connectionStatsCommand 返回连接统计
func (db *Database) connectionStatsCommand() bson.D {
	return bson.D{
		{Key: "current", Value: int32(1)},
		{Key: "available", Value: int32(1000)},
		{Key: "totalCreated", Value: int64(1)},
		{Key: "ok", Value: int32(1)},
	}
}

// startTransactionCommand 开始事务
//
// DEPRECATED: MongoDB 标准中，事务通过 CRUD 命令中的字段来启动，而非独立命令：
//   - startTransaction: true（标记事务的第一个命令）
//   - lsid: {id: UUID}（会话标识）
//   - txnNumber: int（事务序号）
//   - autocommit: false（多文档事务必须为 false）
//
// 此命令保留仅用于内部测试和旧版兼容。官方驱动应使用标准字段方式启动事务。
func (db *Database) startTransactionCommand(cmd bson.D) (bson.D, error) {
	if db.sessionManager == nil {
		return nil, ErrInternalError("session manager not initialized")
	}
	
	// 尝试从命令中提取会话上下文
	ctx, err := db.sessionManager.ExtractCommandContext(cmd)
	if err != nil {
		return nil, err
	}
	
	// 如果有 lsid，使用标准的会话事务
	if ctx.Session != nil {
		// 事务已在 ExtractCommandContext 中启动（如果有 startTransaction: true）
		return bson.D{
			{Key: "ok", Value: int32(1)},
		}, nil
	}
	
	// DEPRECATED: 旧格式兼容路径（无 lsid，使用底层事务管理器）
	// 此路径仅用于内部测试，不应对外暴露
	if db.txnManager == nil {
		return nil, ErrInternalError("transaction manager not initialized")
	}
	
	txn := db.txnManager.Begin()
	
	return bson.D{
		{Key: "txnId", Value: int64(txn.ID)},
		{Key: "ok", Value: int32(1)},
	}, nil
}

// commitTransactionCommand 提交事务
//
// MongoDB 标准格式：
//   - lsid: {id: UUID}
//   - txnNumber: int
//
// DEPRECATED: txnId 格式仅用于内部测试和旧版兼容，不应对外暴露。
func (db *Database) commitTransactionCommand(cmd bson.D) (bson.D, error) {
	// 提取 lsid
	var lsid bson.D
	var txnID int64     // DEPRECATED: 旧格式
	var txnNumber int64 // MongoDB 标准格式
	
	for _, elem := range cmd {
		switch elem.Key {
		case "lsid":
			if l, ok := elem.Value.(bson.D); ok {
				lsid = l
			}
		case "txnNumber":
			switch v := elem.Value.(type) {
			case int32:
				txnNumber = int64(v)
			case int64:
				txnNumber = v
			case int:
				txnNumber = int64(v)
			}
		case "txnId": // DEPRECATED
			switch v := elem.Value.(type) {
			case int32:
				txnID = int64(v)
			case int64:
				txnID = v
			case int:
				txnID = int64(v)
			}
		}
	}
	
	// 优先使用 lsid（MongoDB 标准格式）
	if lsid != nil && db.sessionManager != nil {
		if txnNumber == 0 {
			return nil, ErrBadValue("txnNumber is required for commitTransaction with lsid")
		}
		session, err := db.sessionManager.GetOrCreateSession(lsid)
		if err != nil {
			return nil, err
		}
		
		if err := db.sessionManager.CommitTransaction(session, txnNumber); err != nil {
			return AsMongoError(err).ToBSON(), nil
		}
		
		return bson.D{
			{Key: "ok", Value: int32(1)},
		}, nil
	}
	
	// DEPRECATED: 旧格式兼容路径（txnId）
	if db.txnManager == nil {
		return nil, ErrInternalError("transaction manager not initialized")
	}
	
	if txnID == 0 {
		return nil, ErrBadValue("lsid or txnId is required")
	}
	
	txn := db.txnManager.GetTransaction(TxnID(txnID))
	if txn == nil {
		return nil, NewMongoError(ErrorCodeNoSuchTransaction, "transaction not found")
	}
	
	if err := db.txnManager.Commit(txn); err != nil {
		return AsMongoError(err).ToBSON(), nil
	}
	
	return bson.D{
		{Key: "ok", Value: int32(1)},
	}, nil
}

// abortTransactionCommand 中止事务
//
// MongoDB 标准格式：
//   - lsid: {id: UUID}
//   - txnNumber: int
//
// DEPRECATED: txnId 格式仅用于内部测试和旧版兼容，不应对外暴露。
func (db *Database) abortTransactionCommand(cmd bson.D) (bson.D, error) {
	// 提取 lsid
	var lsid bson.D
	var txnID int64     // DEPRECATED: 旧格式
	var txnNumber int64 // MongoDB 标准格式
	
	for _, elem := range cmd {
		switch elem.Key {
		case "lsid":
			if l, ok := elem.Value.(bson.D); ok {
				lsid = l
			}
		case "txnNumber":
			switch v := elem.Value.(type) {
			case int32:
				txnNumber = int64(v)
			case int64:
				txnNumber = v
			case int:
				txnNumber = int64(v)
			}
		case "txnId": // DEPRECATED
			switch v := elem.Value.(type) {
			case int32:
				txnID = int64(v)
			case int64:
				txnID = v
			case int:
				txnID = int64(v)
			}
		}
	}
	
	// 优先使用 lsid（MongoDB 标准格式）
	if lsid != nil && db.sessionManager != nil {
		if txnNumber == 0 {
			return nil, ErrBadValue("txnNumber is required for abortTransaction with lsid")
		}
		session, err := db.sessionManager.GetOrCreateSession(lsid)
		if err != nil {
			return nil, err
		}
		
		if err := db.sessionManager.AbortTransaction(session, txnNumber); err != nil {
			return AsMongoError(err).ToBSON(), nil
		}
		
		return bson.D{
			{Key: "ok", Value: int32(1)},
		}, nil
	}
	
	// 兼容旧格式：使用 txnId
	if db.txnManager == nil {
		return nil, ErrInternalError("transaction manager not initialized")
	}
	
	if txnID == 0 {
		return nil, ErrBadValue("lsid or txnId is required")
	}
	
	txn := db.txnManager.GetTransaction(TxnID(txnID))
	if txn == nil {
		return nil, NewMongoError(ErrorCodeNoSuchTransaction, "transaction not found")
	}
	
	if err := db.txnManager.Abort(txn); err != nil {
		return AsMongoError(err).ToBSON(), nil
	}
	
	return bson.D{
		{Key: "ok", Value: int32(1)},
	}, nil
}

// GetTransactionManager 获取事务管理器
func (db *Database) GetTransactionManager() *TransactionManager {
	return db.txnManager
}

// GetSessionManager 获取会话管理器
func (db *Database) GetSessionManager() *SessionManager {
	return db.sessionManager
}

// endSessionsCommand 结束会话
// 命令格式: { endSessions: [ { id: UUID }, ... ] }
func (db *Database) endSessionsCommand(cmd bson.D) (bson.D, error) {
	if db.sessionManager == nil {
		return nil, ErrInternalError("session manager not initialized")
	}
	
	var sessions bson.A
	for _, elem := range cmd {
		if elem.Key == "endSessions" {
			if s, ok := elem.Value.(bson.A); ok {
				sessions = s
			}
		}
	}
	
	if sessions == nil {
		return nil, ErrBadValue("endSessions requires an array of session ids")
	}
	
	for _, s := range sessions {
		if lsid, ok := s.(bson.D); ok {
			_ = db.sessionManager.EndSession(lsid)
		}
	}
	
	return bson.D{
		{Key: "ok", Value: int32(1)},
	}, nil
}

// refreshSessionsCommand 刷新会话
// 命令格式: { refreshSessions: [ { id: UUID }, ... ] }
func (db *Database) refreshSessionsCommand(cmd bson.D) (bson.D, error) {
	if db.sessionManager == nil {
		return nil, ErrInternalError("session manager not initialized")
	}
	
	var sessions bson.A
	for _, elem := range cmd {
		if elem.Key == "refreshSessions" {
			if s, ok := elem.Value.(bson.A); ok {
				sessions = s
			}
		}
	}
	
	if sessions == nil {
		return nil, ErrBadValue("refreshSessions requires an array of session ids")
	}
	
	for _, s := range sessions {
		if lsid, ok := s.(bson.D); ok {
			_ = db.sessionManager.RefreshSession(lsid)
		}
	}
	
	return bson.D{
		{Key: "ok", Value: int32(1)},
	}, nil
}

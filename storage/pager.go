// Created by Yanjunhui

package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/monolite/monodb/internal/failpoint"
)

// 文件格式常量
const (
	MagicNumber    uint32 = 0x4D4F4E4F // "MONO" in little endian
	FormatVersion  uint16 = 1
	FileHeaderSize        = 64
)

// FileHeader 文件头结构（64 字节）
// 位于文件最开始，存储数据库元信息
type FileHeader struct {
	Magic         uint32 // 魔数 "MONO"
	Version       uint16 // 文件格式版本
	PageSize      uint16 // 页面大小
	PageCount     uint32 // 总页面数
	FreeListHead  PageId // 空闲页链表头
	MetaPageId    PageId // 元数据页 ID
	CatalogPageId PageId // 目录页 ID
	CreateTime    int64  // 创建时间（Unix 毫秒）
	ModifyTime    int64  // 最后修改时间
	Reserved      [24]byte
}

// Pager 页面管理器，负责页面的读写和缓存
type Pager struct {
	file           *os.File
	path           string
	header         *FileHeader
	pageCount      uint32
	freePages      []PageId
	cache          map[PageId]*Page
	dirty          map[PageId]bool
	pageLSN        map[PageId]LSN // 每个页面的最后写入 LSN
	mu             sync.RWMutex
	maxCached      int
	wal            *WAL  // Write-Ahead Log
	walEnabled     bool  // 是否启用 WAL
	lastFlushError error // 【P0】记录最后一次刷盘错误，用于错误状态追踪
}

// OpenPager 打开或创建一个数据库文件
func OpenPager(path string) (*Pager, error) {
	return OpenPagerWithWAL(path, true) // 默认启用 WAL
}

// OpenPagerWithWAL 打开或创建数据库文件，可选是否启用 WAL
func OpenPagerWithWAL(path string, enableWAL bool) (*Pager, error) {
	// 检查文件是否存在
	exists := true
	if _, err := os.Stat(path); os.IsNotExist(err) {
		exists = false
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	p := &Pager{
		file:       file,
		path:       path,
		cache:      make(map[PageId]*Page),
		dirty:      make(map[PageId]bool),
		pageLSN:    make(map[PageId]LSN),
		maxCached:  1000, // 最多缓存 1000 个页面
		walEnabled: enableWAL,
	}

	if exists {
		// 读取现有文件头
		if err := p.readHeader(); err != nil {
			file.Close()
			return nil, err
		}
		// 加载空闲页列表
		if err := p.loadFreeList(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		// 初始化新文件
		if err := p.initNewFile(); err != nil {
			file.Close()
			return nil, err
		}
	}

	// 初始化 WAL
	if enableWAL {
		walPath := WALPath(path)
		wal, err := NewWAL(walPath)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to open WAL: %w", err)
		}
		p.wal = wal

		// 执行崩溃恢复
		if exists {
			if err := p.recover(); err != nil {
				wal.Close()
				file.Close()
				return nil, fmt.Errorf("failed to recover: %w", err)
			}
		}
	}

	return p, nil
}

// recover 执行崩溃恢复（从 WAL 回放）
// 按照 WAL 先行原则，WAL 中的记录代表"已提交的意图"，需要完整重放
//
// LSN 语义约定：
// - checkpointLSN = "已安全落盘且可丢弃之前日志的最大 LSN"
// - 恢复时从 checkpointLSN + 1 开始，避免重复重放已持久化的记录
func (p *Pager) recover() error {
	if p.wal == nil {
		return nil
	}

	checkpointLSN := p.wal.GetCheckpointLSN()

	// 读取当前数据文件大小，用于判断某个 PageId 对应的物理页是否已存在。
	// 注意：恢复过程中可能会通过 ensureFileSize 扩展文件；这里的大小主要用于避免对“尚未落盘且不存在的页”做稀疏写入。
	fi, err := p.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat data file for recovery: %w", err)
	}
	actualSize := fi.Size()

	// 记录“分配页”的类型，用于处理崩溃点落在“已记录 PageCount，但页本身尚未落盘”的窗口。
	// 恢复时如果需要扩展文件，应尽量使用 WAL 中记录的 pageType 初始化缺失页，而不是默认写成 Data。
	allocPageTypes := make(map[PageId]uint8)

	// 从 checkpointLSN + 1 开始读取：checkpointLSN 及之前的记录已安全落盘
	// 这确保了恢复的幂等性：重复恢复不会重复应用已持久化的变更
	records, err := p.wal.ReadRecordsFrom(checkpointLSN + 1)
	if err != nil {
		return fmt.Errorf("failed to read WAL records: %w", err)
	}

	if len(records) == 0 {
		return nil
	}

	// Redo: 回放所有记录
	for _, record := range records {
		switch record.Type {
		case WALRecordPageWrite:
			// 重写页面
			if record.DataLen > 0 && len(record.Data) == PageSize {
				offset := p.pageOffset(record.PageId)
				if _, err := p.file.WriteAt(record.Data, offset); err != nil {
					return fmt.Errorf("failed to redo page write: %w", err)
				}
			}

		case WALRecordAllocPage:
			// 页面分配：确保 pageCount 正确（如果是从新页面分配而非 free list）
			if uint32(record.PageId) >= p.pageCount {
				p.pageCount = uint32(record.PageId) + 1
				p.header.PageCount = p.pageCount
			}
			// 记录 pageType（record.Data: [pageType]）
			pageType := PageTypeData
			if record.DataLen >= 1 && len(record.Data) >= 1 {
				pageType = record.Data[0]
				allocPageTypes[record.PageId] = pageType
			}

			// 【关键修复】确保“从 free list 复用的页”在崩溃恢复后不会仍然以 Free 页形式存在。
			//
			// AllocatePage 在从 free list 分配时，WAL 先行后只更新 header，不会立即把新页类型写入数据文件。
			// 若此时崩溃，恢复如果仅重放 header，会导致该页仍然是 PageTypeFree，但已经从 free list 脱链，破坏一致性。
			//
			// 做法：如果该 PageId 的物理页已经存在于文件中，则在恢复阶段用 WAL 记录的 pageType 写入一张“初始化页”。
			// - 若后续还有 WALRecordPageWrite，会覆盖该初始化页，保持 redo 顺序正确。
			// - 若物理页尚不存在（例如 PageCount 增长但页未落盘/半页），交给 ensureFileSize 使用 allocPageTypes 补齐。
			offset := p.pageOffset(record.PageId)
			if offset+int64(PageSize) <= actualSize {
				initPage := NewPage(record.PageId, pageType)
				if _, err := p.file.WriteAt(initPage.Marshal(), offset); err != nil {
					return fmt.Errorf("failed to redo alloc page init (pageId=%d): %w", record.PageId, err)
				}
			}

		case WALRecordFreePage:
			// 页面释放：会在 MetaUpdate 中处理 FreeListHead
			// 这里不需要特殊处理，因为重新加载 free list 会从 header 读取

		case WALRecordMetaUpdate:
			// 元数据更新：重放 header 变更
			if record.DataLen >= 9 {
				metaType := record.Data[0]
				// oldValue := binary.LittleEndian.Uint32(record.Data[1:5]) // 可用于验证
				newValue := binary.LittleEndian.Uint32(record.Data[5:9])

				switch metaType {
				case MetaUpdateFreeListHead:
					p.header.FreeListHead = PageId(newValue)
				case MetaUpdatePageCount:
					p.header.PageCount = newValue
					p.pageCount = newValue
				case MetaUpdateCatalogPageId:
					p.header.CatalogPageId = PageId(newValue)
				}
			}

		case WALRecordCheckpoint:
			// 检查点记录，更新检查点 LSN
			if record.DataLen >= 8 {
				_ = LSN(binary.LittleEndian.Uint64(record.Data))
			}
		}
	}

	// 同步数据文件
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync after recovery: %w", err)
	}

	// 【关键】确保文件大小与 PageCount 一致
	// 如果 WAL 中记录了 PageCount 增长但页面未实际写入文件，
	// 这里扩展文件以避免后续读取时遇到 EOF
	if err := p.ensureFileSize(allocPageTypes); err != nil {
		return fmt.Errorf("failed to ensure file size: %w", err)
	}

	// 更新文件头（持久化恢复后的状态）
	if err := p.writeHeader(); err != nil {
		return fmt.Errorf("failed to update header after recovery: %w", err)
	}

	// 重新加载 free list（从恢复后的 header.FreeListHead 开始）
	p.freePages = nil
	if err := p.loadFreeList(); err != nil {
		return fmt.Errorf("failed to reload free list: %w", err)
	}

	return nil
}

// ensureFileSize 确保文件大小与 PageCount 一致
// 如果文件实际大小小于 PageCount 指示的大小，则扩展文件
func (p *Pager) ensureFileSize(allocPageTypes map[PageId]uint8) error {
	expectedSize := int64(FileHeaderSize) + int64(p.pageCount)*int64(PageSize)

	fi, err := p.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	actualSize := fi.Size()

	if actualSize < expectedSize {
		// 需要扩展/补齐文件：
		// - 如果尾部存在“半页”（崩溃导致短写），从该页起重写为完整初始化页
		// - 尽量使用 WAL 中记录的分配页类型初始化缺失页；未知则回退为 Data
		startOffset := actualSize
		if actualSize > int64(FileHeaderSize) {
			rel := actualSize - int64(FileHeaderSize)
			rem := rel % int64(PageSize)
			if rem != 0 {
				startOffset = actualSize - rem // 回到半页起始位置
			}
		} else {
			startOffset = int64(FileHeaderSize)
		}

		for offset := startOffset; offset < expectedSize; offset += int64(PageSize) {
			pageId := PageId((offset - int64(FileHeaderSize)) / int64(PageSize))
			pageType := PageTypeData
			if allocPageTypes != nil {
				if t, ok := allocPageTypes[pageId]; ok {
					pageType = t
				}
			}
			page := NewPage(pageId, pageType)
			data := page.Marshal()
			if _, err := p.file.WriteAt(data, offset); err != nil {
				return fmt.Errorf("failed to extend file at offset %d: %w", offset, err)
			}
		}
	}

	return nil
}

// initNewFile 初始化新数据库文件
func (p *Pager) initNewFile() error {
	now := currentTimeMillis()
	p.header = &FileHeader{
		Magic:         MagicNumber,
		Version:       FormatVersion,
		PageSize:      PageSize,
		PageCount:     1, // 至少有一个元数据页
		FreeListHead:  0,
		MetaPageId:    0,
		CatalogPageId: 0,
		CreateTime:    now,
		ModifyTime:    now,
	}

	// 写入文件头
	if err := p.writeHeader(); err != nil {
		return err
	}

	// 创建并写入初始元数据页
	metaPage := NewPage(0, PageTypeMeta)
	if err := p.writePage(metaPage); err != nil {
		return err
	}

	p.pageCount = 1
	return nil
}

// readHeader 读取文件头
func (p *Pager) readHeader() error {
	buf := make([]byte, FileHeaderSize)
	if _, err := p.file.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	header := &FileHeader{}
	header.Magic = binary.LittleEndian.Uint32(buf[0:4])
	if header.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: %x, expected %x (file may be corrupted or not a MonoDB file)", header.Magic, MagicNumber)
	}

	header.Version = binary.LittleEndian.Uint16(buf[4:6])
	// 版本校验：确保文件格式版本与当前实现兼容
	if header.Version != FormatVersion {
		return fmt.Errorf("incompatible format version: file has version %d, but this build supports version %d", header.Version, FormatVersion)
	}

	header.PageSize = binary.LittleEndian.Uint16(buf[6:8])
	// PageSize 校验：确保文件的页大小与当前实现一致
	if header.PageSize != PageSize {
		return fmt.Errorf("incompatible page size: file has %d bytes, but this build uses %d bytes", header.PageSize, PageSize)
	}

	header.PageCount = binary.LittleEndian.Uint32(buf[8:12])
	header.FreeListHead = PageId(binary.LittleEndian.Uint32(buf[12:16]))
	header.MetaPageId = PageId(binary.LittleEndian.Uint32(buf[16:20]))
	header.CatalogPageId = PageId(binary.LittleEndian.Uint32(buf[20:24]))
	header.CreateTime = int64(binary.LittleEndian.Uint64(buf[24:32]))
	header.ModifyTime = int64(binary.LittleEndian.Uint64(buf[32:40]))

	p.header = header
	p.pageCount = header.PageCount
	return nil
}

// writeHeader 写入文件头
func (p *Pager) writeHeader() error {
	buf := make([]byte, FileHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], p.header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], p.header.Version)
	binary.LittleEndian.PutUint16(buf[6:8], p.header.PageSize)
	binary.LittleEndian.PutUint32(buf[8:12], p.header.PageCount)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(p.header.FreeListHead))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(p.header.MetaPageId))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(p.header.CatalogPageId))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(p.header.CreateTime))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(p.header.ModifyTime))

	if _, err := p.file.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	return nil
}

// loadFreeList 加载空闲页列表
func (p *Pager) loadFreeList() error {
	p.freePages = make([]PageId, 0)

	if p.header.FreeListHead == 0 {
		return nil
	}

	// 遍历空闲页链表
	currentId := p.header.FreeListHead
	for currentId != 0 {
		page, err := p.ReadPage(currentId)
		if err != nil {
			return err
		}
		p.freePages = append(p.freePages, currentId)
		currentId = page.NextPageId()
	}

	return nil
}

// pageOffset 计算页面在文件中的偏移
func (p *Pager) pageOffset(id PageId) int64 {
	return int64(FileHeaderSize) + int64(id)*int64(PageSize)
}

// ReadPage 读取指定页面
func (p *Pager) ReadPage(id PageId) (*Page, error) {
	p.mu.RLock()
	if page, ok := p.cache[id]; ok {
		p.mu.RUnlock()
		return page, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if page, ok := p.cache[id]; ok {
		return page, nil
	}

	// 从文件读取
	offset := p.pageOffset(id)
	buf := make([]byte, PageSize)
	if _, err := p.file.ReadAt(buf, offset); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("page %d does not exist", id)
		}
		return nil, fmt.Errorf("failed to read page %d: %w", id, err)
	}

	page, err := UnmarshalPage(buf)
	if err != nil {
		return nil, err
	}

	// 加入缓存
	p.addToCache(page)

	return page, nil
}

// writePage 写入页面到文件（先写 WAL）
func (p *Pager) writePage(page *Page) error {
	// 【FAILPOINT】用于测试写失败场景
	if err := failpoint.Hit("pager.writePage"); err != nil {
		return fmt.Errorf("failpoint: pager.writePage: %w", err)
	}

	data := page.Marshal()

	// 先写 WAL（WAL 先行原则）
	if p.wal != nil && p.walEnabled {
		lsn, err := p.wal.WritePageRecord(page.ID(), data)
		if err != nil {
			return fmt.Errorf("failed to write WAL for page %d: %w", page.ID(), err)
		}
		p.pageLSN[page.ID()] = lsn
	}

	// 再写数据文件
	offset := p.pageOffset(page.ID())
	if _, err := p.file.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID(), err)
	}

	page.ClearDirty()
	return nil
}

// AllocatePage 分配一个新页面
// 遵循 WAL 先行原则：先写 WAL，再修改内存/磁盘状态
// 否则崩溃后可能出现"无日志的结构变更"，导致恢复时状态不一致
func (p *Pager) AllocatePage(pageType uint8) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pageId PageId
	var oldFreeListHead PageId
	var newFreeListHead PageId
	var oldPageCount uint32
	var newPageCount uint32
	fromFreeList := false

	// 优先从空闲列表分配
	if len(p.freePages) > 0 {
		fromFreeList = true
		// 从链表头部取出（LIFO）
		pageId = p.freePages[0]

		// 读取被分配的页面，获取其 NextPageId
		page, err := p.readPageUnlocked(pageId)
		if err != nil {
			return nil, fmt.Errorf("failed to read free page %d: %w", pageId, err)
		}

		oldFreeListHead = p.header.FreeListHead
		newFreeListHead = page.NextPageId()
	} else {
		// 分配新页面
		pageId = PageId(p.pageCount)
		oldPageCount = p.pageCount
		newPageCount = p.pageCount + 1
	}

	// 【WAL 先行】Step 1: 先写 WAL 记录（分配意图 + 元数据变更）
	if p.wal != nil && p.walEnabled {
		// 写入分配记录
		if _, err := p.wal.WriteAllocRecord(pageId, pageType); err != nil {
			return nil, fmt.Errorf("failed to write WAL alloc record: %w", err)
		}

		// 写入元数据变更记录
		if fromFreeList {
			if _, err := p.wal.WriteMetaRecord(MetaUpdateFreeListHead, uint32(oldFreeListHead), uint32(newFreeListHead)); err != nil {
				return nil, fmt.Errorf("failed to write WAL meta record for FreeListHead: %w", err)
			}
		} else {
			if _, err := p.wal.WriteMetaRecord(MetaUpdatePageCount, oldPageCount, newPageCount); err != nil {
				return nil, fmt.Errorf("failed to write WAL meta record for PageCount: %w", err)
			}
		}

		// WAL 刷盘（确保记录持久化）
		if err := p.wal.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	// 【WAL 先行】Step 2: WAL 已持久化，现在安全地更新内存和数据文件
	if fromFreeList {
		p.freePages = p.freePages[1:]
		p.header.FreeListHead = newFreeListHead
	} else {
		p.pageCount++
		p.header.PageCount = p.pageCount
	}
	p.header.ModifyTime = currentTimeMillis()

	// 创建新页面
	page := NewPage(pageId, pageType)

	// 【关键】分配新页时，立即将初始化的页面写入文件
	// 这确保 PageCount 与文件物理大小一致：
	// - 如果只更新 PageCount 但不写页面，崩溃后恢复时会出现
	//   "逻辑页存在但物理页 EOF" 的边界问题
	// - 通过立即写入初始化页头，确保文件大小与 PageCount 一致
	if !fromFreeList {
		data := page.Marshal()
		offset := p.pageOffset(pageId)
		if _, err := p.file.WriteAt(data, offset); err != nil {
			return nil, fmt.Errorf("failed to write initialized page %d: %w", pageId, err)
		}
	}

	// 持久化 header
	if err := p.writeHeader(); err != nil {
		return nil, fmt.Errorf("failed to update header after allocate: %w", err)
	}

	p.addToCache(page)
	p.dirty[pageId] = true

	return page, nil
}

// readPageUnlocked 无锁版本的 ReadPage（调用者需要持有锁）
func (p *Pager) readPageUnlocked(id PageId) (*Page, error) {
	// 先检查缓存
	if page, ok := p.cache[id]; ok {
		return page, nil
	}

	// 从文件读取
	offset := p.pageOffset(id)
	buf := make([]byte, PageSize)
	if _, err := p.file.ReadAt(buf, offset); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("page %d does not exist", id)
		}
		return nil, fmt.Errorf("failed to read page %d: %w", id, err)
	}

	page, err := UnmarshalPage(buf)
	if err != nil {
		return nil, err
	}

	// 加入缓存
	p.addToCache(page)

	return page, nil
}

// FreePage 释放一个页面
// 遵循 WAL 先行原则：先写 WAL，再修改内存/磁盘状态
func (p *Pager) FreePage(id PageId) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldFreeListHead := p.header.FreeListHead
	newFreeListHead := id

	// 【WAL 先行】Step 1: 先写 WAL 记录（释放意图 + 元数据变更）
	if p.wal != nil && p.walEnabled {
		// 写入释放记录
		if _, err := p.wal.WriteFreeRecord(id); err != nil {
			return fmt.Errorf("failed to write WAL free record: %w", err)
		}

		// 写入元数据变更记录
		if _, err := p.wal.WriteMetaRecord(MetaUpdateFreeListHead, uint32(oldFreeListHead), uint32(newFreeListHead)); err != nil {
			return fmt.Errorf("failed to write WAL meta record for FreeListHead: %w", err)
		}

		// WAL 刷盘（确保记录持久化）
		if err := p.wal.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	// 【WAL 先行】Step 2: WAL 已持久化，现在安全地更新内存和数据文件

	// 使用无锁版本读取，避免死锁
	page, err := p.readPageUnlocked(id)
	if err != nil {
		return err
	}

	// 标记为空闲页，设置下一个空闲页指针
	page.pageType = PageTypeFree
	page.nextPageId = oldFreeListHead
	page.dirty = true

	// 将页面写入磁盘（确保链表结构持久化）
	if err := p.writePage(page); err != nil {
		return fmt.Errorf("failed to write free page %d: %w", id, err)
	}

	// 更新空闲列表头
	p.header.FreeListHead = newFreeListHead
	p.header.ModifyTime = currentTimeMillis()

	// 持久化 header
	if err := p.writeHeader(); err != nil {
		return fmt.Errorf("failed to update header after free: %w", err)
	}

	// 插入到空闲列表头部（与 AllocatePage 保持一致：LIFO）
	p.freePages = append([]PageId{id}, p.freePages...)
	p.dirty[id] = true

	return nil
}

// MarkDirty 标记页面为脏
func (p *Pager) MarkDirty(id PageId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if page, ok := p.cache[id]; ok {
		page.MarkDirty()
		p.dirty[id] = true
	}
}

// Flush 将所有脏页写入磁盘
func (p *Pager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 先刷 WAL
	if p.wal != nil && p.walEnabled {
		if err := p.wal.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	for id := range p.dirty {
		if page, ok := p.cache[id]; ok && page.IsDirty() {
			if err := p.writePage(page); err != nil {
				return err
			}
		}
	}
	p.dirty = make(map[PageId]bool)

	// 更新文件头
	p.header.ModifyTime = currentTimeMillis()
	if err := p.writeHeader(); err != nil {
		return err
	}

	// 同步数据文件到磁盘
	if err := p.file.Sync(); err != nil {
		return err
	}

	// 创建检查点（所有脏页已刷盘，可以安全更新检查点）
	// LSN 语义：GetCurrentLSN() 返回"下一条要写的 LSN"
	// checkpointLSN 应为"已安全落盘的最大 LSN"，即 currentLSN - 1
	if p.wal != nil && p.walEnabled {
		currentLSN := p.wal.GetCurrentLSN()
		if currentLSN > 1 {
			// currentLSN - 1 = 最后写入的 LSN（已安全落盘）
			if err := p.wal.Checkpoint(currentLSN - 1); err != nil {
				return fmt.Errorf("failed to checkpoint: %w", err)
			}
		}
	}

	return nil
}

// Close 关闭 Pager
func (p *Pager) Close() error {
	if err := p.Flush(); err != nil {
		return err
	}

	// 关闭 WAL
	if p.wal != nil {
		if err := p.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}

	return p.file.Close()
}

// addToCache 添加页面到缓存
// 【BUG-003 修复】当所有页面都是脏页时，强制刷盘最老的脏页
func (p *Pager) addToCache(page *Page) {
	if len(p.cache) >= p.maxCached {
		// 第一轮：尝试移除一个非脏页
		evicted := false
		for id, pg := range p.cache {
			if !pg.IsDirty() {
				delete(p.cache, id)
				evicted = true
				break
			}
		}

		// 【BUG-003 修复】第二轮：如果所有页都是脏页，强制刷盘并移除一个
		// 【P0 修复】刷盘失败时不得清除 dirty 标记或从缓存中移除
		if !evicted && len(p.cache) >= p.maxCached {
			// 选择一个脏页刷盘（简单策略：选第一个找到的）
			var oldestID PageId
			var oldestPage *Page

			for id, pg := range p.cache {
				if pg.IsDirty() {
					oldestID = id
					oldestPage = pg
					break // 简化实现，选第一个脏页
				}
			}

			if oldestPage != nil {
				// 强制刷盘
				if err := p.writePage(oldestPage); err != nil {
					// 【P0 关键修复】刷盘失败，不清除 dirty 标记，不从缓存移除
					// 记录错误并设置 pager 进入错误状态
					LogError("force flush failed during cache eviction", map[string]interface{}{
						"pageId": oldestID,
						"error":  err.Error(),
					})
					// 标记 pager 进入错误状态（可选：拒绝后续写入）
					p.lastFlushError = err
					// 不移除页面，保留在缓存中等待重试
					// 允许继续添加新页面到缓存，但发出警告
					LogError("WARNING: cache may grow beyond limit due to flush failure", nil)
				} else {
					// 刷盘成功，才能清除 dirty 标记并从缓存移除
					// 注意：writePage 内部已经调用了 ClearDirty()
					delete(p.cache, oldestID)
					delete(p.dirty, oldestID)
				}
			}
		}
	}
	p.cache[page.ID()] = page
}

// LogError 记录错误日志（如果 engine 包的 LogError 不可用）
func LogError(msg string, fields map[string]interface{}) {
	// 简单实现：打印到标准输出
	// 在实际使用中，应该使用统一的日志框架
	fmt.Printf("[ERROR] %s: %v\n", msg, fields)
}

// PageCount 返回总页面数
// 【BUG-008 修复】添加锁保护，避免数据竞争
func (p *Pager) PageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

// GetPageCount 返回总页面数（带锁）
func (p *Pager) GetPageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

// GetFreePageCount 返回空闲页面数
func (p *Pager) GetFreePageCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.freePages)
}

// Header 返回文件头
func (p *Pager) Header() *FileHeader {
	return p.header
}

// SetCatalogPageId 设置目录页 ID
func (p *Pager) SetCatalogPageId(id PageId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.header.CatalogPageId = id
}

// CatalogPageId 获取目录页 ID
func (p *Pager) CatalogPageId() PageId {
	return p.header.CatalogPageId
}

// currentTimeMillis 获取当前时间毫秒数
func currentTimeMillis() int64 {
	return currentTimeMillisImpl()
}

// 可在测试中替换
var currentTimeMillisImpl = func() int64 {
	return time.Now().UnixMilli()
}

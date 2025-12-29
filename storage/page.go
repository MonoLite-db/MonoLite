// Created by Yanjunhui

package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/monolite/monodb/internal/failpoint"
)

// Page 大小常量
// EN: Page size constants.
const (
	// PageSize 4KB 页大小
	// EN: PageSize is the page size in bytes (4KB).
	PageSize = 4096
	// PageHeaderSize 页头大小
	// EN: PageHeaderSize is the page header size in bytes.
	PageHeaderSize = 24
	MaxPageData    = PageSize - PageHeaderSize
)

// Page 类型
// EN: Page types.
const (
	// PageTypeFree 空闲页
	// EN: PageTypeFree is a free page.
	PageTypeFree uint8 = 0x00
	// PageTypeMeta 元数据页
	// EN: PageTypeMeta is a metadata page.
	PageTypeMeta uint8 = 0x01
	// PageTypeCatalog 目录页（Collection/Index 目录）
	// EN: PageTypeCatalog is a catalog page (collection/index catalog).
	PageTypeCatalog uint8 = 0x02
	// PageTypeData 数据页
	// EN: PageTypeData is a data page.
	PageTypeData uint8 = 0x03
	// PageTypeIndex 索引页
	// EN: PageTypeIndex is an index page.
	PageTypeIndex uint8 = 0x04
	// PageTypeOverflow 溢出页（存储大文档）
	// EN: PageTypeOverflow is an overflow page (stores large documents).
	PageTypeOverflow uint8 = 0x05
	// PageTypeFreeList 空闲列表页
	// EN: PageTypeFreeList is a free-list page.
	PageTypeFreeList uint8 = 0x06
)

// PageId 页面唯一标识
// EN: PageId uniquely identifies a page.
type PageId uint32

// Page 表示一个存储页
// EN: Page represents a storage page.
// 页头结构（24 字节）：
// EN: Header layout (24 bytes):
//   - PageId (4 bytes)
//   - Type (1 byte)
//   - Flags (1 byte)
//   - ItemCount (2 bytes)
//   - FreeSpace (2 bytes)
//   - NextPageId (4 bytes) - 用于链表
//
// EN:   - NextPageId (4 bytes) - used for linked lists
//   - PrevPageId (4 bytes) - 用于双向链表
//
// EN:   - PrevPageId (4 bytes) - used for doubly-linked lists
//   - Checksum (4 bytes)
//   - Reserved (2 bytes)
type Page struct {
	id         PageId
	pageType   uint8
	flags      uint8
	itemCount  uint16
	freeSpace  uint16
	nextPageId PageId
	prevPageId PageId
	checksum   uint32
	data       []byte
	dirty      bool
	mu         sync.RWMutex
}

// NewPage 创建一个新页面
// EN: NewPage creates a new page.
func NewPage(id PageId, pageType uint8) *Page {
	return &Page{
		id:        id,
		pageType:  pageType,
		flags:     0,
		itemCount: 0,
		freeSpace: MaxPageData,
		data:      make([]byte, MaxPageData),
		dirty:     true,
	}
}

// ID 返回页面 ID
// EN: ID returns the page ID.
func (p *Page) ID() PageId {
	return p.id
}

// Type 返回页面类型
// EN: Type returns the page type.
func (p *Page) Type() uint8 {
	return p.pageType
}

// SetType 设置页面类型
// EN: SetType sets the page type.
func (p *Page) SetType(t uint8) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pageType = t
	p.dirty = true
}

// ItemCount 返回项目数量
// EN: ItemCount returns the item count.
func (p *Page) ItemCount() uint16 {
	return p.itemCount
}

// FreeSpace 返回剩余空间
// EN: FreeSpace returns remaining free space.
func (p *Page) FreeSpace() uint16 {
	return p.freeSpace
}

// NextPageId 返回下一页 ID
// EN: NextPageId returns the next page ID.
func (p *Page) NextPageId() PageId {
	return p.nextPageId
}

// SetNextPageId 设置下一页 ID
// EN: SetNextPageId sets the next page ID.
func (p *Page) SetNextPageId(id PageId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextPageId = id
	p.dirty = true
}

// PrevPageId 返回上一页 ID
// EN: PrevPageId returns the previous page ID.
func (p *Page) PrevPageId() PageId {
	return p.prevPageId
}

// SetPrevPageId 设置上一页 ID
// EN: SetPrevPageId sets the previous page ID.
func (p *Page) SetPrevPageId(id PageId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prevPageId = id
	p.dirty = true
}

// IsDirty 检查页面是否被修改
// EN: IsDirty reports whether the page has been modified.
func (p *Page) IsDirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirty
}

// MarkDirty 标记页面为脏
// EN: MarkDirty marks the page as dirty.
func (p *Page) MarkDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty = true
}

// ClearDirty 清除脏标记
// EN: ClearDirty clears the dirty flag.
func (p *Page) ClearDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty = false
}

// Data 返回页面数据的副本
// EN: Data returns a copy of the page data.
func (p *Page) Data() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]byte, len(p.data))
	copy(result, p.data)
	return result
}

// SetData 设置页面数据
// EN: SetData sets the page data.
func (p *Page) SetData(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(data) > MaxPageData {
		return fmt.Errorf("data too large: %d > %d", len(data), MaxPageData)
	}
	copy(p.data, data)
	p.dirty = true
	return nil
}

// Marshal 将页面序列化为字节
// EN: Marshal serializes the page into bytes.
func (p *Page) Marshal() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	buf := make([]byte, PageSize)

	// 写入页头
	// EN: Write header.
	binary.LittleEndian.PutUint32(buf[0:4], uint32(p.id))
	buf[4] = p.pageType
	buf[5] = p.flags
	binary.LittleEndian.PutUint16(buf[6:8], p.itemCount)
	binary.LittleEndian.PutUint16(buf[8:10], p.freeSpace)
	binary.LittleEndian.PutUint32(buf[10:14], uint32(p.nextPageId))
	binary.LittleEndian.PutUint32(buf[14:18], uint32(p.prevPageId))
	// checksum 位置 18:22，稍后计算
	// EN: checksum position 18:22; filled later.
	// reserved 22:24

	// 写入数据
	// EN: Write data.
	copy(buf[PageHeaderSize:], p.data)

	// 计算校验和（简单的 CRC32 或 XOR）
	// EN: Compute checksum (simple XOR-based checksum).
	checksum := calculateChecksum(buf[PageHeaderSize:])
	binary.LittleEndian.PutUint32(buf[18:22], checksum)

	return buf
}

// UnmarshalPage 从字节反序列化页面
// EN: UnmarshalPage deserializes a page from bytes.
func UnmarshalPage(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("invalid page size: %d", len(data))
	}

	p := &Page{
		id:         PageId(binary.LittleEndian.Uint32(data[0:4])),
		pageType:   data[4],
		flags:      data[5],
		itemCount:  binary.LittleEndian.Uint16(data[6:8]),
		freeSpace:  binary.LittleEndian.Uint16(data[8:10]),
		nextPageId: PageId(binary.LittleEndian.Uint32(data[10:14])),
		prevPageId: PageId(binary.LittleEndian.Uint32(data[14:18])),
		checksum:   binary.LittleEndian.Uint32(data[18:22]),
		data:       make([]byte, MaxPageData),
		dirty:      false,
	}

	copy(p.data, data[PageHeaderSize:])

	// 验证校验和
	// EN: Verify checksum.
	expectedChecksum := calculateChecksum(p.data)
	if p.checksum != expectedChecksum {
		return nil, fmt.Errorf("page checksum mismatch: expected %x, got %x", expectedChecksum, p.checksum)
	}

	return p, nil
}

// calculateChecksum 计算简单的校验和
// EN: calculateChecksum computes a simple checksum.
func calculateChecksum(data []byte) uint32 {
	var sum uint32
	for i := 0; i < len(data); i += 4 {
		if i+4 <= len(data) {
			sum ^= binary.LittleEndian.Uint32(data[i : i+4])
		} else {
			// 处理尾部不足 4 字节的情况
			// EN: Handle tail shorter than 4 bytes.
			var last uint32
			for j := i; j < len(data); j++ {
				last |= uint32(data[j]) << (8 * (j - i))
			}
			sum ^= last
		}
	}
	return sum
}

// SlottedPage 支持可变长度记录的槽页面
// EN: SlottedPage is a slot-based page for variable-length records.
// 槽页面在数据页中使用，支持高效的记录插入和删除
// EN: It is used in data pages and supports efficient insert/delete.
type SlottedPage struct {
	*Page
	// 槽目录
	// EN: Slot directory.
	slots []Slot
}

// Slot 表示一个槽
// EN: Slot describes one slot entry.
type Slot struct {
	// 记录在页面中的偏移
	// EN: Offset is the record offset within the page.
	Offset uint16
	// 记录长度
	// EN: Length is the record length in bytes.
	Length uint16
	// 标志（删除标记等）
	// EN: Flags stores slot flags (e.g., deleted).
	Flags uint16
}

const (
	// SlotSize 每个槽占用 6 字节
	// EN: SlotSize is the size of a slot entry in bytes (6).
	SlotSize        = 6
	SlotFlagDeleted = 0x01
)

// NewSlottedPage 创建一个新的槽页面
// EN: NewSlottedPage creates a new slotted page.
func NewSlottedPage(id PageId) *SlottedPage {
	return &SlottedPage{
		Page:  NewPage(id, PageTypeData),
		slots: make([]Slot, 0),
	}
}

// WrapSlottedPage 将现有 Page 包装为 SlottedPage
// EN: WrapSlottedPage wraps an existing Page as a SlottedPage.
func WrapSlottedPage(page *Page) *SlottedPage {
	sp := &SlottedPage{
		Page:  page,
		slots: make([]Slot, 0),
	}

	// 从页面数据中恢复 slots
	// EN: Restore slots from page data.
	// slots 存储在页面数据的开头，格式：[slot1][slot2]...
	// EN: Slots are stored at the beginning of page data: [slot1][slot2]...
	// 每个 slot 6 字节：offset(2) + length(2) + flags(2)
	// EN: Each slot is 6 bytes: offset(2) + length(2) + flags(2).
	numSlots := int(page.ItemCount())
	if numSlots > 0 && len(page.data) >= numSlots*SlotSize {
		sp.slots = make([]Slot, numSlots)
		for i := 0; i < numSlots; i++ {
			pos := i * SlotSize
			sp.slots[i] = Slot{
				Offset: binary.LittleEndian.Uint16(page.data[pos:]),
				Length: binary.LittleEndian.Uint16(page.data[pos+2:]),
				Flags:  binary.LittleEndian.Uint16(page.data[pos+4:]),
			}
		}
	}

	return sp
}

// InsertRecord 插入一条记录，返回槽索引
// EN: InsertRecord inserts a record and returns the slot index.
func (sp *SlottedPage) InsertRecord(data []byte) (int, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	recordLen := uint16(len(data))
	slotSpace := uint16(SlotSize)
	totalNeeded := recordLen + slotSpace

	// 计算可用空间（需要考虑 slot 目录占用的空间）
	// EN: Compute available space (accounting for slot directory space).
	slotDirEnd := uint16((len(sp.slots) + 1) * SlotSize)
	var minRecordOffset uint16
	if len(sp.slots) == 0 {
		minRecordOffset = MaxPageData
	} else {
		minRecordOffset = MaxPageData
		for _, slot := range sp.slots {
			if slot.Flags&SlotFlagDeleted == 0 && slot.Offset < minRecordOffset {
				minRecordOffset = slot.Offset
			}
		}
	}

	// 检查空间：slot 目录从前往后增长，记录从后往前增长
	// EN: Space check: slot directory grows forward, records grow backward.
	if slotDirEnd+recordLen > minRecordOffset {
		return -1, fmt.Errorf("not enough space: need %d, have %d", totalNeeded, minRecordOffset-slotDirEnd)
	}

	// 计算新记录的偏移（从页面尾部向前增长）
	// EN: Compute new record offset (growing backward from the end of the page).
	offset := minRecordOffset - recordLen

	// 写入数据
	// EN: Write data.
	copy(sp.data[offset:], data)

	// 添加槽
	// EN: Add slot.
	slot := Slot{
		Offset: offset,
		Length: recordLen,
		Flags:  0,
	}
	sp.slots = append(sp.slots, slot)

	// 将新 slot 写入页面数据开头的 slot 目录区
	// EN: Write the new slot into the slot directory at the beginning of page data.
	slotIndex := len(sp.slots) - 1
	pos := slotIndex * SlotSize
	binary.LittleEndian.PutUint16(sp.data[pos:], slot.Offset)
	binary.LittleEndian.PutUint16(sp.data[pos+2:], slot.Length)
	binary.LittleEndian.PutUint16(sp.data[pos+4:], slot.Flags)

	// 更新页面元数据
	// EN: Update page metadata.
	sp.itemCount++
	sp.freeSpace = minRecordOffset - slotDirEnd - recordLen
	sp.dirty = true

	return slotIndex, nil
}

// GetRecord 获取指定槽的记录
// EN: GetRecord returns the record for the given slot.
// 【BUG-015 修复】增加边界检查
// EN: [BUG-015 fix] Add bounds checks.
func (sp *SlottedPage) GetRecord(slotIndex int) ([]byte, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if slotIndex < 0 || slotIndex >= len(sp.slots) {
		return nil, fmt.Errorf("invalid slot index: %d", slotIndex)
	}

	slot := sp.slots[slotIndex]
	if slot.Flags&SlotFlagDeleted != 0 {
		return nil, fmt.Errorf("slot %d is deleted", slotIndex)
	}

	// 【BUG-015 修复】边界检查
	// EN: [BUG-015 fix] Bounds checks.
	endOffset := int(slot.Offset) + int(slot.Length)
	if int(slot.Offset) < 0 || endOffset > len(sp.data) {
		return nil, fmt.Errorf("slot %d has invalid range: offset=%d, length=%d, page_size=%d",
			slotIndex, slot.Offset, slot.Length, len(sp.data))
	}

	data := make([]byte, slot.Length)
	copy(data, sp.data[slot.Offset:endOffset])
	return data, nil
}

// DeleteRecord 删除指定槽的记录
// EN: DeleteRecord deletes the record at the given slot.
// 注意：删除只标记 slot 为 deleted，不减少 itemCount（槽总数）
// EN: Note: deletion only marks the slot as deleted and does not decrement itemCount (total slots).
// itemCount 单调递增，重启后可正确恢复所有槽目录
// EN: itemCount is monotonic so the slot directory can be recovered correctly after restart.
func (sp *SlottedPage) DeleteRecord(slotIndex int) error {
	// 【FAILPOINT】用于测试记录删除失败场景
	// EN: [FAILPOINT] used to test record-delete failure paths.
	if err := failpoint.Hit("slotted.delete"); err != nil {
		return fmt.Errorf("failpoint: slotted.delete: %w", err)
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()

	if slotIndex < 0 || slotIndex >= len(sp.slots) {
		return fmt.Errorf("invalid slot index: %d", slotIndex)
	}

	slot := &sp.slots[slotIndex]
	if slot.Flags&SlotFlagDeleted != 0 {
		return fmt.Errorf("slot %d already deleted", slotIndex)
	}

	slot.Flags |= SlotFlagDeleted
	// 注意：不再减少 itemCount，因为 itemCount 表示槽总数（slotCount）
	// EN: Note: do not decrement itemCount; it represents total slots (slotCount).
	// 删除只是标记槽为已删除，重启后仍能正确恢复槽目录
	// EN: Deletion only marks the slot deleted, so the directory can still be recovered after restart.

	// 更新页面数据中的 slot 目录
	// EN: Update slot directory in page data.
	pos := slotIndex * SlotSize
	binary.LittleEndian.PutUint16(sp.data[pos+4:], slot.Flags)

	sp.dirty = true

	return nil
}

// UpdateRecord 更新指定槽的记录
// EN: UpdateRecord updates the record at the given slot.
func (sp *SlottedPage) UpdateRecord(slotIndex int, data []byte) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if slotIndex < 0 || slotIndex >= len(sp.slots) {
		return fmt.Errorf("invalid slot index: %d", slotIndex)
	}

	slot := &sp.slots[slotIndex]
	if slot.Flags&SlotFlagDeleted != 0 {
		return fmt.Errorf("slot %d is deleted", slotIndex)
	}

	newLen := uint16(len(data))

	// 如果新数据能放入原位置
	// EN: If the new data fits in the original location.
	if newLen <= slot.Length {
		copy(sp.data[slot.Offset:], data)
		// 回收多余空间（简化处理：不回收）
		// EN: Reclaim extra space (simplified: do not reclaim).
		slot.Length = newLen
		// 将更新后的 slot 写回页面数据
		// EN: Write updated slot back to page data.
		pos := slotIndex * SlotSize
		binary.LittleEndian.PutUint16(sp.data[pos+2:], slot.Length)
		sp.dirty = true
		return nil
	}

	// 需要更多空间
	// EN: Need more space.
	extraSpace := newLen - slot.Length
	if extraSpace > sp.freeSpace {
		return fmt.Errorf("not enough space for update: need %d extra, have %d", extraSpace, sp.freeSpace)
	}

	// 简化处理：标记旧记录为删除，插入新记录
	// EN: Simplified: mark old record deleted and place the new record elsewhere.
	// 实际实现可以做页面压缩
	// EN: A full implementation can compact the page.
	slot.Flags |= SlotFlagDeleted
	sp.freeSpace += slot.Length

	// 找到新位置
	// EN: Find new location.
	minOffset := uint16(MaxPageData)
	for _, s := range sp.slots {
		if s.Flags&SlotFlagDeleted == 0 && s.Offset < minOffset {
			minOffset = s.Offset
		}
	}
	newOffset := minOffset - newLen

	copy(sp.data[newOffset:], data)
	slot.Offset = newOffset
	slot.Length = newLen
	slot.Flags = 0
	sp.freeSpace -= newLen

	// 将更新后的 slot 写回页面数据
	// EN: Write updated slot back to page data.
	pos := slotIndex * SlotSize
	binary.LittleEndian.PutUint16(sp.data[pos:], slot.Offset)
	binary.LittleEndian.PutUint16(sp.data[pos+2:], slot.Length)
	binary.LittleEndian.PutUint16(sp.data[pos+4:], slot.Flags)
	sp.dirty = true

	return nil
}

// SlotCount 返回槽总数（包括已删除的槽）
// EN: SlotCount returns total slots (including deleted slots).
func (sp *SlottedPage) SlotCount() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return len(sp.slots)
}

// LiveCount 返回活跃记录数（不包括已删除的槽）
// EN: LiveCount returns the number of live (non-deleted) records.
func (sp *SlottedPage) LiveCount() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	count := 0
	for _, slot := range sp.slots {
		if slot.Flags&SlotFlagDeleted == 0 {
			count++
		}
	}
	return count
}

// IsSlotDeleted 检查指定槽是否已删除
// EN: IsSlotDeleted reports whether the given slot is deleted.
func (sp *SlottedPage) IsSlotDeleted(slotIndex int) bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	if slotIndex < 0 || slotIndex >= len(sp.slots) {
		return true
	}
	return sp.slots[slotIndex].Flags&SlotFlagDeleted != 0
}

// Compact 压缩页面，回收已删除记录的空间
// EN: Compact compacts the page and reclaims space from deleted records.
// 注意：Compact 会重新整理槽目录，这是唯一减少槽数量的操作
// EN: Note: Compact rebuilds the slot directory; it is the only operation that reduces slot count.
// 【BUG-005 修复】返回旧索引到新索引的映射，供调用者更新外部引用（如索引）
// EN: [BUG-005 fix] Returns a mapping from old slot index to new slot index for updating external references (e.g., indexes).
func (sp *SlottedPage) Compact() map[int]int {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// 【BUG-005 修复】记录旧索引到新索引的映射
	// EN: [BUG-005 fix] Record old-to-new slot index mapping.
	mapping := make(map[int]int) // oldSlotIndex -> newSlotIndex

	// 收集所有活跃记录，同时记录映射关系
	// EN: Collect all live records and track mapping.
	type record struct {
		data     []byte
		oldIndex int
	}
	var records []record

	for i, slot := range sp.slots {
		if slot.Flags&SlotFlagDeleted == 0 {
			data := make([]byte, slot.Length)
			copy(data, sp.data[slot.Offset:slot.Offset+slot.Length])
			records = append(records, record{data: data, oldIndex: i})
		}
	}

	// 清空数据区
	// EN: Clear data area.
	for i := range sp.data {
		sp.data[i] = 0
	}

	// 创建新的槽目录
	// EN: Create a new slot directory.
	sp.slots = make([]Slot, len(records))

	// 重新写入记录（从页面末尾向前增长）
	// EN: Rewrite records (growing backward from the page end).
	offset := uint16(MaxPageData)
	for newIndex, r := range records {
		recordLen := uint16(len(r.data))
		offset -= recordLen
		copy(sp.data[offset:], r.data)

		sp.slots[newIndex] = Slot{
			Offset: offset,
			Length: recordLen,
			Flags:  0,
		}

		// 【BUG-005 修复】记录映射关系
		// EN: [BUG-005 fix] Record mapping.
		mapping[r.oldIndex] = newIndex

		// 写入槽目录
		// EN: Write slot directory.
		pos := newIndex * SlotSize
		binary.LittleEndian.PutUint16(sp.data[pos:], offset)
		binary.LittleEndian.PutUint16(sp.data[pos+2:], recordLen)
		binary.LittleEndian.PutUint16(sp.data[pos+4:], 0)
	}

	// 更新页面元数据
	// EN: Update page metadata.
	sp.itemCount = uint16(len(sp.slots))

	// 更新空闲空间
	// EN: Update free space.
	usedSpace := uint16(0)
	for _, slot := range sp.slots {
		usedSpace += slot.Length
	}
	slotSpace := uint16(len(sp.slots) * SlotSize)
	sp.freeSpace = MaxPageData - usedSpace - slotSpace
	sp.dirty = true

	return mapping
}

// Created by Yanjunhui

package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"

	"github.com/monolite/monodb/internal/failpoint"
)

// WAL 常量
// EN: WAL constants.
const (
	WALMagic      uint32 = 0x57414C4D // "WALM"
	WALVersion    uint16 = 1
	WALHeaderSize        = 32 // magic(4) + version(2) + reserved(2) + checkpointLSN(8) + fileSize(8) + checksum(4) + reserved(4)
	// WALRecordAlign 记录对齐到 8 字节
	// EN: WALRecordAlign aligns records to 8 bytes.
	WALRecordAlign = 8
)

// WAL 记录类型
// EN: WAL record types.
const (
	// WALRecordPageWrite 完整页面写入
	// EN: WALRecordPageWrite is a full page write.
	WALRecordPageWrite uint8 = 1
	// WALRecordAllocPage 分配页面
	// EN: WALRecordAllocPage allocates a page.
	WALRecordAllocPage uint8 = 2
	// WALRecordFreePage 释放页面
	// EN: WALRecordFreePage frees a page.
	WALRecordFreePage uint8 = 3
	// WALRecordCommit 事务提交
	// EN: WALRecordCommit marks a commit.
	WALRecordCommit uint8 = 4
	// WALRecordCheckpoint 检查点标记
	// EN: WALRecordCheckpoint marks a checkpoint.
	WALRecordCheckpoint uint8 = 5
	// WALRecordMetaUpdate 文件头元数据更新（FreeListHead/PageCount/CatalogPageId）
	// EN: WALRecordMetaUpdate updates file header metadata (FreeListHead/PageCount/CatalogPageId).
	WALRecordMetaUpdate uint8 = 6
)

// MetaUpdateType 元数据更新类型
// EN: MetaUpdateType describes metadata update types.
const (
	MetaUpdateFreeListHead  uint8 = 1
	MetaUpdatePageCount     uint8 = 2
	MetaUpdateCatalogPageId uint8 = 3
)

// LSN Log Sequence Number（日志序列号）
// EN: LSN is a Log Sequence Number.
type LSN uint64

// WALHeader WAL 文件头
// EN: WALHeader is the WAL file header.
type WALHeader struct {
	Magic     uint32
	Version   uint16
	Reserved1 uint16
	// 最近的检查点 LSN
	// EN: CheckpointLSN is the most recent checkpoint LSN.
	CheckpointLSN LSN
	// WAL 文件当前大小
	// EN: FileSize is the current WAL file size.
	FileSize  uint64
	Checksum  uint32
	Reserved2 uint32
}

// WALRecord WAL 记录结构
// EN: WALRecord represents one WAL record.
// 格式：[RecordHeader][Data][Padding to align]
// EN: Layout: [RecordHeader][Data][Padding to align]
// RecordHeader: LSN(8) + Type(1) + Flags(1) + DataLen(2) + PageId(4) + Checksum(4) = 20 bytes
type WALRecord struct {
	LSN      LSN
	Type     uint8
	Flags    uint8
	DataLen  uint16
	PageId   PageId
	Checksum uint32
	Data     []byte
}

const WALRecordHeaderSize = 20

// WAL 自动截断阈值
// EN: WAL auto-truncation thresholds.
const (
	// 64MB 后自动截断
	// EN: Auto-truncate after 64MB.
	WALTruncateThreshold = 64 * 1024 * 1024
	// 截断后至少保留 4MB
	// EN: Retain at least 4MB after truncation.
	WALMinRetainSize = 4 * 1024 * 1024
)

// WAL Write-Ahead Log 管理器
// EN: WAL is a write-ahead log manager.
type WAL struct {
	file          *os.File
	header        *WALHeader
	currentLSN    LSN
	writeOffset   int64
	mu            sync.Mutex
	bufferPool    sync.Pool
	checkpointLSN LSN
	// 自动截断控制
	// EN: Auto-truncation control.
	// 是否启用自动截断
	// EN: Whether auto-truncation is enabled.
	autoTruncate bool
}

// NewWAL 创建或打开 WAL 文件
// EN: NewWAL creates or opens a WAL file.
func NewWAL(path string) (*WAL, error) {
	exists := true
	if _, err := os.Stat(path); os.IsNotExist(err) {
		exists = false
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file: file,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, PageSize+WALRecordHeaderSize+WALRecordAlign)
			},
		},
	}

	if exists {
		if err := wal.readHeader(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		if err := wal.initNewWAL(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return wal, nil
}

// initNewWAL 初始化新的 WAL 文件
// EN: initNewWAL initializes a new WAL file.
func (w *WAL) initNewWAL() error {
	w.header = &WALHeader{
		Magic:         WALMagic,
		Version:       WALVersion,
		CheckpointLSN: 0,
		FileSize:      WALHeaderSize,
	}
	w.currentLSN = 1
	w.writeOffset = WALHeaderSize
	w.checkpointLSN = 0

	return w.writeHeader()
}

// readHeader 读取 WAL 文件头
// EN: readHeader reads the WAL file header.
func (w *WAL) readHeader() error {
	buf := make([]byte, WALHeaderSize)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		if err == io.EOF {
			// 文件为空，初始化
			// EN: File is empty; initialize.
			return w.initNewWAL()
		}
		return fmt.Errorf("failed to read WAL header: %w", err)
	}

	header := &WALHeader{
		Magic:         binary.LittleEndian.Uint32(buf[0:4]),
		Version:       binary.LittleEndian.Uint16(buf[4:6]),
		Reserved1:     binary.LittleEndian.Uint16(buf[6:8]),
		CheckpointLSN: LSN(binary.LittleEndian.Uint64(buf[8:16])),
		FileSize:      binary.LittleEndian.Uint64(buf[16:24]),
		Checksum:      binary.LittleEndian.Uint32(buf[24:28]),
	}

	if header.Magic != WALMagic {
		return fmt.Errorf("invalid WAL magic: %x", header.Magic)
	}
	if header.Version != WALVersion {
		return fmt.Errorf("unsupported WAL version: %d", header.Version)
	}

	// 验证头部校验和
	// EN: Verify header checksum.
	expectedChecksum := crc32.ChecksumIEEE(buf[0:24])
	if header.Checksum != expectedChecksum {
		return fmt.Errorf("WAL header checksum mismatch")
	}

	w.header = header
	w.checkpointLSN = header.CheckpointLSN
	w.writeOffset = int64(header.FileSize)

	// 扫描找到最大 LSN
	// EN: Scan to find the maximum LSN.
	w.currentLSN = w.checkpointLSN + 1
	if err := w.scanForMaxLSN(); err != nil {
		return err
	}

	return nil
}

// writeHeader 写入 WAL 文件头
// EN: writeHeader writes the WAL file header.
func (w *WAL) writeHeader() error {
	buf := make([]byte, WALHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], w.header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], w.header.Version)
	binary.LittleEndian.PutUint16(buf[6:8], w.header.Reserved1)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(w.header.CheckpointLSN))
	binary.LittleEndian.PutUint64(buf[16:24], w.header.FileSize)

	// 计算校验和
	// EN: Compute checksum.
	checksum := crc32.ChecksumIEEE(buf[0:24])
	binary.LittleEndian.PutUint32(buf[24:28], checksum)
	w.header.Checksum = checksum

	if _, err := w.file.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	return nil
}

// scanForMaxLSN 扫描 WAL 找到最大 LSN
// EN: scanForMaxLSN scans the WAL to find the maximum LSN.
// 【BUG-004 修复】扫描到文件实际末尾，并验证校验和
// EN: [BUG-004 fix] Scan to the actual file end and verify checksums.
func (w *WAL) scanForMaxLSN() error {
	// 【BUG-004 修复】使用实际文件大小，而非 header.FileSize
	// EN: [BUG-004 fix] Use actual file size, not header.FileSize.
	fi, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}
	actualFileSize := fi.Size()

	offset := int64(WALHeaderSize)
	headerBuf := make([]byte, WALRecordHeaderSize)
	var lastValidOffset int64 = offset

	for offset < actualFileSize {
		// 读取记录头
		// EN: Read record header.
		n, err := w.file.ReadAt(headerBuf, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			// 【BUG-004 修复】读取失败，停止扫描
			// EN: [BUG-004 fix] Read failed; stop scanning.
			break
		}
		if n < WALRecordHeaderSize {
			break
		}

		// 解析记录头
		// EN: Parse record header.
		lsn := LSN(binary.LittleEndian.Uint64(headerBuf[0:8]))
		dataLen := binary.LittleEndian.Uint16(headerBuf[10:12])
		checksum := binary.LittleEndian.Uint32(headerBuf[16:20])

		// 【BUG-004 修复】验证校验和
		// EN: [BUG-004 fix] Verify checksum.
		expectedChecksum := crc32.ChecksumIEEE(headerBuf[0:16])
		if dataLen > 0 {
			// 检查数据是否完整
			// EN: Check whether data is complete.
			if offset+WALRecordHeaderSize+int64(dataLen) > actualFileSize {
				// 数据不完整，说明是部分写入的记录
				// EN: Incomplete data indicates a partially written record.
				log.Printf("[WAL] incomplete record at offset %d, stopping scan", offset)
				break
			}
			data := make([]byte, dataLen)
			if _, err := w.file.ReadAt(data, offset+WALRecordHeaderSize); err != nil {
				break
			}
			expectedChecksum = crc32.Update(expectedChecksum, crc32.IEEETable, data)
		}

		if checksum != expectedChecksum {
			// 【BUG-004 修复】校验和不匹配，说明记录损坏
			// EN: [BUG-004 fix] Checksum mismatch indicates record corruption.
			log.Printf("[WAL] corrupted record at offset %d (checksum mismatch), stopping scan", offset)
			break
		}

		// 记录有效
		// EN: Record is valid.
		if lsn >= w.currentLSN {
			w.currentLSN = lsn + 1
		}

		// 移动到下一条记录
		// EN: Move to the next record.
		recordSize := WALRecordHeaderSize + int(dataLen)
		// 对齐
		// EN: Align.
		if recordSize%WALRecordAlign != 0 {
			recordSize += WALRecordAlign - (recordSize % WALRecordAlign)
		}
		lastValidOffset = offset + int64(recordSize)
		offset = lastValidOffset
	}

	// 【BUG-004 修复】更新 writeOffset 为最后有效位置
	// EN: [BUG-004 fix] Update writeOffset to the last valid position.
	w.writeOffset = lastValidOffset
	w.header.FileSize = uint64(lastValidOffset)

	return nil
}

// WritePageRecord 写入页面更新记录
// EN: WritePageRecord writes a page-write record.
func (w *WAL) WritePageRecord(pageId PageId, data []byte) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordPageWrite,
		Flags:   0,
		DataLen: uint16(len(data)),
		PageId:  pageId,
		Data:    data,
	}

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	lsn := w.currentLSN
	w.currentLSN++
	return lsn, nil
}

// WriteAllocRecord 写入页面分配记录
// EN: WriteAllocRecord writes a page-allocation record.
func (w *WAL) WriteAllocRecord(pageId PageId, pageType uint8) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := []byte{pageType}
	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordAllocPage,
		Flags:   0,
		DataLen: 1,
		PageId:  pageId,
		Data:    data,
	}

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	lsn := w.currentLSN
	w.currentLSN++
	return lsn, nil
}

// WriteFreeRecord 写入页面释放记录
// EN: WriteFreeRecord writes a page-free record.
func (w *WAL) WriteFreeRecord(pageId PageId) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordFreePage,
		Flags:   0,
		DataLen: 0,
		PageId:  pageId,
		Data:    nil,
	}

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	lsn := w.currentLSN
	w.currentLSN++
	return lsn, nil
}

// WriteCommitRecord 写入提交记录
// EN: WriteCommitRecord writes a commit record.
func (w *WAL) WriteCommitRecord() (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordCommit,
		Flags:   0,
		DataLen: 0,
		PageId:  0,
		Data:    nil,
	}

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	lsn := w.currentLSN
	w.currentLSN++
	return lsn, nil
}

// WriteMetaRecord 写入元数据更新记录（用于 FreeListHead/PageCount/CatalogPageId 等变更）
// EN: WriteMetaRecord writes a metadata-update record (for FreeListHead/PageCount/CatalogPageId changes).
// 这确保 header 变更在 WAL 中有记录，遵循 WAL 先行原则
// EN: This ensures header changes are recorded in the WAL, following the WAL-ahead principle.
func (w *WAL) WriteMetaRecord(metaType uint8, oldValue, newValue uint32) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 数据格式：[metaType(1)][oldValue(4)][newValue(4)] = 9 bytes
	// EN: Data layout: [metaType(1)][oldValue(4)][newValue(4)] = 9 bytes.
	data := make([]byte, 9)
	data[0] = metaType
	binary.LittleEndian.PutUint32(data[1:5], oldValue)
	binary.LittleEndian.PutUint32(data[5:9], newValue)

	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordMetaUpdate,
		Flags:   0,
		DataLen: 9,
		PageId:  0,
		Data:    data,
	}

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	lsn := w.currentLSN
	w.currentLSN++
	return lsn, nil
}

// writeRecord 写入单条 WAL 记录
// EN: writeRecord writes a single WAL record.
func (w *WAL) writeRecord(record *WALRecord) error {
	// 计算记录大小（对齐）
	// EN: Compute record size (aligned).
	recordSize := WALRecordHeaderSize + int(record.DataLen)
	if recordSize%WALRecordAlign != 0 {
		recordSize += WALRecordAlign - (recordSize % WALRecordAlign)
	}

	// 获取缓冲区
	// EN: Get buffer.
	bufInterface := w.bufferPool.Get()
	buf := bufInterface.([]byte)
	if len(buf) < recordSize {
		buf = make([]byte, recordSize)
	}
	defer w.bufferPool.Put(buf)

	// 清零缓冲区
	// EN: Zero buffer.
	for i := range buf[:recordSize] {
		buf[i] = 0
	}

	// 写入记录头
	// EN: Write record header.
	binary.LittleEndian.PutUint64(buf[0:8], uint64(record.LSN))
	buf[8] = record.Type
	buf[9] = record.Flags
	binary.LittleEndian.PutUint16(buf[10:12], record.DataLen)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(record.PageId))

	// 写入数据
	// EN: Write data.
	if record.DataLen > 0 {
		copy(buf[WALRecordHeaderSize:], record.Data)
	}

	// 计算校验和（不包含校验和字段本身）
	// EN: Compute checksum (excluding the checksum field itself).
	checksum := crc32.ChecksumIEEE(buf[0:16])
	if record.DataLen > 0 {
		checksum = crc32.Update(checksum, crc32.IEEETable, record.Data)
	}
	binary.LittleEndian.PutUint32(buf[16:20], checksum)

	// 写入文件
	// EN: Write to file.
	if _, err := w.file.WriteAt(buf[:recordSize], w.writeOffset); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	w.writeOffset += int64(recordSize)

	// 更新头部文件大小
	// EN: Update header file size.
	w.header.FileSize = uint64(w.writeOffset)

	return nil
}

// Sync 将 WAL 刷盘
// EN: Sync flushes WAL contents to disk.
func (w *WAL) Sync() error {
	// 【FAILPOINT】用于测试 WAL Sync 失败场景
	// EN: [FAILPOINT] used to test WAL Sync failure paths.
	if err := failpoint.Hit("wal.sync"); err != nil {
		return fmt.Errorf("failpoint: wal.sync: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 先更新头部
	// EN: Update header first.
	if err := w.writeHeader(); err != nil {
		return err
	}

	return w.file.Sync()
}

// Checkpoint 创建检查点
// EN: Checkpoint creates a checkpoint.
// 调用者需要确保所有脏页已刷盘
// EN: Caller must ensure all dirty pages have been flushed.
// Checkpoint 设置检查点
// EN: Checkpoint sets the checkpoint.
//
// LSN 语义约定：
// EN: LSN semantics:
// - lsn 参数 = "已安全落盘且可丢弃之前日志的最大 LSN"
// EN: - lsn is the maximum LSN that is safely persisted and before which logs may be discarded.
// - checkpoint 之后的恢复应从 lsn + 1 开始
// EN: - Recovery after checkpoint should start from lsn + 1.
// - 这确保了恢复的幂等性：重复恢复不会重复应用已持久化的变更
// EN: - This ensures idempotent recovery: replaying does not reapply already persisted changes.
func (w *WAL) Checkpoint(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 写入检查点记录
	// EN: Write checkpoint record.
	record := &WALRecord{
		LSN:     w.currentLSN,
		Type:    WALRecordCheckpoint,
		Flags:   0,
		DataLen: 8,
		PageId:  0,
		Data:    make([]byte, 8),
	}
	binary.LittleEndian.PutUint64(record.Data, uint64(lsn))

	if err := w.writeRecord(record); err != nil {
		return err
	}
	w.currentLSN++

	// 更新检查点 LSN
	// EN: Update checkpoint LSN.
	w.header.CheckpointLSN = lsn
	w.checkpointLSN = lsn

	// 写入头部并刷盘
	// EN: Write header and sync.
	if err := w.writeHeader(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	// 自动截断：如果 WAL 超过阈值且启用了自动截断
	// EN: Auto-truncate if WAL exceeds threshold and auto-truncate is enabled.
	if w.autoTruncate && w.writeOffset > WALTruncateThreshold {
		if err := w.truncateAfterCheckpointLocked(); err != nil {
			// 截断失败不影响 checkpoint 成功，只记录日志
			// EN: Truncation failure does not affect checkpoint success; just log it.
			log.Printf("[WAL] auto-truncate failed: %v (size=%d)", err, w.writeOffset)
		}
	}

	return nil
}

// SetAutoTruncate 设置是否启用自动截断
// EN: SetAutoTruncate enables or disables auto-truncation.
func (w *WAL) SetAutoTruncate(enable bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.autoTruncate = enable
}

// truncateAfterCheckpointLocked 截断 checkpoint 之前的记录（调用者需持有锁）
// EN: truncateAfterCheckpointLocked truncates records before the checkpoint (caller must hold the lock).
// 实现策略：直接截断整个 WAL（checkpoint 之后的记录已在数据文件中）
// EN: Strategy: truncate the entire WAL (records after checkpoint are already in the data file).
func (w *WAL) truncateAfterCheckpointLocked() error {
	// 重置 WAL 文件到只有 header
	// EN: Reset WAL file to only the header.
	if err := w.file.Truncate(WALHeaderSize); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// 重置写入偏移
	// EN: Reset write offset.
	w.writeOffset = WALHeaderSize
	w.header.FileSize = WALHeaderSize
	// 保持 checkpointLSN 和 currentLSN 不变，确保恢复语义正确
	// EN: Keep checkpointLSN/currentLSN unchanged to preserve recovery semantics.

	if err := w.writeHeader(); err != nil {
		return err
	}

	log.Printf("[WAL] truncated after checkpoint (checkpointLSN=%d, currentLSN=%d)", w.checkpointLSN, w.currentLSN)

	return w.file.Sync()
}

// GetCheckpointLSN 获取检查点 LSN
// EN: GetCheckpointLSN returns the checkpoint LSN.
func (w *WAL) GetCheckpointLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.checkpointLSN
}

// GetCurrentLSN 获取当前 LSN
// EN: GetCurrentLSN returns the current LSN.
func (w *WAL) GetCurrentLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// ReadRecordsFrom 从指定 LSN 开始读取记录（用于 Recovery）
// EN: ReadRecordsFrom reads records from the given LSN (for recovery).
func (w *WAL) ReadRecordsFrom(startLSN LSN) ([]*WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	records := make([]*WALRecord, 0)
	offset := int64(WALHeaderSize)
	headerBuf := make([]byte, WALRecordHeaderSize)

	for offset < w.writeOffset {
		// 读取记录头
		// EN: Read record header.
		n, err := w.file.ReadAt(headerBuf, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n < WALRecordHeaderSize {
			break
		}

		// 解析记录头
		// EN: Parse record header.
		lsn := LSN(binary.LittleEndian.Uint64(headerBuf[0:8]))
		recordType := headerBuf[8]
		flags := headerBuf[9]
		dataLen := binary.LittleEndian.Uint16(headerBuf[10:12])
		pageId := PageId(binary.LittleEndian.Uint32(headerBuf[12:16]))
		checksum := binary.LittleEndian.Uint32(headerBuf[16:20])

		// 读取数据
		// EN: Read data.
		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := w.file.ReadAt(data, offset+WALRecordHeaderSize); err != nil {
				return nil, fmt.Errorf("failed to read WAL record data: %w", err)
			}
		}

		// 验证校验和
		// EN: Verify checksum.
		expectedChecksum := crc32.ChecksumIEEE(headerBuf[0:16])
		if dataLen > 0 {
			expectedChecksum = crc32.Update(expectedChecksum, crc32.IEEETable, data)
		}
		if checksum != expectedChecksum {
			return nil, fmt.Errorf("WAL record checksum mismatch at offset %d", offset)
		}

		// 只返回 startLSN 之后的记录
		// EN: Only return records at/after startLSN.
		if lsn >= startLSN {
			records = append(records, &WALRecord{
				LSN:      lsn,
				Type:     recordType,
				Flags:    flags,
				DataLen:  dataLen,
				PageId:   pageId,
				Checksum: checksum,
				Data:     data,
			})
		}

		// 移动到下一条记录
		// EN: Move to next record.
		recordSize := WALRecordHeaderSize + int(dataLen)
		if recordSize%WALRecordAlign != 0 {
			recordSize += WALRecordAlign - (recordSize % WALRecordAlign)
		}
		offset += int64(recordSize)
	}

	return records, nil
}

// Truncate 截断 WAL（在 checkpoint 之后可以安全截断旧记录）
// EN: Truncate truncates the WAL (after a checkpoint it is safe to discard old records).
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 重置 WAL 文件
	// EN: Reset WAL file.
	if err := w.file.Truncate(WALHeaderSize); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.writeOffset = WALHeaderSize
	w.header.FileSize = WALHeaderSize

	if err := w.writeHeader(); err != nil {
		return err
	}

	return w.file.Sync()
}

// Close 关闭 WAL
// EN: Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writeHeader(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// WALPath 根据数据库路径生成 WAL 文件路径
// EN: WALPath returns the WAL file path for a given database path.
func WALPath(dbPath string) string {
	return dbPath + ".wal"
}

// Created by Yanjunhui

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALBasic(t *testing.T) {
	// 创建临时目录
	// EN: Create a temporary directory.
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// 创建新 WAL
	// EN: Create a new WAL.
	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// 写入几条记录
	// EN: Write a few records.
	testData := []byte("test page data")
	paddedData := make([]byte, PageSize)
	copy(paddedData, testData)

	lsn1, err := wal.WritePageRecord(1, paddedData)
	if err != nil {
		t.Fatalf("Failed to write page record: %v", err)
	}
	if lsn1 != 1 {
		t.Errorf("Expected LSN 1, got %d", lsn1)
	}

	lsn2, err := wal.WriteAllocRecord(2, PageTypeData)
	if err != nil {
		t.Fatalf("Failed to write alloc record: %v", err)
	}
	if lsn2 != 2 {
		t.Errorf("Expected LSN 2, got %d", lsn2)
	}

	lsn3, err := wal.WriteFreeRecord(1)
	if err != nil {
		t.Fatalf("Failed to write free record: %v", err)
	}
	if lsn3 != 3 {
		t.Errorf("Expected LSN 3, got %d", lsn3)
	}

	// 同步
	// EN: Sync.
	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// 关闭
	// EN: Close.
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// 重新打开 WAL
	// EN: Reopen WAL.
	wal2, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// 验证 LSN 恢复
	// EN: Verify LSN recovery.
	currentLSN := wal2.GetCurrentLSN()
	if currentLSN != 4 {
		t.Errorf("Expected current LSN 4, got %d", currentLSN)
	}

	// 读取记录
	// EN: Read records.
	records, err := wal2.ReadRecordsFrom(1)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// 验证记录内容
	// EN: Verify record contents.
	if records[0].Type != WALRecordPageWrite {
		t.Errorf("Expected PageWrite record, got %d", records[0].Type)
	}
	if records[0].PageId != 1 {
		t.Errorf("Expected PageId 1, got %d", records[0].PageId)
	}

	if records[1].Type != WALRecordAllocPage {
		t.Errorf("Expected AllocPage record, got %d", records[1].Type)
	}
	if records[1].PageId != 2 {
		t.Errorf("Expected PageId 2, got %d", records[1].PageId)
	}

	if records[2].Type != WALRecordFreePage {
		t.Errorf("Expected FreePage record, got %d", records[2].Type)
	}
	if records[2].PageId != 1 {
		t.Errorf("Expected PageId 1, got %d", records[2].PageId)
	}
}

func TestWALCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// 写入一些记录
	// EN: Write some records.
	for i := 0; i < 5; i++ {
		_, err := wal.WriteAllocRecord(PageId(i), PageTypeData)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	// 创建检查点
	// EN: Create a checkpoint.
	checkpointLSN := wal.GetCurrentLSN() - 1
	if err := wal.Checkpoint(checkpointLSN); err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	// 验证检查点 LSN
	// EN: Verify checkpoint LSN.
	if wal.GetCheckpointLSN() != checkpointLSN {
		t.Errorf("Expected checkpoint LSN %d, got %d", checkpointLSN, wal.GetCheckpointLSN())
	}

	// 关闭并重新打开
	// EN: Close and reopen.
	wal.Close()

	wal2, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// 验证检查点 LSN 持久化
	// EN: Verify checkpoint LSN persisted.
	if wal2.GetCheckpointLSN() != checkpointLSN {
		t.Errorf("Expected checkpoint LSN %d after reopen, got %d", checkpointLSN, wal2.GetCheckpointLSN())
	}
}

func TestWALTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// 写入一些记录
	// EN: Write some records.
	for i := 0; i < 10; i++ {
		_, err := wal.WriteAllocRecord(PageId(i), PageTypeData)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	// 获取文件大小
	// EN: Get file size.
	fi, _ := os.Stat(walPath)
	sizeBefore := fi.Size()

	// 截断
	// EN: Truncate.
	if err := wal.Truncate(); err != nil {
		t.Fatalf("Failed to truncate WAL: %v", err)
	}

	// 验证文件大小减小
	// EN: Verify file size decreased.
	fi, _ = os.Stat(walPath)
	sizeAfter := fi.Size()

	if sizeAfter >= sizeBefore {
		t.Errorf("WAL should be smaller after truncate: before=%d, after=%d", sizeBefore, sizeAfter)
	}

	if sizeAfter != WALHeaderSize {
		t.Errorf("WAL size should be %d after truncate, got %d", WALHeaderSize, sizeAfter)
	}

	wal.Close()
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// 创建数据库并写入一些数据
	// EN: Create a database and write some data.
	pager, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	// 分配几个页面并写入数据
	// EN: Allocate a few pages and write data.
	page1, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	testData1 := []byte("page 1 data")
	page1.SetData(testData1)
	pager.MarkDirty(page1.ID())

	page2, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	testData2 := []byte("page 2 data")
	page2.SetData(testData2)
	pager.MarkDirty(page2.ID())

	// Flush 以触发 WAL 写入和检查点
	// EN: Flush to trigger WAL writes and checkpoint.
	if err := pager.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// 正常关闭
	// EN: Close normally.
	if err := pager.Close(); err != nil {
		t.Fatalf("Failed to close pager: %v", err)
	}

	// 重新打开（模拟重启）
	// EN: Reopen (simulate restart).
	pager2, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager2.Close()

	// 验证数据仍然存在
	// EN: Verify data still exists.
	readPage1, err := pager2.ReadPage(page1.ID())
	if err != nil {
		t.Fatalf("Failed to read page 1: %v", err)
	}

	readPage2, err := pager2.ReadPage(page2.ID())
	if err != nil {
		t.Fatalf("Failed to read page 2: %v", err)
	}

	// 验证数据内容
	// EN: Verify data contents.
	data1 := readPage1.Data()
	for i, b := range testData1 {
		if data1[i] != b {
			t.Errorf("Page 1 data mismatch at %d: expected %d, got %d", i, b, data1[i])
		}
	}

	data2 := readPage2.Data()
	for i, b := range testData2 {
		if data2[i] != b {
			t.Errorf("Page 2 data mismatch at %d: expected %d, got %d", i, b, data2[i])
		}
	}
}

func TestPagerWithoutWAL(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_no_wal.db")

	// 创建不带 WAL 的 pager
	// EN: Create a pager without WAL.
	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager without WAL: %v", err)
	}

	// 分配页面
	// EN: Allocate a page.
	page, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	testData := []byte("no wal test")
	page.SetData(testData)
	pager.MarkDirty(page.ID())

	// Flush
	if err := pager.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// 验证没有创建 WAL 文件
	// EN: Verify no WAL file was created.
	walPath := WALPath(dbPath)
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Error("WAL file should not exist when WAL is disabled")
	}

	pager.Close()
}

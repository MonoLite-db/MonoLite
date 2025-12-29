// Created by Yanjunhui

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPagerRecovery_EnsureFileSizeRepairsTruncatedTail(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "truncate_tail.db")

	// 1) 创建数据库并分配一页（会写 WAL，并更新 header.PageCount）
	// EN: 1) Create a database and allocate a page (writes WAL and updates header.PageCount).
	p1, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("OpenPagerWithWAL failed: %v", err)
	}

	allocated, err := p1.AllocatePage(PageTypeIndex)
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	allocID := allocated.ID()

	expectedSize := int64(FileHeaderSize) + int64(p1.pageCount)*int64(PageSize)
	if expectedSize <= int64(FileHeaderSize)+int64(PageSize) {
		t.Fatalf("unexpected expectedSize=%d (pageCount=%d)", expectedSize, p1.pageCount)
	}

	// 2) 模拟“尾部半页/短写”：把数据文件截断到一个非页对齐的大小
	// EN: 2) Simulate a partial tail/short write: truncate the file to a non-page-aligned size.
	truncatedSize := expectedSize - 100
	if err := p1.file.Truncate(truncatedSize); err != nil {
		t.Fatalf("failed to truncate db file: %v", err)
	}

	// 3) 模拟崩溃：绕过 Flush/Close，直接关 fd（WAL 在 AllocatePage 中已 Sync，因此日志应可用于恢复）
	// EN: 3) Simulate a crash: bypass Flush/Close and close the FD directly (WAL is synced in AllocatePage, so recovery should work).
	if p1.wal != nil && p1.wal.file != nil {
		_ = p1.wal.file.Close()
	}
	if p1.file != nil {
		_ = p1.file.Close()
	}

	// 4) 重启：recover() 应调用 ensureFileSize() 修复尾部短写，使页可被正常读取
	// EN: 4) Restart: recover() should call ensureFileSize() to repair the truncated tail so the page can be read.
	p2, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer p2.Close()

	fi, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat db failed: %v", err)
	}
	if fi.Size() != expectedSize {
		t.Fatalf("file size not repaired: got=%d want=%d", fi.Size(), expectedSize)
	}

	page, err := p2.ReadPage(allocID)
	if err != nil {
		t.Fatalf("ReadPage failed after recovery: %v", err)
	}
	if page.Type() != PageTypeIndex {
		t.Fatalf("recovered page type mismatch: got=%d want=%d", page.Type(), PageTypeIndex)
	}
}

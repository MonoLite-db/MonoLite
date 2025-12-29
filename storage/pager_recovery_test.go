// Created by Yanjunhui

package storage

import (
	"path/filepath"
	"testing"
)

// 这个用例专门覆盖一个容易遗漏的崩溃窗口：
// - 从 free list 复用页时，AllocatePage 会先写 WAL（alloc + meta），再更新 header，但不会立刻把“新页类型”写入数据文件；
// - 如果此时崩溃，恢复时必须确保该页不会仍然以 PageTypeFree 的形式存在（否则上层会把“已分配的页”当成 free 页读出来）。
func TestPagerRecovery_AllocateFromFreeListCrashDoesNotLeaveFreePage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// 1) 创建数据库并分配一个页，确保物理文件里真实存在该页。
	p1, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("OpenPagerWithWAL failed: %v", err)
	}
	page, err := p1.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	freeID := page.ID()
	if err := p1.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := p1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2) 释放该页，进入 free list，并刷盘确保其在物理文件中被标记为 free。
	p2, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	if err := p2.FreePage(freeID); err != nil {
		t.Fatalf("FreePage failed: %v", err)
	}
	if err := p2.Flush(); err != nil {
		t.Fatalf("Flush after FreePage failed: %v", err)
	}
	if err := p2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 3) 再次打开并从 free list 复用该页，然后立刻“模拟崩溃”（不调用 Flush/Close）。
	p3, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("reopen for crash simulation failed: %v", err)
	}
	reused, err := p3.AllocatePage(PageTypeIndex)
	if err != nil {
		t.Fatalf("AllocatePage (from free list) failed: %v", err)
	}
	if reused.ID() != freeID {
		t.Fatalf("expected AllocatePage to reuse free page %d, got %d", freeID, reused.ID())
	}

	// 模拟进程崩溃：直接关闭 fd，绕过 Pager.Close()/Flush()。
	// 注意：AllocatePage 内部已经对 WAL 做过 Sync()，因此 WAL 记录应可用于恢复。
	if p3.wal != nil && p3.wal.file != nil {
		_ = p3.wal.file.Close()
	}
	if p3.file != nil {
		_ = p3.file.Close()
	}

	// 4) 重启打开，触发 recover()，验证复用页不应仍然是 PageTypeFree。
	p4, err := OpenPagerWithWAL(dbPath, true)
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	defer p4.Close()

	got, err := p4.ReadPage(freeID)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	if got.Type() == PageTypeFree {
		t.Fatalf("recovered page %d is still PageTypeFree; expected it to be allocated (type=%d)", freeID, PageTypeIndex)
	}
	if got.Type() != PageTypeIndex {
		t.Fatalf("recovered page %d has unexpected type: got=%d, want=%d", freeID, got.Type(), PageTypeIndex)
	}
	if p4.header != nil && p4.header.FreeListHead == freeID {
		t.Fatalf("FreeListHead still points to %d after recovery; expected it to be removed from free list", freeID)
	}
}



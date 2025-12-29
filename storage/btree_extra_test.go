// Created by Yanjunhui

package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestRecordIdMarshalUnmarshalRoundtrip(t *testing.T) {
	orig := RecordId{PageId: 12345, SlotIndex: 54321}
	data := MarshalRecordId(orig)
	if len(data) != RecordIdSize {
		t.Fatalf("unexpected RecordIdSize: got=%d want=%d", len(data), RecordIdSize)
	}

	got := UnmarshalRecordId(data)
	if got != orig {
		t.Fatalf("roundtrip mismatch: got=%+v want=%+v", got, orig)
	}
}

func TestBTreeNodeNeedsSplitAndCanAccommodate(t *testing.T) {
	// 构造一个“字节大小”接近阈值的叶子节点
	n := &BTreeNode{
		PageId:   1,
		IsLeaf:   true,
		KeyCount: 0,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Next:     0,
		Prev:     0,
	}

	key := bytes.Repeat([]byte("k"), 200)
	val := bytes.Repeat([]byte("v"), 50)

	// 先验证在空节点里 CanAccommodate 应该能放下
	if !n.CanAccommodate(key, val) {
		t.Fatalf("expected empty node to accommodate one entry")
	}

	// 不断塞入直到 NeedsSplit 变为 true（上限给个保护，避免死循环）
	for i := 0; i < 1000 && !n.NeedsSplit(); i++ {
		n.Keys = append(n.Keys, append([]byte(nil), key...))
		n.Values = append(n.Values, append([]byte(nil), val...))
		n.KeyCount++
	}

	if !n.NeedsSplit() {
		t.Fatalf("expected node to need split after many inserts (byte-driven)")
	}

	// NeedsSplit 为 true 时，再加同样的 entry 大概率无法容纳（至少应逼近上限）
	_ = n.CanAccommodate(key, val)
}

func TestBTreeOpenBTreeAndName(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "openbtree.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("OpenPagerWithWAL failed: %v", err)
	}
	defer pager.Close()

	tree1, err := NewBTree(pager, "idx_name", false)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}

	tree2 := OpenBTree(pager, tree1.RootPage(), "idx_name_2", true)
	if tree2.RootPage() != tree1.RootPage() {
		t.Fatalf("RootPage mismatch: got=%d want=%d", tree2.RootPage(), tree1.RootPage())
	}
	if tree2.Name() != "idx_name_2" {
		t.Fatalf("Name mismatch: got=%q want=%q", tree2.Name(), "idx_name_2")
	}
}

func TestBTreeSearchRangeLimitSkip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "skip.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("OpenPagerWithWAL failed: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "skip_index", false)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}

	// 插入足够多的数据，确保会跨多个叶子节点（便于覆盖链表遍历）
	n := 300
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		val := []byte(fmt.Sprintf("v%05d", i))
		if err := tree.Insert(key, val); err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// limit<=0 分支
	gotNil, err := tree.SearchRangeLimitSkip(10, 0)
	if err != nil {
		t.Fatalf("SearchRangeLimitSkip failed: %v", err)
	}
	if gotNil != nil {
		t.Fatalf("expected nil when limit<=0, got len=%d", len(gotNil))
	}

	skip := 120
	limit := 25
	got, err := tree.SearchRangeLimitSkip(skip, limit)
	if err != nil {
		t.Fatalf("SearchRangeLimitSkip failed: %v", err)
	}
	if len(got) != limit {
		t.Fatalf("result count mismatch: got=%d want=%d", len(got), limit)
	}

	for i := 0; i < limit; i++ {
		want := []byte(fmt.Sprintf("v%05d", skip+i))
		if !bytes.Equal(got[i], want) {
			t.Fatalf("value mismatch at %d: got=%q want=%q", i, string(got[i]), string(want))
		}
	}
}



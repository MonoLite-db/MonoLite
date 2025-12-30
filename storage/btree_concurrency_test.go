// Created by Yanjunhui

package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestBTreeConcurrentInsert_VerifyAndCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "btree_concurrent.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("OpenPagerWithWAL failed: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "concurrent_index", false)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}

	const goroutines = 8
	const perG = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// 构造唯一 key，避免重复键导致语义歧义
				// EN: Use unique keys to avoid ambiguity from duplicate keys.
				key := []byte(fmt.Sprintf("g%02d-k%04d", g, i))
				val := []byte(fmt.Sprintf("v%02d-%04d", g, i))
				if err := tree.Insert(key, val); err != nil {
					t.Errorf("Insert failed (g=%d i=%d): %v", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// 验证树结构不应被并发写入破坏
	// EN: Verify tree structure should not be corrupted by concurrent inserts.
	if err := tree.Verify(); err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	// 校验键数量
	// EN: Verify total key count.
	got, err := tree.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	want := goroutines * perG
	if got != want {
		t.Fatalf("Count mismatch: got=%d want=%d", got, want)
	}
}

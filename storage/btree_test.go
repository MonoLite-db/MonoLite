// Created by Yanjunhui

package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestBTreeBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "test_index", false)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// 插入测试数据
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// 验证树结构
	if err := tree.Verify(); err != nil {
		t.Fatalf("Tree verification failed: %v", err)
	}

	// 搜索测试
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		expectedValue := []byte(fmt.Sprintf("value%03d", i))
		value, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", i, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value mismatch for key %d: expected %s, got %s", i, expectedValue, value)
		}
	}

	// 计数测试
	count, err := tree.Count()
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 100 {
		t.Errorf("Count mismatch: expected 100, got %d", count)
	}
}

func TestBTreeDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_delete.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "test_index", false)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// 插入数据
	n := 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// 删除一半的键
	for i := 0; i < n; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := tree.Delete(key); err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// 验证树结构
	if err := tree.Verify(); err != nil {
		t.Fatalf("Tree verification failed after delete: %v", err)
	}

	// 验证删除成功
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value, _ := tree.Search(key)
		if i%2 == 0 {
			// 应该被删除
			if value != nil {
				t.Errorf("Key %d should be deleted but found", i)
			}
		} else {
			// 应该存在
			if value == nil {
				t.Errorf("Key %d should exist but not found", i)
			}
		}
	}

	// 计数
	count, _ := tree.Count()
	if count != n/2 {
		t.Errorf("Count mismatch after delete: expected %d, got %d", n/2, count)
	}
}

func TestBTreeRangeSearch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_range.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "test_index", false)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// 插入数据
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("%03d", i))
		value := []byte(fmt.Sprintf("v%03d", i))
		tree.Insert(key, value)
	}

	// 范围查询 [020, 030]
	results, err := tree.SearchRange([]byte("020"), []byte("030"), true, true)
	if err != nil {
		t.Fatalf("Range search failed: %v", err)
	}

	if len(results) != 11 {
		t.Errorf("Range search returned wrong count: expected 11, got %d", len(results))
	}

	// 范围查询 (020, 030)
	results, err = tree.SearchRange([]byte("020"), []byte("030"), false, false)
	if err != nil {
		t.Fatalf("Range search failed: %v", err)
	}

	if len(results) != 9 {
		t.Errorf("Range search returned wrong count: expected 9, got %d", len(results))
	}
}

func TestBTreeUnique(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_unique.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "unique_index", true)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	key := []byte("unique_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 第一次插入应该成功
	if err := tree.Insert(key, value1); err != nil {
		t.Fatalf("First insert should succeed: %v", err)
	}

	// 第二次插入相同键应该失败
	if err := tree.Insert(key, value2); err == nil {
		t.Error("Second insert of duplicate key should fail")
	}
}

func TestBTreeLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_large.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "large_index", false)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	n := 10000

	// 随机顺序插入
	keys := make([]int, n)
	for i := 0; i < n; i++ {
		keys[i] = i
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, k := range keys {
		key := []byte(fmt.Sprintf("key%06d", k))
		value := []byte(fmt.Sprintf("value%06d", k))
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", k, err)
		}
	}

	// 验证
	if err := tree.Verify(); err != nil {
		t.Fatalf("Tree verification failed: %v", err)
	}

	count, _ := tree.Count()
	if count != n {
		t.Errorf("Count mismatch: expected %d, got %d", n, count)
	}

	// 获取高度
	height, _ := tree.Height()
	t.Logf("Tree height for %d keys: %d", n, height)

	// 随机删除一半
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i := 0; i < n/2; i++ {
		key := []byte(fmt.Sprintf("key%06d", keys[i]))
		if err := tree.Delete(key); err != nil {
			t.Fatalf("Failed to delete key %d: %v", keys[i], err)
		}
	}

	// 再次验证
	if err := tree.Verify(); err != nil {
		t.Fatalf("Tree verification failed after delete: %v", err)
	}

	count, _ = tree.Count()
	if count != n/2 {
		t.Errorf("Count mismatch after delete: expected %d, got %d", n/2, count)
	}
}

func TestBTreeLeafChain(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_chain.db")

	pager, err := OpenPagerWithWAL(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBTree(pager, "chain_index", false)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// 插入足够多的数据触发分裂
	n := 200
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		value := []byte(fmt.Sprintf("v%05d", i))
		tree.Insert(key, value)
	}

	// 验证叶子链表
	if err := tree.Verify(); err != nil {
		t.Fatalf("Leaf chain verification failed: %v", err)
	}

	// 获取所有键，验证顺序
	keys, err := tree.GetAllKeys()
	if err != nil {
		t.Fatalf("Failed to get all keys: %v", err)
	}

	if len(keys) != n {
		t.Errorf("Key count mismatch: expected %d, got %d", n, len(keys))
	}

	// 验证顺序
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Errorf("Keys not in order at index %d", i)
		}
	}
}


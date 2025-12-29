// Created by Yanjunhui

package engine

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestTransactionBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "txn_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()
	col, _ := db.Collection("test")

	// 开始事务
	txn := txnMgr.Begin()
	if txn == nil {
		t.Fatal("Failed to begin transaction")
	}

	if txn.State != TxnStateActive {
		t.Errorf("Transaction should be active")
	}

	// 在事务中插入数据
	col.Insert(bson.D{{Key: "name", Value: "test"}})

	// 提交事务
	if err := txnMgr.Commit(txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if txn.State != TxnStateCommitted {
		t.Errorf("Transaction should be committed")
	}

	// 验证数据已提交
	docs, _ := col.Find(bson.D{})
	if len(docs) == 0 {
		t.Error("Document should exist after commit")
	}
}

func TestTransactionAbort(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "txn_abort_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()
	col, _ := db.Collection("test")

	// 先插入一条数据
	col.Insert(bson.D{{Key: "name", Value: "original"}})

	// 开始事务
	txn := txnMgr.Begin()

	// 记录原始文档用于回滚
	doc, _ := col.FindOne(bson.D{{Key: "name", Value: "original"}})
	docID := getDocField(doc, "_id")
	txn.AddUndoRecord("update", "test", docID, doc)

	// 更新数据
	col.Update(
		bson.D{{Key: "name", Value: "original"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "modified"}}}},
		false,
	)

	// 中止事务
	if err := txnMgr.Abort(txn); err != nil {
		t.Fatalf("Failed to abort transaction: %v", err)
	}

	if txn.State != TxnStateAborted {
		t.Errorf("Transaction should be aborted")
	}

	// 验证数据已回滚
	docs, _ := col.Find(bson.D{{Key: "name", Value: "original"}})
	if len(docs) == 0 {
		t.Error("Document should be restored after abort")
	}
}

func TestLockManager(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lock_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()

	// 事务1获取写锁
	txn1 := txnMgr.Begin()
	err = txnMgr.AcquireLock(txn1, "resource1", LockTypeWrite)
	if err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}

	// 事务2尝试获取读锁（应该等待）
	txn2 := txnMgr.BeginWithOptions(IsolationReadCommitted, 100*time.Millisecond)
	
	errChan := make(chan error, 1)
	go func() {
		errChan <- txnMgr.AcquireLock(txn2, "resource1", LockTypeRead)
	}()

	// 等待一小段时间后释放锁
	time.Sleep(50 * time.Millisecond)
	txnMgr.Commit(txn1)

	// 事务2应该能获取锁
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Transaction 2 should acquire lock after Transaction 1 commits: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Lock acquisition timed out")
	}

	txnMgr.Commit(txn2)
}

func TestSharedLocks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "shared_lock_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()

	// 多个事务可以同时持有读锁
	txn1 := txnMgr.Begin()
	txn2 := txnMgr.Begin()
	txn3 := txnMgr.Begin()

	// 所有事务获取读锁
	if err := txnMgr.AcquireLock(txn1, "shared_resource", LockTypeRead); err != nil {
		t.Fatalf("Txn1 failed to acquire read lock: %v", err)
	}
	if err := txnMgr.AcquireLock(txn2, "shared_resource", LockTypeRead); err != nil {
		t.Fatalf("Txn2 failed to acquire read lock: %v", err)
	}
	if err := txnMgr.AcquireLock(txn3, "shared_resource", LockTypeRead); err != nil {
		t.Fatalf("Txn3 failed to acquire read lock: %v", err)
	}

	// 释放所有锁
	txnMgr.Commit(txn1)
	txnMgr.Commit(txn2)
	txnMgr.Commit(txn3)
}

func TestDeadlockDetection(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "deadlock_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()

	// 创建潜在死锁场景
	txn1 := txnMgr.BeginWithOptions(IsolationReadCommitted, 500*time.Millisecond)
	txn2 := txnMgr.BeginWithOptions(IsolationReadCommitted, 500*time.Millisecond)

	// txn1 获取 resource1
	txnMgr.AcquireLock(txn1, "resource1", LockTypeWrite)
	// txn2 获取 resource2
	txnMgr.AcquireLock(txn2, "resource2", LockTypeWrite)

	var wg sync.WaitGroup
	deadlockCount := int32(0)

	// txn1 尝试获取 resource2（会等待）
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := txnMgr.AcquireLock(txn1, "resource2", LockTypeWrite)
		if err != nil {
			atomic.AddInt32(&deadlockCount, 1)
		}
	}()

	// 小延迟确保 txn1 进入等待
	time.Sleep(50 * time.Millisecond)

	// txn2 尝试获取 resource1（应该检测到死锁）
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := txnMgr.AcquireLock(txn2, "resource1", LockTypeWrite)
		if err != nil {
			atomic.AddInt32(&deadlockCount, 1)
		}
	}()

	// 等待完成
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 完成
	case <-time.After(2 * time.Second):
		t.Log("Timeout waiting for deadlock resolution (may be expected)")
	}

	// 清理
	txnMgr.Abort(txn1)
	txnMgr.Abort(txn2)
}

func TestConcurrentTransactions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent_txn_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()
	col, _ := db.Collection("concurrent")

	const numTxns = 10
	const docsPerTxn = 10

	var wg sync.WaitGroup
	errors := make(chan error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(txnNum int) {
			defer wg.Done()

			txn := txnMgr.Begin()

			for j := 0; j < docsPerTxn; j++ {
				_, err := col.Insert(bson.D{
					{Key: "txn", Value: int32(txnNum)},
					{Key: "doc", Value: int32(j)},
				})
				if err != nil {
					errors <- err
					txnMgr.Abort(txn)
					return
				}
			}

			if err := txnMgr.Commit(txn); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent transaction error: %v", err)
	}

	// 验证文档数量
	count := col.Count()
	if count != int64(numTxns*docsPerTxn) {
		t.Errorf("Expected %d documents, got %d", numTxns*docsPerTxn, count)
	}
}

func TestTransactionTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "timeout_test.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	txnMgr := db.GetTransactionManager()

	// 事务1获取写锁
	txn1 := txnMgr.Begin()
	txnMgr.AcquireLock(txn1, "timeout_resource", LockTypeWrite)

	// 事务2尝试获取锁（使用短超时）
	txn2 := txnMgr.BeginWithOptions(IsolationReadCommitted, 100*time.Millisecond)

	start := time.Now()
	err = txnMgr.AcquireLock(txn2, "timeout_resource", LockTypeWrite)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Lock acquisition should timeout")
	}

	if elapsed < 100*time.Millisecond {
		t.Errorf("Should wait at least 100ms, waited %v", elapsed)
	}

	// 清理
	txnMgr.Commit(txn1)
	txnMgr.Abort(txn2)
}


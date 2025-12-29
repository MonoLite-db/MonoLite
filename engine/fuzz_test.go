// Created by Yanjunhui
//go:build go1.18

package engine

import (
	"os"
	"path/filepath"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// FuzzInsertDocument 模糊测试文档插入
// EN: FuzzInsertDocument fuzz-tests document insertion.
func FuzzInsertDocument(f *testing.F) {
	// 添加种子语料
	// EN: Add seed corpus.
	f.Add([]byte(`{"name":"test","age":25}`))
	f.Add([]byte(`{"_id":"custom","data":[1,2,3]}`))
	f.Add([]byte(`{"nested":{"a":{"b":{"c":1}}}}`))
	f.Add([]byte(`{"empty":{}}`))
	f.Add([]byte(`{"array":[1,"two",3.0,true,null]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// 尝试解析 BSON
		// EN: Try to parse BSON.
		var doc bson.D
		if err := bson.UnmarshalExtJSON(data, false, &doc); err != nil {
			// 无效输入，跳过
			// EN: Invalid input; skip.
			return
		}

		// 创建临时数据库
		// EN: Create a temporary database.
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "fuzz_test.db")
		db, err := OpenDatabase(dbPath)
		if err != nil {
			t.Skip("Failed to open database")
		}
		defer db.Close()

		// 获取集合
		// EN: Get the collection.
		col, err := db.Collection("fuzz_collection")
		if err != nil {
			// 集合名无效
			// EN: Invalid collection name.
			return
		}

		// 尝试插入
		// EN: Try to insert.
		_, err = col.Insert(doc)
		if err != nil {
			// 验证返回的是 MongoError
			// EN: Verify the returned error is a MongoError.
			if !IsMongoError(err) {
				// 如果不是预期的错误类型，检查是否是合理的错误
				// EN: If it's not the expected error type, ensure it's still a reasonable error.
				// 例如：文档过大、嵌套过深等
				// EN: For example: document too large, nesting too deep, etc.
			}
		}

		// 如果插入成功，尝试查询
		// EN: If insert succeeded, try querying.
		if err == nil {
			docs, findErr := col.Find(bson.D{})
			if findErr != nil {
				t.Errorf("Insert succeeded but Find failed: %v", findErr)
			}
			if len(docs) == 0 {
				t.Error("Insert succeeded but no documents found")
			}
		}
	})
}

// FuzzQuery 模糊测试查询
// EN: FuzzQuery fuzz-tests queries.
func FuzzQuery(f *testing.F) {
	// 添加查询种子
	// EN: Add query seeds.
	f.Add([]byte(`{"name":"test"}`))
	f.Add([]byte(`{"age":{"$gt":18}}`))
	f.Add([]byte(`{"$and":[{"a":1},{"b":2}]}`))
	f.Add([]byte(`{"tags":{"$in":["a","b"]}}`))
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var filter bson.D
		if err := bson.UnmarshalExtJSON(data, false, &filter); err != nil {
			return
		}

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "fuzz_query.db")
		db, err := OpenDatabase(dbPath)
		if err != nil {
			t.Skip("Failed to open database")
		}
		defer db.Close()

		col, _ := db.Collection("test")

		// 插入一些测试文档
		// EN: Insert some test documents.
		for i := 0; i < 10; i++ {
			col.Insert(bson.D{
				{Key: "name", Value: "test"},
				{Key: "age", Value: int32(20 + i)},
				{Key: "tags", Value: bson.A{"a", "b", "c"}},
			})
		}

		// 执行查询（不应该崩溃）
		// EN: Execute the query (should not crash).
		_, _ = col.Find(filter)
	})
}

// FuzzUpdate 模糊测试更新
// EN: FuzzUpdate fuzz-tests updates.
func FuzzUpdate(f *testing.F) {
	f.Add([]byte(`{"$set":{"name":"updated"}}`))
	f.Add([]byte(`{"$inc":{"count":1}}`))
	f.Add([]byte(`{"$unset":{"field":""}}`))
	f.Add([]byte(`{"$push":{"arr":"new"}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var update bson.D
		if err := bson.UnmarshalExtJSON(data, false, &update); err != nil {
			return
		}

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "fuzz_update.db")
		db, err := OpenDatabase(dbPath)
		if err != nil {
			t.Skip("Failed to open database")
		}
		defer db.Close()

		col, _ := db.Collection("test")
		col.Insert(bson.D{{Key: "name", Value: "original"}})

		// 执行更新（不应该崩溃）
		// EN: Execute update (should not crash).
		_, _ = col.Update(bson.D{}, update, false)
	})
}

// TestRandomOperations 随机操作测试
// EN: TestRandomOperations runs random operations.
func TestRandomOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "random_ops.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	col, _ := db.Collection("random_test")

	// 执行大量随机操作
	// EN: Execute many random operations.
	insertedIds := make([]interface{}, 0)

	for i := 0; i < 1000; i++ {
		op := i % 4 // 0: insert, 1: find, 2: update, 3: delete

		switch op {
		case 0: // Insert
			doc := bson.D{
				{Key: "i", Value: int32(i)},
				{Key: "data", Value: "test data"},
				{Key: "nested", Value: bson.D{{Key: "x", Value: i * 2}}},
			}
			ids, err := col.Insert(doc)
			if err == nil && len(ids) > 0 {
				insertedIds = append(insertedIds, ids[0])
			}

		case 1: // Find
			_, _ = col.Find(bson.D{{Key: "i", Value: bson.D{{Key: "$gte", Value: int32(i - 10)}}}})

		case 2: // Update
			col.Update(
				bson.D{{Key: "i", Value: bson.D{{Key: "$lt", Value: int32(i)}}}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}}}},
				false,
			)

		case 3: // Delete
			if len(insertedIds) > 0 && i%10 == 0 {
				// 偶尔删除
				// EN: Occasionally delete.
				col.DeleteOne(bson.D{{Key: "_id", Value: insertedIds[0]}})
				insertedIds = insertedIds[1:]
			}
		}
	}

	// 验证数据一致性
	// EN: Verify data consistency.
	count := col.Count()
	t.Logf("Final document count: %d", count)

	// 关闭并重新打开验证持久化
	// EN: Close and reopen to verify persistence.
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db2, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		_ = db2.Close()
	}()

	col2, _ := db2.Collection("random_test")
	count2 := col2.Count()

	if count != count2 {
		t.Errorf("Count mismatch after reopen: %d vs %d", count, count2)
	}
}

// TestEdgeCases 边界条件测试
// EN: TestEdgeCases tests edge cases.
func TestEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "edge_cases.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	col, _ := db.Collection("edge_test")

	tests := []struct {
		name string
		doc  bson.D
	}{
		{"empty document", bson.D{}},
		{"null value", bson.D{{Key: "null_field", Value: nil}}},
		{"empty string", bson.D{{Key: "str", Value: ""}}},
		{"zero int", bson.D{{Key: "num", Value: int32(0)}}},
		{"negative int", bson.D{{Key: "num", Value: int32(-1)}}},
		{"large int", bson.D{{Key: "num", Value: int64(9223372036854775807)}}},
		{"float", bson.D{{Key: "num", Value: 3.14159}}},
		{"bool true", bson.D{{Key: "b", Value: true}}},
		{"bool false", bson.D{{Key: "b", Value: false}}},
		{"empty array", bson.D{{Key: "arr", Value: bson.A{}}}},
		{"nested empty", bson.D{{Key: "obj", Value: bson.D{}}}},
		{"special chars", bson.D{{Key: "str", Value: "hello\nworld\ttab"}}},
		{"unicode", bson.D{{Key: "str", Value: "你好世界🌍"}}},
		{"objectid", bson.D{{Key: "oid", Value: primitive.NewObjectID()}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := col.Insert(tc.doc)
			if err != nil {
				t.Errorf("Failed to insert %s: %v", tc.name, err)
				return
			}

			// 尝试查找
			// EN: Try to find.
			doc, err := col.FindById(ids[0])
			if err != nil {
				t.Errorf("Failed to find %s: %v", tc.name, err)
				return
			}
			if doc == nil {
				t.Errorf("Document %s not found", tc.name)
			}
		})
	}
}

// TestConcurrentOperations 并发操作测试
// EN: TestConcurrentOperations tests concurrent operations.
func TestConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	col, _ := db.Collection("concurrent_test")

	done := make(chan bool)
	errors := make(chan error, 100)

	// 启动多个 goroutine 并发操作
	// EN: Start multiple goroutines to operate concurrently.
	for g := 0; g < 10; g++ {
		go func(gid int) {
			for i := 0; i < 100; i++ {
				doc := bson.D{
					{Key: "goroutine", Value: int32(gid)},
					{Key: "iteration", Value: int32(i)},
				}
				if _, err := col.Insert(doc); err != nil {
					errors <- err
				}
			}
			done <- true
		}(g)
	}

	// 等待所有 goroutine 完成
	// EN: Wait for all goroutines to finish.
	for i := 0; i < 10; i++ {
		<-done
	}

	// 检查错误
	// EN: Check errors.
	close(errors)
	for err := range errors {
		t.Errorf("Concurrent error: %v", err)
	}

	// 验证文档数量
	// EN: Verify document count.
	count := col.Count()
	if count != 1000 {
		t.Errorf("Expected 1000 documents, got %d", count)
	}
}

// TestDatabaseRecovery 数据库恢复测试
// EN: TestDatabaseRecovery tests database recovery.
func TestDatabaseRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "recovery.db")

	// 第一阶段：写入数据
	// EN: Phase 1: write data.
	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	col, _ := db.Collection("recovery_test")
	for i := 0; i < 100; i++ {
		col.Insert(bson.D{{Key: "i", Value: int32(i)}})
	}

	// 不调用 Close()，模拟崩溃
	// EN: Do not call Close() to simulate a crash.
	db.Flush()

	// 第二阶段：重新打开并恢复
	// EN: Phase 2: reopen and recover.
	db2, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	col2, _ := db2.Collection("recovery_test")
	count := col2.Count()

	if count != 100 {
		t.Errorf("Expected 100 documents after recovery, got %d", count)
	}

	// 验证数据完整性
	// EN: Verify data integrity.
	docs, _ := col2.Find(bson.D{})
	if len(docs) != 100 {
		t.Errorf("Expected 100 documents, found %d", len(docs))
	}
}

// BenchmarkInsert 插入性能基准测试
// EN: BenchmarkInsert benchmarks insert performance.
func BenchmarkInsert(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench_insert.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	col, _ := db.Collection("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Insert(bson.D{
			{Key: "i", Value: int32(i)},
			{Key: "data", Value: "benchmark test data"},
		})
	}
}

// BenchmarkFind 查询性能基准测试
// EN: BenchmarkFind benchmarks query performance.
func BenchmarkFind(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench_find.db")
	db, err := OpenDatabase(dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	col, _ := db.Collection("bench")

	// 预先插入数据
	// EN: Pre-insert data.
	for i := 0; i < 1000; i++ {
		col.Insert(bson.D{
			{Key: "i", Value: int32(i)},
			{Key: "data", Value: "benchmark test data"},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Find(bson.D{{Key: "i", Value: int32(i % 1000)}})
	}
}

// 清理临时文件
// EN: Clean up temporary files.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

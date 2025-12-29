// Created by Yanjunhui

package engine

import (
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestCursorManagerBasic(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Stop()

	// 创建测试文档
	// EN: Create test documents.
	docs := []bson.D{
		{{Key: "a", Value: 1}},
		{{Key: "a", Value: 2}},
		{{Key: "a", Value: 3}},
		{{Key: "a", Value: 4}},
		{{Key: "a", Value: 5}},
	}

	// 创建游标
	// EN: Create a cursor.
	cursor := cm.CreateCursor("test.collection", docs, 2)
	if cursor == nil {
		t.Fatal("创建游标失败")
	}

	if cursor.ID == 0 {
		t.Error("游标 ID 不应该为 0")
	}

	if cm.Count() != 1 {
		t.Errorf("期望 1 个游标，实际 %d 个", cm.Count())
	}
}

func TestCursorGetMore(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Stop()

	// 创建 10 个文档
	// EN: Create 10 documents.
	docs := make([]bson.D, 10)
	for i := 0; i < 10; i++ {
		docs[i] = bson.D{{Key: "index", Value: i}}
	}

	cursor := cm.CreateCursor("test.collection", docs, 3)
	cursorID := cursor.ID

	// 第一次 GetMore：获取 3 个
	// EN: First GetMore: fetch 3.
	batch1, hasMore1, err := cm.GetMore(cursorID, 3)
	if err != nil {
		t.Fatalf("GetMore 失败: %v", err)
	}
	if len(batch1) != 3 {
		t.Errorf("第一批期望 3 个，实际 %d 个", len(batch1))
	}
	if !hasMore1 {
		t.Error("应该还有更多文档")
	}

	// 第二次 GetMore：获取 3 个
	// EN: Second GetMore: fetch 3.
	batch2, hasMore2, err := cm.GetMore(cursorID, 3)
	if err != nil {
		t.Fatalf("GetMore 失败: %v", err)
	}
	if len(batch2) != 3 {
		t.Errorf("第二批期望 3 个，实际 %d 个", len(batch2))
	}
	if !hasMore2 {
		t.Error("应该还有更多文档")
	}

	// 第三次 GetMore：获取 3 个（但只剩 4 个）
	// EN: Third GetMore: fetch 3 (but only 4 remain).
	batch3, hasMore3, err := cm.GetMore(cursorID, 3)
	if err != nil {
		t.Fatalf("GetMore 失败: %v", err)
	}
	if len(batch3) != 3 {
		t.Errorf("第三批期望 3 个，实际 %d 个", len(batch3))
	}
	if !hasMore3 {
		t.Error("应该还有更多文档")
	}

	// 第四次 GetMore：获取最后 1 个
	// EN: Fourth GetMore: fetch the last 1.
	batch4, hasMore4, err := cm.GetMore(cursorID, 3)
	if err != nil {
		t.Fatalf("GetMore 失败: %v", err)
	}
	if len(batch4) != 1 {
		t.Errorf("第四批期望 1 个，实际 %d 个", len(batch4))
	}
	if hasMore4 {
		t.Error("不应该还有更多文档")
	}

	// 游标应该已被删除
	// EN: The cursor should have been removed.
	if cm.Count() != 0 {
		t.Errorf("期望 0 个游标，实际 %d 个", cm.Count())
	}
}

func TestCursorKill(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Stop()

	docs := []bson.D{
		{{Key: "a", Value: 1}},
		{{Key: "a", Value: 2}},
	}

	cursor := cm.CreateCursor("test.collection", docs, 1)
	cursorID := cursor.ID

	if cm.Count() != 1 {
		t.Errorf("期望 1 个游标，实际 %d 个", cm.Count())
	}

	// 关闭游标
	// EN: Kill the cursor.
	killed := cm.KillCursor(cursorID)
	if !killed {
		t.Error("关闭游标应该返回 true")
	}

	if cm.Count() != 0 {
		t.Errorf("期望 0 个游标，实际 %d 个", cm.Count())
	}

	// 再次关闭应该返回 false
	// EN: Killing again should return false.
	killed = cm.KillCursor(cursorID)
	if killed {
		t.Error("再次关闭游标应该返回 false")
	}
}

func TestCursorKillMultiple(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Stop()

	docs := []bson.D{{{Key: "a", Value: 1}}}

	cursor1 := cm.CreateCursor("test.collection", docs, 1)
	cursor2 := cm.CreateCursor("test.collection", docs, 1)
	cursor3 := cm.CreateCursor("test.collection", docs, 1)

	if cm.Count() != 3 {
		t.Errorf("期望 3 个游标，实际 %d 个", cm.Count())
	}

	// 关闭两个游标
	// EN: Kill two cursors.
	killed := cm.KillCursors([]int64{cursor1.ID, cursor3.ID})
	if len(killed) != 2 {
		t.Errorf("期望关闭 2 个游标，实际 %d 个", len(killed))
	}

	if cm.Count() != 1 {
		t.Errorf("期望 1 个游标，实际 %d 个", cm.Count())
	}

	// cursor2 应该还在
	// EN: cursor2 should still exist.
	if cm.GetCursor(cursor2.ID) == nil {
		t.Error("cursor2 应该还存在")
	}
}

func TestGetFirstBatch(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Stop()

	// 测试所有文档都能在首批返回的情况
	// EN: Case where all documents fit in the first batch.
	docs := []bson.D{
		{{Key: "a", Value: 1}},
		{{Key: "a", Value: 2}},
		{{Key: "a", Value: 3}},
	}

	firstBatch, cursorID := cm.GetFirstBatch("test.collection", docs, 10)
	if len(firstBatch) != 3 {
		t.Errorf("期望 3 个文档，实际 %d 个", len(firstBatch))
	}
	if cursorID != 0 {
		t.Error("所有文档都返回时，cursorID 应该为 0")
	}

	// 测试需要游标的情况
	// EN: Case where a cursor is needed.
	firstBatch2, cursorID2 := cm.GetFirstBatch("test.collection", docs, 2)
	if len(firstBatch2) != 2 {
		t.Errorf("期望 2 个文档，实际 %d 个", len(firstBatch2))
	}
	if cursorID2 == 0 {
		t.Error("还有剩余文档时，cursorID 不应该为 0")
	}

	// 获取剩余文档
	// EN: Fetch remaining documents.
	remaining, hasMore, _ := cm.GetMore(cursorID2, 10)
	if len(remaining) != 1 {
		t.Errorf("期望 1 个剩余文档，实际 %d 个", len(remaining))
	}
	if hasMore {
		t.Error("不应该还有更多文档")
	}
}

func TestGetMoreCommandIntegration(t *testing.T) {
	db, cleanup := setupTestDBForCursor(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入 20 个文档
	// EN: Insert 20 documents.
	for i := 0; i < 20; i++ {
		col.Insert(bson.D{{Key: "index", Value: i}})
	}

	// 使用 batchSize=5 执行 find
	// EN: Run find with batchSize=5.
	cmd := bson.D{
		{Key: "find", Value: "test"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: int32(5)},
		{Key: "$db", Value: "testdb"},
	}

	result, err := db.RunCommand(cmd)
	if err != nil {
		t.Fatalf("find 命令失败: %v", err)
	}

	cursor := getDocField(result, "cursor").(bson.D)
	cursorID := getDocField(cursor, "id").(int64)
	firstBatch := getDocField(cursor, "firstBatch").(bson.A)

	if len(firstBatch) != 5 {
		t.Errorf("期望首批 5 个文档，实际 %d 个", len(firstBatch))
	}

	if cursorID == 0 {
		t.Error("应该返回有效的 cursorID")
	}

	// 使用 getMore 获取更多
	// EN: Use getMore to fetch more.
	getMoreCmd := bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "test"},
		{Key: "batchSize", Value: int32(10)},
	}

	result2, err := db.RunCommand(getMoreCmd)
	if err != nil {
		t.Fatalf("getMore 命令失败: %v", err)
	}

	cursor2 := getDocField(result2, "cursor").(bson.D)
	nextBatch := getDocField(cursor2, "nextBatch").(bson.A)

	if len(nextBatch) != 10 {
		t.Errorf("期望第二批 10 个文档，实际 %d 个", len(nextBatch))
	}

	// 继续获取剩余 5 个
	// EN: Continue to fetch the remaining 5.
	cursorID2 := getDocField(cursor2, "id").(int64)
	getMoreCmd2 := bson.D{
		{Key: "getMore", Value: cursorID2},
		{Key: "collection", Value: "test"},
	}

	result3, err := db.RunCommand(getMoreCmd2)
	if err != nil {
		t.Fatalf("getMore 命令失败: %v", err)
	}

	cursor3 := getDocField(result3, "cursor").(bson.D)
	nextBatch2 := getDocField(cursor3, "nextBatch").(bson.A)
	cursorID3 := getDocField(cursor3, "id").(int64)

	if len(nextBatch2) != 5 {
		t.Errorf("期望最后 5 个文档，实际 %d 个", len(nextBatch2))
	}

	if cursorID3 != 0 {
		t.Error("所有文档返回后，cursorID 应该为 0")
	}
}

func TestKillCursorsCommand(t *testing.T) {
	db, cleanup := setupTestDBForCursor(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入文档
	// EN: Insert documents.
	for i := 0; i < 10; i++ {
		col.Insert(bson.D{{Key: "index", Value: i}})
	}

	// 创建游标
	// EN: Create a cursor.
	cmd := bson.D{
		{Key: "find", Value: "test"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: int32(2)},
	}

	result, _ := db.RunCommand(cmd)
	cursor := getDocField(result, "cursor").(bson.D)
	cursorID := getDocField(cursor, "id").(int64)

	if cursorID == 0 {
		t.Fatal("应该创建游标")
	}

	// 关闭游标
	// EN: Kill the cursor.
	killCmd := bson.D{
		{Key: "killCursors", Value: "test"},
		{Key: "cursors", Value: bson.A{cursorID}},
	}

	killResult, err := db.RunCommand(killCmd)
	if err != nil {
		t.Fatalf("killCursors 命令失败: %v", err)
	}

	ok := getDocField(killResult, "ok")
	if ok != int32(1) {
		t.Error("killCursors 应该成功")
	}

	cursorsKilled := getDocField(killResult, "cursorsKilled").(bson.A)
	if len(cursorsKilled) != 1 {
		t.Errorf("期望关闭 1 个游标，实际 %d 个", len(cursorsKilled))
	}

	// 尝试再次获取应该失败
	// EN: Trying to fetch again should fail.
	getMoreCmd := bson.D{
		{Key: "getMore", Value: cursorID},
		{Key: "collection", Value: "test"},
	}

	result2, _ := db.RunCommand(getMoreCmd)
	ok2 := getDocField(result2, "ok")
	if ok2 == int32(1) {
		t.Error("游标已关闭，getMore 应该失败")
	}
}

func setupTestDBForCursor(t *testing.T) (*Database, func()) {
	tmpFile, err := os.CreateTemp("", "monodb_cursor_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)

	db, err := OpenDatabase(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("打开数据库失败: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpPath)
	}

	return db, cleanup
}

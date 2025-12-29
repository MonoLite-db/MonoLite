// Created by Yanjunhui

package engine

import (
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// 创建测试数据库
// EN: Create a test database.
func setupTestDB(t *testing.T) (*Database, func()) {
	tmpFile, err := os.CreateTemp("", "monodb_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	// 删除文件，让 OpenDatabase 创建新数据库
	// EN: Remove the file so OpenDatabase creates a new database.
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

// 插入测试数据
// EN: Insert test data.
func insertTestData(t *testing.T, col *Collection) {
	docs := []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(25)}, {Key: "city", Value: "Beijing"}, {Key: "score", Value: int32(85)}},
		{{Key: "name", Value: "Bob"}, {Key: "age", Value: int32(30)}, {Key: "city", Value: "Shanghai"}, {Key: "score", Value: int32(90)}},
		{{Key: "name", Value: "Charlie"}, {Key: "age", Value: int32(25)}, {Key: "city", Value: "Beijing"}, {Key: "score", Value: int32(78)}},
		{{Key: "name", Value: "David"}, {Key: "age", Value: int32(35)}, {Key: "city", Value: "Shenzhen"}, {Key: "score", Value: int32(92)}},
		{{Key: "name", Value: "Eve"}, {Key: "age", Value: int32(28)}, {Key: "city", Value: "Shanghai"}, {Key: "score", Value: int32(88)}},
	}

	for _, doc := range docs {
		_, err := col.Insert(doc)
		if err != nil {
			t.Fatalf("插入文档失败: %v", err)
		}
	}
}

func TestMatchStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 测试简单匹配
	// EN: Test a simple match.
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "city", Value: "Beijing"}}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("期望 2 条结果，实际 %d 条", len(results))
	}

	// 验证结果中的 city 都是 Beijing
	// EN: Verify all results have city=Beijing.
	for _, doc := range results {
		city := getDocField(doc, "city")
		if city != "Beijing" {
			t.Errorf("期望 city=Beijing，实际 city=%v", city)
		}
	}
}

func TestMatchStageWithOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 测试比较运算符
	// EN: Test comparison operators.
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(30)}}}}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("期望 2 条结果（age >= 30），实际 %d 条", len(results))
	}
}

func TestProjectStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 测试字段包含
	// EN: Test field inclusion.
	pipeline := []bson.D{
		{{Key: "$project", Value: bson.D{{Key: "name", Value: int32(1)}, {Key: "city", Value: int32(1)}, {Key: "_id", Value: int32(0)}}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("期望 5 条结果，实际 %d 条", len(results))
	}

	// 验证只包含 name 和 city 字段
	// EN: Verify only name and city fields are present.
	for _, doc := range results {
		if getDocField(doc, "age") != nil {
			t.Error("不应该包含 age 字段")
		}
		if getDocField(doc, "_id") != nil {
			t.Error("不应该包含 _id 字段")
		}
		if getDocField(doc, "name") == nil {
			t.Error("应该包含 name 字段")
		}
		if getDocField(doc, "city") == nil {
			t.Error("应该包含 city 字段")
		}
	}
}

func TestSortStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 测试升序排序
	// EN: Test ascending sort.
	pipeline := []bson.D{
		{{Key: "$sort", Value: bson.D{{Key: "age", Value: int32(1)}}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	// 验证按 age 升序排列
	// EN: Verify results are sorted by age ascending.
	prevAge := int32(0)
	for _, doc := range results {
		age := getDocField(doc, "age").(int32)
		if age < prevAge {
			t.Errorf("排序错误：%d 应该 >= %d", age, prevAge)
		}
		prevAge = age
	}

	// 测试降序排序
	// EN: Test descending sort.
	pipeline = []bson.D{
		{{Key: "$sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}},
	}

	results, err = col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	// 验证按 score 降序排列
	// EN: Verify results are sorted by score descending.
	prevScore := int32(100)
	for _, doc := range results {
		score := getDocField(doc, "score").(int32)
		if score > prevScore {
			t.Errorf("排序错误：%d 应该 <= %d", score, prevScore)
		}
		prevScore = score
	}
}

func TestLimitStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	pipeline := []bson.D{
		{{Key: "$limit", Value: int32(3)}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("期望 3 条结果，实际 %d 条", len(results))
	}
}

func TestSkipStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	pipeline := []bson.D{
		{{Key: "$skip", Value: int32(2)}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("期望 3 条结果（跳过 2 条），实际 %d 条", len(results))
	}
}

func TestGroupStageSum(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 按城市分组，计算总分
	// EN: Group by city and compute total score.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "totalScore", Value: bson.D{{Key: "$sum", Value: "$score"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("期望 3 个分组（Beijing, Shanghai, Shenzhen），实际 %d 个", len(results))
	}

	// 验证分组结果
	// EN: Verify grouping results.
	for _, doc := range results {
		city := getDocField(doc, "_id")
		total := getDocField(doc, "totalScore").(float64)

		switch city {
		case "Beijing":
			expected := float64(85 + 78)
			if total != expected {
				t.Errorf("Beijing 总分期望 %v，实际 %v", expected, total)
			}
		case "Shanghai":
			expected := float64(90 + 88)
			if total != expected {
				t.Errorf("Shanghai 总分期望 %v，实际 %v", expected, total)
			}
		case "Shenzhen":
			expected := float64(92)
			if total != expected {
				t.Errorf("Shenzhen 总分期望 %v，实际 %v", expected, total)
			}
		}
	}
}

func TestGroupStageAvg(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 按城市分组，计算平均年龄
	// EN: Group by city and compute average age.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "avgAge", Value: bson.D{{Key: "$avg", Value: "$age"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	for _, doc := range results {
		city := getDocField(doc, "_id")
		avg := getDocField(doc, "avgAge").(float64)

		switch city {
		case "Beijing":
			expected := float64(25+25) / 2
			if avg != expected {
				t.Errorf("Beijing 平均年龄期望 %v，实际 %v", expected, avg)
			}
		case "Shanghai":
			expected := float64(30+28) / 2
			if avg != expected {
				t.Errorf("Shanghai 平均年龄期望 %v，实际 %v", expected, avg)
			}
		}
	}
}

func TestGroupStageMinMax(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 按城市分组，找最高分和最低分
	// EN: Group by city and find max/min score.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "maxScore", Value: bson.D{{Key: "$max", Value: "$score"}}},
			{Key: "minScore", Value: bson.D{{Key: "$min", Value: "$score"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	for _, doc := range results {
		city := getDocField(doc, "_id")
		maxScore := getDocField(doc, "maxScore")
		minScore := getDocField(doc, "minScore")

		if city == "Beijing" {
			if toFloat64(maxScore) != 85 {
				t.Errorf("Beijing 最高分期望 85，实际 %v", maxScore)
			}
			if toFloat64(minScore) != 78 {
				t.Errorf("Beijing 最低分期望 78，实际 %v", minScore)
			}
		}
	}
}

func TestGroupStageCount(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 按城市分组，统计人数
	// EN: Group by city and count documents.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "count", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	for _, doc := range results {
		city := getDocField(doc, "_id")
		count := getDocField(doc, "count").(int64)

		switch city {
		case "Beijing":
			if count != 2 {
				t.Errorf("Beijing 人数期望 2，实际 %v", count)
			}
		case "Shanghai":
			if count != 2 {
				t.Errorf("Shanghai 人数期望 2，实际 %v", count)
			}
		case "Shenzhen":
			if count != 1 {
				t.Errorf("Shenzhen 人数期望 1，实际 %v", count)
			}
		}
	}
}

func TestGroupStagePush(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 按城市分组，收集所有姓名
	// EN: Group by city and collect all names.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "names", Value: bson.D{{Key: "$push", Value: "$name"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	for _, doc := range results {
		city := getDocField(doc, "_id")
		names := getDocField(doc, "names").(bson.A)

		if city == "Beijing" {
			if len(names) != 2 {
				t.Errorf("Beijing 姓名列表长度期望 2，实际 %d", len(names))
			}
		}
	}
}

func TestGroupStageAddToSet(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入包含重复年龄的数据
	// EN: Insert data with duplicate ages.
	docs := []bson.D{
		{{Key: "name", Value: "A"}, {Key: "category", Value: "X"}, {Key: "value", Value: int32(1)}},
		{{Key: "name", Value: "B"}, {Key: "category", Value: "X"}, {Key: "value", Value: int32(1)}},
		{{Key: "name", Value: "C"}, {Key: "category", Value: "X"}, {Key: "value", Value: int32(2)}},
	}

	for _, doc := range docs {
		col.Insert(doc)
	}

	// 按 category 分组，收集唯一的 value
	// EN: Group by category and collect unique values.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$category"},
			{Key: "uniqueValues", Value: bson.D{{Key: "$addToSet", Value: "$value"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("期望 1 个分组，实际 %d 个", len(results))
	}

	uniqueValues := getDocField(results[0], "uniqueValues").(bson.A)
	if len(uniqueValues) != 2 {
		t.Errorf("唯一值数量期望 2，实际 %d", len(uniqueValues))
	}
}

func TestCountStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "city", Value: "Beijing"}}}},
		{{Key: "$count", Value: "total"}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("期望 1 条结果，实际 %d 条", len(results))
	}

	total := getDocField(results[0], "total").(int64)
	if total != 2 {
		t.Errorf("期望计数 2，实际 %d", total)
	}
}

func TestUnwindStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入包含数组的文档
	// EN: Insert documents containing arrays.
	docs := []bson.D{
		{{Key: "name", Value: "Doc1"}, {Key: "tags", Value: bson.A{"a", "b", "c"}}},
		{{Key: "name", Value: "Doc2"}, {Key: "tags", Value: bson.A{"x", "y"}}},
	}

	for _, doc := range docs {
		col.Insert(doc)
	}

	pipeline := []bson.D{
		{{Key: "$unwind", Value: "$tags"}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	// Doc1 有 3 个 tag，Doc2 有 2 个 tag，共 5 条
	// EN: Doc1 has 3 tags, Doc2 has 2 tags, total 5.
	if len(results) != 5 {
		t.Errorf("期望 5 条结果，实际 %d 条", len(results))
	}

	// 验证展开后 tags 是单个值而非数组
	// EN: Verify after unwind, tags is a single value (not an array).
	for _, doc := range results {
		tags := getDocField(doc, "tags")
		if _, ok := tags.(bson.A); ok {
			t.Error("展开后 tags 不应该是数组")
		}
	}
}

func TestUnwindStageWithPreserve(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入一些文档，包括空数组和无该字段的文档
	// EN: Insert documents including empty arrays and documents without the field.
	docs := []bson.D{
		{{Key: "name", Value: "HasTags"}, {Key: "tags", Value: bson.A{"a", "b"}}},
		{{Key: "name", Value: "EmptyTags"}, {Key: "tags", Value: bson.A{}}},
		{{Key: "name", Value: "NoTags"}},
	}

	for _, doc := range docs {
		col.Insert(doc)
	}

	// 不保留空数组
	// EN: Do not preserve empty arrays.
	pipeline := []bson.D{
		{{Key: "$unwind", Value: "$tags"}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("期望 2 条结果（只有 HasTags 的展开），实际 %d 条", len(results))
	}

	// 保留空数组
	// EN: Preserve empty arrays.
	pipeline = []bson.D{
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$tags"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},
	}

	results, err = col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 4 {
		t.Errorf("期望 4 条结果（2 + 1 空 + 1 无字段），实际 %d 条", len(results))
	}
}

func TestAddFieldsStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	pipeline := []bson.D{
		{{Key: "$addFields", Value: bson.D{
			{Key: "status", Value: "active"},
			{Key: "scoreCopy", Value: "$score"},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	for _, doc := range results {
		status := getDocField(doc, "status")
		if status != "active" {
			t.Errorf("期望 status=active，实际 %v", status)
		}

		score := getDocField(doc, "score")
		scoreCopy := getDocField(doc, "scoreCopy")
		if score != scoreCopy {
			t.Errorf("期望 scoreCopy=%v，实际 %v", score, scoreCopy)
		}
	}
}

func TestComplexPipeline(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 复杂管道：筛选 -> 分组 -> 排序 -> 限制
	// EN: Complex pipeline: match -> group -> sort -> limit.
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(25)}}}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "avgScore", Value: bson.D{{Key: "$avg", Value: "$score"}}},
			{Key: "count", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "avgScore", Value: int32(-1)}}}},
		{{Key: "$limit", Value: int32(2)}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("期望 2 条结果，实际 %d 条", len(results))
	}

	// 验证按平均分降序排列
	// EN: Verify sorted by average score descending.
	if len(results) >= 2 {
		score1 := getDocField(results[0], "avgScore").(float64)
		score2 := getDocField(results[1], "avgScore").(float64)
		if score1 < score2 {
			t.Errorf("排序错误：第一个平均分 %v 应该 >= 第二个 %v", score1, score2)
		}
	}
}

func TestEmptyPipeline(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 空管道应返回所有文档
	// EN: An empty pipeline should return all documents.
	pipeline := []bson.D{}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("空管道期望返回 5 条文档，实际 %d 条", len(results))
	}
}

func TestGroupWithNullId(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// _id 为 null 时，所有文档归入一组
	// EN: When _id is null, all documents should fall into one group.
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "totalScore", Value: bson.D{{Key: "$sum", Value: "$score"}}},
			{Key: "count", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("期望 1 个分组，实际 %d 个", len(results))
	}

	count := getDocField(results[0], "count").(int64)
	if count != 5 {
		t.Errorf("期望计数 5，实际 %d", count)
	}

	totalScore := getDocField(results[0], "totalScore").(float64)
	expected := float64(85 + 90 + 78 + 92 + 88)
	if totalScore != expected {
		t.Errorf("期望总分 %v，实际 %v", expected, totalScore)
	}
}

func TestGroupWithFirstLast(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")

	// 插入有序数据
	// EN: Insert ordered data.
	docs := []bson.D{
		{{Key: "group", Value: "A"}, {Key: "order", Value: int32(1)}, {Key: "value", Value: "first"}},
		{{Key: "group", Value: "A"}, {Key: "order", Value: int32(2)}, {Key: "value", Value: "middle"}},
		{{Key: "group", Value: "A"}, {Key: "order", Value: int32(3)}, {Key: "value", Value: "last"}},
	}

	for _, doc := range docs {
		col.Insert(doc)
	}

	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$group"},
			{Key: "firstValue", Value: bson.D{{Key: "$first", Value: "$value"}}},
			{Key: "lastValue", Value: bson.D{{Key: "$last", Value: "$value"}}},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("期望 1 个分组，实际 %d 个", len(results))
	}

	firstValue := getDocField(results[0], "firstValue")
	lastValue := getDocField(results[0], "lastValue")

	if firstValue != "first" {
		t.Errorf("期望 firstValue=first，实际 %v", firstValue)
	}
	if lastValue != "last" {
		t.Errorf("期望 lastValue=last，实际 %v", lastValue)
	}
}

func TestInvalidPipelineStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 无效的阶段
	// EN: Invalid stage.
	pipeline := []bson.D{
		{{Key: "$invalidStage", Value: bson.D{}}},
	}

	_, err := col.Aggregate(pipeline)
	if err == nil {
		t.Error("应该返回错误，因为 $invalidStage 不是有效的阶段")
	}
}

func TestAggregateCommand(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 通过 RunCommand 执行聚合
	// EN: Run aggregation via RunCommand.
	cmd := bson.D{
		{Key: "aggregate", Value: "test"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "city", Value: "Beijing"}}}},
			bson.D{{Key: "$count", Value: "total"}},
		}},
		{Key: "$db", Value: "testdb"},
	}

	result, err := db.RunCommand(cmd)
	if err != nil {
		t.Fatalf("执行聚合命令失败: %v", err)
	}

	// 检查返回格式
	// EN: Check response format.
	ok := getDocField(result, "ok")
	if ok != int32(1) {
		t.Errorf("期望 ok=1，实际 %v", ok)
	}

	cursor := getDocField(result, "cursor").(bson.D)
	firstBatch := getDocField(cursor, "firstBatch").(bson.A)

	if len(firstBatch) != 1 {
		t.Fatalf("期望 1 条结果，实际 %d 条", len(firstBatch))
	}

	doc := firstBatch[0].(bson.D)
	total := getDocField(doc, "total").(int64)
	if total != 2 {
		t.Errorf("期望计数 2，实际 %d", total)
	}
}

func TestSkipLimitCombination(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 先排序，再跳过，再限制
	// EN: Sort first, then skip, then limit.
	pipeline := []bson.D{
		{{Key: "$sort", Value: bson.D{{Key: "age", Value: int32(1)}}}},
		{{Key: "$skip", Value: int32(1)}},
		{{Key: "$limit", Value: int32(2)}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("期望 2 条结果，实际 %d 条", len(results))
	}
}

func TestMatchAndProject(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// 筛选后投影
	// EN: Project after match.
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int32(25)}}}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "name", Value: int32(1)},
			{Key: "age", Value: int32(1)},
			{Key: "_id", Value: int32(0)},
		}}},
	}

	results, err := col.Aggregate(pipeline)
	if err != nil {
		t.Fatalf("聚合失败: %v", err)
	}

	// age > 25 的有 Bob(30), David(35), Eve(28)
	// EN: age > 25 includes Bob(30), David(35), Eve(28).
	if len(results) != 3 {
		t.Errorf("期望 3 条结果，实际 %d 条", len(results))
	}

	for _, doc := range results {
		if getDocField(doc, "_id") != nil {
			t.Error("不应该包含 _id 字段")
		}
		if getDocField(doc, "city") != nil {
			t.Error("不应该包含 city 字段")
		}
		if getDocField(doc, "score") != nil {
			t.Error("不应该包含 score 字段")
		}
	}
}

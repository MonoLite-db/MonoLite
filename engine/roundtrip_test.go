// Created by Yanjunhui
//
// BSON Round-trip 测试：验证数据导入导出的一致性

package engine

import (
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestBSONRoundtrip 测试 BSON 序列化/反序列化的一致性
func TestBSONRoundtrip(t *testing.T) {
	db, cleanup := setupTestDBForRoundtrip(t)
	defer cleanup()

	col, _ := db.Collection("roundtrip")

	// 创建包含各种 BSON 类型的测试文档
	testDocs := createTestDocuments()

	// 插入文档
	for _, doc := range testDocs {
		_, err := col.Insert(doc)
		if err != nil {
			t.Fatalf("插入文档失败: %v", err)
		}
	}

	// 查询所有文档
	results, err := col.Find(nil)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	if len(results) != len(testDocs) {
		t.Errorf("期望 %d 条文档，实际 %d 条", len(testDocs), len(results))
	}

	// 验证每个文档的字段
	for i, result := range results {
		verifyDocument(t, i, result)
	}
}

// TestBSONTypesPreservation 测试各种 BSON 类型的保持
func TestBSONTypesPreservation(t *testing.T) {
	db, cleanup := setupTestDBForRoundtrip(t)
	defer cleanup()

	col, _ := db.Collection("types")

	// 测试 String
	t.Run("String", func(t *testing.T) {
		doc := bson.D{{Key: "str", Value: "hello 你好"}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "str", Value: "hello 你好"}})
		if result == nil {
			t.Error("String 类型查询失败")
		}
	})

	// 测试 Int32
	t.Run("Int32", func(t *testing.T) {
		doc := bson.D{{Key: "int32", Value: int32(42)}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "int32", Value: int32(42)}})
		if result == nil {
			t.Error("Int32 类型查询失败")
		}
	})

	// 测试 Int64
	t.Run("Int64", func(t *testing.T) {
		doc := bson.D{{Key: "int64", Value: int64(9223372036854775807)}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "int64", Value: int64(9223372036854775807)}})
		if result == nil {
			t.Error("Int64 类型查询失败")
		}
	})

	// 测试 Float64
	t.Run("Float64", func(t *testing.T) {
		doc := bson.D{{Key: "float64", Value: 3.14159265358979}}
		col.Insert(doc)
		results, _ := col.Find(nil)
		found := false
		for _, r := range results {
			if v := getDocField(r, "float64"); v != nil {
				if f, ok := v.(float64); ok && f == 3.14159265358979 {
					found = true
				}
			}
		}
		if !found {
			t.Error("Float64 类型未保持")
		}
	})

	// 测试 Boolean
	t.Run("Boolean", func(t *testing.T) {
		doc := bson.D{{Key: "bool", Value: true}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "bool", Value: true}})
		if result == nil {
			t.Error("Boolean 类型查询失败")
		}
	})

	// 测试 Null
	t.Run("Null", func(t *testing.T) {
		doc := bson.D{{Key: "null", Value: nil}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "null", Value: nil}})
		if result == nil {
			t.Error("Null 类型查询失败")
		}
	})

	// 测试 ObjectID
	t.Run("ObjectID", func(t *testing.T) {
		oid := primitive.NewObjectID()
		doc := bson.D{{Key: "oid", Value: oid}}
		col.Insert(doc)
		result, _ := col.FindOne(bson.D{{Key: "oid", Value: oid}})
		if result == nil {
			t.Error("ObjectID 类型查询失败")
		}
	})

	// 测试 Array
	t.Run("Array", func(t *testing.T) {
		doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3, "four", true}}}
		col.Insert(doc)
		results, _ := col.Find(nil)
		found := false
		for _, r := range results {
			if v := getDocField(r, "arr"); v != nil {
				if arr, ok := v.(bson.A); ok && len(arr) == 5 {
					found = true
				}
			}
		}
		if !found {
			t.Error("Array 类型未保持")
		}
	})

	// 测试 Embedded Document
	t.Run("EmbeddedDocument", func(t *testing.T) {
		doc := bson.D{{Key: "nested", Value: bson.D{
			{Key: "a", Value: 1},
			{Key: "b", Value: "two"},
		}}}
		col.Insert(doc)
		results, _ := col.Find(nil)
		found := false
		for _, r := range results {
			if v := getDocField(r, "nested"); v != nil {
				if nested, ok := v.(bson.D); ok && len(nested) == 2 {
					found = true
				}
			}
		}
		if !found {
			t.Error("Embedded Document 类型未保持")
		}
	})

	// 测试 DateTime
	t.Run("DateTime", func(t *testing.T) {
		now := primitive.NewDateTimeFromTime(time.Now())
		doc := bson.D{{Key: "date", Value: now}}
		col.Insert(doc)
		results, _ := col.Find(nil)
		found := false
		for _, r := range results {
			if v := getDocField(r, "date"); v != nil {
				if _, ok := v.(primitive.DateTime); ok {
					found = true
				}
			}
		}
		if !found {
			t.Error("DateTime 类型未保持")
		}
	})
}

// TestDocumentUpdateRoundtrip 测试文档更新后的一致性
func TestDocumentUpdateRoundtrip(t *testing.T) {
	db, cleanup := setupTestDBForRoundtrip(t)
	defer cleanup()

	col, _ := db.Collection("update_test")

	// 插入原始文档
	original := bson.D{
		{Key: "name", Value: "test"},
		{Key: "count", Value: int32(0)},
		{Key: "tags", Value: bson.A{"a", "b"}},
	}
	ids, _ := col.Insert(original)
	id := ids[0]

	// 更新文档
	col.Update(
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "count", Value: int32(10)}}}},
		false,
	)

	// 验证更新
	result, _ := col.FindById(id)
	if result == nil {
		t.Fatal("更新后查询失败")
	}

	count := getDocField(result, "count")
	if count != int32(10) && count != float64(10) {
		t.Errorf("期望 count=10，实际 %v (%T)", count, count)
	}

	name := getDocField(result, "name")
	if name != "test" {
		t.Errorf("期望 name=test，实际 %v", name)
	}
}

// TestMediumDocumentRoundtrip 测试中等大小文档的一致性
func TestMediumDocumentRoundtrip(t *testing.T) {
	db, cleanup := setupTestDBForRoundtrip(t)
	defer cleanup()

	col, _ := db.Collection("medium")

	// 创建包含多个字段的文档（适合单页存储）
	doc := bson.D{}
	for i := 0; i < 50; i++ {
		doc = append(doc, bson.E{
			Key:   "field_" + string(rune('a'+i%26)) + "_" + string(rune('0'+i/26)),
			Value: i,
		})
	}

	// 添加一个中等字符串（约 500 字节）
	mediumString := ""
	for i := 0; i < 50; i++ {
		mediumString += "abcdefghij"
	}
	doc = append(doc, bson.E{Key: "medium_string", Value: mediumString})

	// 添加嵌套数组
	arr := bson.A{}
	for i := 0; i < 10; i++ {
		arr = append(arr, bson.D{{Key: "index", Value: i}, {Key: "value", Value: i * i}})
	}
	doc = append(doc, bson.E{Key: "nested_array", Value: arr})

	// 插入
	ids, err := col.Insert(doc)
	if err != nil {
		t.Fatalf("插入文档失败: %v", err)
	}

	// 查询
	result, err := col.FindById(ids[0])
	if err != nil {
		t.Fatalf("查询文档失败: %v", err)
	}

	// 验证字段数量（50 + 2 额外字段 + _id）
	if len(result) < 52 {
		t.Errorf("期望至少 52 个字段，实际 %d 个", len(result))
	}

	// 验证中等字符串
	str := getDocField(result, "medium_string")
	if s, ok := str.(string); !ok || len(s) != 500 {
		t.Error("字符串未正确保持")
	}

	// 验证嵌套数组
	nestedArr := getDocField(result, "nested_array")
	if arr, ok := nestedArr.(bson.A); !ok || len(arr) != 10 {
		t.Error("嵌套数组未正确保持")
	}
}

// TestBSONMarshalUnmarshal 测试 BSON 序列化反序列化
func TestBSONMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		name string
		doc  bson.D
	}{
		{
			name: "简单文档",
			doc:  bson.D{{Key: "a", Value: 1}, {Key: "b", Value: "hello"}},
		},
		{
			name: "嵌套文档",
			doc: bson.D{{Key: "outer", Value: bson.D{
				{Key: "inner", Value: bson.D{{Key: "deep", Value: true}}},
			}}},
		},
		{
			name: "数组",
			doc:  bson.D{{Key: "arr", Value: bson.A{1, 2, 3, bson.D{{Key: "x", Value: 4}}}}},
		},
		{
			name: "混合类型",
			doc: bson.D{
				{Key: "int", Value: int32(42)},
				{Key: "long", Value: int64(1234567890123)},
				{Key: "double", Value: 3.14},
				{Key: "string", Value: "test"},
				{Key: "bool", Value: true},
				{Key: "null", Value: nil},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 序列化
			data, err := bson.Marshal(tc.doc)
			if err != nil {
				t.Fatalf("序列化失败: %v", err)
			}

			// 反序列化
			var result bson.D
			if err := bson.Unmarshal(data, &result); err != nil {
				t.Fatalf("反序列化失败: %v", err)
			}

			// 验证字段数量
			if len(result) != len(tc.doc) {
				t.Errorf("字段数量不匹配: 期望 %d，实际 %d", len(tc.doc), len(result))
			}
		})
	}
}

// TestQueryConsistency 测试查询结果的一致性
func TestQueryConsistency(t *testing.T) {
	db, cleanup := setupTestDBForRoundtrip(t)
	defer cleanup()

	col, _ := db.Collection("query_test")

	// 插入测试数据
	docs := []bson.D{
		{{Key: "x", Value: int32(1)}, {Key: "y", Value: int32(10)}},
		{{Key: "x", Value: int32(2)}, {Key: "y", Value: int32(20)}},
		{{Key: "x", Value: int32(3)}, {Key: "y", Value: int32(30)}},
		{{Key: "x", Value: int32(4)}, {Key: "y", Value: int32(40)}},
		{{Key: "x", Value: int32(5)}, {Key: "y", Value: int32(50)}},
	}

	for _, doc := range docs {
		col.Insert(doc)
	}

	// 测试等值查询
	t.Run("等值查询", func(t *testing.T) {
		results, _ := col.Find(bson.D{{Key: "x", Value: int32(3)}})
		if len(results) != 1 {
			t.Errorf("期望 1 条结果，实际 %d 条", len(results))
		}
	})

	// 测试范围查询
	t.Run("范围查询", func(t *testing.T) {
		results, _ := col.Find(bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(2)}}}})
		if len(results) != 3 {
			t.Errorf("期望 3 条结果 (x > 2)，实际 %d 条", len(results))
		}
	})

	// 测试多条件查询
	t.Run("多条件查询", func(t *testing.T) {
		results, _ := col.Find(bson.D{
			{Key: "x", Value: bson.D{{Key: "$gte", Value: int32(2)}}},
			{Key: "y", Value: bson.D{{Key: "$lte", Value: int32(40)}}},
		})
		if len(results) != 3 {
			t.Errorf("期望 3 条结果 (x >= 2 && y <= 40)，实际 %d 条", len(results))
		}
	})

	// 测试排序
	t.Run("排序", func(t *testing.T) {
		results, _ := col.FindWithOptions(nil, &QueryOptions{
			Sort: bson.D{{Key: "x", Value: int32(-1)}},
		})
		if len(results) != 5 {
			t.Errorf("期望 5 条结果，实际 %d 条", len(results))
		}
		if len(results) > 0 {
			first := getDocField(results[0], "x")
			if first != int32(5) {
				t.Errorf("降序排序第一条应该是 x=5，实际 %v", first)
			}
		}
	})

	// 测试 Skip 和 Limit
	t.Run("Skip和Limit", func(t *testing.T) {
		results, _ := col.FindWithOptions(nil, &QueryOptions{
			Sort:  bson.D{{Key: "x", Value: int32(1)}},
			Skip:  1,
			Limit: 2,
		})
		if len(results) != 2 {
			t.Errorf("期望 2 条结果，实际 %d 条", len(results))
		}
		if len(results) > 0 {
			first := getDocField(results[0], "x")
			if first != int32(2) {
				t.Errorf("Skip 1 后第一条应该是 x=2，实际 %v", first)
			}
		}
	})

	// 测试投影
	t.Run("投影", func(t *testing.T) {
		results, _ := col.FindWithOptions(nil, &QueryOptions{
			Projection: bson.D{{Key: "x", Value: int32(1)}, {Key: "_id", Value: int32(0)}},
		})
		if len(results) != 5 {
			t.Errorf("期望 5 条结果，实际 %d 条", len(results))
		}
		for _, r := range results {
			if getDocField(r, "y") != nil {
				t.Error("投影后不应该包含 y 字段")
			}
			if getDocField(r, "_id") != nil {
				t.Error("投影后不应该包含 _id 字段")
			}
		}
	})
}

// 辅助函数

func setupTestDBForRoundtrip(t *testing.T) (*Database, func()) {
	tmpFile, err := os.CreateTemp("", "monodb_roundtrip_test_*.db")
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

func createTestDocuments() []bson.D {
	return []bson.D{
		{
			{Key: "type", Value: "simple"},
			{Key: "name", Value: "Alice"},
			{Key: "age", Value: int32(25)},
		},
		{
			{Key: "type", Value: "nested"},
			{Key: "user", Value: bson.D{
				{Key: "name", Value: "Bob"},
				{Key: "email", Value: "bob@example.com"},
			}},
		},
		{
			{Key: "type", Value: "array"},
			{Key: "tags", Value: bson.A{"go", "mongodb", "database"}},
			{Key: "scores", Value: bson.A{95, 87, 92}},
		},
		{
			{Key: "type", Value: "mixed"},
			{Key: "active", Value: true},
			{Key: "count", Value: int64(1000000000000)},
			{Key: "ratio", Value: 0.75},
			{Key: "data", Value: nil},
		},
	}
}

func verifyDocument(t *testing.T, index int, doc bson.D) {
	docType := getDocField(doc, "type")
	if docType == nil {
		t.Errorf("文档 %d: 缺少 type 字段", index)
		return
	}

	switch docType {
	case "simple":
		if getDocField(doc, "name") != "Alice" {
			t.Errorf("文档 %d: name 不匹配", index)
		}
	case "nested":
		user := getDocField(doc, "user")
		if user == nil {
			t.Errorf("文档 %d: 缺少 user 字段", index)
		}
	case "array":
		tags := getDocField(doc, "tags")
		if arr, ok := tags.(bson.A); !ok || len(arr) != 3 {
			t.Errorf("文档 %d: tags 数组不正确", index)
		}
	case "mixed":
		active := getDocField(doc, "active")
		if active != true {
			t.Errorf("文档 %d: active 应该为 true", index)
		}
	}
}

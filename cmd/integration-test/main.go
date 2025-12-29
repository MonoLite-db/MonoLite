// Created by Yanjunhui
//
// 集成测试：使用 MongoDB 官方驱动测试 MonoDB 兼容性

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client
var db *mongo.Database
var testsPassed int
var testsFailed int

func main() {
	port := "27018"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	uri := fmt.Sprintf("mongodb://localhost:%s", port)
	fmt.Printf("MonoDB Integration Test\n")
	fmt.Printf("连接到: %s\n\n", uri)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	clientOpts := options.Client().ApplyURI(uri).SetDirect(true)
	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Disconnect(ctx)

	db = client.Database("integration_test")

	// 运行测试
	runTest("Ping", testPing)
	runTest("Insert One", testInsertOne)
	runTest("Insert Many", testInsertMany)
	runTest("Find All", testFindAll)
	runTest("Find One", testFindOne)
	runTest("Find with Filter", testFindWithFilter)
	runTest("Find with Sort", testFindWithSort)
	runTest("Find with Limit Skip", testFindWithLimitSkip)
	runTest("Find with Projection", testFindWithProjection)
	runTest("Update One", testUpdateOne)
	runTest("Update with $set", testUpdateWithSet)
	runTest("Update with $inc", testUpdateWithInc)
	runTest("Delete One", testDeleteOne)
	runTest("Delete Many", testDeleteMany)
	runTest("Count Documents", testCountDocuments)
	runTest("Aggregate Match", testAggregateMatch)
	runTest("Aggregate Group", testAggregateGroup)
	runTest("Aggregate Pipeline", testAggregatePipeline)
	runTest("Create Index", testCreateIndex)
	runTest("List Indexes", testListIndexes)
	runTest("Drop Collection", testDropCollection)

	// 输出结果
	fmt.Printf("\n========================================\n")
	fmt.Printf("测试结果: %d 通过, %d 失败\n", testsPassed, testsFailed)

	if testsFailed > 0 {
		os.Exit(1)
	}
}

func runTest(name string, testFunc func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := testFunc(ctx)
	if err != nil {
		fmt.Printf("❌ %s: %v\n", name, err)
		testsFailed++
	} else {
		fmt.Printf("✓ %s\n", name)
		testsPassed++
	}
}

func testPing(ctx context.Context) error {
	return client.Ping(ctx, nil)
}

func testInsertOne(ctx context.Context) error {
	col := db.Collection("insert_test")
	col.Drop(ctx)

	doc := bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: 25},
	}

	result, err := col.InsertOne(ctx, doc)
	if err != nil {
		return err
	}

	if result.InsertedID == nil {
		return fmt.Errorf("InsertedID 为空")
	}

	return nil
}

func testInsertMany(ctx context.Context) error {
	col := db.Collection("insert_many_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "name", Value: "Bob"}, {Key: "age", Value: 30}},
		bson.D{{Key: "name", Value: "Charlie"}, {Key: "age", Value: 35}},
		bson.D{{Key: "name", Value: "David"}, {Key: "age", Value: 28}},
	}

	result, err := col.InsertMany(ctx, docs)
	if err != nil {
		return err
	}

	if len(result.InsertedIDs) != 3 {
		return fmt.Errorf("期望插入 3 条, 实际 %d 条", len(result.InsertedIDs))
	}

	return nil
}

func testFindAll(ctx context.Context) error {
	col := db.Collection("find_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
		bson.D{{Key: "x", Value: 3}},
	}
	col.InsertMany(ctx, docs)

	cursor, err := col.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return err
	}

	if len(results) != 3 {
		return fmt.Errorf("期望 3 条结果, 实际 %d 条", len(results))
	}

	return nil
}

func testFindOne(ctx context.Context) error {
	col := db.Collection("find_one_test")
	col.Drop(ctx)

	col.InsertOne(ctx, bson.D{{Key: "name", Value: "test"}, {Key: "value", Value: 42}})

	var result bson.M
	err := col.FindOne(ctx, bson.D{{Key: "name", Value: "test"}}).Decode(&result)
	if err != nil {
		return err
	}

	if result["name"] != "test" {
		return fmt.Errorf("name 不匹配")
	}

	return nil
}

func testFindWithFilter(ctx context.Context) error {
	col := db.Collection("filter_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "score", Value: 85}},
		bson.D{{Key: "score", Value: 90}},
		bson.D{{Key: "score", Value: 75}},
		bson.D{{Key: "score", Value: 95}},
	}
	col.InsertMany(ctx, docs)

	// 测试 $gt
	cursor, err := col.Find(ctx, bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: 80}}}})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 3 {
		return fmt.Errorf("$gt 过滤: 期望 3 条, 实际 %d 条", len(results))
	}

	return nil
}

func testFindWithSort(ctx context.Context) error {
	col := db.Collection("sort_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "n", Value: 3}},
		bson.D{{Key: "n", Value: 1}},
		bson.D{{Key: "n", Value: 2}},
	}
	col.InsertMany(ctx, docs)

	opts := options.Find().SetSort(bson.D{{Key: "n", Value: 1}})
	cursor, err := col.Find(ctx, bson.D{}, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 3 {
		return fmt.Errorf("排序结果数量错误")
	}

	// 验证升序
	for i := 0; i < len(results)-1; i++ {
		if results[i]["n"].(int32) > results[i+1]["n"].(int32) {
			return fmt.Errorf("排序不正确")
		}
	}

	return nil
}

func testFindWithLimitSkip(ctx context.Context) error {
	col := db.Collection("limit_skip_test")
	col.Drop(ctx)

	docs := []interface{}{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.D{{Key: "i", Value: i}})
	}
	col.InsertMany(ctx, docs)

	opts := options.Find().SetSort(bson.D{{Key: "i", Value: 1}}).SetSkip(2).SetLimit(3)
	cursor, err := col.Find(ctx, bson.D{}, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 3 {
		return fmt.Errorf("期望 3 条结果, 实际 %d 条", len(results))
	}

	// 第一条应该是 i=2
	if results[0]["i"].(int32) != 2 {
		return fmt.Errorf("Skip 后第一条应该是 i=2, 实际 i=%v", results[0]["i"])
	}

	return nil
}

func testFindWithProjection(ctx context.Context) error {
	col := db.Collection("projection_test")
	col.Drop(ctx)

	col.InsertOne(ctx, bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}, {Key: "c", Value: 3}})

	opts := options.Find().SetProjection(bson.D{{Key: "a", Value: 1}, {Key: "_id", Value: 0}})
	cursor, err := col.Find(ctx, bson.D{}, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 1 {
		return fmt.Errorf("期望 1 条结果")
	}

	if _, ok := results[0]["b"]; ok {
		return fmt.Errorf("投影后不应该包含 b 字段")
	}

	if _, ok := results[0]["a"]; !ok {
		return fmt.Errorf("投影后应该包含 a 字段")
	}

	return nil
}

func testUpdateOne(ctx context.Context) error {
	col := db.Collection("update_test")
	col.Drop(ctx)

	col.InsertOne(ctx, bson.D{{Key: "name", Value: "test"}, {Key: "count", Value: 0}})

	result, err := col.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "test"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "count", Value: 10}}}},
	)
	if err != nil {
		return err
	}

	if result.ModifiedCount != 1 {
		return fmt.Errorf("期望修改 1 条, 实际 %d 条", result.ModifiedCount)
	}

	return nil
}

func testUpdateWithSet(ctx context.Context) error {
	col := db.Collection("update_set_test")
	col.Drop(ctx)

	col.InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

	_, err := col.UpdateOne(ctx,
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "y", Value: 2}}}},
	)
	if err != nil {
		return err
	}

	var result bson.M
	col.FindOne(ctx, bson.D{{Key: "x", Value: 1}}).Decode(&result)

	if result["y"] == nil {
		return fmt.Errorf("$set 未添加新字段")
	}

	return nil
}

func testUpdateWithInc(ctx context.Context) error {
	col := db.Collection("update_inc_test")
	col.Drop(ctx)

	col.InsertOne(ctx, bson.D{{Key: "counter", Value: 10}})

	_, err := col.UpdateOne(ctx,
		bson.D{},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "counter", Value: 5}}}},
	)
	if err != nil {
		return err
	}

	var result bson.M
	col.FindOne(ctx, bson.D{}).Decode(&result)

	counter := result["counter"]
	if c, ok := counter.(float64); ok {
		if c != 15 {
			return fmt.Errorf("$inc 后期望 15, 实际 %v", counter)
		}
	} else if c, ok := counter.(int32); ok {
		if c != 15 {
			return fmt.Errorf("$inc 后期望 15, 实际 %v", counter)
		}
	}

	return nil
}

func testDeleteOne(ctx context.Context) error {
	col := db.Collection("delete_test")
	col.Drop(ctx)

	col.InsertMany(ctx, []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
	})

	result, err := col.DeleteOne(ctx, bson.D{{Key: "x", Value: 1}})
	if err != nil {
		return err
	}

	if result.DeletedCount != 1 {
		return fmt.Errorf("期望删除 1 条, 实际 %d 条", result.DeletedCount)
	}

	return nil
}

func testDeleteMany(ctx context.Context) error {
	col := db.Collection("delete_many_test")
	col.Drop(ctx)

	col.InsertMany(ctx, []interface{}{
		bson.D{{Key: "type", Value: "a"}},
		bson.D{{Key: "type", Value: "a"}},
		bson.D{{Key: "type", Value: "b"}},
	})

	result, err := col.DeleteMany(ctx, bson.D{{Key: "type", Value: "a"}})
	if err != nil {
		return err
	}

	if result.DeletedCount != 2 {
		return fmt.Errorf("期望删除 2 条, 实际 %d 条", result.DeletedCount)
	}

	return nil
}

func testCountDocuments(ctx context.Context) error {
	col := db.Collection("count_test")
	col.Drop(ctx)

	docs := []interface{}{}
	for i := 0; i < 5; i++ {
		docs = append(docs, bson.D{{Key: "i", Value: i}})
	}
	col.InsertMany(ctx, docs)

	count, err := col.CountDocuments(ctx, bson.D{})
	if err != nil {
		return err
	}

	if count != 5 {
		return fmt.Errorf("期望 5 条, 实际 %d 条", count)
	}

	return nil
}

func testAggregateMatch(ctx context.Context) error {
	col := db.Collection("agg_match_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "status", Value: "active"}, {Key: "score", Value: 80}},
		bson.D{{Key: "status", Value: "active"}, {Key: "score", Value: 90}},
		bson.D{{Key: "status", Value: "inactive"}, {Key: "score", Value: 70}},
	}
	col.InsertMany(ctx, docs)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "status", Value: "active"}}}},
	}

	cursor, err := col.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 2 {
		return fmt.Errorf("期望 2 条结果, 实际 %d 条", len(results))
	}

	return nil
}

func testAggregateGroup(ctx context.Context) error {
	col := db.Collection("agg_group_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "category", Value: "A"}, {Key: "amount", Value: 100}},
		bson.D{{Key: "category", Value: "A"}, {Key: "amount", Value: 200}},
		bson.D{{Key: "category", Value: "B"}, {Key: "amount", Value: 150}},
	}
	col.InsertMany(ctx, docs)

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$category"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
		}}},
	}

	cursor, err := col.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 2 {
		return fmt.Errorf("期望 2 个分组, 实际 %d 个", len(results))
	}

	return nil
}

func testAggregatePipeline(ctx context.Context) error {
	col := db.Collection("agg_pipeline_test")
	col.Drop(ctx)

	docs := []interface{}{
		bson.D{{Key: "name", Value: "A"}, {Key: "score", Value: 85}},
		bson.D{{Key: "name", Value: "B"}, {Key: "score", Value: 90}},
		bson.D{{Key: "name", Value: "C"}, {Key: "score", Value: 75}},
		bson.D{{Key: "name", Value: "D"}, {Key: "score", Value: 95}},
	}
	col.InsertMany(ctx, docs)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$gte", Value: 80}}}}}},
		{{Key: "$sort", Value: bson.D{{Key: "score", Value: -1}}}},
		{{Key: "$limit", Value: 2}},
	}

	cursor, err := col.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	cursor.All(ctx, &results)

	if len(results) != 2 {
		return fmt.Errorf("期望 2 条结果, 实际 %d 条", len(results))
	}

	return nil
}

func testCreateIndex(ctx context.Context) error {
	col := db.Collection("index_test")
	col.Drop(ctx)

	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	_, err := col.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	return nil
}

func testListIndexes(ctx context.Context) error {
	col := db.Collection("list_index_test")
	col.Drop(ctx)

	// 插入数据确保集合存在
	col.InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

	cursor, err := col.Indexes().List(ctx)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	cursor.All(ctx, &indexes)

	// 至少应该有 _id 索引
	if len(indexes) < 1 {
		return fmt.Errorf("期望至少 1 个索引")
	}

	return nil
}

func testDropCollection(ctx context.Context) error {
	col := db.Collection("drop_test")
	col.InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

	err := col.Drop(ctx)
	if err != nil {
		return err
	}

	// 验证集合已删除
	count, _ := col.CountDocuments(ctx, bson.D{})
	if count != 0 {
		return fmt.Errorf("删除后集合应该为空")
	}

	return nil
}

// 辅助类型
var _ = primitive.ObjectID{}

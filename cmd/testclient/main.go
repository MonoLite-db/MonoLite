// Created by Yanjunhui

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 连接到 MonoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用 directConnection=true 避免副本集发现
	clientOpts := options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping 测试
	fmt.Println("=== Ping Test ===")
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("Ping successful!")

	// 获取数据库和集合
	db := client.Database("testdb")
	users := db.Collection("users")

	// 清理旧数据
	fmt.Println("\n=== Cleanup ===")
	users.Drop(ctx)

	// Insert 测试
	fmt.Println("\n=== Insert Test ===")
	result, err := users.InsertOne(ctx, bson.D{
		{Key: "name", Value: "张三"},
		{Key: "age", Value: 25},
		{Key: "email", Value: "zhangsan@example.com"},
	})
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Printf("Inserted ID: %v\n", result.InsertedID)

	// InsertMany 测试
	manyResult, err := users.InsertMany(ctx, []interface{}{
		bson.D{{Key: "name", Value: "李四"}, {Key: "age", Value: 30}},
		bson.D{{Key: "name", Value: "王五"}, {Key: "age", Value: 28}},
	})
	if err != nil {
		log.Fatalf("InsertMany failed: %v", err)
	}
	fmt.Printf("Inserted %d documents\n", len(manyResult.InsertedIDs))

	// Find 测试
	fmt.Println("\n=== Find Test ===")
	cursor, err := users.Find(ctx, bson.D{})
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		log.Fatalf("Cursor decode failed: %v", err)
	}
	fmt.Printf("Found %d documents:\n", len(docs))
	for _, doc := range docs {
		fmt.Printf("  - %v\n", doc)
	}

	// FindOne 测试
	fmt.Println("\n=== FindOne Test ===")
	var found bson.M
	err = users.FindOne(ctx, bson.D{{Key: "name", Value: "张三"}}).Decode(&found)
	if err != nil {
		log.Fatalf("FindOne failed: %v", err)
	}
	fmt.Printf("Found: %v\n", found)

	// Update 测试
	fmt.Println("\n=== Update Test ===")
	updateResult, err := users.UpdateOne(
		ctx,
		bson.D{{Key: "name", Value: "张三"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 26}}}},
	)
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Modified %d document(s)\n", updateResult.ModifiedCount)

	// 验证更新
	err = users.FindOne(ctx, bson.D{{Key: "name", Value: "张三"}}).Decode(&found)
	if err != nil {
		log.Fatalf("FindOne after update failed: %v", err)
	}
	fmt.Printf("After update: %v\n", found)

	// Count 测试
	fmt.Println("\n=== Count Test ===")
	count, err := users.CountDocuments(ctx, bson.D{})
	if err != nil {
		log.Fatalf("Count failed: %v", err)
	}
	fmt.Printf("Total documents: %d\n", count)

	// Delete 测试
	fmt.Println("\n=== Delete Test ===")
	deleteResult, err := users.DeleteOne(ctx, bson.D{{Key: "name", Value: "王五"}})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Deleted %d document(s)\n", deleteResult.DeletedCount)

	// 最终计数
	count, _ = users.CountDocuments(ctx, bson.D{})
	fmt.Printf("Final count: %d\n", count)

	fmt.Println("\n=== All Tests Passed! ===")
	os.Exit(0)
}

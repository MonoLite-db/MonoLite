// Created by Yanjunhui

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 连接到 MonoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping 测试
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("Connected to MonoDB!")

	db := client.Database("testdb")
	products := db.Collection("products")

	// 清理
	products.Drop(ctx)

	// 插入测试数据
	fmt.Println("\n=== Inserting Test Data ===")
	docs := []interface{}{
		bson.D{{Key: "name", Value: "iPhone"}, {Key: "price", Value: 999}, {Key: "category", Value: "electronics"}},
		bson.D{{Key: "name", Value: "MacBook"}, {Key: "price", Value: 1999}, {Key: "category", Value: "electronics"}},
		bson.D{{Key: "name", Value: "iPad"}, {Key: "price", Value: 799}, {Key: "category", Value: "electronics"}},
		bson.D{{Key: "name", Value: "AirPods"}, {Key: "price", Value: 199}, {Key: "category", Value: "electronics"}},
		bson.D{{Key: "name", Value: "T-Shirt"}, {Key: "price", Value: 29}, {Key: "category", Value: "clothing"}},
		bson.D{{Key: "name", Value: "Jeans"}, {Key: "price", Value: 59}, {Key: "category", Value: "clothing"}},
	}
	result, err := products.InsertMany(ctx, docs)
	if err != nil {
		log.Fatalf("InsertMany failed: %v", err)
	}
	fmt.Printf("Inserted %d documents\n", len(result.InsertedIDs))

	// 测试比较运算符 $gt
	fmt.Println("\n=== Test $gt: price > 500 ===")
	cursor, err := products.Find(ctx, bson.D{{Key: "price", Value: bson.D{{Key: "$gt", Value: 500}}}})
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Found %d documents:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试 $in 运算符
	fmt.Println("\n=== Test $in: price in [199, 999] ===")
	cursor, err = products.Find(ctx, bson.D{{Key: "price", Value: bson.D{{Key: "$in", Value: bson.A{199, 999}}}}})
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Found %d documents:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试排序
	fmt.Println("\n=== Test Sort: price descending ===")
	findOpts := options.Find().SetSort(bson.D{{Key: "price", Value: -1}})
	cursor, err = products.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Println("Products sorted by price (descending):")
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试 Limit
	fmt.Println("\n=== Test Limit: top 3 ===")
	findOpts = options.Find().SetSort(bson.D{{Key: "price", Value: -1}}).SetLimit(3)
	cursor, err = products.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Top 3 most expensive:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试 Skip
	fmt.Println("\n=== Test Skip: skip 2, limit 2 ===")
	findOpts = options.Find().SetSort(bson.D{{Key: "price", Value: -1}}).SetSkip(2).SetLimit(2)
	cursor, err = products.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Skipped 2, limited 2:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试 Projection
	fmt.Println("\n=== Test Projection: only name ===")
	findOpts = options.Find().SetProjection(bson.D{{Key: "name", Value: 1}, {Key: "_id", Value: 0}})
	cursor, err = products.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Only names:\n")
	for _, doc := range results {
		fmt.Printf("  - %v\n", doc)
	}

	// 测试组合查询
	fmt.Println("\n=== Test Combined: electronics, price > 500, sorted, limited ===")
	findOpts = options.Find().
		SetSort(bson.D{{Key: "price", Value: 1}}).
		SetLimit(2)
	filter := bson.D{
		{Key: "category", Value: "electronics"},
		{Key: "price", Value: bson.D{{Key: "$gt", Value: 500}}},
	}
	cursor, err = products.Find(ctx, filter, findOpts)
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Electronics over $500, cheapest 2:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["name"], doc["price"])
	}

	// 测试创建索引
	fmt.Println("\n=== Test CreateIndex ===")
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "price", Value: 1}},
		Options: options.Index().SetName("price_asc"),
	}
	indexName, err := products.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		log.Printf("CreateIndex failed (expected for now): %v", err)
	} else {
		fmt.Printf("Created index: %s\n", indexName)
	}

	// 测试列出索引
	fmt.Println("\n=== Test ListIndexes ===")
	indexCursor, err := products.Indexes().List(ctx)
	if err != nil {
		log.Printf("ListIndexes failed: %v", err)
	} else {
		var indexes []bson.M
		if err := indexCursor.All(ctx, &indexes); err != nil {
			log.Printf("Failed to decode indexes: %v", err)
		} else {
			fmt.Printf("Indexes:\n")
			for _, idx := range indexes {
				fmt.Printf("  - %v\n", idx)
			}
		}
	}

	fmt.Println("\n=== All Index Tests Completed! ===")
}

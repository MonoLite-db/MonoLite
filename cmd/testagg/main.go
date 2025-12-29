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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("Connected to MonoDB!")

	db := client.Database("testdb")
	sales := db.Collection("sales")
	sales.Drop(ctx)

	// 插入销售数据
	// EN: Insert sales data.
	fmt.Println("\n=== Inserting Sales Data ===")
	docs := []interface{}{
		bson.D{{Key: "item", Value: "apple"}, {Key: "quantity", Value: 10}, {Key: "price", Value: 2.5}, {Key: "category", Value: "fruit"}},
		bson.D{{Key: "item", Value: "banana"}, {Key: "quantity", Value: 15}, {Key: "price", Value: 1.0}, {Key: "category", Value: "fruit"}},
		bson.D{{Key: "item", Value: "orange"}, {Key: "quantity", Value: 8}, {Key: "price", Value: 3.0}, {Key: "category", Value: "fruit"}},
		bson.D{{Key: "item", Value: "carrot"}, {Key: "quantity", Value: 20}, {Key: "price", Value: 0.5}, {Key: "category", Value: "vegetable"}},
		bson.D{{Key: "item", Value: "potato"}, {Key: "quantity", Value: 30}, {Key: "price", Value: 0.8}, {Key: "category", Value: "vegetable"}},
		bson.D{{Key: "item", Value: "tomato"}, {Key: "quantity", Value: 25}, {Key: "price", Value: 1.5}, {Key: "category", Value: "vegetable"}},
	}
	result, err := sales.InsertMany(ctx, docs)
	if err != nil {
		log.Fatalf("InsertMany failed: %v", err)
	}
	fmt.Printf("Inserted %d documents\n", len(result.InsertedIDs))

	// 测试 $match
	// EN: Test $match.
	fmt.Println("\n=== Test $match: category = fruit ===")
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "category", Value: "fruit"}}}},
	}
	cursor, err := sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Found %d fruits:\n", len(results))
	for _, doc := range results {
		fmt.Printf("  - %s\n", doc["item"])
	}

	// 测试 $sort + $limit
	// EN: Test $sort + $limit.
	fmt.Println("\n=== Test $sort + $limit: top 3 by quantity ===")
	pipeline = mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "quantity", Value: -1}}}},
		{{Key: "$limit", Value: 3}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Top 3 by quantity:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: %v\n", doc["item"], doc["quantity"])
	}

	// 测试 $project
	// EN: Test $project.
	fmt.Println("\n=== Test $project: only item and totalPrice ===")
	pipeline = mongo.Pipeline{
		{{Key: "$project", Value: bson.D{
			{Key: "item", Value: 1},
			{Key: "price", Value: 1},
			{Key: "_id", Value: 0},
		}}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Projected results:\n")
	for _, doc := range results {
		fmt.Printf("  - %v\n", doc)
	}

	// 测试 $skip
	// EN: Test $skip.
	fmt.Println("\n=== Test $sort + $skip + $limit: skip 2, limit 2 ===")
	pipeline = mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "price", Value: -1}}}},
		{{Key: "$skip", Value: 2}},
		{{Key: "$limit", Value: 2}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Skip 2, limit 2:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: $%v\n", doc["item"], doc["price"])
	}

	// 测试 $group
	// EN: Test $group.
	fmt.Println("\n=== Test $group: total quantity by category ===")
	pipeline = mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$category"},
			{Key: "totalQuantity", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
			{Key: "avgPrice", Value: bson.D{{Key: "$avg", Value: "$price"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Grouped by category:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: totalQty=%v, avgPrice=%v, count=%v\n",
			doc["_id"], doc["totalQuantity"], doc["avgPrice"], doc["count"])
	}

	// 测试 $count
	// EN: Test $count.
	fmt.Println("\n=== Test $count ===")
	pipeline = mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "category", Value: "vegetable"}}}},
		{{Key: "$count", Value: "totalVegetables"}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Count result: %v\n", results)

	// 测试 $group with $max/$min
	// EN: Test $group with $max/$min.
	fmt.Println("\n=== Test $group with $max/$min ===")
	pipeline = mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$category"},
			{Key: "maxPrice", Value: bson.D{{Key: "$max", Value: "$price"}}},
			{Key: "minPrice", Value: bson.D{{Key: "$min", Value: "$price"}}},
		}}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Max/Min prices by category:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: max=$%v, min=$%v\n", doc["_id"], doc["maxPrice"], doc["minPrice"])
	}

	// 测试复杂管道
	// EN: Test a complex pipeline.
	fmt.Println("\n=== Test Complex Pipeline: match + group + sort ===")
	pipeline = mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "quantity", Value: bson.D{{Key: "$gte", Value: 10}}}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$category"},
			{Key: "totalQuantity", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "totalQuantity", Value: -1}}}},
	}
	cursor, err = sales.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	results = nil
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatalf("Cursor All failed: %v", err)
	}
	fmt.Printf("Complex pipeline result:\n")
	for _, doc := range results {
		fmt.Printf("  - %s: totalQty=%v\n", doc["_id"], doc["totalQuantity"])
	}

	fmt.Println("\n=== All Aggregation Tests Completed! ===")
}

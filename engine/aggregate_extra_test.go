// Created by Yanjunhui

package engine

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestUnsetStage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	results, err := col.Aggregate([]bson.D{
		{{Key: "$unset", Value: bson.A{"score", "age"}}},
	})
	if err != nil {
		t.Fatalf("Aggregate($unset) failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 docs, got %d", len(results))
	}
	for _, doc := range results {
		if getDocField(doc, "score") != nil || getDocField(doc, "age") != nil {
			t.Fatalf("expected score/age removed, got score=%v age=%v", getDocField(doc, "score"), getDocField(doc, "age"))
		}
		if getDocField(doc, "name") == nil {
			t.Fatalf("expected name to remain")
		}
	}
}

func TestReplaceRootStage_NewRootFieldAndExpr(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("rr")
	_, _ = col.Insert(bson.D{
		{Key: "name", Value: "n1"},
		{Key: "nested", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}},
		{Key: "age", Value: int32(10)},
	})

	// newRoot="$nested"
	out1, err := col.Aggregate([]bson.D{
		{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$nested"}}}},
	})
	if err != nil {
		t.Fatalf("Aggregate($replaceRoot field) failed: %v", err)
	}
	if len(out1) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out1))
	}
	if getDocField(out1[0], "a") != int32(1) || getDocField(out1[0], "b") != "x" {
		t.Fatalf("unexpected replaceRoot result: %v", out1[0])
	}

	// newRoot 为表达式文档：{x:"$age", y:"lit"}
	// EN: newRoot is an expression document: {x:"$age", y:"lit"}.
	out2, err := col.Aggregate([]bson.D{
		{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: bson.D{
			{Key: "x", Value: "$age"},
			{Key: "y", Value: "lit"},
		}}}}},
	})
	if err != nil {
		t.Fatalf("Aggregate($replaceRoot expr) failed: %v", err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out2))
	}
	if getDocField(out2[0], "x") != int32(10) || getDocField(out2[0], "y") != "lit" {
		t.Fatalf("unexpected replaceRoot expr result: %v", out2[0])
	}
}

func TestLookupStage_SuccessAndMissingFrom(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	users, _ := db.Collection("users")
	orders, _ := db.Collection("orders")

	// 用 int32 作为 _id，便于 join
	// EN: Use int32 as _id to make joining easier.
	_, _ = users.Insert(
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "name", Value: "u1"}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "name", Value: "u2"}},
	)
	_, _ = orders.Insert(
		bson.D{{Key: "orderId", Value: int32(10)}, {Key: "userId", Value: int32(1)}},
		bson.D{{Key: "orderId", Value: int32(11)}, {Key: "userId", Value: int32(9)}}, // unmatched
	)

	out, err := orders.Aggregate([]bson.D{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "users"},
			{Key: "localField", Value: "userId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "user"},
		}}},
	})
	if err != nil {
		t.Fatalf("Aggregate($lookup) failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(out))
	}

	// orderId=10 应匹配到一个 user
	// EN: orderId=10 should match exactly one user.
	var matched bson.D
	for _, d := range out {
		if getDocField(d, "orderId") == int32(10) {
			matched = d
			break
		}
	}
	if matched == nil {
		t.Fatalf("expected to find orderId=10 in output")
	}
	userArr, ok := getDocField(matched, "user").(bson.A)
	if !ok || len(userArr) != 1 {
		t.Fatalf("expected user array len=1, got %T %v", getDocField(matched, "user"), getDocField(matched, "user"))
	}

	// from 集合不存在：应返回空数组且不报错
	// EN: If the "from" collection does not exist, return an empty array and no error.
	out2, err := orders.Aggregate([]bson.D{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "no_such_collection"},
			{Key: "localField", Value: "userId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "none"},
		}}},
	})
	if err != nil {
		t.Fatalf("Aggregate($lookup missing from) failed: %v", err)
	}
	if len(out2) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(out2))
	}
	if a, ok := getDocField(out2[0], "none").(bson.A); !ok || len(a) != 0 {
		t.Fatalf("expected empty array for missing from, got %T %v", getDocField(out2[0], "none"), getDocField(out2[0], "none"))
	}
}

func TestPipelineLookupWithoutDBContextReturnsStageError(t *testing.T) {
	// 直接使用 NewPipeline（db=nil）构造包含 $lookup 的 pipeline，触发错误路径
	// EN: Build a pipeline with $lookup via NewPipeline (db=nil) to hit the error path.
	p, err := NewPipeline([]bson.D{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "x"},
			{Key: "as", Value: "y"},
		}}},
	})
	if err != nil {
		t.Fatalf("NewPipeline failed unexpectedly: %v", err)
	}
	_, err = p.Execute([]bson.D{{{Key: "a", Value: 1}}})
	if err == nil {
		t.Fatalf("expected Execute to fail due to missing db context")
	}
	// 期望包含 stage.Name() 拼出来的上下文信息（覆盖 Name 的错误路径调用）
	// EN: Expect error text to include context built from stage.Name() (covers Name() in the error path).
	if !bytes.Contains([]byte(err.Error()), []byte("error in $lookup stage")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSortByFields_UsesMongoCompareRules(t *testing.T) {
	docs := []bson.D{
		{{Key: "v", Value: int32(2)}},
		{{Key: "v", Value: int32(1)}},
		{{Key: "v", Value: int32(3)}},
	}
	sorted := SortByFields(docs, bson.D{{Key: "v", Value: int32(1)}})
	if getDocField(sorted[0], "v") != int32(1) || getDocField(sorted[2], "v") != int32(3) {
		t.Fatalf("unexpected sort result: %v", sorted)
	}
}

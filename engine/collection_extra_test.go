// Created by Yanjunhui

package engine

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestCollectionDeleteAndDeleteOne(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// DeleteOne：删除一条 Beijing 的文档
	// EN: DeleteOne: delete one document with city=Beijing.
	deletedOne, err := col.DeleteOne(bson.D{{Key: "city", Value: "Beijing"}})
	if err != nil {
		t.Fatalf("DeleteOne failed: %v", err)
	}
	if deletedOne != 1 {
		t.Fatalf("expected DeleteOne deleted=1, got %d", deletedOne)
	}

	remainingBeijing, err := col.Find(bson.D{{Key: "city", Value: "Beijing"}})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(remainingBeijing) != 1 {
		t.Fatalf("expected 1 Beijing doc remaining, got %d", len(remainingBeijing))
	}

	// Delete：删除所有 Shanghai 的文档（应有 2 条）
	// EN: Delete: delete all documents with city=Shanghai (should be 2).
	deletedMany, err := col.Delete(bson.D{{Key: "city", Value: "Shanghai"}})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deletedMany != 2 {
		t.Fatalf("expected Delete deleted=2, got %d", deletedMany)
	}

	remainingShanghai, err := col.Find(bson.D{{Key: "city", Value: "Shanghai"}})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(remainingShanghai) != 0 {
		t.Fatalf("expected 0 Shanghai docs remaining, got %d", len(remainingShanghai))
	}
}

func TestCollectionFindAndModify_UpdateNewAndSort(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// city=Shanghai 有两条：Bob(score=90) 和 Eve(score=88)
	// EN: There are two docs with city=Shanghai: Bob(score=90) and Eve(score=88).
	// sort 按 score 降序，应选 Bob
	// EN: Sort by score descending; Bob should be chosen.
	original, err := col.FindAndModify(&FindAndModifyOptions{
		Query:  bson.D{{Key: "city", Value: "Shanghai"}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "tag", Value: "chosen"}}}},
		New:    false,
		Sort:   bson.D{{Key: "score", Value: int32(-1)}},
	})
	if err != nil {
		t.Fatalf("FindAndModify update failed: %v", err)
	}
	if original == nil {
		t.Fatalf("expected original doc, got nil")
	}
	if getDocField(original, "name") != "Bob" {
		t.Fatalf("expected Bob to be chosen by sort, got %v", getDocField(original, "name"))
	}
	if getDocField(original, "tag") != nil {
		t.Fatalf("expected original doc to not include updated field when New=false")
	}

	modified, err := col.FindAndModify(&FindAndModifyOptions{
		Query:  bson.D{{Key: "city", Value: "Shanghai"}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "tag2", Value: "chosen2"}}}},
		New:    true,
		Sort:   bson.D{{Key: "score", Value: int32(-1)}},
	})
	if err != nil {
		t.Fatalf("FindAndModify update(New=true) failed: %v", err)
	}
	if getDocField(modified, "name") != "Bob" {
		t.Fatalf("expected Bob to be chosen by sort, got %v", getDocField(modified, "name"))
	}
	if getDocField(modified, "tag2") != "chosen2" {
		t.Fatalf("expected updated field tag2 to be present, got %v", getDocField(modified, "tag2"))
	}
}

func TestCollectionFindAndModify_RemoveAndUpsert(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	// Remove：删除一条 Shenzhen 的文档（David）
	// EN: Remove: delete one document with city=Shenzhen (David).
	removed, err := col.FindAndModify(&FindAndModifyOptions{
		Query:  bson.D{{Key: "city", Value: "Shenzhen"}},
		Remove: true,
	})
	if err != nil {
		t.Fatalf("FindAndModify remove failed: %v", err)
	}
	if getDocField(removed, "name") != "David" {
		t.Fatalf("expected removed doc to be David, got %v", getDocField(removed, "name"))
	}
	afterRemove, _ := col.Find(bson.D{{Key: "city", Value: "Shenzhen"}})
	if len(afterRemove) != 0 {
		t.Fatalf("expected Shenzhen docs to be 0 after remove, got %d", len(afterRemove))
	}

	// Upsert：未命中则插入
	// EN: Upsert: insert if no document matches.
	upsertDoc, err := col.FindAndModify(&FindAndModifyOptions{
		Query:  bson.D{{Key: "name", Value: "Zoe"}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: int32(18)}}}},
		Upsert: true,
		New:    true,
	})
	if err != nil {
		t.Fatalf("FindAndModify upsert failed: %v", err)
	}
	if upsertDoc == nil {
		t.Fatalf("expected upsert to return new doc when New=true")
	}
	if getDocField(upsertDoc, "name") != "Zoe" {
		t.Fatalf("expected name=Zoe, got %v", getDocField(upsertDoc, "name"))
	}
	if getDocField(upsertDoc, "age") != int32(18) {
		t.Fatalf("expected age=18, got %v", getDocField(upsertDoc, "age"))
	}
	if getDocField(upsertDoc, "_id") == nil {
		t.Fatalf("expected upsert doc to have _id")
	}

	found, err := col.FindOne(bson.D{{Key: "name", Value: "Zoe"}})
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if found == nil {
		t.Fatalf("expected upserted doc to be findable")
	}
}

func TestCollectionDistinct(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	col, _ := db.Collection("test")
	insertTestData(t, col)

	cities, err := col.Distinct("city", nil)
	if err != nil {
		t.Fatalf("Distinct failed: %v", err)
	}
	// 期望 Beijing/Shanghai/Shenzhen 三个
	// EN: Expect three cities: Beijing/Shanghai/Shenzhen.
	if len(cities) != 3 {
		t.Fatalf("expected 3 distinct cities, got %d: %v", len(cities), cities)
	}

	// 过滤后 distinct：Beijing 的 age 只有 25
	// EN: Distinct with a filter: Beijing's age should only be 25.
	ages, err := col.Distinct("age", bson.D{{Key: "city", Value: "Beijing"}})
	if err != nil {
		t.Fatalf("Distinct failed: %v", err)
	}
	if len(ages) != 1 || ages[0] != int32(25) {
		t.Fatalf("expected distinct ages=[25], got %v", ages)
	}
}

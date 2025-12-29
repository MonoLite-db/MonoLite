// Created by Yanjunhui

package engine

import (
	"path/filepath"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestNonUniqueIndexAllowsDuplicateKeys(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonunique_index.db")

	db, err := OpenDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	col, err := db.Collection("t")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// non-unique index on field "a"
	indexName, err := col.CreateIndex(
		bson.D{{Key: "a", Value: int32(1)}},
		bson.D{{Key: "name", Value: "a_1"}, {Key: "unique", Value: false}},
	)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	if indexName == "" {
		t.Fatalf("index name should not be empty")
	}

	// insert two documents with the same indexed key (a=1)
	if _, err := col.Insert(bson.D{{Key: "a", Value: int32(1)}}); err != nil {
		t.Fatalf("failed to insert first doc: %v", err)
	}
	if _, err := col.Insert(bson.D{{Key: "a", Value: int32(1)}}); err != nil {
		t.Fatalf("failed to insert second doc: %v", err)
	}

	// Ensure pages are persisted before validate (avoid false negatives due to crash-simulated states)
	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// validate must pass (B+Tree should remain strictly ordered)
	result := db.Validate()
	if !result.Valid {
		t.Fatalf("database validation failed: errors=%v warnings=%v", result.Errors, result.Warnings)
	}

	// sanity: the index tree should contain at least 2 entries now
	if col.indexManager == nil {
		t.Fatalf("expected indexManager to be initialized")
	}
	idx, ok := col.indexManager.indexes[indexName]
	if !ok || idx == nil || idx.tree == nil {
		t.Fatalf("expected index %q to exist", indexName)
	}
	count, err := idx.tree.Count()
	if err != nil {
		t.Fatalf("failed to count index keys: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected at least 2 index entries, got %d", count)
	}
}



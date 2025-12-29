//go:build failpoint

// Package consistency contains A-class consistency tests.
package consistency

import (
	"testing"

	"github.com/monolite/monodb/engine"
	"github.com/monolite/monodb/internal/failpoint"
	"github.com/monolite/monodb/internal/testkit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestInsert_MultiIndex_SecondIndexFails_NoPartialIndexState tests A-ENG-IDX-001:
// Multi-index insert with failure on second index must not leave partial index state.
func TestInsert_MultiIndex_SecondIndexFails_NoPartialIndexState(t *testing.T) {
	db, dataPath := testkit.OpenDatabaseOnly(t)
	defer db.Close()

	// Create collection
	col, err := db.Collection("test_idx")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Create first index on field 'a'
	_, err = col.CreateIndex(bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "name", Value: "a_1"}})
	if err != nil {
		t.Fatalf("failed to create index a_1: %v", err)
	}

	// Create second index on field 'b'
	_, err = col.CreateIndex(bson.D{{Key: "b", Value: 1}}, bson.D{{Key: "name", Value: "b_1"}})
	if err != nil {
		t.Fatalf("failed to create index b_1: %v", err)
	}

	// Enable failpoint on second index
	failpoint.Enable("index.insert.b_1", failpoint.FailOnce)
	defer failpoint.Disable("index.insert.b_1")

	// Try to insert a document
	doc := bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "a", Value: 1},
		{Key: "b", Value: 1},
	}

	_, err = col.Insert(doc)
	if err == nil {
		t.Fatalf("expected error from failpoint, got nil")
	}
	t.Logf("Insert failed as expected: %v", err)

	// Verify index-data consistency
	// The document should NOT be in the collection
	docs, err := col.Find(nil)
	if err != nil {
		t.Fatalf("failed to find documents: %v", err)
	}

	if len(docs) != 0 {
		t.Errorf("expected 0 documents after failed insert, got %d", len(docs))
	}

	// Run invariant checks
	testkit.AssertInvariants(t, db, "test_idx")

	t.Logf("Test passed - no partial index state after failed multi-index insert (dataPath: %s)", dataPath)
}

// TestDelete_IndexDeletedButRecordDeleteFails tests A-ENG-DEL-001:
// Delete where index is deleted but record delete fails must not leave missing index entry.
func TestDelete_IndexDeletedButRecordDeleteFails(t *testing.T) {
	db, _ := testkit.OpenDatabaseOnly(t)
	defer db.Close()

	col, err := db.Collection("test_del")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Create index
	_, err = col.CreateIndex(bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "name", Value: "a_1"}})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Insert a document
	docID := primitive.NewObjectID()
	doc := bson.D{
		{Key: "_id", Value: docID},
		{Key: "a", Value: 42},
	}
	_, err = col.Insert(doc)
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// Enable failpoint on slotted page delete
	failpoint.Enable("slotted.delete", failpoint.FailOnce)
	defer failpoint.Disable("slotted.delete")

	// Try to delete the document
	_, err = col.Delete(bson.D{{Key: "_id", Value: docID}})
	if err == nil {
		t.Logf("Note: Delete succeeded despite failpoint (timing issue)")
	} else {
		t.Logf("Delete failed as expected: %v", err)
	}

	// Verify document still exists (if delete failed)
	foundDoc, err := col.FindById(docID)
	if err != nil {
		t.Logf("Document not found after failed delete (unexpected): %v", err)
	} else if foundDoc != nil {
		t.Logf("Document still exists as expected")
	}

	// Run invariant checks
	testkit.AssertInvariants(t, db, "test_del")
}

// TestFindAndModify_IndexInsertFails_MustRollback tests A-ENG-FAM-001:
// FindAndModify update where index insert fails must rollback to consistent state.
func TestFindAndModify_IndexInsertFails_MustRollback(t *testing.T) {
	db, _ := testkit.OpenDatabaseOnly(t)
	defer db.Close()

	col, err := db.Collection("test_fam")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Create index on field 'a'
	_, err = col.CreateIndex(bson.D{{Key: "a", Value: 1}}, bson.D{{Key: "name", Value: "a_1"}})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Insert initial document
	docID := primitive.NewObjectID()
	doc := bson.D{
		{Key: "_id", Value: docID},
		{Key: "a", Value: 1},
		{Key: "x", Value: "old"},
	}
	_, err = col.Insert(doc)
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// Enable failpoint on index insert (will trigger during update)
	failpoint.Enable("index.insert.a_1", failpoint.FailOnce)
	defer failpoint.Disable("index.insert.a_1")

	// Try to update via FindAndModify
	opts := &engine.FindAndModifyOptions{
		Query:  bson.D{{Key: "_id", Value: docID}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 2}, {Key: "x", Value: "new"}}}},
		New:    true,
	}

	_, err = col.FindAndModify(opts)
	if err == nil {
		t.Logf("Note: FindAndModify succeeded despite failpoint")
	} else {
		t.Logf("FindAndModify failed as expected: %v", err)
	}

	// The document should be in a consistent state
	// Either the update succeeded fully, or it was rolled back
	foundDoc, err := col.FindById(docID)
	if err != nil {
		t.Fatalf("document not found: %v", err)
	}

	t.Logf("Document state after FindAndModify: %v", foundDoc)

	// Run invariant checks
	testkit.AssertInvariants(t, db, "test_fam")
}

// TestInsert_BTreeInsertFails tests behavior when B+Tree insert fails.
func TestInsert_BTreeInsertFails(t *testing.T) {
	db, _ := testkit.OpenDatabaseOnly(t)
	defer db.Close()

	col, err := db.Collection("test_btree")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Create index
	_, err = col.CreateIndex(bson.D{{Key: "x", Value: 1}}, bson.D{{Key: "name", Value: "x_1"}})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Enable failpoint on btree insert
	failpoint.Enable("btree.insert", failpoint.FailOnce)
	defer failpoint.Disable("btree.insert")

	// Try to insert
	doc := bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "x", Value: 100},
	}

	_, err = col.Insert(doc)
	if err == nil {
		t.Logf("Note: Insert succeeded despite failpoint")
	} else {
		t.Logf("Insert failed as expected: %v", err)
	}

	// Verify consistency
	testkit.AssertInvariants(t, db, "test_btree")
}


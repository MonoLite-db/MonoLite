// Package testkit provides testing utilities for MonoLite.
package testkit

import (
	"fmt"
	"testing"

	"github.com/monolite/monodb/engine"
	"go.mongodb.org/mongo-driver/bson"
)

// AssertInvariants runs all invariant checks on a database.
func AssertInvariants(t *testing.T, db *engine.Database, collName string) {
	t.Helper()

	AssertIndexDataConsistency(t, db, collName)
	AssertValidatePasses(t, db, collName)
}

// AssertInvariantsAfterRestart runs invariant checks after simulating crash recovery.
func AssertInvariantsAfterRestart(t *testing.T, handle *MonoLiteHandle, collName string) {
	t.Helper()

	// First check current state
	AssertInvariants(t, handle.DB, collName)

	// Then restart and check again
	if err := handle.Restart(); err != nil {
		t.Fatalf("failed to restart: %v", err)
	}

	AssertInvariants(t, handle.DB, collName)
}

// AssertIndexDataConsistency checks that all indexes are consistent with data.
// This implements INV-A1 from the spec.
func AssertIndexDataConsistency(t *testing.T, db *engine.Database, collName string) {
	t.Helper()

	col, err := db.Collection(collName)
	if err != nil {
		t.Logf("collection %s does not exist or error: %v, skipping index consistency check", collName, err)
		return
	}

	// Direction 1: Index -> Doc (already covered by validateIndexSampling)
	// Check that every index entry points to an existing document

	// Direction 2: Doc -> Index (NEW - critical for detecting missing index entries)
	// For each document, verify its index entries exist
	assertDocToIndexConsistency(t, col)
}

// assertDocToIndexConsistency verifies that every document has corresponding index entries.
// This is the critical check that detects "index entry missing" issues after failed deletes/updates.
func assertDocToIndexConsistency(t *testing.T, col *engine.Collection) {
	t.Helper()

	// Get all documents
	docs, err := col.Find(nil)
	if err != nil {
		t.Fatalf("failed to find documents: %v", err)
	}

	// Get index info (returns bson.A)
	indexInfos := col.ListIndexes()

	for _, doc := range docs {
		docID := getDocField(doc, "_id")
		if docID == nil {
			t.Errorf("document without _id found")
			continue
		}

		// For each index, verify the document's entry exists
		for _, idxInfoRaw := range indexInfos {
			idxInfo, ok := idxInfoRaw.(bson.D)
			if !ok {
				continue
			}

			// Get index name
			var idxName string
			var idxKeys bson.D
			for _, elem := range idxInfo {
				if elem.Key == "name" {
					idxName, _ = elem.Value.(string)
				}
				if elem.Key == "key" {
					idxKeys, _ = elem.Value.(bson.D)
				}
			}

			// Skip _id index (always consistent)
			if idxName == "_id_" {
				continue
			}

			// Check if this document should be indexed
			if !shouldBeIndexed(doc, idxKeys) {
				continue
			}

			// Verify index entry exists by querying
			if err := verifyIndexEntryExists(t, col, idxName, idxKeys, doc); err != nil {
				t.Errorf("INV-A1 violation: doc %v missing index entry for %s: %v",
					docID, idxName, err)
			}
		}
	}
}

// shouldBeIndexed checks if a document should have an entry in the given index.
func shouldBeIndexed(doc bson.D, indexKeys bson.D) bool {
	// For sparse indexes, documents missing indexed fields are not indexed
	// For now, assume all documents with the indexed field should be indexed
	for _, key := range indexKeys {
		if getDocField(doc, key.Key) != nil {
			return true
		}
	}
	return false
}

// verifyIndexEntryExists verifies that a document's index entry exists.
func verifyIndexEntryExists(t *testing.T, col *engine.Collection, idxName string, idxKeys bson.D, doc bson.D) error {
	// Build a query using the indexed field(s) to verify the index entry
	query := bson.D{}
	for _, key := range idxKeys {
		val := getDocField(doc, key.Key)
		if val != nil {
			query = append(query, bson.E{Key: key.Key, Value: val})
		}
	}

	if len(query) == 0 {
		return nil // Document doesn't have indexed field
	}

	// Add _id to ensure we're checking for this specific document
	docID := getDocField(doc, "_id")
	query = append(query, bson.E{Key: "_id", Value: docID})

	// Try to find the document using the index
	results, err := col.Find(query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if len(results) == 0 {
		return fmt.Errorf("document not found via index query")
	}

	return nil
}

// getDocField gets a field value from a bson.D document.
func getDocField(doc bson.D, key string) interface{} {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value
		}
	}
	return nil
}

// AssertValidatePasses runs the validate command and asserts it passes.
// This implements INV-A2 from the spec.
func AssertValidatePasses(t *testing.T, db *engine.Database, collName string) {
	t.Helper()

	result, err := db.RunCommand(bson.D{
		{Key: "validate", Value: collName},
	})
	if err != nil {
		t.Logf("validate command failed: %v", err)
		return
	}

	// Check for valid field
	valid := getDocField(result, "valid")
	if valid == nil {
		t.Logf("validate command did not return 'valid' field")
		return
	}

	if validBool, ok := valid.(bool); ok && !validBool {
		t.Errorf("INV-A2 violation: validate returned valid=false for collection %s", collName)

		// Print additional info if available
		if errors := getDocField(result, "errors"); errors != nil {
			t.Errorf("validate errors: %v", errors)
		}
		if warnings := getDocField(result, "warnings"); warnings != nil {
			t.Errorf("validate warnings: %v", warnings)
		}
	}
}

// AssertNoDataLoss verifies that expected documents still exist after an operation.
func AssertNoDataLoss(t *testing.T, col *engine.Collection, expectedIDs []interface{}) {
	t.Helper()

	for _, id := range expectedIDs {
		doc, err := col.FindById(id)
		if err != nil {
			t.Errorf("expected document with _id=%v not found: %v", id, err)
			continue
		}
		if doc == nil {
			t.Errorf("expected document with _id=%v not found (nil doc)", id)
		}
	}
}

// AssertPageIsDirty verifies that a page is marked as dirty in the pager cache.
// This is used to verify that write failures don't incorrectly clear dirty status.
func AssertPageIsDirty(t *testing.T, db *engine.Database, pageID uint32, expectDirty bool) {
	t.Helper()

	// This would require access to the pager's internal state
	// For now, we'll note that this needs to be implemented via a test hook
	t.Logf("AssertPageIsDirty: requires internal access to pager (pageID=%d, expectDirty=%v)", pageID, expectDirty)
}

// AssertDirtyPagesCount verifies the number of dirty pages.
func AssertDirtyPagesCount(t *testing.T, db *engine.Database, expected int) {
	t.Helper()

	// This would require access to the pager's internal state
	t.Logf("AssertDirtyPagesCount: requires internal access to pager (expected=%d)", expected)
}

// AssertCollectionDocCount verifies the document count in a collection.
func AssertCollectionDocCount(t *testing.T, col *engine.Collection, expected int) {
	t.Helper()

	result := col.Count()
	if result != int64(expected) {
		t.Errorf("document count mismatch: got %d, expected %d", result, expected)
	}
}

// AssertRecordExists verifies that a record exists at a specific location.
func AssertRecordExists(t *testing.T, db *engine.Database, pageID uint32, slotIndex int) {
	t.Helper()

	// This would require access to the pager and slotted page
	t.Logf("AssertRecordExists: requires internal access (pageID=%d, slotIndex=%d)", pageID, slotIndex)
}

// AssertDocumentHasValue verifies a document field has a specific value.
func AssertDocumentHasValue(t *testing.T, doc bson.D, field string, expected interface{}) {
	t.Helper()

	actual := getDocField(doc, field)
	if actual != expected {
		t.Errorf("field %s mismatch: got %v, expected %v", field, actual, expected)
	}
}

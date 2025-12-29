// Package testkit provides testing utilities for MonoLite.
package testkit

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/monolite/monodb/engine"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DiffResult holds the results from both MonoLite and MongoDB.
type DiffResult struct {
	MonoLiteDocs  []interface{}
	MongoDBDocs   []interface{}
	MonoLiteError error
	MongoDBError  error
}

// DiffSetup is called to prepare the test environment.
type DiffSetup func(ctx context.Context, clients *DiffClients) error

// DiffAction performs the operation on both databases and returns results.
type DiffAction func(ctx context.Context, clients *DiffClients) (*DiffResult, error)

// RunDiffCase runs a differential test case.
func RunDiffCase(t *testing.T, name string, setup DiffSetup, action DiffAction) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		clients := StartDiffClients(t)
		defer clients.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Run setup
		if setup != nil {
			if err := setup(ctx, clients); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
		}

		// Run action
		result, err := action(ctx, clients)
		if err != nil {
			t.Fatalf("action failed: %v", err)
		}

		// Compare results
		AssertDiffEqual(t, result)
	})
}

// AssertDiffEqual asserts that MonoLite and MongoDB results are equivalent.
func AssertDiffEqual(t *testing.T, result *DiffResult) {
	t.Helper()

	// Compare errors
	if !errorsEquivalent(result.MonoLiteError, result.MongoDBError) {
		t.Errorf("Error mismatch:\n  MonoLite: %v\n  MongoDB: %v",
			result.MonoLiteError, result.MongoDBError)
	}

	// If both have errors, we're done
	if result.MonoLiteError != nil && result.MongoDBError != nil {
		return
	}

	// Compare document counts
	if len(result.MonoLiteDocs) != len(result.MongoDBDocs) {
		t.Errorf("Document count mismatch:\n  MonoLite: %d\n  MongoDB: %d",
			len(result.MonoLiteDocs), len(result.MongoDBDocs))
		return
	}

	// Canonicalize and compare documents
	monoCanon, err := CanonicalizeDocs(result.MonoLiteDocs)
	if err != nil {
		t.Errorf("Failed to canonicalize MonoLite docs: %v", err)
		return
	}

	mongoCanon, err := CanonicalizeDocs(result.MongoDBDocs)
	if err != nil {
		t.Errorf("Failed to canonicalize MongoDB docs: %v", err)
		return
	}

	for i := range monoCanon {
		if !monoCanon[i].Equal(mongoCanon[i]) {
			t.Errorf("Document %d mismatch:\n  MonoLite: %s\n  MongoDB: %s",
				i, monoCanon[i].String(), mongoCanon[i].String())
		}
	}
}

// errorsEquivalent checks if two errors are equivalent.
func errorsEquivalent(monoErr, mongoErr error) bool {
	// Both nil
	if monoErr == nil && mongoErr == nil {
		return true
	}

	// Both non-nil
	if monoErr != nil && mongoErr != nil {
		// For now, just check that both have errors
		// More sophisticated comparison could check error codes
		return true
	}

	// One nil, one not
	return false
}

// InsertDocs inserts documents into both databases.
func InsertDocs(ctx context.Context, clients *DiffClients, dbName, collName string, docs []interface{}) error {
	// Insert into MonoLite
	monoCol, err := clients.MonoLite.DB.Collection(collName)
	if err != nil {
		return fmt.Errorf("failed to get MonoLite collection: %w", err)
	}

	for _, doc := range docs {
		var d bson.D
		switch v := doc.(type) {
		case bson.D:
			d = v
		default:
			// Try to convert to bson.D
			data, err := bson.Marshal(doc)
			if err != nil {
				return fmt.Errorf("failed to marshal doc for MonoLite: %w", err)
			}
			if err := bson.Unmarshal(data, &d); err != nil {
				return fmt.Errorf("failed to unmarshal doc for MonoLite: %w", err)
			}
		}
		if _, err := monoCol.Insert(d); err != nil {
			return fmt.Errorf("failed to insert into MonoLite: %w", err)
		}
	}

	// Insert into MongoDB
	mongoCol := clients.MongoDB.Database(dbName).Collection(collName)
	if _, err := mongoCol.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("failed to insert into MongoDB: %w", err)
	}

	return nil
}

// FindAll retrieves all documents from a collection.
func FindAll(ctx context.Context, clients *DiffClients, dbName, collName string, filter interface{}) (*DiffResult, error) {
	result := &DiffResult{}

	// Query MonoLite
	monoCol, err := clients.MonoLite.DB.Collection(collName)
	if err != nil {
		result.MonoLiteError = err
	} else {
		var filterD bson.D
		if filter != nil {
			switch v := filter.(type) {
			case bson.D:
				filterD = v
			default:
				data, _ := bson.Marshal(filter)
				bson.Unmarshal(data, &filterD)
			}
		}

		monoDocs, err := monoCol.Find(filterD)
		if err != nil {
			result.MonoLiteError = err
		} else {
			for _, d := range monoDocs {
				result.MonoLiteDocs = append(result.MonoLiteDocs, d)
			}
		}
	}

	// Query MongoDB
	mongoCol := clients.MongoDB.Database(dbName).Collection(collName)
	cursor, err := mongoCol.Find(ctx, filter)
	if err != nil {
		result.MongoDBError = err
	} else {
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var doc bson.D
			if err := cursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("failed to decode MongoDB doc: %w", err)
			}
			result.MongoDBDocs = append(result.MongoDBDocs, doc)
		}
	}

	return result, nil
}

// FindSorted retrieves documents with sorting.
func FindSorted(ctx context.Context, clients *DiffClients, dbName, collName string, filter, sort interface{}) (*DiffResult, error) {
	result := &DiffResult{}

	// Query MonoLite
	monoCol, err := clients.MonoLite.DB.Collection(collName)
	if err != nil {
		result.MonoLiteError = err
	} else {
		var filterD bson.D
		if filter != nil {
			switch v := filter.(type) {
			case bson.D:
				filterD = v
			default:
				data, _ := bson.Marshal(filter)
				bson.Unmarshal(data, &filterD)
			}
		}

		var sortD bson.D
		if sort != nil {
			switch v := sort.(type) {
			case bson.D:
				sortD = v
			default:
				data, _ := bson.Marshal(sort)
				bson.Unmarshal(data, &sortD)
			}
		}

		// MonoLite uses FindWithOptions
		opts := &engine.QueryOptions{Sort: sortD}
		monoDocs, err := monoCol.FindWithOptions(filterD, opts)
		if err != nil {
			result.MonoLiteError = err
		} else {
			for _, d := range monoDocs {
				result.MonoLiteDocs = append(result.MonoLiteDocs, d)
			}
		}
	}

	// Query MongoDB
	mongoCol := clients.MongoDB.Database(dbName).Collection(collName)
	findOpts := options.Find()
	if sort != nil {
		findOpts.SetSort(sort)
	}
	cursor, err := mongoCol.Find(ctx, filter, findOpts)
	if err != nil {
		result.MongoDBError = err
	} else {
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var doc bson.D
			if err := cursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("failed to decode MongoDB doc: %w", err)
			}
			result.MongoDBDocs = append(result.MongoDBDocs, doc)
		}
	}

	return result, nil
}

// CompareErrorCodes checks if error codes match between MonoLite and MongoDB.
func CompareErrorCodes(monoErr, mongoErr error) bool {
	if monoErr == nil && mongoErr == nil {
		return true
	}
	if monoErr == nil || mongoErr == nil {
		return false
	}

	// Extract error codes if available
	// MongoDB errors typically have a Code field
	// MonoLite errors should implement a similar interface

	monoCode := extractErrorCode(monoErr)
	mongoCode := extractErrorCode(mongoErr)

	return monoCode == mongoCode
}

// extractErrorCode extracts an error code from an error if available.
func extractErrorCode(err error) int {
	if err == nil {
		return 0
	}

	// Try to get error code via reflection (mongo driver uses CommandError)
	v := reflect.ValueOf(err)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		codeField := v.FieldByName("Code")
		if codeField.IsValid() && codeField.Kind() == reflect.Int32 {
			return int(codeField.Int())
		}
	}

	return -1 // Unknown code
}

// CleanupCollection drops a collection in both databases.
func CleanupCollection(ctx context.Context, clients *DiffClients, dbName, collName string) error {
	// Drop in MonoLite (if supported)
	// For now, MonoLite might not support drop, so we'll skip errors

	// Drop in MongoDB
	if err := clients.MongoDB.Database(dbName).Collection(collName).Drop(ctx); err != nil {
		// Ignore "ns not found" error
		if !mongo.IsDuplicateKeyError(err) {
			return err
		}
	}

	return nil
}

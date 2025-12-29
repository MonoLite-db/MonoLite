// Created by Yanjunhui

package engine

import (
	"testing"
)

func TestDatabaseValidateBasic(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// 空数据库也应能通过 validate（至少不应 panic）
	// EN: An empty database should pass validate (at least it must not panic).
	res := db.Validate()
	if res == nil {
		t.Fatalf("expected Validate() result, got nil")
	}
	// 允许 warnings，但不应有 errors
	// EN: Warnings are allowed, but errors are not.
	if !res.Valid {
		t.Fatalf("expected Validate() to be valid, errors=%v warnings=%v", res.Errors, res.Warnings)
	}
}

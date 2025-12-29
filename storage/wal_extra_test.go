// Created by Yanjunhui

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriteCommitRecord(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "commit.wal")

	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	lsn1, err := w.WriteAllocRecord(1, PageTypeData)
	if err != nil {
		t.Fatalf("WriteAllocRecord failed: %v", err)
	}
	lsn2, err := w.WriteCommitRecord()
	if err != nil {
		t.Fatalf("WriteCommitRecord failed: %v", err)
	}
	if lsn2 != lsn1+1 {
		t.Fatalf("unexpected LSN sequence: alloc=%d commit=%d", lsn1, lsn2)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	records, err := w.ReadRecordsFrom(1)
	if err != nil {
		t.Fatalf("ReadRecordsFrom failed: %v", err)
	}
	if len(records) < 2 {
		t.Fatalf("expected at least 2 records, got %d", len(records))
	}
	if records[len(records)-1].Type != WALRecordCommit {
		t.Fatalf("expected last record type=%d(commit), got=%d", WALRecordCommit, records[len(records)-1].Type)
	}
}

func TestWALTruncateAfterCheckpointLockedAndSetAutoTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "truncate_locked.wal")

	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	// 写几条记录，确保 WAL 文件大于 header
	for i := 0; i < 5; i++ {
		if _, err := w.WriteAllocRecord(PageId(i+1), PageTypeData); err != nil {
			t.Fatalf("WriteAllocRecord failed: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal failed: %v", err)
	}
	if fi.Size() <= WALHeaderSize {
		t.Fatalf("expected wal file size > header before truncate, got %d", fi.Size())
	}

	// 覆盖 SetAutoTruncate（本测试不强制触发阈值分支，只覆盖该函数本身）
	w.SetAutoTruncate(true)
	if !w.autoTruncate {
		t.Fatalf("expected autoTruncate to be enabled")
	}

	// 覆盖 truncateAfterCheckpointLocked（按注释要求：调用者需要持有锁）
	beforeCurrent := w.currentLSN
	beforeCheckpoint := w.checkpointLSN

	w.mu.Lock()
	err = w.truncateAfterCheckpointLocked()
	w.mu.Unlock()
	if err != nil {
		t.Fatalf("truncateAfterCheckpointLocked failed: %v", err)
	}

	fi2, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after truncate failed: %v", err)
	}
	if fi2.Size() != WALHeaderSize {
		t.Fatalf("expected wal size=%d after truncate, got %d", WALHeaderSize, fi2.Size())
	}
	if w.writeOffset != WALHeaderSize || w.header.FileSize != WALHeaderSize {
		t.Fatalf("unexpected offsets after truncate: writeOffset=%d fileSize=%d", w.writeOffset, w.header.FileSize)
	}
	// 语义要求：currentLSN/checkpointLSN 不变
	if w.currentLSN != beforeCurrent || w.checkpointLSN != beforeCheckpoint {
		t.Fatalf("lsn changed unexpectedly: current %d->%d checkpoint %d->%d",
			beforeCurrent, w.currentLSN, beforeCheckpoint, w.checkpointLSN)
	}
}



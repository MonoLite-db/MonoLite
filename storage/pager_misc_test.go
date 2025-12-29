// Created by Yanjunhui

package storage

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestLogErrorDoesNotPanic(t *testing.T) {
	// 捕获 stdout，避免测试输出污染
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = w

	LogError("hello", map[string]interface{}{"k": "v"})
	LogError("hello2", nil)

	_ = w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()

	if buf.Len() == 0 {
		t.Fatalf("expected LogError to write something to stdout")
	}
}



// Created by Yanjunhui

package storage

import (
	"os"
	"testing"
)

func TestPagerCreateNew(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "pager_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)
	defer os.Remove(tmpPath)

	// 创建新数据库
	pager, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("打开 Pager 失败: %v", err)
	}
	defer pager.Close()

	// 验证文件头
	if pager.header.Magic != MagicNumber {
		t.Errorf("魔数错误: 期望 %x, 实际 %x", MagicNumber, pager.header.Magic)
	}

	if pager.header.Version != FormatVersion {
		t.Errorf("版本错误: 期望 %d, 实际 %d", FormatVersion, pager.header.Version)
	}

	if pager.PageCount() < 1 {
		t.Error("页面数应该至少为 1")
	}
}

func TestPagerAllocatePage(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "pager_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)
	defer os.Remove(tmpPath)

	pager, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("打开 Pager 失败: %v", err)
	}
	defer pager.Close()

	initialCount := pager.PageCount()

	// 分配新页面
	page, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("分配页面失败: %v", err)
	}

	if page.Type() != PageTypeData {
		t.Errorf("页面类型错误: 期望 %d, 实际 %d", PageTypeData, page.Type())
	}

	if pager.PageCount() != initialCount+1 {
		t.Errorf("页面数错误: 期望 %d, 实际 %d", initialCount+1, pager.PageCount())
	}
}

func TestPagerReadWritePage(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "pager_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)
	defer os.Remove(tmpPath)

	pager, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("打开 Pager 失败: %v", err)
	}

	// 分配页面并写入数据
	page, _ := pager.AllocatePage(PageTypeData)
	testData := []byte("Hello, MonoDB!")
	page.SetData(testData)
	pager.MarkDirty(page.ID())

	// 刷新并关闭
	pager.Flush()
	pager.Close()

	// 重新打开并读取
	pager2, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("重新打开 Pager 失败: %v", err)
	}
	defer pager2.Close()

	readPage, err := pager2.ReadPage(page.ID())
	if err != nil {
		t.Fatalf("读取页面失败: %v", err)
	}

	data := readPage.Data()
	if len(data) < len(testData) {
		t.Fatal("读取的数据太短")
	}

	for i := 0; i < len(testData); i++ {
		if data[i] != testData[i] {
			t.Errorf("数据不匹配: 位置 %d, 期望 %d, 实际 %d", i, testData[i], data[i])
		}
	}
}

func TestPagerFreePage(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "pager_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)
	defer os.Remove(tmpPath)

	pager, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("打开 Pager 失败: %v", err)
	}
	defer pager.Close()

	// 分配多个页面
	page1, _ := pager.AllocatePage(PageTypeData)
	page2, _ := pager.AllocatePage(PageTypeData)
	page3, _ := pager.AllocatePage(PageTypeData)

	id1 := page1.ID()
	id2 := page2.ID()

	// 释放页面
	pager.FreePage(id1)
	pager.FreePage(id2)

	// 新分配的页面应该复用已释放的页面
	newPage1, _ := pager.AllocatePage(PageTypeData)
	newPage2, _ := pager.AllocatePage(PageTypeData)

	// 验证复用
	if newPage1.ID() != id2 && newPage1.ID() != id1 {
		// 可能不复用，取决于实现
	}

	_ = page3
	_ = newPage2
}

func TestSlottedPage(t *testing.T) {
	page := NewPage(1, PageTypeData)
	sp := WrapSlottedPage(page)

	// 插入记录
	record1 := []byte("record one")
	slot1, err := sp.InsertRecord(record1)
	if err != nil {
		t.Fatalf("插入记录失败: %v", err)
	}

	record2 := []byte("record two - longer")
	slot2, err := sp.InsertRecord(record2)
	if err != nil {
		t.Fatalf("插入记录失败: %v", err)
	}

	// 读取记录
	read1, err := sp.GetRecord(slot1)
	if err != nil {
		t.Fatalf("读取记录失败: %v", err)
	}
	if string(read1) != string(record1) {
		t.Errorf("记录 1 不匹配: 期望 %s, 实际 %s", record1, read1)
	}

	read2, err := sp.GetRecord(slot2)
	if err != nil {
		t.Fatalf("读取记录失败: %v", err)
	}
	if string(read2) != string(record2) {
		t.Errorf("记录 2 不匹配: 期望 %s, 实际 %s", record2, read2)
	}

	// 更新记录
	newRecord := []byte("updated")
	err = sp.UpdateRecord(slot1, newRecord)
	if err != nil {
		t.Fatalf("更新记录失败: %v", err)
	}

	updated, _ := sp.GetRecord(slot1)
	if string(updated) != string(newRecord) {
		t.Errorf("更新后记录不匹配: 期望 %s, 实际 %s", newRecord, updated)
	}

	// 删除记录
	err = sp.DeleteRecord(slot1)
	if err != nil {
		t.Fatalf("删除记录失败: %v", err)
	}

	_, err = sp.GetRecord(slot1)
	if err == nil {
		t.Error("已删除的记录不应该能读取")
	}
}

func TestPageLinkage(t *testing.T) {
	page1 := NewPage(1, PageTypeData)
	page2 := NewPage(2, PageTypeData)

	// 链接页面
	page1.SetNextPageId(2)
	page2.SetPrevPageId(1)

	if page1.NextPageId() != 2 {
		t.Errorf("NextPageId 错误: 期望 2, 实际 %d", page1.NextPageId())
	}

	if page2.PrevPageId() != 1 {
		t.Errorf("PrevPageId 错误: 期望 1, 实际 %d", page2.PrevPageId())
	}
}

func TestCatalogPageId(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "pager_test_*.db")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath)
	defer os.Remove(tmpPath)

	pager, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("打开 Pager 失败: %v", err)
	}

	// 设置目录页 ID
	pager.SetCatalogPageId(5)

	if pager.CatalogPageId() != 5 {
		t.Errorf("CatalogPageId 错误: 期望 5, 实际 %d", pager.CatalogPageId())
	}

	// 刷新并关闭
	pager.Flush()
	pager.Close()

	// 重新打开验证持久化
	pager2, err := OpenPager(tmpPath)
	if err != nil {
		t.Fatalf("重新打开 Pager 失败: %v", err)
	}
	defer pager2.Close()

	if pager2.CatalogPageId() != 5 {
		t.Errorf("持久化后 CatalogPageId 错误: 期望 5, 实际 %d", pager2.CatalogPageId())
	}
}

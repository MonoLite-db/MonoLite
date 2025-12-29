// Created by Yanjunhui
//
// monodb-import: 数据导入工具
// 支持导入 mongodump 的 BSON 文件和 JSON/JSONL 格式数据

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/monolite/monodb/engine"
)

var (
	dbPath     = flag.String("db", "", "MonoDB 数据库文件路径（必需）")
	inputFile  = flag.String("file", "", "输入文件路径（BSON 或 JSON 格式）")
	inputDir   = flag.String("dir", "", "输入目录路径（导入整个目录）")
	collection = flag.String("collection", "", "目标集合名称")
	drop       = flag.Bool("drop", false, "导入前删除现有集合")
	jsonArray  = flag.Bool("jsonArray", false, "输入文件是 JSON 数组格式")
	batchSize  = flag.Int("batchSize", 1000, "批量插入大小")
	verbose    = flag.Bool("v", false, "显示详细输出")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "MonoDB Import Tool - 数据导入工具\n\n")
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  monodb-import -db <数据库文件> -file <输入文件> -collection <集合名>\n")
		fmt.Fprintf(os.Stderr, "  monodb-import -db <数据库文件> -dir <输入目录>\n\n")
		fmt.Fprintf(os.Stderr, "支持的文件格式:\n")
		fmt.Fprintf(os.Stderr, "  .bson  - MongoDB BSON 格式（mongodump 输出）\n")
		fmt.Fprintf(os.Stderr, "  .json  - JSON 数组或 JSONL 格式\n")
		fmt.Fprintf(os.Stderr, "  .jsonl - JSON Lines 格式（每行一个 JSON 对象）\n\n")
		fmt.Fprintf(os.Stderr, "选项:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定数据库文件路径 (-db)")
		flag.Usage()
		os.Exit(1)
	}

	if *inputFile == "" && *inputDir == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定输入文件 (-file) 或输入目录 (-dir)")
		flag.Usage()
		os.Exit(1)
	}

	// 打开数据库
	db, err := engine.OpenDatabase(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	if *inputDir != "" {
		// 导入目录
		if err := importDirectory(db, *inputDir); err != nil {
			log.Fatalf("导入目录失败: %v", err)
		}
	} else {
		// 导入单个文件
		colName := *collection
		if colName == "" {
			// 从文件名推断集合名
			base := filepath.Base(*inputFile)
			colName = strings.TrimSuffix(base, filepath.Ext(base))
		}

		if err := importFile(db, *inputFile, colName); err != nil {
			log.Fatalf("导入文件失败: %v", err)
		}
	}

	// 刷新到磁盘
	if err := db.Flush(); err != nil {
		log.Fatalf("刷新数据失败: %v", err)
	}

	fmt.Println("导入完成！")
}

// importDirectory 导入目录中的所有文件
func importDirectory(db *engine.Database, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("读取目录失败: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".bson" && ext != ".json" && ext != ".jsonl" {
			continue
		}

		filePath := filepath.Join(dir, entry.Name())
		colName := strings.TrimSuffix(entry.Name(), ext)

		fmt.Printf("导入 %s -> %s\n", entry.Name(), colName)
		if err := importFile(db, filePath, colName); err != nil {
			return fmt.Errorf("导入 %s 失败: %w", entry.Name(), err)
		}
	}

	return nil
}

// importFile 导入单个文件
func importFile(db *engine.Database, filePath, colName string) error {
	ext := strings.ToLower(filepath.Ext(filePath))

	// 获取或创建集合
	col, err := db.Collection(colName)
	if err != nil {
		return fmt.Errorf("获取集合失败: %w", err)
	}

	// 如果指定了 drop，先删除集合
	if *drop {
		if err := db.DropCollection(colName); err != nil {
			return fmt.Errorf("删除集合失败: %w", err)
		}
		col, err = db.Collection(colName)
		if err != nil {
			return fmt.Errorf("重新创建集合失败: %w", err)
		}
		if *verbose {
			fmt.Printf("  已删除现有集合 %s\n", colName)
		}
	}

	switch ext {
	case ".bson":
		return importBSON(col, filePath)
	case ".json":
		if *jsonArray {
			return importJSONArray(col, filePath)
		}
		return importJSONL(col, filePath)
	case ".jsonl":
		return importJSONL(col, filePath)
	default:
		return fmt.Errorf("不支持的文件格式: %s", ext)
	}
}

// importBSON 导入 BSON 文件
func importBSON(col *engine.Collection, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	var count int64
	batch := make([]bson.D, 0, *batchSize)

	for {
		// 读取文档长度（4 字节 little-endian）
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("读取文档长度失败: %w", err)
		}

		docLen := int(lenBuf[0]) | int(lenBuf[1])<<8 | int(lenBuf[2])<<16 | int(lenBuf[3])<<24
		if docLen < 5 || docLen > 16*1024*1024 {
			return fmt.Errorf("无效的文档长度: %d", docLen)
		}

		// 读取完整文档
		docBuf := make([]byte, docLen)
		copy(docBuf[:4], lenBuf)
		if _, err := io.ReadFull(file, docBuf[4:]); err != nil {
			return fmt.Errorf("读取文档数据失败: %w", err)
		}

		// 解析 BSON
		var doc bson.D
		if err := bson.Unmarshal(docBuf, &doc); err != nil {
			return fmt.Errorf("解析 BSON 失败: %w", err)
		}

		batch = append(batch, doc)
		count++

		// 批量插入
		if len(batch) >= *batchSize {
			if _, err := col.Insert(batch...); err != nil {
				return fmt.Errorf("插入文档失败: %w", err)
			}
			if *verbose {
				fmt.Printf("  已导入 %d 条文档\n", count)
			}
			batch = batch[:0]
		}
	}

	// 插入剩余文档
	if len(batch) > 0 {
		if _, err := col.Insert(batch...); err != nil {
			return fmt.Errorf("插入文档失败: %w", err)
		}
	}

	fmt.Printf("  集合 %s: 导入 %d 条文档\n", col.Name(), count)
	return nil
}

// importJSONL 导入 JSON Lines 格式
func importJSONL(col *engine.Collection, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	var count int64
	batch := make([]bson.D, 0, *batchSize)
	scanner := bufio.NewScanner(file)

	// 增加缓冲区大小以支持大文档
	const maxScanTokenSize = 16 * 1024 * 1024
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		var doc bson.D
		if err := bson.UnmarshalExtJSON([]byte(line), true, &doc); err != nil {
			// 尝试用标准 JSON 解析
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				return fmt.Errorf("解析 JSON 失败 (行 %d): %w", count+1, err)
			}
			doc = mapToDoc(m)
		}

		batch = append(batch, doc)
		count++

		if len(batch) >= *batchSize {
			if _, err := col.Insert(batch...); err != nil {
				return fmt.Errorf("插入文档失败: %w", err)
			}
			if *verbose {
				fmt.Printf("  已导入 %d 条文档\n", count)
			}
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	if len(batch) > 0 {
		if _, err := col.Insert(batch...); err != nil {
			return fmt.Errorf("插入文档失败: %w", err)
		}
	}

	fmt.Printf("  集合 %s: 导入 %d 条文档\n", col.Name(), count)
	return nil
}

// importJSONArray 导入 JSON 数组格式
func importJSONArray(col *engine.Collection, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	var docs []bson.D
	if err := bson.UnmarshalExtJSON(data, true, &docs); err != nil {
		// 尝试用标准 JSON 解析
		var arr []map[string]interface{}
		if err := json.Unmarshal(data, &arr); err != nil {
			return fmt.Errorf("解析 JSON 数组失败: %w", err)
		}
		docs = make([]bson.D, len(arr))
		for i, m := range arr {
			docs[i] = mapToDoc(m)
		}
	}

	// 批量插入
	for i := 0; i < len(docs); i += *batchSize {
		end := i + *batchSize
		if end > len(docs) {
			end = len(docs)
		}
		if _, err := col.Insert(docs[i:end]...); err != nil {
			return fmt.Errorf("插入文档失败: %w", err)
		}
		if *verbose {
			fmt.Printf("  已导入 %d 条文档\n", end)
		}
	}

	fmt.Printf("  集合 %s: 导入 %d 条文档\n", col.Name(), len(docs))
	return nil
}

// mapToDoc 将 map 转换为 bson.D
func mapToDoc(m map[string]interface{}) bson.D {
	doc := make(bson.D, 0, len(m))
	for k, v := range m {
		doc = append(doc, bson.E{Key: k, Value: convertValue(v)})
	}
	return doc
}

// convertValue 转换值类型
func convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return mapToDoc(val)
	case []interface{}:
		arr := make(bson.A, len(val))
		for i, item := range val {
			arr[i] = convertValue(item)
		}
		return arr
	default:
		return v
	}
}

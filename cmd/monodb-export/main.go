// Created by Yanjunhui
//
// monodb-export: 数据导出工具
// 支持导出为 BSON（MongoDB 兼容）和 JSON 格式

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/monolite/monodb/engine"
)

var (
	dbPath     = flag.String("db", "", "MonoDB 数据库文件路径（必需）")
	outputFile = flag.String("out", "", "输出文件路径")
	outputDir  = flag.String("dir", "", "输出目录路径（导出所有集合）")
	collection = flag.String("collection", "", "要导出的集合名称")
	query      = flag.String("query", "", "查询过滤器（JSON 格式）")
	format     = flag.String("format", "json", "输出格式: json, jsonl, bson")
	pretty     = flag.Bool("pretty", false, "格式化 JSON 输出")
	verbose    = flag.Bool("v", false, "显示详细输出")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "MonoDB Export Tool - 数据导出工具\n\n")
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  monodb-export -db <数据库文件> -collection <集合名> -out <输出文件>\n")
		fmt.Fprintf(os.Stderr, "  monodb-export -db <数据库文件> -dir <输出目录>\n\n")
		fmt.Fprintf(os.Stderr, "输出格式:\n")
		fmt.Fprintf(os.Stderr, "  json  - JSON 数组格式\n")
		fmt.Fprintf(os.Stderr, "  jsonl - JSON Lines 格式（每行一个文档）\n")
		fmt.Fprintf(os.Stderr, "  bson  - MongoDB BSON 格式（可用于 mongorestore）\n\n")
		fmt.Fprintf(os.Stderr, "选项:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定数据库文件路径 (-db)")
		flag.Usage()
		os.Exit(1)
	}

	// 打开数据库
	db, err := engine.OpenDatabase(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	if *outputDir != "" {
		// 导出所有集合到目录
		if err := exportAllCollections(db, *outputDir); err != nil {
			log.Fatalf("导出失败: %v", err)
		}
	} else {
		// 导出单个集合
		if *collection == "" {
			fmt.Fprintln(os.Stderr, "错误: 必须指定集合名称 (-collection) 或输出目录 (-dir)")
			flag.Usage()
			os.Exit(1)
		}

		output := *outputFile
		if output == "" {
			output = *collection + "." + *format
		}

		if err := exportCollection(db, *collection, output); err != nil {
			log.Fatalf("导出失败: %v", err)
		}
	}

	fmt.Println("导出完成！")
}

// exportAllCollections 导出所有集合
func exportAllCollections(db *engine.Database, dir string) error {
	// 创建输出目录
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %w", err)
	}

	collections := db.ListCollections()
	if len(collections) == 0 {
		fmt.Println("数据库中没有集合")
		return nil
	}

	for _, colName := range collections {
		outputPath := filepath.Join(dir, colName+"."+*format)
		fmt.Printf("导出 %s -> %s\n", colName, outputPath)

		if err := exportCollection(db, colName, outputPath); err != nil {
			return fmt.Errorf("导出集合 %s 失败: %w", colName, err)
		}
	}

	return nil
}

// exportCollection 导出单个集合
func exportCollection(db *engine.Database, colName, outputPath string) error {
	col := db.GetCollection(colName)
	if col == nil {
		return fmt.Errorf("集合 %s 不存在", colName)
	}

	// 解析查询过滤器
	var filter bson.D
	if *query != "" {
		if err := bson.UnmarshalExtJSON([]byte(*query), true, &filter); err != nil {
			return fmt.Errorf("解析查询过滤器失败: %w", err)
		}
	}

	// 查询文档
	docs, err := col.Find(filter)
	if err != nil {
		return fmt.Errorf("查询失败: %w", err)
	}

	if *verbose {
		fmt.Printf("  找到 %d 条文档\n", len(docs))
	}

	// 创建输出文件
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	// 根据格式导出
	switch *format {
	case "bson":
		return exportBSON(file, docs)
	case "jsonl":
		return exportJSONL(file, docs)
	case "json":
		return exportJSON(file, docs)
	default:
		return fmt.Errorf("不支持的格式: %s", *format)
	}
}

// exportBSON 导出为 BSON 格式
func exportBSON(file *os.File, docs []bson.D) error {
	for _, doc := range docs {
		data, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("序列化 BSON 失败: %w", err)
		}
		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	fmt.Printf("  导出 %d 条文档 (BSON 格式)\n", len(docs))
	return nil
}

// exportJSONL 导出为 JSON Lines 格式
func exportJSONL(file *os.File, docs []bson.D) error {
	for _, doc := range docs {
		var data []byte
		var err error

		if *pretty {
			data, err = bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		} else {
			data, err = bson.MarshalExtJSON(doc, false, false)
		}
		if err != nil {
			return fmt.Errorf("序列化 JSON 失败: %w", err)
		}

		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	fmt.Printf("  导出 %d 条文档 (JSONL 格式)\n", len(docs))
	return nil
}

// exportJSON 导出为 JSON 数组格式
func exportJSON(file *os.File, docs []bson.D) error {
	// 转换为可 JSON 序列化的格式
	arr := make([]interface{}, len(docs))
	for i, doc := range docs {
		arr[i] = docToMap(doc)
	}

	var data []byte
	var err error

	if *pretty {
		data, err = json.MarshalIndent(arr, "", "  ")
	} else {
		data, err = json.Marshal(arr)
	}
	if err != nil {
		return fmt.Errorf("序列化 JSON 失败: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}
	if _, err := file.WriteString("\n"); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	fmt.Printf("  导出 %d 条文档 (JSON 格式)\n", len(docs))
	return nil
}

// docToMap 将 bson.D 转换为 map
func docToMap(doc bson.D) map[string]interface{} {
	m := make(map[string]interface{})
	for _, elem := range doc {
		m[elem.Key] = convertValue(elem.Value)
	}
	return m
}

// convertValue 转换值类型以便 JSON 序列化
func convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case bson.D:
		return docToMap(val)
	case bson.A:
		arr := make([]interface{}, len(val))
		for i, item := range val {
			arr[i] = convertValue(item)
		}
		return arr
	default:
		return v
	}
}

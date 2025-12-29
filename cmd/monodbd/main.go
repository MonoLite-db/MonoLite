// Created by Yanjunhui

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/monolite/monodb/engine"
	"github.com/monolite/monodb/protocol"
)

func main() {
	// 命令行参数
	// EN: Command-line flags.
	var (
		dbFile = flag.String("file", "data.monodb", "数据库文件路径")
		addr   = flag.String("addr", ":27017", "监听地址")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("MonoDB v0.1.0 starting...")
	log.Printf("Database file: %s", *dbFile)
	log.Printf("Listen address: %s", *addr)

	// 打开数据库
	// EN: Open database.
	db, err := engine.OpenDatabase(*dbFile)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		log.Println("Closing database...")
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// 创建并启动服务器
	// EN: Create and start the server.
	server := protocol.NewServer(*addr, db)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("MonoDB server started successfully")
	log.Printf("Connect with: mongosh mongodb://localhost%s", *addr)

	// 等待退出信号
	// EN: Wait for termination signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	if err := server.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}

	log.Println("MonoDB server stopped")
}

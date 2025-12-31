# MonoLite

MonoLite 是一个**单文件、可嵌入的文档数据库**，专为 Go 设计，兼容 MongoDB Wire Protocol。纯 Go 实现，嵌入优先设计。

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

**[文档 (中文)](docs/COMPATIBILITY_CN.md)** · **[Docs (EN)](docs/COMPATIBILITY.md)** · **[README (中文)](README_CN.md)** · **[README (EN)](README.md)** · **[问题反馈](https://github.com/monolite/monodb/issues)** · **[贡献指南](CONTRIBUTING.md)**

</div>

## 项目定位

> **像 SQLite 一样简单，却能用 MongoDB 方式思考和使用的文档数据库。**

- **单文件存储** — 一个 `.monodb` 文件即是完整数据库
- **零部署、零运维** — 无需安装、无需配置，开箱即用
- **嵌入式优先** — Library-first 设计，可直接嵌入应用
- **兼容 MongoDB 官方驱动** — 使用熟悉的 API 和工具

## 为什么选择 MonoLite？我们解决的痛点

### SQLite 的困境

SQLite 是一个优秀的嵌入式数据库，但当你的应用处理**文档型数据**时，会遇到这些困扰：

| 痛点 | SQLite 现状 | MonoLite 方案 |
|------|-------------|-------------|
| **僵化的 Schema** | 必须用 `CREATE TABLE` 预定义表结构，修改需要 `ALTER TABLE` 迁移 | Schema-free — 文档可以有不同字段，自然演进 |
| **嵌套数据** | 需要 JSON1 扩展或序列化，查询笨拙 | 原生嵌套文档，支持点号路径查询（`address.city`） |
| **数组操作** | 无原生数组类型，需序列化或使用关联表 | 原生数组，支持 `$push`、`$pull`、`$elemMatch` 等操作符 |
| **对象-关系阻抗不匹配** | 应用对象 ↔ 关系表需要 ORM 或手动映射 | 文档直接映射到应用对象 |
| **查询复杂性** | 层级数据需要复杂 JOIN，SQL 冗长 | 直观的查询操作符（`$gt`、`$in`、`$or`）和聚合管道 |
| **学习曲线** | SQL 语法各异，JOIN 逻辑复杂 | MongoDB 查询语言基于 JavaScript，广为人知 |

### 何时选择 MonoLite 而非 SQLite

✅ **选择 MonoLite 当：**
- 你的数据天然是层级或文档形态（类 JSON）
- 文档结构多变（可选字段、演进中的 Schema）
- 你需要强大的数组操作
- 你的团队已经熟悉 MongoDB
- 你想用 MongoDB 兼容的方式原型开发，未来可迁移到真正的 MongoDB

✅ **继续使用 SQLite 当：**
- 你的数据高度关系化，有大量多对多关系
- 你需要复杂的多表 JOIN
- 你需要严格的 Schema 约束
- 你使用现有的 SQL 工具链

### MonoLite vs SQLite：功能对比

| 特性 | MonoLite | SQLite |
|------|--------|--------|
| **数据模型** | 文档（BSON） | 关系型（表） |
| **Schema** | 灵活，无 Schema 约束 | 固定，需要迁移 |
| **嵌套数据** | 原生支持 | JSON1 扩展 |
| **数组** | 原生支持，丰富操作符 | 需要序列化 |
| **查询语言** | MongoDB 查询语言 | SQL |
| **事务** | ✅ 多文档 ACID | ✅ ACID |
| **索引** | B+Tree（单字段、复合、唯一） | B-Tree（多种类型） |
| **文件格式** | 单个 `.monodb` 文件 | 单个 `.db` 文件 |
| **崩溃恢复** | WAL | WAL/回滚日志 |
| **成熟度** | 新项目 | 20+ 年久经考验 |
| **生态系统** | MongoDB 驱动兼容 | 庞大的生态系统 |

## 快速开始

### 安装

```bash
go get github.com/monolite/monodb
```

### 基本使用（库 API）

```go
package main

import (
    "fmt"
    "log"

    "github.com/monolite/monodb/engine"
    "go.mongodb.org/mongo-driver/bson"
)

func main() {
    // 打开数据库
    db, err := engine.OpenDatabase("data.monodb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 获取集合
    users, err := db.Collection("users")
    if err != nil {
        log.Fatal(err)
    }

    // 插入文档
    users.Insert(bson.D{
        {Key: "name", Value: "Alice"},
        {Key: "age", Value: 25},
        {Key: "email", Value: "alice@example.com"},
    })

    // 批量插入
    users.Insert(
        bson.D{
            {Key: "name", Value: "Bob"},
            {Key: "age", Value: 30},
            {Key: "tags", Value: bson.A{"dev", "go"}},
        },
        bson.D{
            {Key: "name", Value: "Carol"},
            {Key: "age", Value: 28},
            {Key: "address", Value: bson.D{{Key: "city", Value: "Beijing"}}},
        },
    )

    // 查询文档
    results, _ := users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 20}}}})
    for _, doc := range results {
        fmt.Println(doc)
    }

    // 查找单个文档
    alice, _ := users.FindOne(bson.D{{Key: "name", Value: "Alice"}})
    fmt.Println("找到:", alice)

    // 点号路径查询
    results, _ = users.Find(bson.D{{Key: "address.city", Value: "Beijing"}})

    // 更新文档
    users.Update(
        bson.D{{Key: "name", Value: "Alice"}},
        bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 26}}}},
        false, // upsert
    )

    // 带 upsert 的更新
    users.Update(
        bson.D{{Key: "name", Value: "Dave"}},
        bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 35}}}},
        true, // upsert
    )

    // 删除文档
    users.DeleteOne(bson.D{{Key: "name", Value: "Alice"}})
    users.Delete(bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: 18}}}})
}
```

### 聚合管道

```go
orders, _ := db.Collection("orders")

pipeline := []bson.D{
    {{Key: "$match", Value: bson.D{{Key: "status", Value: "completed"}}}},
    {{Key: "$group", Value: bson.D{
        {Key: "_id", Value: "$customerId"},
        {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
    }}},
    {{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
    {{Key: "$limit", Value: 10}},
}

results, _ := orders.Aggregate(pipeline)
```

### 索引管理

```go
users, _ := db.Collection("users")

// 创建唯一索引
users.CreateIndex(bson.D{{Key: "email", Value: 1}}, true) // unique: true

// 创建复合索引
users.CreateIndex(bson.D{
    {Key: "name", Value: 1},
    {Key: "age", Value: -1},
}, false)

// 列出索引
indexes := users.ListIndexes()

// 删除索引
users.DropIndex("email_1")
```

### 使用事务

```go
// 开启会话
session, _ := db.StartSession()

// 启动事务
txn, _ := session.StartTransaction()

// 在事务中执行操作
users, _ := db.Collection("users")

// 转账操作
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$inc", Value: bson.D{{Key: "balance", Value: -100}}}},
    false,
)
users.Update(
    bson.D{{Key: "name", Value: "Bob"}},
    bson.D{{Key: "$inc", Value: bson.D{{Key: "balance", Value: 100}}}},
    false,
)

// 提交或中止
if err := txn.Commit(); err != nil {
    txn.Abort()
}
```

### Wire Protocol 服务器（可选）

如果需要 MongoDB 驱动兼容性，可以启动 Wire Protocol 服务器：

```go
import "github.com/monolite/monodb/protocol"

// 启动 MongoDB 兼容服务器
db, _ := engine.OpenDatabase("data.monodb")
server := protocol.NewServer(db, ":27017")
server.Start()

// 现在可以用任何 MongoDB 驱动或 mongosh 连接：
// mongosh mongodb://localhost:27017
```

## 核心特性

### 🔒 崩溃一致性（WAL）

- **Write-Ahead Logging** — 所有写操作先记录到 WAL，再写入数据文件
- **自动崩溃恢复** — 启动时自动回放 WAL，恢复到一致状态
- **Checkpoint 机制** — 定期创建检查点，加速恢复并控制 WAL 大小
- **原子写入** — 保证单个写操作的原子性

### 💾 完整事务支持

- **多文档事务** — 支持跨集合的多文档事务
- **事务 API** — startTransaction / commitTransaction / abortTransaction
- **锁管理** — 文档级和集合级锁粒度
- **死锁检测** — 基于等待图的死锁检测，自动中止死锁事务
- **事务回滚** — 完整的 Undo Log 支持事务回滚

### 🌳 B+Tree 索引

- **高效查找** — O(log n) 的查找复杂度
- **多种索引类型** — 单字段索引、复合索引、唯一索引
- **点号路径支持** — 支持嵌套字段索引（如 `address.city`）
- **叶子节点链表** — 支持高效范围查询和排序
- **字节驱动分裂** — 智能的节点分裂策略，优化空间利用

### 🔐 资源限制与安全

| 限制项 | 值 |
|--------|-----|
| 最大文档大小 | 16 MB |
| 最大嵌套深度 | 100 层 |
| 最大索引数/集合 | 64 个 |
| 最大批量写入 | 100,000 文档 |
| 最大字段名长度 | 1,024 字符 |

- **输入校验** — 严格的文档结构和大小校验
- **防 DoS 攻击** — 限制请求大小和嵌套深度

### 📊 可观测性

- **结构化日志** — JSON 格式日志，便于分析
- **慢查询日志** — 自动记录执行时间超阈值的查询
- **serverStatus 命令** — 实时查看服务器状态
- **内存/存储统计** — 详细的资源使用统计

```go
// 查看服务器状态
status, _ := db.RunCommand(bson.D{{Key: "serverStatus", Value: 1}})

// 查看数据库统计
stats, _ := db.RunCommand(bson.D{{Key: "dbStats", Value: 1}})

// 查看集合统计
users, _ := db.Collection("users")
colStats := users.Stats()
```

## 功能支持状态

### 已支持的核心功能

| 功能分类 | 已支持 |
|----------|--------|
| **CRUD** | insert, find, update, delete, findAndModify, replaceOne, distinct |
| **查询操作符** | $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or, $not, $nor, $exists, $type, $all, $elemMatch, $size, $regex |
| **更新操作符** | $set, $unset, $inc, $min, $max, $mul, $rename, $push, $pop, $pull, $pullAll, $addToSet |
| **聚合阶段** | $match, $project, $sort, $limit, $skip, $group, $count, $unwind, $addFields, $set, $unset, $lookup, $replaceRoot |
| **$group 累加器** | $sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last |
| **索引** | 单字段索引, 复合索引, 唯一索引, 点号路径（嵌套字段） |
| **游标** | getMore, killCursors, batchSize |
| **命令** | dbStats, collStats, listCollections, listIndexes, serverStatus, validate, explain |
| **事务** | startTransaction, commitTransaction, abortTransaction |

### 查询操作符详情

| 类别 | 操作符 |
|------|--------|
| 比较 | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` |
| 逻辑 | `$and` `$or` `$not` `$nor` |
| 元素 | `$exists` `$type` |
| 数组 | `$all` `$elemMatch` `$size` |
| 求值 | `$regex` |

### 更新操作符详情

| 类别 | 操作符 |
|------|--------|
| 字段 | `$set` `$unset` `$inc` `$min` `$max` `$mul` `$rename` |
| 数组 | `$push` `$pop` `$pull` `$pullAll` `$addToSet` |

### 聚合管道阶段详情

| 阶段 | 说明 |
|------|------|
| `$match` | 文档过滤（支持所有查询操作符） |
| `$project` | 字段投影（包含/排除模式） |
| `$sort` | 排序（支持复合排序） |
| `$limit` | 限制结果数量 |
| `$skip` | 跳过指定数量 |
| `$group` | 分组聚合（支持 9 种累加器） |
| `$count` | 文档计数 |
| `$unwind` | 展开数组（支持 preserveNullAndEmptyArrays） |
| `$addFields` / `$set` | 添加/设置字段 |
| `$unset` | 移除字段 |
| `$lookup` | 集合连接（左外连接） |
| `$replaceRoot` | 替换根文档 |

### 不支持的功能（非目标）

- ❌ 复制集 / 分片（分布式）
- ❌ 认证授权
- ❌ Change Streams
- ❌ 地理空间功能
- ❌ 全文搜索
- ❌ GridFS

> 完整兼容性列表请参阅：
> - 中文： [docs/COMPATIBILITY_CN.md](docs/COMPATIBILITY_CN.md)
> - English: [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)

## 数据迁移

### 从 MongoDB 导入

```bash
# 方式一：使用 mongodump + monodb-import
mongodump --db mydb --out backup/
./monodb-import -db data.monodb -dir backup/mydb/

# 方式二：导入 JSON/JSONL 文件
./monodb-import -db data.monodb -file users.json -collection users
./monodb-import -db data.monodb -file orders.jsonl -collection orders

# 支持的文件格式
# - .bson  (mongodump 输出)
# - .json  (JSON 数组或 Extended JSON)
# - .jsonl (JSON Lines，每行一个文档)
```

### 导出到 MongoDB

```bash
# 导出所有集合
./monodb-export -db data.monodb -dir export/

# 导出单个集合
./monodb-export -db data.monodb -collection users -out users.json

# 导出为不同格式
./monodb-export -db data.monodb -collection users -format bson -out users.bson
./monodb-export -db data.monodb -collection users -format jsonl -out users.jsonl

# 带过滤条件导出
./monodb-export -db data.monodb -collection users -query '{"age":{"$gt":18}}'

# 导入回 MongoDB
mongorestore --db mydb export/
```

## 存储引擎架构

```
┌────────────────────────────────────────────────────────────────┐
│                      Wire Protocol                              │
│              (OP_MSG / OP_QUERY / OP_REPLY)                    │
├────────────────────────────────────────────────────────────────┤
│                      Query Engine                               │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │   Parser    │  Executor   │  Optimizer  │             │
│        │  (BSON)     │  (Pipeline) │  (Index)    │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                   Transaction Manager                           │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │    Lock     │  Deadlock   │    Undo     │             │
│        │   Manager   │  Detector   │    Log      │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                     Storage Engine                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│   │   B+Tree     │  │    Pager     │  │     WAL      │        │
│   │   Index      │  │    Cache     │  │   Recovery   │        │
│   └──────────────┘  └──────────────┘  └──────────────┘        │
├────────────────────────────────────────────────────────────────┤
│                       Single File                               │
│                     (.monodb 文件)                              │
└────────────────────────────────────────────────────────────────┘
```

### 文件格式

```
┌────────────────────────────────────────────────────────────────┐
│  File Header (64 bytes)                                         │
│  - Magic: "MONO" (0x4D4F4E4F)                                  │
│  - Version, PageSize, PageCount, FreeListHead                   │
│  - MetaPageId, CatalogPageId, CreateTime, ModifyTime            │
├────────────────────────────────────────────────────────────────┤
│  Page 0: Meta Page                                              │
├────────────────────────────────────────────────────────────────┤
│  Page 1: Catalog Page (集合目录，BSON 格式，支持多页)            │
├────────────────────────────────────────────────────────────────┤
│  Page 2..N: Data Pages / Index Pages / Free Pages               │
│  - Data Pages: Slotted Page 格式存储 BSON 文档                  │
│  - Index Pages: B+Tree 节点存储                                 │
│  - Free Pages: 空闲页链表                                       │
└────────────────────────────────────────────────────────────────┘
```

## 设计理念

> **80% MongoDB 使用体验，20% 的复杂度**

MonoLite 不追求 MongoDB 全功能兼容，而是专注于以下场景：

- **桌面应用** — macOS / Windows / Linux 本地数据存储
- **开发工具** — 本地调试、离线数据分析、原型验证
- **小型服务** — 单机服务或 Sidecar 数据存储
- **嵌入式场景** — IoT 设备、边缘计算、零部署需求

### 设计原则

1. **简单优先** — 单文件，无外部依赖，零配置启动
2. **兼容性** — 使用标准 MongoDB 协议，官方驱动直接可用
3. **可靠性** — WAL 保证崩溃一致性，事务保证数据完整性
4. **可扩展** — 模块化设计，易于添加新功能

## 项目结构

```
monodb/
├── cmd/
│   ├── monodbd/          # 服务器入口
│   ├── monodb-import/    # 数据导入工具
│   └── monodb-export/    # 数据导出工具
├── engine/               # 数据库引擎
│   ├── database.go       # 数据库核心（命令路由）
│   ├── collection.go     # 集合操作（CRUD）
│   ├── transaction.go    # 事务管理（锁、死锁检测、回滚）
│   ├── session.go        # 会话管理（MongoDB 标准会话）
│   ├── index.go          # 索引管理（B+Tree 索引）
│   ├── aggregate.go      # 聚合管道
│   ├── cursor.go         # 游标管理
│   ├── bson_compare.go   # BSON 类型比较（MongoDB 标准）
│   ├── validate.go       # 文档验证
│   ├── limits.go         # 资源限制
│   ├── errors.go         # 错误码体系（MongoDB 兼容）
│   ├── logger.go         # 结构化日志
│   └── explain.go        # 查询计划解释
├── protocol/             # MongoDB Wire Protocol
│   ├── server.go         # TCP 服务器
│   ├── message.go        # 消息解析
│   ├── opmsg.go          # OP_MSG 处理
│   ├── opquery.go        # OP_QUERY 处理（握手兼容）
│   └── errors.go         # 协议错误
├── storage/              # 存储引擎
│   ├── pager.go          # 页面管理器（缓存、读写）
│   ├── page.go           # 页面结构（Slotted Page）
│   ├── btree.go          # B+Tree 实现
│   ├── wal.go            # Write-Ahead Log
│   └── keystring.go      # 索引键编码（MongoDB KeyString）
├── docs/                 # 文档
│   ├── COMPATIBILITY.md     # Compatibility details (English)
│   └── COMPATIBILITY_CN.md  # 兼容性详情（中文）
├── CLAUDE.md             # 项目规则
├── go.mod
└── README.md
```

## 技术规格

| 项目 | 规格 |
|------|------|
| 最大文档大小 | 16 MB |
| 最大嵌套深度 | 100 层 |
| 最大索引数/集合 | 64 个 |
| 最大批量写入 | 100,000 文档 |
| 页面大小 | 4 KB |
| 默认游标批量大小 | 101 文档 |
| 游标超时时间 | 10 分钟 |
| 事务锁超时 | 30 秒 |
| WAL 格式版本 | 1 |
| 文件格式版本 | 1 |
| Wire Protocol 版本 | 13 (MongoDB 5.0) |

## 性能特点

- **写入优化** — WAL 批量刷盘，减少 fsync 调用
- **读取缓存** — 页面缓存，减少磁盘 IO
- **索引加速** — B+Tree 索引自动用于匹配查询
- **内存友好** — 可配置的缓存大小，默认 1000 页

## 开发与贡献

```bash
# 运行测试
go test ./...

# 运行特定测试
go test ./engine -run TestTransaction
go test ./storage -run TestBTree

# 代码格式化
go fmt ./...

# 静态检查
go vet ./...
```

### 代码规范

- 遵循 Go 标准代码风格
- 单文件代码行数不超过 800 行
- 公共 API 必须有文档注释
- 提交信息使用 Conventional Commits 格式

## 许可证

MIT License

---

<div align="center">

**[文档 (中文)](docs/COMPATIBILITY_CN.md)** · **[Docs (EN)](docs/COMPATIBILITY.md)** · **[README (中文)](README_CN.md)** · **[README (EN)](README.md)** · **[问题反馈](https://github.com/monolite/monodb/issues)** · **[贡献指南](CONTRIBUTING.md)**

</div>

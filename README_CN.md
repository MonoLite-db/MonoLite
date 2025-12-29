# MonoDB

MonoDB 是一个**单文件、可嵌入的文档数据库**，兼容 MongoDB Wire Protocol，可使用 MongoDB 官方驱动和 mongosh 直接连接。

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

</div>

## 项目定位

> **像 SQLite 一样简单，却能用 MongoDB 方式思考和使用的文档数据库。**

- **单文件存储** — 一个 `.monodb` 文件即是完整数据库
- **零部署、零运维** — 无需安装、无需配置，开箱即用
- **嵌入式优先** — Library-first 设计，可直接嵌入应用
- **兼容 MongoDB 官方驱动** — 使用熟悉的 API 和工具

## 快速开始

### 安装与构建

```bash
# 克隆项目
git clone https://github.com/monolite/monodb.git
cd monodb

# 构建服务器
go build -o monodbd ./cmd/monodbd

# 构建导入/导出工具（可选）
go build -o monodb-import ./cmd/monodb-import
go build -o monodb-export ./cmd/monodb-export
```

### 启动服务器

```bash
# 启动（默认端口 27017，数据文件 data.monodb）
./monodbd

# 自定义配置
./monodbd -file mydata.monodb -addr :27018
```

### 使用 mongosh 连接

```bash
mongosh mongodb://localhost:27017
```

### 基本操作

```javascript
// 插入文档
db.users.insertOne({name: "Alice", age: 25, email: "alice@example.com"})
db.users.insertMany([
  {name: "Bob", age: 30, tags: ["dev", "go"]},
  {name: "Carol", age: 28, address: {city: "Beijing"}}
])

// 查询文档
db.users.find({age: {$gt: 20}})
db.users.findOne({name: "Alice"})
db.users.find({tags: {$in: ["dev"]}})
db.users.find({"address.city": "Beijing"})  // 点号路径查询

// 更新文档
db.users.updateOne({name: "Alice"}, {$set: {age: 26}})
db.users.updateMany({}, {$inc: {age: 1}})
db.users.updateOne({name: "Dave"}, {$set: {age: 35}}, {upsert: true})

// 删除文档
db.users.deleteOne({name: "Alice"})
db.users.deleteMany({age: {$lt: 18}})

// 聚合管道
db.orders.aggregate([
  {$match: {status: "completed"}},
  {$group: {_id: "$customerId", total: {$sum: "$amount"}}},
  {$sort: {total: -1}},
  {$limit: 10}
])

// 索引管理
db.users.createIndex({email: 1}, {unique: true})
db.users.createIndex({name: 1, age: -1})  // 复合索引
db.users.getIndexes()
db.users.dropIndex("email_1")
```

### 使用事务

```javascript
// 开启会话并启动事务
const session = db.getMongo().startSession()
session.startTransaction()

try {
  const users = session.getDatabase("test").users
  const accounts = session.getDatabase("test").accounts
  
  // 转账操作
  users.updateOne({name: "Alice"}, {$inc: {balance: -100}})
  users.updateOne({name: "Bob"}, {$inc: {balance: 100}})
  
  session.commitTransaction()
} catch (e) {
  session.abortTransaction()
}
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

```javascript
// 查看服务器状态
db.runCommand({serverStatus: 1})

// 查看数据库统计
db.runCommand({dbStats: 1})

// 查看集合统计
db.users.stats()
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

> 完整兼容性列表请参阅 [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)

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

MonoDB 不追求 MongoDB 全功能兼容，而是专注于以下场景：

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
│   └── COMPATIBILITY.md  # 兼容性详情
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

**[文档](docs/COMPATIBILITY.md)** · **[问题反馈](https://github.com/monolite/monodb/issues)** · **[贡献指南](CONTRIBUTING.md)**

</div>

Created by Yanjunhui

## 兼容性说明（MongoDB）

本文档说明 **MonoDB / MonoLite** 与 MongoDB 的兼容性范围（Wire Protocol + 命令语义）。

- **English**：[`docs/COMPATIBILITY.md`](COMPATIBILITY.md)
- **返回中文 README**：[`README_CN.md`](../README_CN.md)

---

## 范围与目标

MonoDB 的目标是在本地/嵌入式/单机场景提供 **“足够接近 MongoDB 的使用体验”**：

- 可使用 **MongoDB 官方驱动**与工具（例如 `mongosh`）直接连接
- 覆盖最常见的 **CRUD / 索引 / 聚合 / 会话与事务**
- 通过 WAL 提供 **崩溃一致性**

当前非目标（或暂未支持）：

- 复制集 / 分片
- 认证与授权
- OP_COMPRESSED / 压缩传输
- 与 MongoDB Server 的全功能对齐

---

## Wire Protocol 兼容性

| 项目 | 状态 |
|------|------|
| OP_MSG | ✅ 支持 |
| OP_QUERY | ✅ 兼容握手（legacy） |
| OP_COMPRESSED | ❌ 不支持 |
| maxWireVersion | 13（MongoDB 5.0） |

说明：
- `hello` / `isMaster` 会宣称 `maxWireVersion=13`，避免驱动启用尚未支持的新特性。
- 如果客户端发送 OP_COMPRESSED，会返回结构化协议错误。

---

## 命令兼容性（概览）

MonoDB 在 `engine/database.go` 中进行命令路由与执行。

### 已实现命令（服务端）

| 命令 | 状态 | 说明 |
|------|------|------|
| `ping` | ✅ | |
| `hello` / `isMaster` | ✅ | 能力宣称与握手 |
| `buildInfo` | ✅ | |
| `listCollections` | ✅ | |
| `insert` | ✅ | 支持 OP_MSG 的 `documents` 序列 |
| `find` | ✅ | 支持游标与 getMore |
| `update` | ✅ | 支持常用更新操作符（高级 option 逐步补齐） |
| `delete` | ✅ | **按 per-delete 的 `limit` 区分 deleteOne/deleteMany** |
| `count` | ✅ | |
| `drop` | ✅ | drop collection |
| `createIndexes` / `listIndexes` / `dropIndexes` | ✅ | B+Tree 索引 |
| `aggregate` | ✅ | 支持常用 pipeline 子集 |
| `getMore` / `killCursors` | ✅ | |
| `findAndModify` | ✅ | |
| `distinct` | ✅ | |
| `dbStats` / `collStats` / `serverStatus` | ✅ | |
| `validate` | ✅ | 结构一致性校验 |
| `explain` | ✅ | |
| `connectionStatus` | ✅ | |
| 会话与事务（`startTransaction` / `commitTransaction` / `abortTransaction` / `endSessions` / `refreshSessions`） | ✅ | 单机事务 |

### 未实现 / 部分实现

| 项目 | 状态 | 说明 |
|------|------|------|
| `dropDatabase` | ❌ | 规范测试 runner 会避免调用（MonoDB 暂未实现该命令） |
| 事件断言（command monitoring） | 🚧 | runner 目前会跳过 `expectEvents` |
| Unified Test Format 全量支持 | 🚧 | runner 先跑通小子集，再逐步扩展 |

---

## 查询过滤器操作符

过滤器匹配由 `engine/index.go` 的 `FilterMatcher` 实现，`find` 与 `$match` 都复用它。

当前支持的操作符：

- **逻辑**：`$and`、`$or`、`$not`、`$nor`
- **比较**：`$eq`、`$ne`、`$gt`、`$gte`、`$lt`、`$lte`
- **集合/数组**：`$in`、`$nin`、`$all`、`$size`、`$elemMatch`
- **字段**：`$exists`、`$type`
- **正则**：`$regex`

说明：
- 支持点号路径（嵌套文档）以及数组下标访问。

---

## 更新操作符

更新操作符在 `engine/collection.go` 的 `applyUpdate` 中实现。

当前支持：

- **字段**：`$set`、`$unset`、`$inc`、`$mul`、`$min`、`$max`、`$rename`
- **数组**：`$push`、`$pop`、`$pull`、`$pullAll`、`$addToSet`

---

## 索引

| 项目 | 状态 | 说明 |
|------|------|------|
| B+Tree 索引 | ✅ | `storage/btree.go` |
| 唯一索引 | ✅ | 引擎层强一致性检查 |
| 复合键 | ✅ | KeyString 编码 |

---

## 聚合管道

聚合在 `engine/aggregate.go` 中实现。

支持的阶段（子集）：

- `$match`、`$project`、`$sort`、`$limit`、`$skip`
- `$group`（常用累加器）
- `$count`、`$unwind`
- `$addFields` / `$set`、`$unset`
- `$replaceRoot`
- `$lookup`（需要 DB 上下文；通过 `Collection.Aggregate` 支持）

---

## 事务与会话

MonoDB 支持单机事务与会话：

- 锁管理与死锁检测（见 `engine/transaction_test.go`）
- 会话管理（MongoDB standard sessions）

限制：
- 不支持复制集/分布式事务

---

## 官方规范测试（MongoDB specifications）

已接入 MongoDB 官方维护的 CRUD Unified Test Format（最小 runner）：

- Runner：`tests/mongo_spec/crud_unified_test.go`
- 文档：`tests/mongo_spec/README.md`
- 数据：`third_party/mongodb/specifications/source/crud/tests/unified/`

运行方式：

```bash
MONOLITE_RUN_MONGO_SPECS=1 go test ./tests/mongo_spec -count=1
```

默认会跳过未实现的断言/高级 option，避免“假通过”：

- `expectEvents`（事件断言）
- `expectError`（错误断言）
- 多数高级参数（collation / hint / let / arrayFilters 等）

---

## 如何反馈兼容性问题

建议提供：

- 客户端/驱动名称与版本
- 复现命令（Extended JSON）或最小代码片段
- MongoDB 期望行为 vs MonoDB 实际行为
- 如可行：对应的 spec 用例或最小化 spec 片段



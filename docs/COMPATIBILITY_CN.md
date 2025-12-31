# MonoLite Go - MongoDB 兼容性说明

Created by Yanjunhui

本文档说明 **MonoLite** 与 MongoDB 的兼容性范围（Wire Protocol + 命令语义）。

- **English**：[`docs/COMPATIBILITY.md`](COMPATIBILITY.md)
- **返回中文 README**：[`README_CN.md`](../README_CN.md)

---

## 范围与目标

MonoLite 的目标是在本地/嵌入式/单机场景提供 **"足够接近 MongoDB 的使用体验"**：

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

## 命令兼容性

MonoLite 在 `engine/database.go` 中进行命令路由与执行。

### 已实现命令

| 命令 | 状态 | 说明 |
|------|------|------|
| `ping` | ✅ | 连接测试 |
| `hello` / `isMaster` | ✅ | 能力宣称与握手 |
| `buildInfo` | ✅ | 服务器构建信息 |
| `serverStatus` | ✅ | 服务器运行状态 |
| `connectionStatus` | ✅ | 连接信息 |
| `listCollections` | ✅ | 列出所有集合 |
| `create` | ✅ | 创建集合 |
| `drop` | ✅ | 删除集合 |
| `insert` | ✅ | 支持 OP_MSG 的 `documents` 序列 |
| `find` | ✅ | 支持游标与 getMore |
| `update` | ✅ | 支持完整更新操作符 |
| `delete` | ✅ | 按 per-delete 的 `limit` 区分 deleteOne/deleteMany |
| `count` | ✅ | 文档计数 |
| `distinct` | ✅ | 字段去重 |
| `aggregate` | ✅ | 支持常用 pipeline 子集 |
| `findAndModify` | ✅ | 原子查找与修改 |
| `createIndexes` | ✅ | 创建 B+Tree 索引 |
| `listIndexes` | ✅ | 列出集合索引 |
| `dropIndexes` | ✅ | 删除索引 |
| `getMore` | ✅ | 游标迭代 |
| `killCursors` | ✅ | 游标清理 |
| `dbStats` | ✅ | 数据库统计 |
| `collStats` | ✅ | 集合统计 |
| `validate` | ✅ | 结构一致性校验 |
| `explain` | ✅ | 查询计划分析 |
| `startTransaction` | ✅ | 开始事务 |
| `commitTransaction` | ✅ | 提交事务 |
| `abortTransaction` | ✅ | 回滚事务 |
| `endSessions` | ✅ | 结束会话 |
| `refreshSessions` | ✅ | 刷新会话 |

### 未实现命令

| 项目 | 状态 | 说明 |
|------|------|------|
| `dropDatabase` | ❌ | 未实现 |
| `renameCollection` | ❌ | 未实现 |
| `currentOp` | ❌ | 未实现 |
| `killOp` | ❌ | 未实现 |
| 事件监控 | 🚧 | 规范测试 runner 会跳过 `expectEvents` |

---

## 查询过滤器操作符

过滤器匹配由 `engine/index.go` 的 `FilterMatcher` 实现，`find` 与 `$match` 都复用它。

### 比较操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$eq` | ✅ | 等于 |
| `$ne` | ✅ | 不等于 |
| `$gt` | ✅ | 大于 |
| `$gte` | ✅ | 大于等于 |
| `$lt` | ✅ | 小于 |
| `$lte` | ✅ | 小于等于 |
| `$in` | ✅ | 在数组中 |
| `$nin` | ✅ | 不在数组中 |

### 逻辑操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$and` | ✅ | 逻辑与 |
| `$or` | ✅ | 逻辑或 |
| `$not` | ✅ | 逻辑非 |
| `$nor` | ✅ | 逻辑或非 |

### 元素操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$exists` | ✅ | 字段是否存在 |
| `$type` | ✅ | BSON 类型检查 |

### 数组操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$all` | ✅ | 匹配所有元素 |
| `$size` | ✅ | 数组长度 |
| `$elemMatch` | ✅ | 元素匹配 |

### 其他操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$regex` | ✅ | 正则表达式 |
| `$mod` | ✅ | 取模运算 |

说明：
- 支持点号路径（嵌套文档）以及数组下标访问。

---

## 更新操作符

更新操作符在 `engine/collection.go` 的 `applyUpdate` 中实现。

### 字段操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$set` | ✅ | 设置字段值 |
| `$unset` | ✅ | 删除字段 |
| `$inc` | ✅ | 数值增加 |
| `$mul` | ✅ | 数值乘法 |
| `$min` | ✅ | 设置为较小值 |
| `$max` | ✅ | 设置为较大值 |
| `$rename` | ✅ | 重命名字段 |
| `$currentDate` | ✅ | 设置当前日期/时间戳 |
| `$setOnInsert` | ✅ | 仅在插入时设置 |

### 数组操作符

| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$push` | ✅ | 添加到数组 |
| `$push` + `$each` | ✅ | 添加多个元素 |
| `$pop` | ✅ | 移除首个/末尾元素 |
| `$pull` | ✅ | 移除匹配元素 |
| `$pullAll` | ✅ | 移除所有匹配元素 |
| `$addToSet` | ✅ | 添加唯一元素 |
| `$addToSet` + `$each` | ✅ | 添加多个唯一元素 |

---

## 索引

| 项目 | 状态 | 说明 |
|------|------|------|
| B+Tree 索引 | ✅ | `storage/btree.go` |
| 唯一索引 | ✅ | 引擎层强一致性检查 |
| 复合键 | ✅ | KeyString 编码 |
| 稀疏索引 | ❌ | 未实现 |
| TTL 索引 | ❌ | 未实现 |
| 文本索引 | ❌ | 未实现 |
| 地理空间索引 | ❌ | 未实现 |

---

## 聚合管道

聚合在 `engine/aggregate.go` 中实现。

### 已支持阶段

| 阶段 | 状态 | 说明 |
|------|------|------|
| `$match` | ✅ | 过滤文档 |
| `$project` | ✅ | 文档投影 |
| `$sort` | ✅ | 排序 |
| `$limit` | ✅ | 限制数量 |
| `$skip` | ✅ | 跳过文档 |
| `$group` | ✅ | 分组聚合 |
| `$count` | ✅ | 计数 |
| `$unwind` | ✅ | 展开数组 |
| `$addFields` / `$set` | ✅ | 添加字段 |
| `$unset` | ✅ | 移除字段 |
| `$replaceRoot` | ✅ | 替换根文档 |
| `$lookup` | ✅ | 左外连接 |

### 分组累加器

| 累加器 | 状态 | 说明 |
|--------|------|------|
| `$sum` | ✅ | 求和 |
| `$avg` | ✅ | 平均值 |
| `$min` | ✅ | 最小值 |
| `$max` | ✅ | 最大值 |
| `$first` | ✅ | 第一个值 |
| `$last` | ✅ | 最后一个值 |
| `$push` | ✅ | 推入数组 |
| `$addToSet` | ✅ | 添加唯一元素 |

### 未实现阶段

| 阶段 | 状态 |
|------|------|
| `$out` | ❌ |
| `$merge` | ❌ |
| `$facet` | ❌ |
| `$bucket` | ❌ |
| `$graphLookup` | ❌ |
| `$geoNear` | ❌ |

---

## 事务与会话

MonoLite 支持单机事务与会话：

| 功能 | 状态 | 说明 |
|------|------|------|
| 会话管理 | ✅ | 创建/结束/刷新会话 |
| 多文档事务 | ✅ | 单机 ACID |
| 锁管理器 | ✅ | 读/写锁 |
| 死锁检测 | ✅ | 等待图分析 |
| 事务隔离 | ✅ | 读已提交 |
| 回滚支持 | ✅ | Undo 日志 |

限制：
- 不支持复制集/分布式事务
- 不支持因果一致性

---

## BSON 类型支持

| 类型 | 状态 | 说明 |
|------|------|------|
| Double | ✅ | 64 位浮点数 |
| String | ✅ | UTF-8 字符串 |
| Document | ✅ | 嵌套文档 |
| Array | ✅ | BSON 数组 |
| Binary | ✅ | 二进制数据 |
| ObjectId | ✅ | 12 字节标识符 |
| Boolean | ✅ | 布尔值 |
| Date | ✅ | UTC 日期时间 |
| Null | ✅ | 空值 |
| Int32 | ✅ | 32 位整数 |
| Int64 | ✅ | 64 位整数 |
| Timestamp | ✅ | MongoDB 时间戳 |
| Decimal128 | ❌ | 不支持 |
| MinKey/MaxKey | ❌ | 不支持 |
| JavaScript | ❌ | 不支持 |
| Regex | ✅ | 仅查询支持 |

---

## 官方规范测试（MongoDB specifications）

已接入 MongoDB 官方维护的 CRUD Unified Test Format（最小 runner）：

- Runner：`tests/mongo_spec/crud_unified_test.go`
- 文档：`tests/mongo_spec/README.md`
- 数据：`third_party/mongodb/specifications/source/crud/tests/unified/`

### 运行方式

默认会跳过规范测试：

```bash
MONOLITE_RUN_MONGO_SPECS=1 go test ./tests/mongo_spec -count=1
```

运行单个文件：

```bash
MONOLITE_RUN_MONGO_SPECS=1 MONOLITE_MONGO_SPECS_FILENAME=find.json go test ./tests/mongo_spec -count=1
```

### Runner 当前限制

为避免"假通过"，默认会跳过未实现的断言和高级参数：

- `expectEvents`（事件断言）
- `expectError`（错误断言）
- 高级参数（collation / hint / let / arrayFilters 等）

---

## 如何反馈兼容性问题

建议提供：

- 客户端/驱动名称与版本
- 复现命令（Extended JSON）或最小代码片段
- MongoDB 期望行为 vs MonoLite 实际行为
- 如可行：对应的 spec 用例或最小化 spec 片段

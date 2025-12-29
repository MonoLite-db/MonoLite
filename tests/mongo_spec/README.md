Created by Yanjunhui

## MongoDB specifications（Unified CRUD）规范测试

本目录用于运行 MongoDB 官方维护的 `specifications` 仓库中的 **CRUD 规范测试**（Unified Test Format）。

### 测试数据来源

- **上游仓库**：`mongodb/specifications`
- **已同步数据目录**：`third_party/mongodb/specifications`
- **许可证**：见 `third_party/mongodb/specifications/LICENSE.md`
- **上游 commit**：见 `third_party/mongodb/specifications/UPSTREAM_COMMIT`

### 如何运行

默认情况下，规范测试会被跳过（避免影响日常 `go test ./...` 的速度）。

- **运行默认子集**（find / insert / delete / distinct 的基础用例）：

```bash
MONOLITE_RUN_MONGO_SPECS=1 go test ./tests/mongo_spec -count=1
```

- **只跑单个文件**：

```bash
MONOLITE_RUN_MONGO_SPECS=1 MONOLITE_MONGO_SPECS_FILENAME=find.json go test ./tests/mongo_spec -count=1
```

- **用 glob 选择**（相对 unified 目录）：

```bash
MONOLITE_RUN_MONGO_SPECS=1 MONOLITE_MONGO_SPECS_GLOB='find*.json' go test ./tests/mongo_spec -count=1
```

### 当前支持范围（逐步扩展）

目前 runner 只实现了 Unified CRUD 测试里最常见且 MonoLite 已支持的一小部分：

- `find`
- `insertOne`
- `insertMany`
- `deleteOne`
- `deleteMany`
- `distinct`

以下情况会显式 `t.Skip`（不会“假通过”）：

- `expectEvents` / `observeEvents` 事件断言（尚未实现 command monitoring 校验）
- `expectError` 错误断言体系（尚未实现 spec 对齐的错误匹配）
- 复杂 option（collation / hint / arrayFilters / let / comment / pipeline update 等）对应的用例

后续如果你希望继续扩展支持范围，我建议按“先跑通一个文件 → 再扩展一类 operation”的节奏推进。



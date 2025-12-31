# MonoLite Go API 完整示例

Created by Yanjunhui

本文档提供 MonoLite Go 版本的完整 API 使用示例，便于开发者和大模型理解调用方式。

## 目录

- [数据库操作](#数据库操作)
- [集合操作](#集合操作)
- [文档 CRUD](#文档-crud)
- [查询操作符](#查询操作符)
- [更新操作符](#更新操作符)
- [聚合管道](#聚合管道)
- [索引管理](#索引管理)
- [事务操作](#事务操作)
- [游标操作](#游标操作)
- [数据库命令](#数据库命令)

---

## 数据库操作

### 打开数据库

```go
import (
    "github.com/monolite/monodb/engine"
    "go.mongodb.org/mongo-driver/bson"
)

// 打开或创建数据库
db, err := engine.OpenDatabase("data.monodb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### 获取数据库信息

```go
// 获取数据库名称
name := db.Name()

// 获取数据库统计信息
stats := db.Stats()
fmt.Printf("Collections: %d, Documents: %d, Size: %d bytes\n",
    stats.Collections, stats.Objects, stats.DataSize)

// 刷新数据到磁盘
err := db.Flush()

// 关闭数据库
err := db.Close()
```

### 集合管理

```go
// 获取或创建集合
users, err := db.Collection("users")

// 仅获取集合（不自动创建）
users := db.GetCollection("users")
if users == nil {
    fmt.Println("Collection not found")
}

// 列出所有集合
collections := db.ListCollections()
for _, name := range collections {
    fmt.Println(name)
}

// 删除集合
err := db.DropCollection("users")
```

---

## 集合操作

### 基本信息

```go
users, _ := db.Collection("users")

// 获取集合名称
name := users.Name()

// 获取文档数量
count := users.Count()
```

---

## 文档 CRUD

### 插入文档

```go
users, _ := db.Collection("users")

// 插入单个文档
ids, err := users.Insert(bson.D{
    {Key: "name", Value: "Alice"},
    {Key: "age", Value: 25},
    {Key: "email", Value: "alice@example.com"},
})
fmt.Println("Inserted ID:", ids[0])

// 插入多个文档
ids, err := users.Insert(
    bson.D{
        {Key: "name", Value: "Bob"},
        {Key: "age", Value: 30},
        {Key: "tags", Value: bson.A{"developer", "golang"}},
    },
    bson.D{
        {Key: "name", Value: "Carol"},
        {Key: "age", Value: 28},
        {Key: "address", Value: bson.D{
            {Key: "city", Value: "Beijing"},
            {Key: "country", Value: "China"},
        }},
    },
)
fmt.Printf("Inserted %d documents\n", len(ids))

// 插入带自定义 _id 的文档
ids, err := users.Insert(bson.D{
    {Key: "_id", Value: "user_001"},
    {Key: "name", Value: "David"},
})
```

### 查询文档

```go
// 查询所有文档
docs, err := users.Find(bson.D{})

// 按条件查询
docs, err := users.Find(bson.D{
    {Key: "age", Value: bson.D{{Key: "$gt", Value: 20}}},
})

// 查询单个文档
doc, err := users.FindOne(bson.D{{Key: "name", Value: "Alice"}})
if doc != nil {
    fmt.Println("Found:", doc)
}

// 按 ID 查询
doc, err := users.FindById("user_001")

// 带选项查询
opts := &engine.QueryOptions{
    Sort:       bson.D{{Key: "age", Value: -1}},  // 按年龄降序
    Projection: bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}},
    Skip:       0,
    Limit:      10,
}
docs, err := users.FindWithOptions(bson.D{}, opts)
```

### 更新文档

```go
// 更新匹配的所有文档
result, err := users.Update(
    bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: 30}}}}, // filter
    bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: 1}}}}, // update
    false, // upsert
)
fmt.Printf("Matched: %d, Modified: %d\n", result.MatchedCount, result.ModifiedCount)

// 更新单个文档
result, err := users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 26}}}},
    false,
)

// Upsert（不存在则插入）
result, err := users.Update(
    bson.D{{Key: "name", Value: "Eve"}},
    bson.D{{Key: "$set", Value: bson.D{
        {Key: "name", Value: "Eve"},
        {Key: "age", Value: 22},
    }}},
    true, // upsert = true
)
if result.UpsertedId != nil {
    fmt.Println("Upserted ID:", result.UpsertedId)
}
```

### 替换文档

```go
// 替换整个文档（保留 _id）
modifiedCount, err := users.ReplaceOne(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{
        {Key: "name", Value: "Alice"},
        {Key: "age", Value: 27},
        {Key: "status", Value: "active"},
    },
)
```

### 删除文档

```go
// 删除匹配的所有文档
deletedCount, err := users.Delete(bson.D{
    {Key: "age", Value: bson.D{{Key: "$lt", Value: 18}}},
})

// 删除单个文档
deletedCount, err := users.DeleteOne(bson.D{{Key: "name", Value: "Alice"}})
```

### FindAndModify

```go
// 查找并更新，返回更新前的文档
doc, err := users.FindAndModify(&engine.FindAndModifyOptions{
    Query:  bson.D{{Key: "name", Value: "Alice"}},
    Update: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: 1}}}},
    New:    false, // 返回更新前的文档
})

// 查找并更新，返回更新后的文档
doc, err := users.FindAndModify(&engine.FindAndModifyOptions{
    Query:  bson.D{{Key: "name", Value: "Alice"}},
    Update: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "active"}}}},
    New:    true, // 返回更新后的文档
    Upsert: true, // 不存在则插入
})

// 查找并删除
doc, err := users.FindAndModify(&engine.FindAndModifyOptions{
    Query:  bson.D{{Key: "name", Value: "Alice"}},
    Remove: true,
})

// 带排序的 FindAndModify
doc, err := users.FindAndModify(&engine.FindAndModifyOptions{
    Query:  bson.D{{Key: "status", Value: "pending"}},
    Update: bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "processing"}}}},
    Sort:   bson.D{{Key: "createdAt", Value: 1}}, // 处理最早的
    New:    true,
})
```

### Distinct

```go
// 获取字段的不重复值
values, err := users.Distinct("city", bson.D{})

// 带过滤条件
values, err := users.Distinct("status", bson.D{
    {Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}},
})
```

---

## 查询操作符

### 比较操作符

```go
// $eq - 等于
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$eq", Value: 25}}}})
// 简写
users.Find(bson.D{{Key: "age", Value: 25}})

// $ne - 不等于
users.Find(bson.D{{Key: "status", Value: bson.D{{Key: "$ne", Value: "inactive"}}}})

// $gt - 大于
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 18}}}})

// $gte - 大于等于
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}}})

// $lt - 小于
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: 65}}}})

// $lte - 小于等于
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$lte", Value: 65}}}})

// $in - 在数组中
users.Find(bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{"active", "pending"}}}}})

// $nin - 不在数组中
users.Find(bson.D{{Key: "status", Value: bson.D{{Key: "$nin", Value: bson.A{"deleted", "banned"}}}}})
```

### 逻辑操作符

```go
// $and - 与
users.Find(bson.D{{Key: "$and", Value: bson.A{
    bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}}},
    bson.D{{Key: "age", Value: bson.D{{Key: "$lte", Value: 65}}}},
}}})

// 隐式 $and（同一文档中的多个条件）
users.Find(bson.D{
    {Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}},
    {Key: "status", Value: "active"},
})

// $or - 或
users.Find(bson.D{{Key: "$or", Value: bson.A{
    bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: 18}}}},
    bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 65}}}},
}}})

// $nor - 都不满足
users.Find(bson.D{{Key: "$nor", Value: bson.A{
    bson.D{{Key: "status", Value: "deleted"}},
    bson.D{{Key: "status", Value: "banned"}},
}}})

// $not - 非
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: 65}}}}}})
```

### 元素操作符

```go
// $exists - 字段存在
users.Find(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})

// $type - 字段类型
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$type", Value: "int"}}}})
// 类型: "double", "string", "object", "array", "binData", "objectId", "bool", "date", "null", "regex", "int", "long"
```

### 数组操作符

```go
// $all - 包含所有元素
users.Find(bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"go", "mongodb"}}}}})

// $elemMatch - 数组元素匹配
users.Find(bson.D{{Key: "scores", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
    {Key: "$gte", Value: 80},
    {Key: "$lt", Value: 90},
}}}}})

// $size - 数组长度
users.Find(bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 3}}}})
```

### 求值操作符

```go
// $regex - 正则表达式
users.Find(bson.D{{Key: "email", Value: bson.D{{Key: "$regex", Value: "@gmail\\.com$"}}}})

// $mod - 取模
users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$mod", Value: bson.A{2, 0}}}}}) // 偶数年龄
```

### 点号路径查询（嵌套文档）

```go
// 查询嵌套字段
users.Find(bson.D{{Key: "address.city", Value: "Beijing"}})

// 嵌套字段比较
users.Find(bson.D{{Key: "address.zipcode", Value: bson.D{{Key: "$gt", Value: "100000"}}}})

// 数组中的嵌套文档
users.Find(bson.D{{Key: "orders.status", Value: "completed"}})
```

---

## 更新操作符

### 字段操作符

```go
// $set - 设置字段值
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$set", Value: bson.D{
        {Key: "age", Value: 26},
        {Key: "status", Value: "active"},
        {Key: "address.city", Value: "Shanghai"}, // 设置嵌套字段
    }}},
    false,
)

// $unset - 删除字段
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$unset", Value: bson.D{{Key: "tempField", Value: ""}}}},
    false,
)

// $inc - 增加数值
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$inc", Value: bson.D{
        {Key: "age", Value: 1},
        {Key: "score", Value: -5}, // 也可以减少
    }}},
    false,
)

// $mul - 乘以数值
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$mul", Value: bson.D{{Key: "price", Value: 1.1}}}}, // 涨价 10%
    false,
)

// $min - 更新为较小值
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$min", Value: bson.D{{Key: "lowScore", Value: 50}}}},
    false,
)

// $max - 更新为较大值
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$max", Value: bson.D{{Key: "highScore", Value: 100}}}},
    false,
)

// $rename - 重命名字段
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$rename", Value: bson.D{{Key: "oldName", Value: "newName"}}}},
    false,
)

// $currentDate - 设置为当前日期
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$currentDate", Value: bson.D{
        {Key: "lastModified", Value: true},
        {Key: "lastModifiedTS", Value: bson.D{{Key: "$type", Value: "timestamp"}}},
    }}},
    false,
)

// $setOnInsert - 仅在 upsert 插入时设置
users.Update(
    bson.D{{Key: "name", Value: "NewUser"}},
    bson.D{
        {Key: "$set", Value: bson.D{{Key: "lastLogin", Value: time.Now()}}},
        {Key: "$setOnInsert", Value: bson.D{{Key: "createdAt", Value: time.Now()}}},
    },
    true, // upsert
)
```

### 数组操作符

```go
// $push - 添加元素到数组
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: "newTag"}}}},
    false,
)

// $push 多个元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: bson.D{
        {Key: "$each", Value: bson.A{"tag1", "tag2", "tag3"}},
    }}}}},
    false,
)

// $pop - 删除数组首/尾元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$pop", Value: bson.D{{Key: "tags", Value: 1}}}},  // 1: 删除尾部, -1: 删除头部
    false,
)

// $pull - 删除匹配的元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$pull", Value: bson.D{{Key: "tags", Value: "oldTag"}}}},
    false,
)

// $pull 条件删除
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$pull", Value: bson.D{{Key: "scores", Value: bson.D{{Key: "$lt", Value: 60}}}}}},
    false,
)

// $pullAll - 删除多个指定元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$pullAll", Value: bson.D{{Key: "tags", Value: bson.A{"tag1", "tag2"}}}}},
    false,
)

// $addToSet - 添加不重复元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "uniqueTag"}}}},
    false,
)

// $addToSet 多个元素
users.Update(
    bson.D{{Key: "name", Value: "Alice"}},
    bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: bson.D{
        {Key: "$each", Value: bson.A{"tag1", "tag2"}},
    }}}}},
    false,
)
```

---

## 聚合管道

```go
orders, _ := db.Collection("orders")

// 基本聚合
results, err := orders.Aggregate([]bson.D{
    // $match - 过滤
    {{Key: "$match", Value: bson.D{{Key: "status", Value: "completed"}}}},

    // $group - 分组
    {{Key: "$group", Value: bson.D{
        {Key: "_id", Value: "$customerId"},
        {Key: "totalAmount", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
        {Key: "orderCount", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
        {Key: "avgAmount", Value: bson.D{{Key: "$avg", Value: "$amount"}}},
    }}},

    // $sort - 排序
    {{Key: "$sort", Value: bson.D{{Key: "totalAmount", Value: -1}}}},

    // $limit - 限制数量
    {{Key: "$limit", Value: 10}},
})

// 所有聚合阶段示例
pipeline := []bson.D{
    // $match - 过滤文档
    {{Key: "$match", Value: bson.D{
        {Key: "status", Value: "active"},
        {Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}},
    }}},

    // $project - 投影（选择字段）
    {{Key: "$project", Value: bson.D{
        {Key: "name", Value: 1},
        {Key: "email", Value: 1},
        {Key: "age", Value: 1},
        {Key: "_id", Value: 0}, // 排除 _id
    }}},

    // $addFields / $set - 添加计算字段
    {{Key: "$addFields", Value: bson.D{
        {Key: "fullName", Value: bson.D{{Key: "$concat", Value: bson.A{"$firstName", " ", "$lastName"}}}},
    }}},

    // $unset - 移除字段
    {{Key: "$unset", Value: bson.A{"tempField", "internalData"}}},

    // $skip - 跳过
    {{Key: "$skip", Value: 10}},

    // $limit - 限制
    {{Key: "$limit", Value: 20}},

    // $sort - 排序
    {{Key: "$sort", Value: bson.D{
        {Key: "age", Value: -1},
        {Key: "name", Value: 1},
    }}},

    // $count - 计数
    {{Key: "$count", Value: "totalCount"}},
}

// $unwind - 展开数组
pipeline := []bson.D{
    {{Key: "$unwind", Value: "$tags"}},
    // 或带选项
    {{Key: "$unwind", Value: bson.D{
        {Key: "path", Value: "$tags"},
        {Key: "preserveNullAndEmptyArrays", Value: true},
    }}},
}

// $lookup - 关联查询
pipeline := []bson.D{
    {{Key: "$lookup", Value: bson.D{
        {Key: "from", Value: "orders"},
        {Key: "localField", Value: "_id"},
        {Key: "foreignField", Value: "customerId"},
        {Key: "as", Value: "customerOrders"},
    }}},
}

// $replaceRoot - 替换根文档
pipeline := []bson.D{
    {{Key: "$replaceRoot", Value: bson.D{
        {Key: "newRoot", Value: "$address"},
    }}},
}

// $group 累加器
pipeline := []bson.D{
    {{Key: "$group", Value: bson.D{
        {Key: "_id", Value: "$category"},
        {Key: "sum", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
        {Key: "avg", Value: bson.D{{Key: "$avg", Value: "$amount"}}},
        {Key: "min", Value: bson.D{{Key: "$min", Value: "$amount"}}},
        {Key: "max", Value: bson.D{{Key: "$max", Value: "$amount"}}},
        {Key: "count", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
        {Key: "first", Value: bson.D{{Key: "$first", Value: "$name"}}},
        {Key: "last", Value: bson.D{{Key: "$last", Value: "$name"}}},
        {Key: "items", Value: bson.D{{Key: "$push", Value: "$name"}}},
        {Key: "uniqueItems", Value: bson.D{{Key: "$addToSet", Value: "$name"}}},
    }}},
}
```

---

## 索引管理

```go
users, _ := db.Collection("users")

// 创建单字段索引
indexName, err := users.CreateIndex(
    bson.D{{Key: "email", Value: 1}},  // 1: 升序, -1: 降序
    bson.D{},
)

// 创建唯一索引
indexName, err := users.CreateIndex(
    bson.D{{Key: "email", Value: 1}},
    bson.D{{Key: "unique", Value: true}},
)

// 创建复合索引
indexName, err := users.CreateIndex(
    bson.D{
        {Key: "lastName", Value: 1},
        {Key: "firstName", Value: 1},
    },
    bson.D{},
)

// 创建嵌套字段索引
indexName, err := users.CreateIndex(
    bson.D{{Key: "address.city", Value: 1}},
    bson.D{},
)

// 列出所有索引
indexes := users.ListIndexes()
for _, idx := range indexes {
    fmt.Println(idx)
}

// 删除索引
err := users.DropIndex("email_1")

// 查询计划分析
explain := users.Explain(
    bson.D{{Key: "email", Value: "alice@example.com"}},
    nil,
)
fmt.Printf("Index used: %s, Scanned: %d\n", explain.IndexUsed, explain.DocumentsScanned)
```

---

## 事务操作

```go
// 获取会话管理器
sessionMgr := db.GetSessionManager()

// 创建会话
session := sessionMgr.CreateSession()
defer sessionMgr.EndSession(session.ID)

// 获取事务管理器
txnMgr := db.GetTransactionManager()

// 开始事务
txn, err := txnMgr.Begin(session.ID)
if err != nil {
    log.Fatal(err)
}

// 执行操作
users, _ := db.Collection("users")
accounts, _ := db.Collection("accounts")

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

// 提交事务
err = txnMgr.Commit(txn)
if err != nil {
    // 回滚事务
    txnMgr.Abort(txn)
    log.Fatal(err)
}

// 通过命令方式使用事务
result, _ := db.RunCommand(bson.D{
    {Key: "startTransaction", Value: 1},
    {Key: "lsid", Value: bson.D{{Key: "id", Value: sessionId}}},
})

result, _ = db.RunCommand(bson.D{
    {Key: "commitTransaction", Value: 1},
    {Key: "lsid", Value: bson.D{{Key: "id", Value: sessionId}}},
})

result, _ = db.RunCommand(bson.D{
    {Key: "abortTransaction", Value: 1},
    {Key: "lsid", Value: bson.D{{Key: "id", Value: sessionId}}},
})
```

---

## 游标操作

```go
// 获取游标管理器
cursorMgr := db.CursorManager()

// 创建游标（通常通过 find 命令自动创建）
result, _ := db.RunCommand(bson.D{
    {Key: "find", Value: "users"},
    {Key: "filter", Value: bson.D{}},
    {Key: "batchSize", Value: 10},
})

// 从结果中获取 cursorId
cursor := result.Map()["cursor"].(bson.D)
cursorId := cursor.Map()["id"].(int64)

// 获取更多结果
result, _ = db.RunCommand(bson.D{
    {Key: "getMore", Value: cursorId},
    {Key: "collection", Value: "users"},
    {Key: "batchSize", Value: 10},
})

// 关闭游标
result, _ = db.RunCommand(bson.D{
    {Key: "killCursors", Value: "users"},
    {Key: "cursors", Value: bson.A{cursorId}},
})
```

---

## 数据库命令

```go
// 通用命令执行
result, err := db.RunCommand(bson.D{{Key: "ping", Value: 1}})

// 服务器状态
result, _ := db.RunCommand(bson.D{{Key: "serverStatus", Value: 1}})

// 数据库统计
result, _ := db.RunCommand(bson.D{{Key: "dbStats", Value: 1}})

// 集合统计
result, _ := db.RunCommand(bson.D{{Key: "collStats", Value: "users"}})

// 验证数据库
validationResult := db.Validate()
if validationResult.Valid {
    fmt.Println("Database is valid")
} else {
    for _, err := range validationResult.Errors {
        fmt.Println("Error:", err)
    }
}

// 验证单个集合
result, _ := db.RunCommand(bson.D{{Key: "validate", Value: "users"}})

// 列出集合
result, _ := db.RunCommand(bson.D{{Key: "listCollections", Value: 1}})

// 创建集合
result, _ := db.RunCommand(bson.D{{Key: "create", Value: "newCollection"}})

// 删除集合
result, _ := db.RunCommand(bson.D{{Key: "drop", Value: "oldCollection"}})

// isMaster / hello
result, _ := db.RunCommand(bson.D{{Key: "isMaster", Value: 1}})
result, _ := db.RunCommand(bson.D{{Key: "hello", Value: 1}})

// buildInfo
result, _ := db.RunCommand(bson.D{{Key: "buildInfo", Value: 1}})
```

---

## 完整示例：用户管理系统

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/monolite/monodb/engine"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
    // 打开数据库
    db, err := engine.OpenDatabase("userdb.monodb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 获取用户集合
    users, _ := db.Collection("users")

    // 创建唯一索引
    users.CreateIndex(bson.D{{Key: "email", Value: 1}}, bson.D{{Key: "unique", Value: true}})
    users.CreateIndex(bson.D{{Key: "username", Value: 1}}, bson.D{{Key: "unique", Value: true}})

    // 创建用户
    newUser := bson.D{
        {Key: "username", Value: "alice"},
        {Key: "email", Value: "alice@example.com"},
        {Key: "password", Value: "hashed_password"},
        {Key: "profile", Value: bson.D{
            {Key: "firstName", Value: "Alice"},
            {Key: "lastName", Value: "Smith"},
            {Key: "age", Value: 25},
        }},
        {Key: "roles", Value: bson.A{"user"}},
        {Key: "createdAt", Value: time.Now()},
        {Key: "updatedAt", Value: time.Now()},
    }

    ids, err := users.Insert(newUser)
    if err != nil {
        log.Printf("Failed to create user: %v", err)
    } else {
        fmt.Printf("Created user with ID: %v\n", ids[0])
    }

    // 查找用户
    user, _ := users.FindOne(bson.D{{Key: "username", Value: "alice"}})
    fmt.Printf("Found user: %v\n", user)

    // 更新用户资料
    users.Update(
        bson.D{{Key: "username", Value: "alice"}},
        bson.D{
            {Key: "$set", Value: bson.D{
                {Key: "profile.age", Value: 26},
                {Key: "updatedAt", Value: time.Now()},
            }},
            {Key: "$addToSet", Value: bson.D{
                {Key: "roles", Value: "admin"},
            }},
        },
        false,
    )

    // 查询活跃成年用户
    docs, _ := users.Find(bson.D{
        {Key: "profile.age", Value: bson.D{{Key: "$gte", Value: 18}}},
    })
    fmt.Printf("Found %d adult users\n", len(docs))

    // 按年龄分组统计
    results, _ := users.Aggregate([]bson.D{
        {{Key: "$group", Value: bson.D{
            {Key: "_id", Value: nil},
            {Key: "avgAge", Value: bson.D{{Key: "$avg", Value: "$profile.age"}}},
            {Key: "totalUsers", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
        }}},
    })
    fmt.Printf("Statistics: %v\n", results)

    // 打印数据库统计
    stats := db.Stats()
    fmt.Printf("Database stats - Collections: %d, Documents: %d\n",
        stats.Collections, stats.Objects)
}
```

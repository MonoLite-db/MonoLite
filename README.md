# MonoLite

MonoLite is a **single-file, embeddable document database** for Go, compatible with MongoDB Wire Protocol. A pure Go implementation with embedded-first design.

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

**[Docs (EN)](docs/COMPATIBILITY.md)** · **[文档 (中文)](docs/COMPATIBILITY_CN.md)** · **[README (EN)](README.md)** · **[README (中文)](README_CN.md)** · **[Issues](https://github.com/monolite/monodb/issues)** · **[Contributing](CONTRIBUTING.md)**

</div>

## Project Vision

> **Simple as SQLite, yet think and work the MongoDB way.**

- **Single-File Storage** — One `.monodb` file is the complete database
- **Zero Deployment, Zero Ops** — No installation, no configuration, works out of the box
- **Embedded-First** — Library-first design, embed directly into your application
- **MongoDB Driver Compatible** — Use familiar APIs and tools

## Why MonoLite? The Pain Points We Solve

### The SQLite Dilemma

SQLite is an excellent embedded database, but when your application deals with **document-oriented data**, you'll encounter these frustrations:

| Pain Point | SQLite Reality | MonoLite Solution |
|------------|----------------|-----------------|
| **Rigid Schema** | Must define tables upfront with `CREATE TABLE`, schema changes require `ALTER TABLE` migrations | Schema-free — documents can have different fields, evolve naturally |
| **Nested Data** | Requires JSON1 extension or serialization, clunky to query | Native nested documents with dot notation (`address.city`) |
| **Array Operations** | No native array type, must serialize or use junction tables | Native arrays with operators like `$push`, `$pull`, `$elemMatch` |
| **Object-Relational Mismatch** | Application objects ↔ relational tables require ORM or manual mapping | Documents map directly to application objects |
| **Query Complexity** | Complex JOINs for hierarchical data, verbose SQL | Intuitive query operators (`$gt`, `$in`, `$or`) and aggregation pipelines |
| **Learning Curve** | SQL syntax varies, JOIN logic can be complex | MongoDB query language is JavaScript-native and widely known |

### When to Choose MonoLite over SQLite

✅ **Choose MonoLite when:**
- Your data is naturally hierarchical or document-shaped (JSON-like)
- Documents have varying structures (optional fields, evolving schemas)
- You need powerful array operations
- Your team already knows MongoDB
- You want to prototype with MongoDB compatibility, then scale to real MongoDB later

✅ **Stick with SQLite when:**
- Your data is highly relational with many-to-many relationships
- You need complex multi-table JOINs
- You require strict schema enforcement
- You're working with existing SQL-based tooling

### MonoLite vs SQLite: Feature Comparison

| Feature | MonoLite | SQLite |
|---------|--------|--------|
| **Data Model** | Document (BSON) | Relational (Tables) |
| **Schema** | Flexible, schema-free | Fixed, requires migrations |
| **Nested Data** | Native support | JSON1 extension |
| **Arrays** | Native with operators | Serialization required |
| **Query Language** | MongoDB Query Language | SQL |
| **Transactions** | ✅ Multi-document ACID | ✅ ACID |
| **Indexes** | B+Tree (single, compound, unique) | B-Tree (various types) |
| **File Format** | Single `.monodb` file | Single `.db` file |
| **Crash Recovery** | WAL | WAL/Rollback Journal |
| **Maturity** | New | 20+ years battle-tested |
| **Ecosystem** | MongoDB driver compatible | Massive ecosystem |

## Quick Start

### Installation

```bash
go get github.com/monolite/monodb
```

### Basic Usage (Library API)

```go
package main

import (
    "fmt"
    "log"

    "github.com/monolite/monodb/engine"
    "go.mongodb.org/mongo-driver/bson"
)

func main() {
    // Open database
    db, err := engine.OpenDatabase("data.monodb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Get collection
    users, err := db.Collection("users")
    if err != nil {
        log.Fatal(err)
    }

    // Insert documents
    users.Insert(bson.D{
        {Key: "name", Value: "Alice"},
        {Key: "age", Value: 25},
        {Key: "email", Value: "alice@example.com"},
    })

    // Insert multiple documents
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

    // Query documents
    results, _ := users.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 20}}}})
    for _, doc := range results {
        fmt.Println(doc)
    }

    // Find one document
    alice, _ := users.FindOne(bson.D{{Key: "name", Value: "Alice"}})
    fmt.Println("Found:", alice)

    // Query with dot notation
    results, _ = users.Find(bson.D{{Key: "address.city", Value: "Beijing"}})

    // Update documents
    users.Update(
        bson.D{{Key: "name", Value: "Alice"}},
        bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 26}}}},
        false, // upsert
    )

    // Update with upsert
    users.Update(
        bson.D{{Key: "name", Value: "Dave"}},
        bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 35}}}},
        true, // upsert
    )

    // Delete documents
    users.DeleteOne(bson.D{{Key: "name", Value: "Alice"}})
    users.Delete(bson.D{{Key: "age", Value: bson.D{{Key: "$lt", Value: 18}}}})
}
```

### Aggregation Pipeline

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

### Index Management

```go
users, _ := db.Collection("users")

// Create unique index
users.CreateIndex(bson.D{{Key: "email", Value: 1}}, true) // unique: true

// Create compound index
users.CreateIndex(bson.D{
    {Key: "name", Value: 1},
    {Key: "age", Value: -1},
}, false)

// List indexes
indexes := users.ListIndexes()

// Drop index
users.DropIndex("email_1")
```

### Using Transactions

```go
// Start a session
session, _ := db.StartSession()

// Start transaction
txn, _ := session.StartTransaction()

// Perform operations within transaction
users, _ := db.Collection("users")

// Transfer operation
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

// Commit or abort
if err := txn.Commit(); err != nil {
    txn.Abort()
}
```

### Wire Protocol Server (Optional)

If you need MongoDB driver compatibility, you can start the Wire Protocol server:

```go
import "github.com/monolite/monodb/protocol"

// Start MongoDB-compatible server
db, _ := engine.OpenDatabase("data.monodb")
server := protocol.NewServer(db, ":27017")
server.Start()

// Now connect with any MongoDB driver or mongosh:
// mongosh mongodb://localhost:27017
```

## Core Features

### 🔒 Crash Consistency (WAL)

- **Write-Ahead Logging** — All writes are logged to WAL before being written to data files
- **Automatic Crash Recovery** — WAL replay on startup restores to a consistent state
- **Checkpoint Mechanism** — Periodic checkpoints accelerate recovery and control WAL size
- **Atomic Writes** — Guarantees atomicity of individual write operations

### 💾 Full Transaction Support

- **Multi-Document Transactions** — Support for transactions spanning multiple collections
- **Transaction API** — startTransaction / commitTransaction / abortTransaction
- **Lock Management** — Document-level and collection-level lock granularity
- **Deadlock Detection** — Wait-graph based deadlock detection with automatic transaction abort
- **Transaction Rollback** — Complete Undo Log support for transaction rollback

### 🌳 B+Tree Indexes

- **Efficient Lookup** — O(log n) lookup complexity
- **Multiple Index Types** — Single-field, compound, and unique indexes
- **Dot Notation Support** — Support for nested field indexes (e.g., `address.city`)
- **Leaf Node Linked List** — Efficient range queries and sorting
- **Byte-Driven Split** — Intelligent node splitting strategy for optimized space utilization

### 🔐 Resource Limits & Security

| Limit | Value |
|-------|-------|
| Maximum document size | 16 MB |
| Maximum nesting depth | 100 levels |
| Maximum indexes per collection | 64 |
| Maximum batch write | 100,000 documents |
| Maximum field name length | 1,024 characters |

- **Input Validation** — Strict document structure and size validation
- **DoS Prevention** — Request size and nesting depth limits

### 📊 Observability

- **Structured Logging** — JSON format logs for easy analysis
- **Slow Query Logging** — Automatic logging of queries exceeding threshold
- **serverStatus Command** — Real-time server status monitoring
- **Memory/Storage Statistics** — Detailed resource usage statistics

```go
// View server status
status, _ := db.RunCommand(bson.D{{Key: "serverStatus", Value: 1}})

// View database statistics
stats, _ := db.RunCommand(bson.D{{Key: "dbStats", Value: 1}})

// View collection statistics
users, _ := db.Collection("users")
colStats := users.Stats()
```

## Feature Support Status

### Supported Core Features

| Category | Supported |
|----------|-----------|
| **CRUD** | insert, find, update, delete, findAndModify, replaceOne, distinct |
| **Query Operators** | $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or, $not, $nor, $exists, $type, $all, $elemMatch, $size, $regex |
| **Update Operators** | $set, $unset, $inc, $min, $max, $mul, $rename, $push, $pop, $pull, $pullAll, $addToSet |
| **Aggregation Stages** | $match, $project, $sort, $limit, $skip, $group, $count, $unwind, $addFields, $set, $unset, $lookup, $replaceRoot |
| **$group Accumulators** | $sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last |
| **Indexes** | Single-field, compound, unique indexes, dot notation (nested fields) |
| **Cursors** | getMore, killCursors, batchSize |
| **Commands** | dbStats, collStats, listCollections, listIndexes, serverStatus, validate, explain |
| **Transactions** | startTransaction, commitTransaction, abortTransaction |

### Query Operators Details

| Category | Operators |
|----------|-----------|
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` |
| Logical | `$and` `$or` `$not` `$nor` |
| Element | `$exists` `$type` |
| Array | `$all` `$elemMatch` `$size` |
| Evaluation | `$regex` |

### Update Operators Details

| Category | Operators |
|----------|-----------|
| Field | `$set` `$unset` `$inc` `$min` `$max` `$mul` `$rename` |
| Array | `$push` `$pop` `$pull` `$pullAll` `$addToSet` |

### Aggregation Pipeline Stages Details

| Stage | Description |
|-------|-------------|
| `$match` | Document filtering (supports all query operators) |
| `$project` | Field projection (include/exclude mode) |
| `$sort` | Sorting (supports compound sorting) |
| `$limit` | Limit result count |
| `$skip` | Skip specified count |
| `$group` | Group aggregation (supports 9 accumulators) |
| `$count` | Document count |
| `$unwind` | Array expansion (supports preserveNullAndEmptyArrays) |
| `$addFields` / `$set` | Add/set fields |
| `$unset` | Remove fields |
| `$lookup` | Collection join (left outer join) |
| `$replaceRoot` | Replace root document |

### Unsupported Features (Non-Goals)

- ❌ Replica Sets / Sharding (distributed)
- ❌ Authentication & Authorization
- ❌ Change Streams
- ❌ Geospatial Features
- ❌ Full-Text Search
- ❌ GridFS

> For complete compatibility details, see:
> - English: [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)
> - 中文: [docs/COMPATIBILITY_CN.md](docs/COMPATIBILITY_CN.md)

## Data Migration

### Import from MongoDB

```bash
# Method 1: Using mongodump + monodb-import
mongodump --db mydb --out backup/
./monodb-import -db data.monodb -dir backup/mydb/

# Method 2: Import JSON/JSONL files
./monodb-import -db data.monodb -file users.json -collection users
./monodb-import -db data.monodb -file orders.jsonl -collection orders

# Supported file formats
# - .bson  (mongodump output)
# - .json  (JSON array or Extended JSON)
# - .jsonl (JSON Lines, one document per line)
```

### Export to MongoDB

```bash
# Export all collections
./monodb-export -db data.monodb -dir export/

# Export single collection
./monodb-export -db data.monodb -collection users -out users.json

# Export in different formats
./monodb-export -db data.monodb -collection users -format bson -out users.bson
./monodb-export -db data.monodb -collection users -format jsonl -out users.jsonl

# Export with filter
./monodb-export -db data.monodb -collection users -query '{"age":{"$gt":18}}'

# Import back to MongoDB
mongorestore --db mydb export/
```

## Storage Engine Architecture

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
│                     (.monodb file)                              │
└────────────────────────────────────────────────────────────────┘
```

### File Format

```
┌────────────────────────────────────────────────────────────────┐
│  File Header (64 bytes)                                         │
│  - Magic: "MONO" (0x4D4F4E4F)                                  │
│  - Version, PageSize, PageCount, FreeListHead                   │
│  - MetaPageId, CatalogPageId, CreateTime, ModifyTime            │
├────────────────────────────────────────────────────────────────┤
│  Page 0: Meta Page                                              │
├────────────────────────────────────────────────────────────────┤
│  Page 1: Catalog Page (collection catalog, BSON format,         │
│          supports multi-page)                                   │
├────────────────────────────────────────────────────────────────┤
│  Page 2..N: Data Pages / Index Pages / Free Pages               │
│  - Data Pages: Slotted Page format for BSON documents          │
│  - Index Pages: B+Tree node storage                             │
│  - Free Pages: Free page linked list                            │
└────────────────────────────────────────────────────────────────┘
```

## Design Philosophy

> **80% of MongoDB experience, 20% of the complexity**

MonoLite doesn't aim for full MongoDB compatibility, but focuses on these scenarios:

- **Desktop Applications** — Local data storage for macOS / Windows / Linux
- **Development Tools** — Local debugging, offline data analysis, prototype validation
- **Small Services** — Single-machine services or Sidecar data storage
- **Embedded Scenarios** — IoT devices, edge computing, zero-deployment requirements

### Design Principles

1. **Simplicity First** — Single file, no external dependencies, zero-configuration startup
2. **Compatibility** — Standard MongoDB protocol, official drivers work directly
3. **Reliability** — WAL guarantees crash consistency, transactions ensure data integrity
4. **Extensibility** — Modular design, easy to add new features

## Project Structure

```
monodb/
├── cmd/
│   ├── monodbd/          # Server entry point
│   ├── monodb-import/    # Data import tool
│   └── monodb-export/    # Data export tool
├── engine/               # Database engine
│   ├── database.go       # Database core (command routing)
│   ├── collection.go     # Collection operations (CRUD)
│   ├── transaction.go    # Transaction management (locks, deadlock detection, rollback)
│   ├── session.go        # Session management (MongoDB standard sessions)
│   ├── index.go          # Index management (B+Tree indexes)
│   ├── aggregate.go      # Aggregation pipeline
│   ├── cursor.go         # Cursor management
│   ├── bson_compare.go   # BSON type comparison (MongoDB standard)
│   ├── validate.go       # Document validation
│   ├── limits.go         # Resource limits
│   ├── errors.go         # Error code system (MongoDB compatible)
│   ├── logger.go         # Structured logging
│   └── explain.go        # Query plan explanation
├── protocol/             # MongoDB Wire Protocol
│   ├── server.go         # TCP server
│   ├── message.go        # Message parsing
│   ├── opmsg.go          # OP_MSG handling
│   ├── opquery.go        # OP_QUERY handling (handshake compatibility)
│   └── errors.go         # Protocol errors
├── storage/              # Storage engine
│   ├── pager.go          # Page manager (caching, read/write)
│   ├── page.go           # Page structure (Slotted Page)
│   ├── btree.go          # B+Tree implementation
│   ├── wal.go            # Write-Ahead Log
│   └── keystring.go      # Index key encoding (MongoDB KeyString)
├── docs/                 # Documentation
│   ├── COMPATIBILITY.md     # Compatibility details (English)
│   └── COMPATIBILITY_CN.md  # 兼容性详情（中文）
├── CLAUDE.md             # Project rules
├── go.mod
└── README.md
```

## Technical Specifications

| Item | Specification |
|------|---------------|
| Maximum document size | 16 MB |
| Maximum nesting depth | 100 levels |
| Maximum indexes per collection | 64 |
| Maximum batch write | 100,000 documents |
| Page size | 4 KB |
| Default cursor batch size | 101 documents |
| Cursor timeout | 10 minutes |
| Transaction lock timeout | 30 seconds |
| WAL format version | 1 |
| File format version | 1 |
| Wire Protocol version | 13 (MongoDB 5.0) |

## Performance Characteristics

- **Write Optimization** — WAL batch flushing reduces fsync calls
- **Read Caching** — Page cache reduces disk IO
- **Index Acceleration** — B+Tree indexes automatically used for matching queries
- **Memory Friendly** — Configurable cache size, default 1000 pages

## Development & Contributing

```bash
# Run tests
go test ./...

# Run specific tests
go test ./engine -run TestTransaction
go test ./storage -run TestBTree

# Code formatting
go fmt ./...

# Static analysis
go vet ./...
```

### Code Standards

- Follow Go standard code style
- Single file should not exceed 800 lines
- Public APIs must have documentation comments
- Commit messages follow Conventional Commits format

## License

MIT License

---

<div align="center">

**[Docs (EN)](docs/COMPATIBILITY.md)** · **[文档 (中文)](docs/COMPATIBILITY_CN.md)** · **[README (EN)](README.md)** · **[README (中文)](README_CN.md)** · **[Issues](https://github.com/monolite/monodb/issues)** · **[Contributing](CONTRIBUTING.md)**

</div>


# MonoDB

MonoDB is a **single-file, embeddable document database** compatible with MongoDB Wire Protocol. Connect directly using official MongoDB drivers and mongosh.

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

</div>

## Project Vision

> **Simple as SQLite, yet think and work the MongoDB way.**

- **Single-File Storage** — One `.monodb` file is the complete database
- **Zero Deployment, Zero Ops** — No installation, no configuration, works out of the box
- **Embedded-First** — Library-first design, embed directly into your application
- **MongoDB Driver Compatible** — Use familiar APIs and tools

## Quick Start

### Installation & Build

```bash
# Clone the project
git clone https://github.com/monolite/monodb.git
cd monodb

# Build the server
go build -o monodbd ./cmd/monodbd

# Build import/export tools (optional)
go build -o monodb-import ./cmd/monodb-import
go build -o monodb-export ./cmd/monodb-export
```

### Start the Server

```bash
# Start (default port 27017, data file data.monodb)
./monodbd

# Custom configuration
./monodbd -file mydata.monodb -addr :27018
```

### Connect with mongosh

```bash
mongosh mongodb://localhost:27017
```

### Basic Operations

```javascript
// Insert documents
db.users.insertOne({name: "Alice", age: 25, email: "alice@example.com"})
db.users.insertMany([
  {name: "Bob", age: 30, tags: ["dev", "go"]},
  {name: "Carol", age: 28, address: {city: "Beijing"}}
])

// Query documents
db.users.find({age: {$gt: 20}})
db.users.findOne({name: "Alice"})
db.users.find({tags: {$in: ["dev"]}})
db.users.find({"address.city": "Beijing"})  // Dot notation query

// Update documents
db.users.updateOne({name: "Alice"}, {$set: {age: 26}})
db.users.updateMany({}, {$inc: {age: 1}})
db.users.updateOne({name: "Dave"}, {$set: {age: 35}}, {upsert: true})

// Delete documents
db.users.deleteOne({name: "Alice"})
db.users.deleteMany({age: {$lt: 18}})

// Aggregation pipeline
db.orders.aggregate([
  {$match: {status: "completed"}},
  {$group: {_id: "$customerId", total: {$sum: "$amount"}}},
  {$sort: {total: -1}},
  {$limit: 10}
])

// Index management
db.users.createIndex({email: 1}, {unique: true})
db.users.createIndex({name: 1, age: -1})  // Compound index
db.users.getIndexes()
db.users.dropIndex("email_1")
```

### Using Transactions

```javascript
// Start a session and begin a transaction
const session = db.getMongo().startSession()
session.startTransaction()

try {
  const users = session.getDatabase("test").users
  const accounts = session.getDatabase("test").accounts
  
  // Transfer operation
  users.updateOne({name: "Alice"}, {$inc: {balance: -100}})
  users.updateOne({name: "Bob"}, {$inc: {balance: 100}})
  
  session.commitTransaction()
} catch (e) {
  session.abortTransaction()
}
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

```javascript
// View server status
db.runCommand({serverStatus: 1})

// View database statistics
db.runCommand({dbStats: 1})

// View collection statistics
db.users.stats()
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

> For complete compatibility details, see [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)

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

MonoDB doesn't aim for full MongoDB compatibility, but focuses on these scenarios:

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
│   └── COMPATIBILITY.md  # Compatibility details
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

**[Documentation](docs/COMPATIBILITY.md)** · **[Issues](https://github.com/monolite/monodb/issues)** · **[Contributing](CONTRIBUTING.md)**

[中文文档](README_CN.md)

</div>


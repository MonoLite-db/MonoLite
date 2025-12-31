# MonoLite Go - MongoDB Compatibility

Created by Yanjunhui

This document describes **MonoLite** compatibility with MongoDB (wire protocol + command semantics).

- **中文版本**：[`docs/COMPATIBILITY_CN.md`](COMPATIBILITY_CN.md)
- **Back to README**：[`README.md`](../README.md)

---

## Scope & Goals

MonoLite aims to provide **"good-enough MongoDB experience"** for local / embedded / single-node scenarios:

- Connect with **official MongoDB drivers** and tools (`mongosh`, etc.)
- Support the most common **CRUD + indexing + aggregation + sessions/transactions** workflows
- Provide **crash consistency** via WAL and deterministic storage behavior

Non-goals (for now):

- Replica sets / sharding
- Authentication / authorization
- OP_COMPRESSED / wire compression
- Full server feature parity with MongoDB

---

## Wire Protocol Compatibility

| Item | Status |
|------|--------|
| OP_MSG | ✅ Supported |
| OP_QUERY | ✅ Handshake compatibility (legacy) |
| OP_COMPRESSED | ❌ Not supported |
| maxWireVersion | 13 (MongoDB 5.0) |

Notes:
- `hello` / `isMaster` report `maxWireVersion=13` to avoid triggering unsupported newer features.
- If a client sends OP_COMPRESSED, server returns a **structured protocol error**.

---

## Command Compatibility

MonoLite routes commands in `engine/database.go` and executes them in the engine.

### Implemented Commands

| Command | Status | Notes |
|---------|--------|-------|
| `ping` | ✅ | Connection test |
| `hello` / `isMaster` | ✅ | Wire version + capabilities |
| `buildInfo` | ✅ | Server build information |
| `serverStatus` | ✅ | Server runtime status |
| `connectionStatus` | ✅ | Connection information |
| `listCollections` | ✅ | List all collections |
| `create` | ✅ | Create collection |
| `drop` | ✅ | Drop collection |
| `insert` | ✅ | `documents` sequence supported |
| `find` | ✅ | Cursor + getMore supported |
| `update` | ✅ | Full update operators support |
| `delete` | ✅ | Respects per-delete `limit` |
| `count` | ✅ | Document counting |
| `distinct` | ✅ | Distinct field values |
| `aggregate` | ✅ | Pipeline subset |
| `findAndModify` | ✅ | Atomic find-and-modify |
| `createIndexes` | ✅ | B+Tree indexes |
| `listIndexes` | ✅ | List collection indexes |
| `dropIndexes` | ✅ | Drop indexes |
| `getMore` | ✅ | Cursor iteration |
| `killCursors` | ✅ | Cursor cleanup |
| `dbStats` | ✅ | Database statistics |
| `collStats` | ✅ | Collection statistics |
| `validate` | ✅ | Structural validation |
| `explain` | ✅ | Query plan explanation |
| `startTransaction` | ✅ | Begin transaction |
| `commitTransaction` | ✅ | Commit transaction |
| `abortTransaction` | ✅ | Rollback transaction |
| `endSessions` | ✅ | End sessions |
| `refreshSessions` | ✅ | Refresh sessions |

### Not Implemented

| Item | Status | Notes |
|------|--------|-------|
| `dropDatabase` | ❌ | Not implemented |
| `renameCollection` | ❌ | Not implemented |
| `currentOp` | ❌ | Not implemented |
| `killOp` | ❌ | Not implemented |
| Command monitoring | 🚧 | Spec runner skips `expectEvents` |

---

## Query Filter Operators

Filters are matched by `engine/index.go` (`FilterMatcher`) and used by both `find` and `$match`.

### Comparison Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$eq` | ✅ | Equal |
| `$ne` | ✅ | Not equal |
| `$gt` | ✅ | Greater than |
| `$gte` | ✅ | Greater than or equal |
| `$lt` | ✅ | Less than |
| `$lte` | ✅ | Less than or equal |
| `$in` | ✅ | In array |
| `$nin` | ✅ | Not in array |

### Logical Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$and` | ✅ | Logical AND |
| `$or` | ✅ | Logical OR |
| `$not` | ✅ | Logical NOT |
| `$nor` | ✅ | Logical NOR |

### Element Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$exists` | ✅ | Field exists |
| `$type` | ✅ | BSON type check |

### Array Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$all` | ✅ | Match all elements |
| `$size` | ✅ | Array size |
| `$elemMatch` | ✅ | Element match |

### Other Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$regex` | ✅ | Regular expression |
| `$mod` | ✅ | Modulo operation |

Notes:
- Dot-path field access is supported, including nested documents and array indexing.

---

## Update Operators

Update operators are implemented in `engine/collection.go` (`applyUpdate`).

### Field Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$set` | ✅ | Set field value |
| `$unset` | ✅ | Remove field |
| `$inc` | ✅ | Increment value |
| `$mul` | ✅ | Multiply value |
| `$min` | ✅ | Set to minimum |
| `$max` | ✅ | Set to maximum |
| `$rename` | ✅ | Rename field |
| `$currentDate` | ✅ | Set current date/timestamp |
| `$setOnInsert` | ✅ | Set on insert only |

### Array Operators

| Operator | Status | Description |
|----------|--------|-------------|
| `$push` | ✅ | Add to array |
| `$push` + `$each` | ✅ | Add multiple to array |
| `$pop` | ✅ | Remove first/last |
| `$pull` | ✅ | Remove matching |
| `$pullAll` | ✅ | Remove all matching |
| `$addToSet` | ✅ | Add unique to array |
| `$addToSet` + `$each` | ✅ | Add multiple unique |

---

## Indexes

| Item | Status | Notes |
|------|--------|-------|
| B+Tree index storage | ✅ | `storage/btree.go` |
| Unique index | ✅ | Enforced in engine |
| Compound keys | ✅ | MongoDB-like KeyString encoding |
| Sparse index | ❌ | Not implemented |
| TTL index | ❌ | Not implemented |
| Text index | ❌ | Not implemented |
| Geospatial index | ❌ | Not implemented |

---

## Aggregation Pipeline

Aggregation is implemented in `engine/aggregate.go`.

### Supported Stages

| Stage | Status | Description |
|-------|--------|-------------|
| `$match` | ✅ | Filter documents |
| `$project` | ✅ | Reshape documents |
| `$sort` | ✅ | Sort documents |
| `$limit` | ✅ | Limit results |
| `$skip` | ✅ | Skip documents |
| `$group` | ✅ | Group by expression |
| `$count` | ✅ | Count documents |
| `$unwind` | ✅ | Deconstruct array |
| `$addFields` / `$set` | ✅ | Add new fields |
| `$unset` | ✅ | Remove fields |
| `$replaceRoot` | ✅ | Replace root document |
| `$lookup` | ✅ | Left outer join |

### Group Accumulators

| Accumulator | Status | Description |
|-------------|--------|-------------|
| `$sum` | ✅ | Sum values |
| `$avg` | ✅ | Average value |
| `$min` | ✅ | Minimum value |
| `$max` | ✅ | Maximum value |
| `$first` | ✅ | First value |
| `$last` | ✅ | Last value |
| `$push` | ✅ | Push to array |
| `$addToSet` | ✅ | Add unique to array |

### Not Implemented Stages

| Stage | Status |
|-------|--------|
| `$out` | ❌ |
| `$merge` | ❌ |
| `$facet` | ❌ |
| `$bucket` | ❌ |
| `$graphLookup` | ❌ |
| `$geoNear` | ❌ |

---

## Transactions & Sessions

MonoLite provides single-node transactions and sessions:

| Feature | Status | Notes |
|---------|--------|-------|
| Session management | ✅ | Start/end/refresh sessions |
| Multi-document transactions | ✅ | Single-node ACID |
| Lock manager | ✅ | Read/write locks |
| Deadlock detection | ✅ | Wait graph analysis |
| Transaction isolation | ✅ | Read committed |
| Rollback on abort | ✅ | Undo log support |

Limitations:
- No replica set / distributed transactions
- No causal consistency

---

## BSON Types

| Type | Status | Notes |
|------|--------|-------|
| Double | ✅ | 64-bit float |
| String | ✅ | UTF-8 string |
| Document | ✅ | Embedded document |
| Array | ✅ | BSON array |
| Binary | ✅ | Binary data |
| ObjectId | ✅ | 12-byte identifier |
| Boolean | ✅ | true/false |
| Date | ✅ | UTC datetime |
| Null | ✅ | Null value |
| Int32 | ✅ | 32-bit integer |
| Int64 | ✅ | 64-bit integer |
| Timestamp | ✅ | MongoDB timestamp |
| Decimal128 | ❌ | Not supported |
| MinKey/MaxKey | ❌ | Not supported |
| JavaScript | ❌ | Not supported |
| Regex | ✅ | Query only |

---

## Official Spec Tests (MongoDB specifications)

MonoLite includes a **minimal runner** for MongoDB official CRUD Unified Test Format:

- Runner: `tests/mongo_spec/crud_unified_test.go`
- Docs: `tests/mongo_spec/README.md`
- Test data: `third_party/mongodb/specifications/source/crud/tests/unified/`

### How to Run

By default, spec tests are skipped.

```bash
MONOLITE_RUN_MONGO_SPECS=1 go test ./tests/mongo_spec -count=1
```

Run a single file:

```bash
MONOLITE_RUN_MONGO_SPECS=1 MONOLITE_MONGO_SPECS_FILENAME=find.json go test ./tests/mongo_spec -count=1
```

### Current Runner Limitations

The runner intentionally skips unsupported parts:

- `expectEvents` / command monitoring assertions
- `expectError` assertion framework
- Advanced options (collation/hint/let/arrayFilters/...)

---

## Reporting Compatibility Issues

When reporting compatibility issues, include:

- Client/driver name + version
- The exact command (Extended JSON) or code snippet
- Expected behavior (MongoDB) vs actual behavior (MonoLite)
- If possible, a reduced spec test case that reproduces the issue

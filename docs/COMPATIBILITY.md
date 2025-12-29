Created by Yanjunhui

## Compatibility (MongoDB)

This document describes **MonoDB / MonoLite** compatibility with MongoDB (wire protocol + command semantics).

- **中文版本**：[`docs/COMPATIBILITY_CN.md`](COMPATIBILITY_CN.md)
- **Back to README**：[`README.md`](../README.md)

---

## Scope & Goals

MonoDB aims to provide **“good-enough MongoDB experience”** for local / embedded / single-node scenarios:

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

## Command Compatibility (High-Level)

MonoDB routes commands in `engine/database.go` and executes them in the engine.

### Implemented commands (server-side)

| Command | Status | Notes |
|--------|--------|------|
| `ping` | ✅ | |
| `hello` / `isMaster` | ✅ | Wire version + capabilities |
| `buildInfo` | ✅ | |
| `listCollections` | ✅ | |
| `insert` | ✅ | `documents` sequence supported |
| `find` | ✅ | Cursor + getMore supported |
| `update` | ✅ | Basic update operators; some options may be skipped by spec runner |
| `delete` | ✅ | **Respects per-delete `limit`** (`limit=1` → deleteOne) |
| `count` | ✅ | |
| `drop` | ✅ | Collection drop |
| `createIndexes` / `listIndexes` / `dropIndexes` | ✅ | B+Tree indexes |
| `aggregate` | ✅ | Pipeline subset |
| `getMore` / `killCursors` | ✅ | Cursor management |
| `findAndModify` | ✅ | |
| `distinct` | ✅ | |
| `dbStats` / `collStats` / `serverStatus` | ✅ | |
| `validate` | ✅ | Structural validation |
| `explain` | ✅ | |
| `connectionStatus` | ✅ | |
| sessions / transactions (`startTransaction` / `commitTransaction` / `abortTransaction` / `endSessions` / `refreshSessions`) | ✅ | Single-node transactions |

### Not implemented / partially implemented

| Item | Status | Notes |
|------|--------|------|
| `dropDatabase` | ❌ | Spec runner avoids calling it (MonoDB doesn’t implement this command yet) |
| Command monitoring / event assertions | 🚧 | Spec runner currently skips `expectEvents` cases |
| Full Unified Test Format support | 🚧 | Runner supports a small subset first |

---

## Query Filter Operators

Filters are matched by `engine/index.go` (`FilterMatcher`) and used by both `find` and `$match`.

Supported operators (current implementation):

- **Logical**: `$and`, `$or`, `$not`, `$nor`
- **Comparison**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- **Array / set**: `$in`, `$nin`, `$all`, `$size`, `$elemMatch`
- **Field**: `$exists`, `$type`
- **Regex**: `$regex`

Notes:
- Dot-path field access is supported, including nested documents and array indexing.

---

## Update Operators

Update operators are implemented in `engine/collection.go` (`applyUpdate`).

Supported operators:

- **Field**: `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`
- **Array**: `$push`, `$pop`, `$pull`, `$pullAll`, `$addToSet`

---

## Indexes

| Item | Status | Notes |
|------|--------|------|
| B+Tree index storage | ✅ | `storage/btree.go` |
| Unique index | ✅ | Enforced in engine/index manager |
| Compound keys | ✅ | Via MongoDB-like KeyString encoding |

---

## Aggregation Pipeline

Aggregation is implemented in `engine/aggregate.go`.

Supported stages (subset):

- `$match`, `$project`, `$sort`, `$limit`, `$skip`
- `$group` (common accumulators)
- `$count`, `$unwind`
- `$addFields` / `$set`, `$unset`
- `$replaceRoot`
- `$lookup` (requires DB context; supported via `Collection.Aggregate`)

---

## Transactions & Sessions

MonoDB provides single-node transactions and sessions:

- Lock manager with deadlock detection (see tests in `engine/transaction_test.go`)
- Session manager for standard MongoDB sessions

Limitations:
- No replica set / distributed transactions

---

## Official Spec Tests (MongoDB specifications)

MonoDB includes a **minimal runner** for MongoDB official CRUD Unified Test Format:

- Runner: `tests/mongo_spec/crud_unified_test.go`
- Docs: `tests/mongo_spec/README.md`
- Test data: `third_party/mongodb/specifications/source/crud/tests/unified/`

### How to run

By default, spec tests are skipped.

```bash
MONOLITE_RUN_MONGO_SPECS=1 go test ./tests/mongo_spec -count=1
```

Run a single file:

```bash
MONOLITE_RUN_MONGO_SPECS=1 MONOLITE_MONGO_SPECS_FILENAME=find.json go test ./tests/mongo_spec -count=1
```

### Current runner limitations

The runner intentionally skips unsupported parts to avoid false positives:

- `expectEvents` / command monitoring assertions
- `expectError` assertion framework
- Many advanced options (collation/hint/let/arrayFilters/...)

---

## Reporting Compatibility Issues

When reporting compatibility issues, include:

- Client/driver name + version
- The exact command (Extended JSON) or code snippet
- Expected behavior (MongoDB) vs actual behavior (MonoDB)
- If possible, a reduced spec test case that reproduces the issue



// Created by Yanjunhui

package engine

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// ExplainResult explain 命令结果
// EN: ExplainResult is the result of the explain command.
type ExplainResult struct {
	// 执行统计
	// EN: Execution statistics.

	// TotalKeysExamined 检查的索引键数量
	// EN: TotalKeysExamined is the number of index keys examined.
	TotalKeysExamined int64

	// TotalDocsExamined 检查的文档数量
	// EN: TotalDocsExamined is the number of documents examined.
	TotalDocsExamined int64

	// ExecutionTimeMs 执行时间（毫秒）
	// EN: ExecutionTimeMs is the execution time in milliseconds.
	ExecutionTimeMs int64

	// 查询计划
	// EN: Query plan.

	// Namespace 命名空间
	// EN: Namespace is the namespace (db.collection).
	Namespace string

	// IndexUsed 使用的索引名称（空表示全表扫描）
	// EN: IndexUsed is the index name used (empty means collection scan).
	IndexUsed string

	// IndexBounds 索引边界
	// EN: IndexBounds is the index bounds (for display).
	IndexBounds bson.D

	// 阶段信息
	// EN: Stage information.

	// Stage 扫描阶段类型（COLLSCAN/IXSCAN/FETCH）
	// EN: Stage is the scan stage type (COLLSCAN/IXSCAN/FETCH).
	Stage string

	// IsMultiKey 是否多键索引
	// EN: IsMultiKey indicates whether the index is multikey.
	IsMultiKey bool

	// HasSortStage 是否有排序阶段
	// EN: HasSortStage indicates whether a sort stage exists.
	HasSortStage bool

	// HasProjection 是否有投影
	// EN: HasProjection indicates whether projection is requested.
	HasProjection bool

	// 计划执行
	// EN: Plan execution.

	// NReturned 返回的文档数量
	// EN: NReturned is the number of documents returned.
	NReturned int64
}

// ExplainVerbosity explain 详细级别
// EN: ExplainVerbosity is the explain verbosity level.
type ExplainVerbosity string

const (
	ExplainQueryPlanner      ExplainVerbosity = "queryPlanner"
	ExplainExecutionStats    ExplainVerbosity = "executionStats"
	ExplainAllPlansExecution ExplainVerbosity = "allPlansExecution"
)

// Explain 解释查询执行计划
// EN: Explain describes the query execution plan.
//
// 注意：当前实现始终输出 COLLSCAN，因为实际执行路径尚未实现索引扫描。
// EN: Note: the current implementation always reports COLLSCAN because index scan execution is not implemented yet.
//
// 这是"诚实化"策略：在真正实现 IXSCAN 执行路径之前，不向用户宣称支持索引扫描。
// EN: This is an “honesty” strategy: we do not claim IXSCAN support until it is truly implemented.
func (c *Collection) Explain(filter bson.D, opts *QueryOptions) *ExplainResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 当前实现始终为全表扫描
	// EN: Always use collection scan for now.
	result := &ExplainResult{
		Namespace: c.db.name + "." + c.info.Name,
		Stage:     "COLLSCAN",
	}

	// 检查是否有排序阶段（当前实现为内存排序）
	// EN: Detect sort stage (currently in-memory sort).
	if opts != nil && len(opts.Sort) > 0 {
		result.HasSortStage = true
	}

	// 检查是否有投影
	// EN: Detect projection.
	if opts != nil && len(opts.Projection) > 0 {
		result.HasProjection = true
	}

	// 文档数量（当前实现扫描所有文档）
	// EN: Document count (current implementation scans all documents).
	result.TotalDocsExamined = int64(c.info.DocumentCount)

	// 当前不使用索引，键检查数量为 0
	// EN: No index usage yet; total keys examined is 0.
	result.TotalKeysExamined = 0

	// 记录可用索引信息（仅供参考，实际未使用）
	// EN: Record “theoretically usable” index info for reference; not used in execution yet.
	// 未来实现 IXSCAN 时会真正利用这些索引
	// EN: When IXSCAN is implemented, these indexes will be used.
	if c.indexManager != nil && len(filter) > 0 {
		for _, idx := range c.indexManager.indexes {
			if canUseIndex(filter, idx.info.Keys) {
				// 仅记录"理论上可用"的索引，但不改变 Stage
				// EN: Only record a “potentially usable” index; do not change Stage.
				// result.IndexUsed 保持为空，表示实际未使用
				// EN: Keep result.IndexUsed empty to indicate it was not actually used.
				result.IndexBounds = buildIndexBounds(filter, idx.info.Keys)
				break
			}
		}
	}

	return result
}

// canUseIndex 检查 filter 是否可以使用索引
// EN: canUseIndex checks whether the filter could use the given index.
func canUseIndex(filter bson.D, indexKeys bson.D) bool {
	if len(indexKeys) == 0 {
		return false
	}

	// 获取索引的第一个键
	// EN: Take the first index key.
	firstIndexKey := indexKeys[0].Key

	// 检查 filter 中是否包含索引前缀字段
	// EN: Check whether filter contains the index prefix field.
	for _, elem := range filter {
		if elem.Key == firstIndexKey {
			return true
		}
	}

	return false
}

// canUseIndexForSort 检查排序是否可以使用索引
// EN: canUseIndexForSort checks whether sort can be satisfied by the index.
func canUseIndexForSort(sortSpec bson.D, indexKeys bson.D) bool {
	if len(sortSpec) == 0 || len(indexKeys) == 0 {
		return false
	}

	// 检查排序字段是否是索引前缀
	// EN: Ensure sort fields match the index prefix.
	if len(sortSpec) > len(indexKeys) {
		return false
	}

	for i, sortField := range sortSpec {
		if i >= len(indexKeys) {
			return false
		}

		if sortField.Key != indexKeys[i].Key {
			return false
		}

		// 检查排序方向是否一致（或完全相反）
		// EN: Check sort direction matches (or is the exact reverse).
		sortDir := getDirection(sortField.Value)
		indexDir := getDirection(indexKeys[i].Value)

		if sortDir != indexDir && sortDir != -indexDir {
			return false
		}
	}

	return true
}

// getDirection 获取排序方向
// EN: getDirection parses sort direction value.
func getDirection(v interface{}) int {
	switch d := v.(type) {
	case int:
		return d
	case int32:
		return int(d)
	case int64:
		return int(d)
	case float64:
		return int(d)
	default:
		return 1
	}
}

// buildIndexBounds 构建索引边界
// EN: buildIndexBounds builds index bounds for display.
func buildIndexBounds(filter bson.D, indexKeys bson.D) bson.D {
	bounds := bson.D{}

	for _, keySpec := range indexKeys {
		fieldName := keySpec.Key

		// 查找 filter 中对应字段的条件
		// EN: Find the condition for this field in the filter.
		var bound interface{} = "[-inf, +inf]" // 默认无界
		// EN: Default is unbounded.

		for _, elem := range filter {
			if elem.Key == fieldName {
				// 解析操作符
				// EN: Parse operators.
				if ops, ok := elem.Value.(bson.D); ok {
					bound = formatBounds(ops)
				} else {
					// 等值查询
					// EN: Equality predicate.
					bound = formatEqualityBound(elem.Value)
				}
				break
			}
		}

		bounds = append(bounds, bson.E{Key: fieldName, Value: bound})
	}

	return bounds
}

// formatBounds 格式化范围条件
// EN: formatBounds formats a range bound for display.
func formatBounds(ops bson.D) string {
	var lower, upper string = "MinKey", "MaxKey"
	var lowerInc, upperInc bool = true, true

	for _, op := range ops {
		switch op.Key {
		case "$gt":
			lower = formatValue(op.Value)
			lowerInc = false
		case "$gte":
			lower = formatValue(op.Value)
			lowerInc = true
		case "$lt":
			upper = formatValue(op.Value)
			upperInc = false
		case "$lte":
			upper = formatValue(op.Value)
			upperInc = true
		case "$eq":
			return "[" + formatValue(op.Value) + ", " + formatValue(op.Value) + "]"
		}
	}

	leftBracket := "("
	rightBracket := ")"
	if lowerInc {
		leftBracket = "["
	}
	if upperInc {
		rightBracket = "]"
	}

	return leftBracket + lower + ", " + upper + rightBracket
}

// formatEqualityBound 格式化等值条件
// EN: formatEqualityBound formats an equality bound for display.
func formatEqualityBound(v interface{}) string {
	return "[" + formatValue(v) + ", " + formatValue(v) + "]"
}

// formatValue 格式化值为字符串
// EN: formatValue formats a value as a string for display.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return strconv.Quote(val)
	case int, int32, int64, float32, float64:
		return fmt.Sprint(val)
	case bool:
		return fmt.Sprint(val)
	default:
		// explain 输出只用于可读性展示；这里保证永不 panic。
		// EN: Explain output is for readability only; never panic here.
		return fmt.Sprintf("%v", v)
	}
}

// ToBSON 将 ExplainResult 转换为 BSON 格式
// EN: ToBSON converts ExplainResult to BSON for wire responses.
func (r *ExplainResult) ToBSON() bson.D {
	executionStats := bson.D{
		{Key: "nReturned", Value: r.NReturned},
		{Key: "executionTimeMillis", Value: r.ExecutionTimeMs},
		{Key: "totalKeysExamined", Value: r.TotalKeysExamined},
		{Key: "totalDocsExamined", Value: r.TotalDocsExamined},
	}

	// 构建执行阶段
	// EN: Build execution stages.
	// 当前实现：始终为 COLLSCAN（全表扫描）
	// EN: Current implementation: always COLLSCAN (collection scan).
	scanStage := bson.D{
		{Key: "stage", Value: r.Stage},
		{Key: "nReturned", Value: r.NReturned},
		{Key: "docsExamined", Value: r.TotalDocsExamined},
	}

	// 注意：IndexUsed 当前始终为空，因为实际执行路径不使用索引
	// EN: Note: IndexUsed is always empty because execution does not use indexes yet.
	// 未来实现 IXSCAN 后，这里会添加索引相关信息
	// EN: After IXSCAN is implemented, index-related fields will be added here.

	executionStages := scanStage

	// 如果有排序，添加 SORT 阶段（当前为内存排序）
	// EN: If sorting is requested, add a SORT stage (currently in-memory).
	if r.HasSortStage {
		executionStages = bson.D{
			{Key: "stage", Value: "SORT"},
			// 标注为内存排序
			// EN: Mark as in-memory sort.
			{Key: "sortPattern", Value: "in-memory"},
			{Key: "inputStage", Value: scanStage},
		}
	}

	winningPlan := bson.D{
		{Key: "stage", Value: executionStages[0].Value},
	}
	if r.HasSortStage {
		winningPlan = append(winningPlan, bson.E{Key: "inputStage", Value: scanStage})
	}

	queryPlanner := bson.D{
		{Key: "namespace", Value: r.Namespace},
		{Key: "indexFilterSet", Value: false},
		{Key: "winningPlan", Value: winningPlan},
		// 添加说明：索引扫描尚未实现
		// EN: Note: index scan is not implemented.
		{Key: "note", Value: "Index scan (IXSCAN) not yet implemented; always using COLLSCAN"},
	}

	return bson.D{
		{Key: "queryPlanner", Value: queryPlanner},
		{Key: "executionStats", Value: executionStats},
		{Key: "ok", Value: 1.0},
	}
}

// explainCommand 实现 explain 命令
// EN: explainCommand implements the explain command.
func (db *Database) explainCommand(cmd bson.D) (bson.D, error) {
	var verbosity ExplainVerbosity = ExplainExecutionStats
	var explainCmd bson.D

	for _, elem := range cmd {
		switch elem.Key {
		case "explain":
			if ec, ok := elem.Value.(bson.D); ok {
				explainCmd = ec
			}
		case "verbosity":
			if v, ok := elem.Value.(string); ok {
				verbosity = ExplainVerbosity(v)
			}
		}
	}

	_ = verbosity // 暂时忽略 verbosity，总是返回 executionStats
	// EN: Verbosity is currently ignored; always return executionStats.

	// 解析要 explain 的命令
	// EN: Parse the inner command to explain.
	var cmdName string
	var colName string
	var filter bson.D
	var opts QueryOptions

	for _, elem := range explainCmd {
		switch elem.Key {
		case "find":
			cmdName = "find"
			colName, _ = elem.Value.(string)
		case "aggregate":
			cmdName = "aggregate"
			colName, _ = elem.Value.(string)
		case "filter":
			filter, _ = elem.Value.(bson.D)
		case "sort":
			opts.Sort, _ = elem.Value.(bson.D)
		case "limit":
			switch v := elem.Value.(type) {
			case int32:
				opts.Limit = int64(v)
			case int64:
				opts.Limit = v
			}
		case "skip":
			switch v := elem.Value.(type) {
			case int32:
				opts.Skip = int64(v)
			case int64:
				opts.Skip = v
			}
		case "projection":
			opts.Projection, _ = elem.Value.(bson.D)
		}
	}

	if colName == "" {
		return ErrBadValue("explain requires a command with collection name").ToBSON(), nil
	}

	col := db.GetCollection(colName)
	if col == nil {
		// 集合不存在，返回空 explain
		// EN: Collection does not exist; return an empty explain.
		result := &ExplainResult{
			Namespace:         db.name + "." + colName,
			Stage:             "EOF",
			TotalDocsExamined: 0,
			TotalKeysExamined: 0,
		}
		return result.ToBSON(), nil
	}

	// 执行 explain
	// EN: Execute explain.
	var result *ExplainResult

	switch cmdName {
	case "find":
		result = col.Explain(filter, &opts)
	case "aggregate":
		// 聚合的 explain 简化处理
		// EN: Explain for aggregate is simplified.
		result = col.Explain(nil, nil)
	default:
		result = col.Explain(filter, &opts)
	}

	return result.ToBSON(), nil
}

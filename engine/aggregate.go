// Created by Yanjunhui

package engine

import (
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

// PipelineStage 表示聚合管道的一个阶段
// EN: PipelineStage represents a stage in an aggregation pipeline.
type PipelineStage interface {
	Execute(docs []bson.D) ([]bson.D, error)
	Name() string
}

// Pipeline 聚合管道
// EN: Pipeline is an aggregation pipeline.
type Pipeline struct {
	stages []PipelineStage
	db     *Database
}

// NewPipeline 创建聚合管道
// EN: NewPipeline creates an aggregation pipeline.
func NewPipeline(stages []bson.D) (*Pipeline, error) {
	return NewPipelineWithDB(stages, nil)
}

// NewPipelineWithDB 创建带数据库引用的聚合管道
// EN: NewPipelineWithDB creates an aggregation pipeline with a database reference.
func NewPipelineWithDB(stages []bson.D, db *Database) (*Pipeline, error) {
	p := &Pipeline{
		stages: make([]PipelineStage, 0, len(stages)),
		db:     db,
	}

	for _, stageDoc := range stages {
		if len(stageDoc) != 1 {
			return nil, fmt.Errorf("invalid pipeline stage: each stage must have exactly one field")
		}

		stageName := stageDoc[0].Key
		stageSpec := stageDoc[0].Value

		stage, err := createStageWithDB(stageName, stageSpec, db)
		if err != nil {
			return nil, err
		}
		p.stages = append(p.stages, stage)
	}

	return p, nil
}

// Execute 执行聚合管道
// EN: Execute runs the pipeline against the given documents.
func (p *Pipeline) Execute(docs []bson.D) ([]bson.D, error) {
	result := docs

	for _, stage := range p.stages {
		var err error
		result, err = stage.Execute(result)
		if err != nil {
			return nil, fmt.Errorf("error in %s stage: %w", stage.Name(), err)
		}
	}

	return result, nil
}

// createStage 创建管道阶段
// EN: createStage creates a pipeline stage.
func createStage(name string, spec interface{}) (PipelineStage, error) {
	return createStageWithDB(name, spec, nil)
}

// createStageWithDB 创建带数据库引用的管道阶段
// EN: createStageWithDB creates a pipeline stage with a database reference.
func createStageWithDB(name string, spec interface{}, db *Database) (PipelineStage, error) {
	switch name {
	case "$match":
		return newMatchStage(spec)
	case "$project":
		return newProjectStage(spec)
	case "$sort":
		return newSortStage(spec)
	case "$limit":
		return newLimitStage(spec)
	case "$skip":
		return newSkipStage(spec)
	case "$group":
		return newGroupStage(spec)
	case "$count":
		return newCountStage(spec)
	case "$unwind":
		return newUnwindStage(spec)
	case "$addFields", "$set":
		return newAddFieldsStage(spec)
	case "$unset":
		return newUnsetStage(spec)
	case "$replaceRoot":
		return newReplaceRootStage(spec)
	case "$lookup":
		return newLookupStage(spec, db)
	default:
		return nil, fmt.Errorf("unsupported pipeline stage: %s", name)
	}
}

// MatchStage $match 阶段
// EN: MatchStage implements the $match stage.
type MatchStage struct {
	filter bson.D
}

func newMatchStage(spec interface{}) (*MatchStage, error) {
	filter, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$match requires a document")
	}
	return &MatchStage{filter: filter}, nil
}

func (s *MatchStage) Name() string { return "$match" }

func (s *MatchStage) Execute(docs []bson.D) ([]bson.D, error) {
	if len(s.filter) == 0 {
		return docs, nil
	}

	matcher := NewFilterMatcher(s.filter)
	result := make([]bson.D, 0)

	for _, doc := range docs {
		if matcher.Match(doc) {
			result = append(result, doc)
		}
	}

	return result, nil
}

// ProjectStage $project 阶段
// EN: ProjectStage implements the $project stage.
type ProjectStage struct {
	projection bson.D
}

func newProjectStage(spec interface{}) (*ProjectStage, error) {
	projection, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$project requires a document")
	}
	return &ProjectStage{projection: projection}, nil
}

func (s *ProjectStage) Name() string { return "$project" }

func (s *ProjectStage) Execute(docs []bson.D) ([]bson.D, error) {
	return applyProjection(docs, s.projection), nil
}

// SortStage $sort 阶段
// EN: SortStage implements the $sort stage.
type SortStage struct {
	sortSpec bson.D
}

func newSortStage(spec interface{}) (*SortStage, error) {
	sortSpec, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$sort requires a document")
	}
	return &SortStage{sortSpec: sortSpec}, nil
}

func (s *SortStage) Name() string { return "$sort" }

func (s *SortStage) Execute(docs []bson.D) ([]bson.D, error) {
	return sortDocuments(docs, s.sortSpec), nil
}

// LimitStage $limit 阶段
// EN: LimitStage implements the $limit stage.
type LimitStage struct {
	limit int64
}

func newLimitStage(spec interface{}) (*LimitStage, error) {
	limit, err := toInt64(spec)
	if err != nil {
		return nil, fmt.Errorf("$limit requires an integer")
	}
	return &LimitStage{limit: limit}, nil
}

func (s *LimitStage) Name() string { return "$limit" }

func (s *LimitStage) Execute(docs []bson.D) ([]bson.D, error) {
	if s.limit <= 0 || int64(len(docs)) <= s.limit {
		return docs, nil
	}
	return docs[:s.limit], nil
}

// SkipStage $skip 阶段
// EN: SkipStage implements the $skip stage.
type SkipStage struct {
	skip int64
}

func newSkipStage(spec interface{}) (*SkipStage, error) {
	skip, err := toInt64(spec)
	if err != nil {
		return nil, fmt.Errorf("$skip requires an integer")
	}
	return &SkipStage{skip: skip}, nil
}

func (s *SkipStage) Name() string { return "$skip" }

func (s *SkipStage) Execute(docs []bson.D) ([]bson.D, error) {
	if s.skip <= 0 {
		return docs, nil
	}
	if int64(len(docs)) <= s.skip {
		return []bson.D{}, nil
	}
	return docs[s.skip:], nil
}

// GroupStage $group 阶段
// EN: GroupStage implements the $group stage.
type GroupStage struct {
	id           interface{}
	accumulators bson.D
}

func newGroupStage(spec interface{}) (*GroupStage, error) {
	groupSpec, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$group requires a document")
	}

	stage := &GroupStage{
		accumulators: bson.D{},
	}

	for _, elem := range groupSpec {
		if elem.Key == "_id" {
			stage.id = elem.Value
		} else {
			stage.accumulators = append(stage.accumulators, elem)
		}
	}

	return stage, nil
}

func (s *GroupStage) Name() string { return "$group" }

func (s *GroupStage) Execute(docs []bson.D) ([]bson.D, error) {
	// 按 _id 分组
	// EN: Group by _id.
	groups := make(map[string]*groupState)
	groupOrder := make([]string, 0)

	for _, doc := range docs {
		// 计算分组键
		// EN: Compute group key.
		groupKey := s.computeGroupKey(doc)
		keyStr := fmt.Sprintf("%v", groupKey)

		if _, exists := groups[keyStr]; !exists {
			groups[keyStr] = &groupState{
				id:     groupKey,
				values: make(map[string][]interface{}),
			}
			groupOrder = append(groupOrder, keyStr)
		}

		// 收集每个累加器字段的值
		// EN: Collect values for each accumulator field.
		for _, acc := range s.accumulators {
			fieldName := acc.Key
			accSpec, ok := acc.Value.(bson.D)
			if !ok {
				continue
			}
			if len(accSpec) != 1 {
				continue
			}

			accOp := accSpec[0].Key
			accExpr := accSpec[0].Value

			// 提取表达式的值
			// EN: Evaluate expression value.
			val := s.evaluateExpression(accExpr, doc)
			groups[keyStr].values[fieldName+"_"+accOp] = append(groups[keyStr].values[fieldName+"_"+accOp], val)
			groups[keyStr].accumulators = s.accumulators
		}
	}

	// 计算最终结果
	// EN: Compute final result.
	result := make([]bson.D, 0, len(groups))
	for _, keyStr := range groupOrder {
		state := groups[keyStr]
		doc := bson.D{{Key: "_id", Value: state.id}}

		for _, acc := range s.accumulators {
			fieldName := acc.Key
			accSpec, ok := acc.Value.(bson.D)
			if !ok {
				continue
			}
			if len(accSpec) != 1 {
				continue
			}

			accOp := accSpec[0].Key
			values := state.values[fieldName+"_"+accOp]

			var finalVal interface{}
			switch accOp {
			case "$sum":
				finalVal = s.computeSum(values)
			case "$avg":
				finalVal = s.computeAvg(values)
			case "$min":
				finalVal = s.computeMin(values)
			case "$max":
				finalVal = s.computeMax(values)
			case "$first":
				if len(values) > 0 {
					finalVal = values[0]
				}
			case "$last":
				if len(values) > 0 {
					finalVal = values[len(values)-1]
				}
			case "$count":
				finalVal = int64(len(values))
			case "$push":
				finalVal = bson.A(values)
			case "$addToSet":
				finalVal = s.computeAddToSet(values)
			default:
				return nil, fmt.Errorf("unsupported accumulator: %s", accOp)
			}

			doc = append(doc, bson.E{Key: fieldName, Value: finalVal})
		}

		result = append(result, doc)
	}

	return result, nil
}

type groupState struct {
	id           interface{}
	values       map[string][]interface{}
	accumulators bson.D
}

func (s *GroupStage) computeGroupKey(doc bson.D) interface{} {
	if s.id == nil {
		return nil
	}

	// 字符串形式的字段引用 "$field"
	// EN: Field reference in string form: "$field".
	if idStr, ok := s.id.(string); ok {
		if len(idStr) > 0 && idStr[0] == '$' {
			return getDocField(doc, idStr[1:])
		}
		return idStr
	}

	// 文档形式的复合键
	// EN: Composite key in document form.
	if idDoc, ok := s.id.(bson.D); ok {
		result := bson.D{}
		for _, elem := range idDoc {
			if exprStr, ok := elem.Value.(string); ok && len(exprStr) > 0 && exprStr[0] == '$' {
				result = append(result, bson.E{Key: elem.Key, Value: getDocField(doc, exprStr[1:])})
			} else {
				result = append(result, elem)
			}
		}
		return result
	}

	return s.id
}

func (s *GroupStage) evaluateExpression(expr interface{}, doc bson.D) interface{} {
	// 字符串形式的字段引用
	// EN: Field reference in string form.
	if exprStr, ok := expr.(string); ok {
		if len(exprStr) > 0 && exprStr[0] == '$' {
			return getDocField(doc, exprStr[1:])
		}
		return exprStr
	}

	// 数值常量
	// EN: Numeric constants.
	if num, ok := expr.(int); ok {
		return num
	}
	if num, ok := expr.(int32); ok {
		return num
	}
	if num, ok := expr.(int64); ok {
		return num
	}
	if num, ok := expr.(float64); ok {
		return num
	}

	return expr
}

func (s *GroupStage) computeSum(values []interface{}) float64 {
	var sum float64
	for _, v := range values {
		sum += toFloat64(v)
	}
	return sum
}

func (s *GroupStage) computeAvg(values []interface{}) float64 {
	if len(values) == 0 {
		return 0
	}
	return s.computeSum(values) / float64(len(values))
}

func (s *GroupStage) computeMin(values []interface{}) interface{} {
	if len(values) == 0 {
		return nil
	}
	min := values[0]
	for _, v := range values[1:] {
		if compareValues(v, min) < 0 {
			min = v
		}
	}
	return min
}

func (s *GroupStage) computeMax(values []interface{}) interface{} {
	if len(values) == 0 {
		return nil
	}
	max := values[0]
	for _, v := range values[1:] {
		if compareValues(v, max) > 0 {
			max = v
		}
	}
	return max
}

func (s *GroupStage) computeAddToSet(values []interface{}) bson.A {
	seen := make(map[string]bool)
	result := bson.A{}
	for _, v := range values {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

// CountStage $count 阶段
// EN: CountStage implements the $count stage.
type CountStage struct {
	field string
}

func newCountStage(spec interface{}) (*CountStage, error) {
	field, ok := spec.(string)
	if !ok {
		return nil, fmt.Errorf("$count requires a string field name")
	}
	return &CountStage{field: field}, nil
}

func (s *CountStage) Name() string { return "$count" }

func (s *CountStage) Execute(docs []bson.D) ([]bson.D, error) {
	return []bson.D{
		{{Key: s.field, Value: int64(len(docs))}},
	}, nil
}

// UnwindStage $unwind 阶段
// EN: UnwindStage implements the $unwind stage.
type UnwindStage struct {
	path                       string
	preserveNullAndEmptyArrays bool
}

func newUnwindStage(spec interface{}) (*UnwindStage, error) {
	switch v := spec.(type) {
	case string:
		if len(v) > 0 && v[0] == '$' {
			return &UnwindStage{path: v[1:]}, nil
		}
		return nil, fmt.Errorf("$unwind path must start with $")
	case bson.D:
		stage := &UnwindStage{}
		for _, elem := range v {
			switch elem.Key {
			case "path":
				if pathStr, ok := elem.Value.(string); ok && len(pathStr) > 0 && pathStr[0] == '$' {
					stage.path = pathStr[1:]
				}
			case "preserveNullAndEmptyArrays":
				if preserve, ok := elem.Value.(bool); ok {
					stage.preserveNullAndEmptyArrays = preserve
				}
			}
		}
		if stage.path == "" {
			return nil, fmt.Errorf("$unwind requires a path")
		}
		return stage, nil
	default:
		return nil, fmt.Errorf("$unwind requires a string or document")
	}
}

func (s *UnwindStage) Name() string { return "$unwind" }

func (s *UnwindStage) Execute(docs []bson.D) ([]bson.D, error) {
	result := make([]bson.D, 0)

	for _, doc := range docs {
		fieldVal := getDocField(doc, s.path)

		arr, ok := fieldVal.(bson.A)
		if !ok {
			if s.preserveNullAndEmptyArrays {
				result = append(result, doc)
			}
			continue
		}

		if len(arr) == 0 {
			if s.preserveNullAndEmptyArrays {
				result = append(result, doc)
			}
			continue
		}

		for _, elem := range arr {
			newDoc := make(bson.D, 0, len(doc))
			for _, e := range doc {
				if e.Key == s.path {
					newDoc = append(newDoc, bson.E{Key: e.Key, Value: elem})
				} else {
					newDoc = append(newDoc, e)
				}
			}
			result = append(result, newDoc)
		}
	}

	return result, nil
}

// AddFieldsStage $addFields 阶段
// EN: AddFieldsStage implements the $addFields stage.
type AddFieldsStage struct {
	fields bson.D
}

func newAddFieldsStage(spec interface{}) (*AddFieldsStage, error) {
	fields, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$addFields requires a document")
	}
	return &AddFieldsStage{fields: fields}, nil
}

func (s *AddFieldsStage) Name() string { return "$addFields" }

func (s *AddFieldsStage) Execute(docs []bson.D) ([]bson.D, error) {
	result := make([]bson.D, len(docs))

	for i, doc := range docs {
		newDoc := make(bson.D, len(doc))
		copy(newDoc, doc)

		for _, field := range s.fields {
			val := s.evaluateExpression(field.Value, doc)
			setField(&newDoc, field.Key, val)
		}

		result[i] = newDoc
	}

	return result, nil
}

func (s *AddFieldsStage) evaluateExpression(expr interface{}, doc bson.D) interface{} {
	if exprStr, ok := expr.(string); ok {
		if len(exprStr) > 0 && exprStr[0] == '$' {
			return getDocField(doc, exprStr[1:])
		}
		return exprStr
	}
	return expr
}

// toInt64 转换为 int64
// EN: toInt64 converts a numeric value to int64.
func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

// Aggregate 在集合上执行聚合管道
// EN: Aggregate executes an aggregation pipeline on the collection.
func (c *Collection) Aggregate(pipeline []bson.D) ([]bson.D, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 获取所有文档作为初始输入
	// EN: Load all documents as the initial input.
	docs, err := c.findUnlocked(nil)
	if err != nil {
		return nil, err
	}

	// 创建并执行管道（传入 Database 以支持 $lookup 等跨集合操作）
	// EN: Create and execute pipeline (pass Database to support cross-collection stages like $lookup).
	p, err := NewPipelineWithDB(pipeline, c.db)
	if err != nil {
		return nil, err
	}

	return p.Execute(docs)
}

// AggregateResult 聚合结果（带光标）
// EN: AggregateResult is an aggregation result with cursor info.
type AggregateResult struct {
	Cursor CursorInfo
	Ok     int32
	Docs   []bson.D
}

// CursorInfo 光标信息
// EN: CursorInfo describes cursor information.
type CursorInfo struct {
	Id         int64
	Ns         string
	FirstBatch bson.A
}

// SortByFields 按字段排序文档（使用 CompareBSON 实现 MongoDB 标准排序规则）
// EN: SortByFields sorts documents by fields using MongoDB comparison semantics (CompareBSON).
func SortByFields(docs []bson.D, fields bson.D) []bson.D {
	result := make([]bson.D, len(docs))
	copy(result, docs)

	sort.Slice(result, func(i, j int) bool {
		for _, field := range fields {
			key := field.Key
			dir := 1
			if d, ok := field.Value.(int); ok {
				dir = d
			} else if d, ok := field.Value.(int32); ok {
				dir = int(d)
			}

			valI := getDocField(result[i], key)
			valJ := getDocField(result[j], key)

			// 使用 CompareBSON 实现 MongoDB 标准 BSON 类型比较规则
			// EN: Use CompareBSON to follow MongoDB BSON type ordering/comparison rules.
			cmp := CompareBSON(valI, valJ)
			if cmp != 0 {
				if dir < 0 {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return result
}

// UnsetStage $unset 阶段 - 移除指定字段
// EN: UnsetStage implements the $unset stage (removes specified fields).
type UnsetStage struct {
	fields []string
}

func newUnsetStage(spec interface{}) (*UnsetStage, error) {
	stage := &UnsetStage{fields: []string{}}

	switch v := spec.(type) {
	case string:
		stage.fields = []string{v}
	case bson.A:
		for _, item := range v {
			if s, ok := item.(string); ok {
				stage.fields = append(stage.fields, s)
			}
		}
	default:
		return nil, fmt.Errorf("$unset requires a string or array of strings")
	}

	return stage, nil
}

func (s *UnsetStage) Name() string { return "$unset" }

func (s *UnsetStage) Execute(docs []bson.D) ([]bson.D, error) {
	result := make([]bson.D, len(docs))

	for i, doc := range docs {
		newDoc := make(bson.D, 0, len(doc))
		for _, elem := range doc {
			shouldRemove := false
			for _, field := range s.fields {
				if elem.Key == field {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newDoc = append(newDoc, elem)
			}
		}
		result[i] = newDoc
	}

	return result, nil
}

// ReplaceRootStage $replaceRoot 阶段 - 替换根文档
// EN: ReplaceRootStage implements the $replaceRoot stage (replaces the root document).
type ReplaceRootStage struct {
	newRoot interface{}
}

func newReplaceRootStage(spec interface{}) (*ReplaceRootStage, error) {
	specDoc, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$replaceRoot requires a document")
	}

	var newRoot interface{}
	for _, elem := range specDoc {
		if elem.Key == "newRoot" {
			newRoot = elem.Value
			break
		}
	}

	if newRoot == nil {
		return nil, fmt.Errorf("$replaceRoot requires newRoot field")
	}

	return &ReplaceRootStage{newRoot: newRoot}, nil
}

func (s *ReplaceRootStage) Name() string { return "$replaceRoot" }

func (s *ReplaceRootStage) Execute(docs []bson.D) ([]bson.D, error) {
	result := make([]bson.D, 0, len(docs))

	for _, doc := range docs {
		newDoc := s.evaluateNewRoot(doc)
		if newDoc != nil {
			result = append(result, newDoc)
		}
	}

	return result, nil
}

func (s *ReplaceRootStage) evaluateNewRoot(doc bson.D) bson.D {
	switch v := s.newRoot.(type) {
	case string:
		// 字段引用 "$field"
		// EN: Field reference: "$field".
		if len(v) > 0 && v[0] == '$' {
			val := getDocField(doc, v[1:])
			if resultDoc, ok := val.(bson.D); ok {
				return resultDoc
			}
		}
	case bson.D:
		// 文档表达式
		// EN: Document expression.
		resultDoc := bson.D{}
		for _, elem := range v {
			val := s.evaluateExpr(elem.Value, doc)
			resultDoc = append(resultDoc, bson.E{Key: elem.Key, Value: val})
		}
		return resultDoc
	}
	return nil
}

func (s *ReplaceRootStage) evaluateExpr(expr interface{}, doc bson.D) interface{} {
	if exprStr, ok := expr.(string); ok {
		if len(exprStr) > 0 && exprStr[0] == '$' {
			return getDocField(doc, exprStr[1:])
		}
		return exprStr
	}
	return expr
}

// LookupStage $lookup 阶段 - 左连接其他集合
// EN: LookupStage implements the $lookup stage (left outer join).
type LookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
	db           *Database
}

func newLookupStage(spec interface{}, db *Database) (*LookupStage, error) {
	specDoc, ok := spec.(bson.D)
	if !ok {
		return nil, fmt.Errorf("$lookup requires a document")
	}

	stage := &LookupStage{db: db}

	for _, elem := range specDoc {
		switch elem.Key {
		case "from":
			if s, ok := elem.Value.(string); ok {
				stage.from = s
			}
		case "localField":
			if s, ok := elem.Value.(string); ok {
				stage.localField = s
			}
		case "foreignField":
			if s, ok := elem.Value.(string); ok {
				stage.foreignField = s
			}
		case "as":
			if s, ok := elem.Value.(string); ok {
				stage.as = s
			}
		}
	}

	if stage.from == "" || stage.as == "" {
		return nil, fmt.Errorf("$lookup requires 'from' and 'as' fields")
	}

	return stage, nil
}

func (s *LookupStage) Name() string { return "$lookup" }

func (s *LookupStage) Execute(docs []bson.D) ([]bson.D, error) {
	if s.db == nil {
		return nil, fmt.Errorf("$lookup requires database context")
	}

	// 获取目标集合
	// EN: Get foreign collection.
	foreignColl := s.db.GetCollection(s.from)
	if foreignColl == nil {
		// 集合不存在，返回空数组
		// EN: Collection does not exist; return empty arrays.
		result := make([]bson.D, len(docs))
		for i, doc := range docs {
			newDoc := make(bson.D, len(doc)+1)
			copy(newDoc, doc)
			newDoc = append(newDoc, bson.E{Key: s.as, Value: bson.A{}})
			result[i] = newDoc
		}
		return result, nil
	}

	// 获取外部集合的所有文档
	// EN: Load all documents from the foreign collection.
	foreignDocs, err := foreignColl.Find(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign collection: %w", err)
	}

	result := make([]bson.D, len(docs))

	for i, doc := range docs {
		// 获取本地字段值
		// EN: Get local field value.
		localVal := getDocField(doc, s.localField)

		// 查找匹配的外部文档
		// EN: Find matching foreign documents.
		matches := bson.A{}
		for _, foreignDoc := range foreignDocs {
			foreignVal := getDocField(foreignDoc, s.foreignField)
			if valuesEqual(localVal, foreignVal) {
				matches = append(matches, foreignDoc)
			}
		}

		// 创建新文档，添加匹配结果
		// EN: Create a new document with the matches appended.
		newDoc := make(bson.D, 0, len(doc)+1)
		for _, elem := range doc {
			newDoc = append(newDoc, elem)
		}
		newDoc = append(newDoc, bson.E{Key: s.as, Value: matches})
		result[i] = newDoc
	}

	return result, nil
}

// valuesEqual 已在 collection.go 中定义
// EN: valuesEqual is defined in collection.go.

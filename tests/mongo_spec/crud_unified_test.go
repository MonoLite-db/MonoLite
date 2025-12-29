// Created by Yanjunhui

package mongo_spec_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/monolite/monodb/internal/testkit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 说明：
// - MongoDB specifications（Unified Test Format）覆盖面非常广；
// - MonoLite 目前尚未完整支持所有操作/选项/事件断言；
// - 因此本 runner 采用“先小步跑通，再逐步扩展”的策略：
//   - 默认跳过（避免影响日常 go test ./...）；
//   - 仅运行我们明确支持的 operation + 断言；
//   - 对包含 expectEvents / observeEvents / 复杂选项的用例直接 Skip，并给出原因。

const (
	envRunSpecs  = "MONOLITE_RUN_MONGO_SPECS"      // 设为 1 才运行
	envSpecsGlob = "MONOLITE_MONGO_SPECS_GLOB"     // 可选：指定 glob（相对 unified 目录），例如 "find*.json"
	envSpecsFile = "MONOLITE_MONGO_SPECS_FILENAME" // 可选：指定单个文件名，例如 "find.json"
)

func TestMongoDBSpecifications_UnifiedCRUD(t *testing.T) {
	if os.Getenv(envRunSpecs) != "1" {
		t.Skipf("MongoDB specifications tests disabled (set %s=1 to enable)", envRunSpecs)
	}

	root, err := repoRoot()
	if err != nil {
		t.Fatalf("failed to locate repo root: %v", err)
	}
	unifiedDir := filepath.Join(root, "third_party", "mongodb", "specifications", "source", "crud", "tests", "unified")

	files, err := selectSpecFiles(unifiedDir)
	if err != nil {
		t.Fatalf("select spec files failed: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no spec files selected (dir=%s)", unifiedDir)
	}

	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			spec, err := loadUnifiedSpecJSON(file)
			if err != nil {
				t.Fatalf("load spec failed: %v", err)
			}

			entities := buildEntities(spec.CreateEntities)

			for _, tc := range spec.Tests {
				tc := tc
				t.Run(tc.Description, func(t *testing.T) {
					// Unified CRUD 规范文件里的多个 test case 通常都假设从相同的 initialData 起步。
					// MonoLite 目前不支持 dropDatabase，因此这里用“每个 test case 独立 server 实例”确保隔离。
					h := testkit.StartMonoLite(t)
					defer h.Close()

					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					client, err := mongo.Connect(ctx, options.Client().
						ApplyURI("mongodb://"+h.Addr).
						SetServerSelectionTimeout(5*time.Second),
					)
					if err != nil {
						t.Fatalf("mongo.Connect failed: %v", err)
					}
					defer client.Disconnect(ctx)

					// 初始化数据（按 spec 的 database/collection 名称直接操作）
					if err := applyInitialData(ctx, client, spec.InitialData); err != nil {
						t.Fatalf("applyInitialData failed: %v", err)
					}

					if len(tc.ExpectEvents) > 0 {
						t.Skip("test includes expectEvents (not supported yet)")
					}

					for _, op := range tc.Operations {
						if op.ExpectError != nil {
							t.Skip("expectError assertions not supported yet")
						}

						res, err := executeOperation(ctx, client, entities, op)
						if err != nil {
							t.Fatalf("operation %q failed: %v", op.Name, err)
						}

						if op.ExpectResult != nil {
							if err := assertExpectResult(op.Name, res, op.ExpectResult); err != nil {
								t.Fatalf("expectResult mismatch for %q: %v", op.Name, err)
							}
						}
					}

					// outcome 断言（很多用例没有 outcome；这里先支持最常见的 collection data 断言）
					if len(tc.Outcome) > 0 {
						if err := assertOutcomeCollections(ctx, client, tc.Outcome); err != nil {
							t.Fatalf("outcome mismatch: %v", err)
						}
					}
				})
			}
		})
	}
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s upwards", wd)
		}
		dir = parent
	}
}

func selectSpecFiles(unifiedDir string) ([]string, error) {
	if name := os.Getenv(envSpecsFile); name != "" {
		p := filepath.Join(unifiedDir, name)
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("spec file not found: %s: %w", p, err)
		}
		return []string{p}, nil
	}

	pattern := os.Getenv(envSpecsGlob)
	if pattern == "" {
		// 默认只跑最基础的一小批（避免一次性把工程拖进“海量跳过/长耗时”）
		pattern = "{find,insertOne,insertMany,deleteOne,deleteMany,distinct}.json"
	}

	matches, err := filepath.Glob(filepath.Join(unifiedDir, pattern))
	if err != nil {
		return nil, err
	}
	// 兜底：如果 pattern 里用了 brace 扩展，filepath.Glob 不支持，会返回空。
	if len(matches) == 0 && strings.Contains(pattern, "{") {
		alts := []string{"find.json", "insertOne.json", "insertMany.json", "deleteOne.json", "deleteMany.json", "distinct.json"}
		for _, a := range alts {
			p := filepath.Join(unifiedDir, a)
			if _, err := os.Stat(p); err == nil {
				matches = append(matches, p)
			}
		}
	}
	return matches, nil
}

// -----------------------------
// Unified spec minimal model
// -----------------------------

type unifiedSpec struct {
	Description    string          `json:"description"`
	SchemaVersion  string          `json:"schemaVersion"`
	CreateEntities []entitySpec    `json:"createEntities"`
	InitialData    []initialData   `json:"initialData"`
	Tests          []unifiedTest   `json:"tests"`
	// 其余字段暂不需要
}

type entitySpec struct {
	Client     *clientEntity     `json:"client,omitempty"`
	Database   *databaseEntity   `json:"database,omitempty"`
	Collection *collectionEntity `json:"collection,omitempty"`
}

type clientEntity struct {
	ID            string   `json:"id"`
	ObserveEvents []string `json:"observeEvents,omitempty"`
}

type databaseEntity struct {
	ID           string `json:"id"`
	Client       string `json:"client"`
	DatabaseName string `json:"databaseName"`
}

type collectionEntity struct {
	ID             string `json:"id"`
	Database       string `json:"database"`
	CollectionName string `json:"collectionName"`
}

type initialData struct {
	DatabaseName   string        `json:"databaseName"`
	CollectionName string        `json:"collectionName"`
	Documents      []interface{} `json:"documents"`
}

type unifiedTest struct {
	Description  string        `json:"description"`
	Operations   []operation   `json:"operations"`
	ExpectEvents []interface{} `json:"expectEvents,omitempty"`
	Outcome      []outcomeColl `json:"outcome,omitempty"`
}

type operation struct {
	Name         string                 `json:"name"`
	Object       string                 `json:"object"`
	Arguments    map[string]interface{} `json:"arguments,omitempty"`
	ExpectResult interface{}            `json:"expectResult,omitempty"`
	ExpectError  interface{}            `json:"expectError,omitempty"`
}

type outcomeColl struct {
	DatabaseName   string        `json:"databaseName"`
	CollectionName string        `json:"collectionName"`
	Documents      []interface{} `json:"documents"`
}

type entityBindings struct {
	dbByID        map[string]string // database id -> name
	collectionByID map[string]struct {
		dbName   string
		collName string
	}
}

func buildEntities(specs []entitySpec) entityBindings {
	dbByID := map[string]string{}
	collByID := map[string]struct {
		dbName   string
		collName string
	}{}

	for _, e := range specs {
		if e.Database != nil {
			dbByID[e.Database.ID] = e.Database.DatabaseName
		}
	}
	for _, e := range specs {
		if e.Collection != nil {
			dbName := dbByID[e.Collection.Database]
			collByID[e.Collection.ID] = struct {
				dbName   string
				collName string
			}{dbName: dbName, collName: e.Collection.CollectionName}
		}
	}
	return entityBindings{dbByID: dbByID, collectionByID: collByID}
}

func loadUnifiedSpecJSON(path string) (*unifiedSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec unifiedSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// -----------------------------
// Execution + assertions (subset)
// -----------------------------

func applyInitialData(ctx context.Context, client *mongo.Client, init []initialData) error {
	for _, item := range init {
		db := client.Database(item.DatabaseName)
		coll := db.Collection(item.CollectionName)

		// runner 的隔离单位是“每个 spec 文件一套新 server + 新数据文件”，因此通常无需清库。
		// 为避免 MonoLite 尚未实现 dropDatabase 导致失败，这里不再调用 db.Drop()。
		// 如果未来改为“复用同一 server 跑多个文件”，可以在这里做更严格的清理策略。

		var docs []interface{}
		for _, d := range item.Documents {
			doc, err := toBsonD(d)
			if err != nil {
				return fmt.Errorf("decode initialData doc: %w", err)
			}
			docs = append(docs, doc)
		}
		if len(docs) > 0 {
			if _, err := coll.InsertMany(ctx, docs); err != nil {
				return fmt.Errorf("seed InsertMany failed: %w", err)
			}
		}
	}
	return nil
}

func executeOperation(ctx context.Context, client *mongo.Client, entities entityBindings, op operation) (interface{}, error) {
	target, ok := entities.collectionByID[op.Object]
	if !ok {
		return nil, fmt.Errorf("unknown object %q (only collection objects supported)", op.Object)
	}
	coll := client.Database(target.dbName).Collection(target.collName)

	switch op.Name {
	case "find":
		return execFind(ctx, coll, op.Arguments)
	case "insertOne":
		return execInsertOne(ctx, coll, op.Arguments)
	case "insertMany":
		return execInsertMany(ctx, coll, op.Arguments)
	case "deleteOne":
		n, err := execDelete(ctx, coll, op.Arguments, true)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"deletedCount": n}, nil
	case "deleteMany":
		n, err := execDelete(ctx, coll, op.Arguments, false)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"deletedCount": n}, nil
	case "distinct":
		return execDistinct(ctx, coll, op.Arguments)
	default:
		return nil, fmt.Errorf("operation %q not supported by runner yet", op.Name)
	}
}

func execFind(ctx context.Context, coll *mongo.Collection, args map[string]interface{}) ([]bson.D, error) {
	filter := bson.D{}
	if v, ok := args["filter"]; ok {
		d, err := toBsonD(v)
		if err != nil {
			return nil, err
		}
		filter = d
	}

	findOpts := options.Find()
	if v, ok := args["sort"]; ok {
		d, err := toBsonD(v)
		if err != nil {
			return nil, err
		}
		findOpts.SetSort(d)
	}
	if v, ok := args["skip"]; ok {
		n, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		findOpts.SetSkip(n)
	}
	if v, ok := args["limit"]; ok {
		n, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		findOpts.SetLimit(n)
	}
	// batchSize 作为 driver 行为/事件断言的一部分，MonoLite runner 先不强行对齐，只保证结果集正确

	cur, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []bson.D
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func execInsertOne(ctx context.Context, coll *mongo.Collection, args map[string]interface{}) (interface{}, error) {
	docAny, ok := args["document"]
	if !ok {
		return nil, fmt.Errorf("insertOne missing document")
	}
	doc, err := toBsonD(docAny)
	if err != nil {
		return nil, err
	}
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"insertedId": res.InsertedID}, nil
}

func execInsertMany(ctx context.Context, coll *mongo.Collection, args map[string]interface{}) (interface{}, error) {
	raw, ok := args["documents"]
	if !ok {
		return nil, fmt.Errorf("insertMany missing documents")
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("insertMany documents has type %T", raw)
	}
	docs := make([]interface{}, 0, len(arr))
	for _, d := range arr {
		doc, err := toBsonD(d)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	res, err := coll.InsertMany(ctx, docs)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"insertedIds": res.InsertedIDs}, nil
}

func execDelete(ctx context.Context, coll *mongo.Collection, args map[string]interface{}, one bool) (int64, error) {
	filter := bson.D{}
	if v, ok := args["filter"]; ok {
		d, err := toBsonD(v)
		if err != nil {
			return 0, err
		}
		filter = d
	}
	if one {
		res, err := coll.DeleteOne(ctx, filter)
		if err != nil {
			return 0, err
		}
		return res.DeletedCount, nil
	}
	res, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

func execDistinct(ctx context.Context, coll *mongo.Collection, args map[string]interface{}) ([]interface{}, error) {
	fieldRaw, ok := args["fieldName"]
	if !ok {
		return nil, fmt.Errorf("distinct missing fieldName")
	}
	field, ok := fieldRaw.(string)
	if !ok {
		return nil, fmt.Errorf("distinct fieldName has type %T", fieldRaw)
	}

	filter := bson.D{}
	if v, ok := args["filter"]; ok {
		d, err := toBsonD(v)
		if err != nil {
			return nil, err
		}
		filter = d
	}
	return coll.Distinct(ctx, field, filter)
}

func assertExpectResult(opName string, got interface{}, expect interface{}) error {
	// 目前只对部分类型做精确断言；其余暂不支持
	switch opName {
	case "find":
		gotDocs, ok := got.([]bson.D)
		if !ok {
			return fmt.Errorf("find result has type %T", got)
		}
		expArr, ok := expect.([]interface{})
		if !ok {
			return fmt.Errorf("find expectResult has type %T", expect)
		}
		var expDocs []interface{}
		for _, e := range expArr {
			d, err := toBsonD(e)
			if err != nil {
				return err
			}
			expDocs = append(expDocs, d)
		}
		return assertDocsEqual(gotDocs, expDocs)

	case "distinct":
		gotArr, ok := got.([]interface{})
		if !ok {
			return fmt.Errorf("distinct result has type %T", got)
		}
		expArr, ok := expect.([]interface{})
		if !ok {
			return fmt.Errorf("distinct expectResult has type %T", expect)
		}
		return assertValuesEqual(gotArr, expArr)

	case "deleteOne", "deleteMany":
		return assertExpectMap(got, expect)

	case "insertOne", "insertMany":
		return assertExpectMap(got, expect)

	default:
		return fmt.Errorf("expectResult assertions for %q not supported yet", opName)
	}
}

func assertOutcomeCollections(ctx context.Context, client *mongo.Client, outcomes []outcomeColl) error {
	for _, oc := range outcomes {
		coll := client.Database(oc.DatabaseName).Collection(oc.CollectionName)
		cur, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return err
		}
		var got []bson.D
		if err := cur.All(ctx, &got); err != nil {
			_ = cur.Close(ctx)
			return err
		}
		_ = cur.Close(ctx)

		var exp []interface{}
		for _, d := range oc.Documents {
			doc, err := toBsonD(d)
			if err != nil {
				return err
			}
			exp = append(exp, doc)
		}
		if err := assertDocsEqual(got, exp); err != nil {
			return fmt.Errorf("outcome mismatch for %s.%s: %w", oc.DatabaseName, oc.CollectionName, err)
		}
	}
	return nil
}

func assertExpectMap(got interface{}, expect interface{}) error {
	gotMap, ok := got.(map[string]interface{})
	if !ok {
		return fmt.Errorf("result has type %T (want map)", got)
	}
	expMap, ok := expect.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expectResult has type %T (want map)", expect)
	}

	// 支持最常见的 $$unsetOrMatches 形态（用于 insertedId/insertedIds 等返回值）
	if inner, ok := unwrapUnsetOrMatches(expMap); ok {
		expMap, ok = inner.(map[string]interface{})
		if !ok {
			return fmt.Errorf("$$unsetOrMatches inner has type %T", inner)
		}
	}

	for k, expVal := range expMap {
		if strings.HasPrefix(k, "$$") {
			return fmt.Errorf("unsupported expectResult operator %q", k)
		}
		gotVal, exists := gotMap[k]
		expVal = unwrapUnsetOrMatchesRecursive(expVal)
		if !exists {
			return fmt.Errorf("missing field %q in result", k)
		}

		// specs 里 insertedIds 通常是一个“索引->id”的文档（JSON object），而 driver 返回 []interface{}。
		// 这里做一个最小兼容：把 []interface{} 转成 map[string]interface{} 再比较。
		if k == "insertedIds" {
			if gotArr, ok := gotVal.([]interface{}); ok {
				if _, ok := expVal.(map[string]interface{}); ok {
					gotObj := make(map[string]interface{}, len(gotArr))
					for i := range gotArr {
						gotObj[fmt.Sprintf("%d", i)] = gotArr[i]
					}
					gotVal = gotObj
				}
			}
		}

		if !looseEqual(gotVal, expVal) {
			return fmt.Errorf("field %q mismatch: got=%T:%v want=%T:%v", k, gotVal, gotVal, expVal, expVal)
		}
	}
	return nil
}

func unwrapUnsetOrMatches(exp map[string]interface{}) (interface{}, bool) {
	v, ok := exp["$$unsetOrMatches"]
	return v, ok
}

func unwrapUnsetOrMatchesRecursive(v interface{}) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	if inner, ok := m["$$unsetOrMatches"]; ok {
		return unwrapUnsetOrMatchesRecursive(inner)
	}
	return v
}

func looseEqual(a, b interface{}) bool {
	// 数字尽量按 int64 比较，减少 int32/int64/float64 差异导致的误报
	if ai, ok := asInt64(a); ok {
		if bi, ok := asInt64(b); ok {
			return ai == bi
		}
	}
	return normalizeForCompare(a) == normalizeForCompare(b)
}

func assertDocsEqual(got []bson.D, expect []interface{}) error {
	gotIface := make([]interface{}, 0, len(got))
	for _, d := range got {
		gotIface = append(gotIface, d)
	}

	gotCanon, err := testkit.CanonicalizeDocs(gotIface)
	if err != nil {
		return err
	}
	expCanon, err := testkit.CanonicalizeDocs(expect)
	if err != nil {
		return err
	}
	if len(gotCanon) != len(expCanon) {
		return fmt.Errorf("doc count mismatch: got=%d want=%d", len(gotCanon), len(expCanon))
	}
	for i := range gotCanon {
		if !reflect.DeepEqual(gotCanon[i], expCanon[i]) {
			return fmt.Errorf("doc mismatch at %d:\n  got=%v\n  want=%v", i, gotCanon[i], expCanon[i])
		}
	}
	return nil
}

func assertValuesEqual(got []interface{}, expect []interface{}) error {
	if len(got) != len(expect) {
		return fmt.Errorf("value count mismatch: got=%d want=%d (got=%v expect=%v)", len(got), len(expect), got, expect)
	}
	// distinct 的结果在很多用例里顺序不敏感；但为避免误报，这里先按字符串化排序再对比
	gs := make([]string, len(got))
	es := make([]string, len(expect))
	for i := range got {
		gs[i] = normalizeForCompare(got[i])
		es[i] = normalizeForCompare(expect[i])
	}
	sortStrings(gs)
	sortStrings(es)
	for i := range gs {
		if gs[i] != es[i] {
			return fmt.Errorf("distinct values mismatch: got=%v expect=%v", got, expect)
		}
	}
	return nil
}

func normalizeForCompare(v interface{}) string {
	if n, ok := asInt64(v); ok {
		return fmt.Sprintf("int64:%d", n)
	}
	return fmt.Sprintf("%T:%v", v, v)
}

func sortStrings(a []string) {
	for i := 0; i < len(a); i++ {
		for j := i + 1; j < len(a); j++ {
			if a[j] < a[i] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

func toBsonD(v interface{}) (bson.D, error) {
	// 通过 JSON 重新序列化，再用 Extended JSON 解码为 bson.D
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out bson.D
	// 这里使用 relaxed=false，尽量保持规范测试的类型语义
	if err := bson.UnmarshalExtJSON(data, false, &out); err != nil {
		// 部分测试使用 relaxed 语义；做一次兜底
		if err2 := bson.UnmarshalExtJSON(data, true, &out); err2 != nil {
			return nil, err
		}
	}
	return out, nil
}

func toInt64(v interface{}) (int64, error) {
	if n, ok := asInt64(v); ok {
		return n, nil
	}
	return 0, fmt.Errorf("cannot convert %T to int64", v)
}

func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}



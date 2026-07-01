# FTS Ngram DDL 路径实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 打通 `NGRAM_V1`、`min_gram` 和 `max_gram` 从 TiDB CSE DDL 到 CSE metadata，再到 TiFlash schema JSON 兼容解析的第一阶段链路。

**架构：** TiDB CSE 负责 SQL 解析、DDL 校验、`SHOW CREATE TABLE` 还原和 TiDB schema JSON 输出。CSE 负责反序列化 TiDB schema、扩展 `FullTextIndexDef` protobuf，并把 gram 参数写入 schema file/shard metadata。TiFlash 只做 schema JSON 解析和回写兼容，不接入废弃的 DeltaMerge FTS writer/reader。

**技术栈：** Go parser/yacc、TiDB DDL metadata、Rust serde/protobuf、C++ Poco JSON、TiFlash schema sync。

---

## 文件结构

### `/DATA/disk1/jinhelin/tidb-cse`

- 修改：`pkg/parser/model/index_full_text.go`  
  定义 `NGRAM_V1` parser 类型、SQL 名称映射，以及 `FullTextIndexInfo` 的 gram 字段。
- 修改：`pkg/parser/ast/ddl.go`  
  扩展 `ast.IndexOption`，保存 fulltext parser 参数，并更新 AST restore。
- 修改：`pkg/parser/parser.y`  
  扩展 `WITH PARSER` 语法，支持括号参数列表。
- 生成：`pkg/parser/parser.go`  
  由 `parser.y` 生成，不手写。
- 修改：`pkg/parser/parser_test.go`  
  增加 parser restore 覆盖，确认 Ngram 参数可以被解析并还原。
- 修改：`pkg/parser/ast/ddl_test.go`  
  增加 `IndexOption` restore 覆盖。
- 检查：`pkg/planner/core/preprocess.go`  
  该文件通过 `GetFullTextParserTypeBySQLName` 校验 parser。任务 1 扩展映射后，它应自动接受 `NGRAM`。
- 修改：`pkg/ddl/index.go`  
  增加 fulltext gram 参数校验和 `FullTextIndexInfo` 构造。
- 修改：`pkg/executor/show.go`  
  让 `SHOW CREATE TABLE` 输出 Ngram parser 参数。

### `/DATA/disk1/jinhelin/cloud-storage-engine`

- 修改：`components/schema/src/schema.rs`  
  TiDB schema 反序列化结构增加 `min_gram/max_gram`。
- 修改：`components/kvenginepb/src/fts.proto`  
  `FullTextIndexDef` 增加 protobuf 字段。
- 生成：`components/kvenginepb/src/fts.rs`  
  由 `cargo build -p kvenginepb` 生成，不手写。
- 生成：`components/kvenginepb/src/changeset.rs`  
  由 `cargo build -p kvenginepb` 生成，不手写。
- 修改：`components/cloud_worker/src/schema_manager.rs`  
  从 TiDB schema 转换 fulltext index 时复制并校验 gram 参数。

### `/DATA/disk1/jinhelin/tiflash-1`

- 修改：`dbms/src/TiDB/Schema/FullTextIndex.h`  
  `FullTextIndexDefinition` 增加可选 gram 字段，并更新 formatter。
- 修改：`dbms/src/TiDB/Schema/TiDB.cpp`  
  schema JSON 解析和序列化支持 `NGRAM_V1/min_gram/max_gram`，使用 TiFlash schema 层 parser 白名单。
- 不修改：`dbms/src/Storages/DeltaMerge/Index/FullTextIndex/*`  
  DeltaMerge FTS 已不用于该链路。
- 不修改：`contrib/cloud-storage-engine/*`  
  除非确认当前 TiFlash 编译依赖该内置 CSE 目录；若依赖，只同步 metadata schema 相关改动。

---

## 任务 1：TiDB CSE parser model 扩展

**文件：**
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/model/index_full_text.go`

- [ ] **步骤 1：扩展 parser 类型常量**

在 `FullTextParserTypeMultilingualV1` 后新增：

```go
// FullTextParserTypeNgramV1 is a case-insensitive Ngram parser.
// The value matches the parser type stored in CSE metadata.
FullTextParserTypeNgramV1 FullTextParserType = "NGRAM_V1"
```

- [ ] **步骤 2：扩展 SQL 名称映射**

在 `SQLName()` 中加入：

```go
case FullTextParserTypeNgramV1:
	return "NGRAM"
```

在 `GetFullTextParserTypeBySQLName` 中加入：

```go
case "NGRAM":
	return FullTextParserTypeNgramV1
```

- [ ] **步骤 3：扩展 `FullTextIndexInfo`**

把结构体改为：

```go
// FullTextIndexInfo is the information of FULLTEXT index of a column.
type FullTextIndexInfo struct {
	ParserType FullTextParserType `json:"parser_type"`
	MinGram    *uint64            `json:"min_gram,omitempty"`
	MaxGram    *uint64            `json:"max_gram,omitempty"`
}
```

- [ ] **步骤 4：格式化并检查当前文件**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse
gofmt -w pkg/parser/model/index_full_text.go
git diff -- pkg/parser/model/index_full_text.go
```

预期：diff 只包含 `NGRAM_V1` 和 gram 字段。

- [ ] **步骤 5：提交 TiDB CSE parser model**

```bash
cd /DATA/disk1/jinhelin/tidb-cse
git add pkg/parser/model/index_full_text.go
git commit -m "fts: add ngram parser metadata"
```

---

## 任务 2：TiDB CSE AST 和 parser 语法

**文件：**
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/ast/ddl.go`
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/parser.y`
- 生成：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/parser.go`
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/parser_test.go`
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/parser/ast/ddl_test.go`

- [ ] **步骤 1：在 AST 中增加 parser 参数结构**

在 `IndexOption` 附近新增：

```go
// FullTextParserParam stores a parameter from WITH PARSER ... (...).
type FullTextParserParam struct {
	Name  model.CIStr
	Value uint64
}
```

给 `IndexOption` 增加字段：

```go
FullTextParserParams []*FullTextParserParam
```

- [ ] **步骤 2：更新 `IndexOption.IsEmpty`**

把 parser 参数纳入非空判断：

```go
len(n.FullTextParserParams) > 0 ||
```

放在 `len(n.ParserName.O) > 0 ||` 后面。

- [ ] **步骤 3：更新 `IndexOption.Restore`**

在现有 `WITH PARSER` 输出之后追加参数 restore：

```go
if len(n.FullTextParserParams) > 0 {
	ctx.WritePlain(" (")
	for i, param := range n.FullTextParserParams {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord(strings.ToUpper(param.Name.O))
		ctx.WritePlainf(" = %d", param.Value)
	}
	ctx.WritePlain(")")
}
```

同文件如果还没有 `strings` import，则加入：

```go
import "strings"
```

- [ ] **步骤 4：扩展 `parser.y` 的 `IndexOption`**

把现有分支：

```go
|	"WITH" "PARSER" Identifier
	{
		$$ = &ast.IndexOption{
			ParserName: model.NewCIStr($3),
		}
	}
```

改成：

```go
|	"WITH" "PARSER" Identifier FullTextParserParamListOpt
	{
		$$ = &ast.IndexOption{
			ParserName:            model.NewCIStr($3),
			FullTextParserParams: $4.([]*ast.FullTextParserParam),
		}
	}
```

如果 gofmt 对齐字段，保留 gofmt 结果。

- [ ] **步骤 5：新增 parser 参数 grammar**

在 `IndexOption` grammar 附近新增：

```go
FullTextParserParamListOpt:
	{
		$$ = []*ast.FullTextParserParam{}
	}
|	'(' FullTextParserParamList ')'
	{
		$$ = $2
	}

FullTextParserParamList:
	FullTextParserParam
	{
		$$ = []*ast.FullTextParserParam{$1.(*ast.FullTextParserParam)}
	}
|	FullTextParserParamList ',' FullTextParserParam
	{
		$$ = append($1.([]*ast.FullTextParserParam), $3.(*ast.FullTextParserParam))
	}

FullTextParserParam:
	Identifier EqOpt LengthNum
	{
		$$ = &ast.FullTextParserParam{
			Name:  model.NewCIStr($1),
			Value: $3.(uint64),
		}
	}
```

使用 `Identifier`，不要新增 `MIN_GRAM` 或 `MAX_GRAM` 保留关键字。

- [ ] **步骤 6：更新 parser restore 测试**

在 `pkg/parser/parser_test.go` 的 create fulltext index cases 附近加入：

```go
{"CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ngram (MIN_GRAM = 2, MAX_GRAM = 3)", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ngram` (MIN_GRAM = 2, MAX_GRAM = 3)"},
{"CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ngram (MAX_GRAM = 3, MIN_GRAM = 2)", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ngram` (MAX_GRAM = 3, MIN_GRAM = 2)"},
```

这里验证 parser 能保留参数顺序。DDL 层再负责 canonical 校验和 `SHOW CREATE` 输出。

- [ ] **步骤 7：更新 AST option restore 测试**

在 `pkg/parser/ast/ddl_test.go` 的 `TestDDLIndexOption` 中加入：

```go
{"with parser ngram (min_gram = 2, max_gram = 3)", "WITH PARSER `ngram` (MIN_GRAM = 2, MAX_GRAM = 3)"},
```

- [ ] **步骤 8：生成 parser 并格式化**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse/pkg/parser
make parser
make fmt
```

预期：`parser.go` 生成成功；`make fmt` 退出码为 `0`。

- [ ] **步骤 9：检查 TiDB CSE parser diff**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse
git diff -- pkg/parser/ast/ddl.go pkg/parser/parser.y pkg/parser/parser.go pkg/parser/parser_test.go pkg/parser/ast/ddl_test.go
```

预期：diff 只包含 fulltext parser 参数解析、restore 和测试。

- [ ] **步骤 10：提交 TiDB CSE parser 语法**

```bash
cd /DATA/disk1/jinhelin/tidb-cse
git add pkg/parser/ast/ddl.go pkg/parser/parser.y pkg/parser/parser.go pkg/parser/parser_test.go pkg/parser/ast/ddl_test.go
git commit -m "parser: support fulltext ngram parser options"
```

---

## 任务 3：TiDB CSE DDL 校验与 `SHOW CREATE TABLE`

**文件：**
- 检查：`/DATA/disk1/jinhelin/tidb-cse/pkg/planner/core/preprocess.go`
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/ddl/index.go`
- 修改：`/DATA/disk1/jinhelin/tidb-cse/pkg/executor/show.go`

- [ ] **步骤 1：确认 preprocess 通过 parser 映射接受 `NGRAM`**

检查 `pkg/planner/core/preprocess.go` 的 fulltext parser 校验使用：

```go
pmodel.GetFullTextParserTypeBySQLName(indexOptions.ParserName.L)
```

任务 1 加入 `NGRAM` 后，这里无需修改。若当前代码已经与上述片段一致，本步骤只记录确认结果，不改文件。

- [ ] **步骤 2：在 DDL 层定义 gram 上限**

在 `pkg/ddl/index.go` 的 fulltext 相关函数附近加入：

```go
const maxFullTextNgramGram = 8
```

- [ ] **步骤 3：添加 parser 参数提取函数**

在 `buildFullTextInfoWithCheck` 附近新增：

```go
func extractFullTextGramParams(indexOption *ast.IndexOption) (minGram *uint64, maxGram *uint64, err error) {
	if indexOption == nil {
		return nil, nil, nil
	}
	for _, param := range indexOption.FullTextParserParams {
		switch param.Name.L {
		case "min_gram":
			if minGram != nil {
				return nil, nil, fmt.Errorf("duplicate fulltext parser parameter MIN_GRAM")
			}
			value := param.Value
			minGram = &value
		case "max_gram":
			if maxGram != nil {
				return nil, nil, fmt.Errorf("duplicate fulltext parser parameter MAX_GRAM")
			}
			value := param.Value
			maxGram = &value
		default:
			return nil, nil, fmt.Errorf("unsupported fulltext parser parameter %s", param.Name.O)
		}
	}
	return minGram, maxGram, nil
}
```

- [ ] **步骤 4：添加 Ngram 校验函数**

继续在 `pkg/ddl/index.go` 中新增：

```go
func validateFullTextParserParams(parser pmodel.FullTextParserType, minGram, maxGram *uint64) error {
	if parser != pmodel.FullTextParserTypeNgramV1 {
		if minGram != nil || maxGram != nil {
			return fmt.Errorf("fulltext parser %s does not support gram parameters", parser.SQLName())
		}
		return nil
	}
	if minGram == nil || maxGram == nil {
		return fmt.Errorf("fulltext parser NGRAM requires MIN_GRAM and MAX_GRAM")
	}
	if *minGram < 1 {
		return fmt.Errorf("MIN_GRAM must be greater than or equal to 1")
	}
	if *maxGram < *minGram {
		return fmt.Errorf("MAX_GRAM must be greater than or equal to MIN_GRAM")
	}
	if *maxGram > maxFullTextNgramGram {
		return fmt.Errorf("MAX_GRAM must be less than or equal to %d", maxFullTextNgramGram)
	}
	return nil
}
```

- [ ] **步骤 5：接入 `buildFullTextInfoWithCheck`**

在确定 `parser` 后、返回 `FullTextIndexInfo` 前加入：

```go
minGram, maxGram, err := extractFullTextGramParams(indexOption)
if err != nil {
	return nil, err
}
if err := validateFullTextParserParams(parser, minGram, maxGram); err != nil {
	return nil, err
}
```

返回值改为：

```go
return &pmodel.FullTextIndexInfo{
	ParserType: parser,
	MinGram:    minGram,
	MaxGram:    maxGram,
}, nil
```

- [ ] **步骤 6：更新 `SHOW CREATE TABLE` 输出**

在 `pkg/executor/show.go` 现有 fulltext parser 输出处，把：

```go
fmt.Fprintf(buf, " WITH PARSER %s", idxInfo.FullTextInfo.ParserType.SQLName())
```

改为：

```go
fmt.Fprintf(buf, " WITH PARSER %s", idxInfo.FullTextInfo.ParserType.SQLName())
if idxInfo.FullTextInfo.ParserType == pmodel.FullTextParserTypeNgramV1 {
	fmt.Fprintf(
		buf,
		" (MIN_GRAM = %d, MAX_GRAM = %d)",
		*idxInfo.FullTextInfo.MinGram,
		*idxInfo.FullTextInfo.MaxGram,
	)
}
```

`SHOW CREATE TABLE` 能到这里时，DDL 已保证 Ngram gram 指针非 `nil`。

- [ ] **步骤 7：格式化 TiDB CSE Go 文件**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse
gofmt -w pkg/planner/core/preprocess.go pkg/ddl/index.go pkg/executor/show.go
```

预期：退出码为 `0`。

- [ ] **步骤 8：运行 TiDB CSE parser/DDL 相关检查**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse
make parser_yacc
make parser_fmt
```

预期：两个命令退出码为 `0`。

- [ ] **步骤 9：提交 TiDB CSE DDL 校验**

```bash
cd /DATA/disk1/jinhelin/tidb-cse
git add pkg/planner/core/preprocess.go pkg/ddl/index.go pkg/executor/show.go
git commit -m "ddl: validate fulltext ngram parser options"
```

---

## 任务 4：CSE schema 和 protobuf metadata

**文件：**
- 修改：`/DATA/disk1/jinhelin/cloud-storage-engine/components/schema/src/schema.rs`
- 修改：`/DATA/disk1/jinhelin/cloud-storage-engine/components/kvenginepb/src/fts.proto`
- 生成：`/DATA/disk1/jinhelin/cloud-storage-engine/components/kvenginepb/src/fts.rs`
- 生成：`/DATA/disk1/jinhelin/cloud-storage-engine/components/kvenginepb/src/changeset.rs`

- [ ] **步骤 1：扩展 TiDB schema 结构**

在 `components/schema/src/schema.rs` 中把 `FullTextIndexInfo` 改为：

```rust
/// Aligned with TiDB: pkg/parser/model/index_full_text.go
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextIndexInfo {
    pub parser_type: String,
    pub min_gram: Option<u32>,
    pub max_gram: Option<u32>,
}
```

`Option` 字段在 serde 中对缺失字段默认反序列化为 `None`。

- [ ] **步骤 2：扩展 protobuf**

在 `components/kvenginepb/src/fts.proto` 中扩展：

```proto
message FullTextIndexDef {
  int64 index_id = 1;
  int64 col_id = 2;

  string parser_type = 10;
  uint32 min_gram = 11;
  uint32 max_gram = 12;
}
```

- [ ] **步骤 3：生成 protobuf Rust 代码**

运行：

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
cargo build -p kvenginepb
```

预期：退出码为 `0`，并更新 `components/kvenginepb/src/fts.rs`。如果构建同时更新 `components/kvenginepb/src/changeset.rs`，也保留该生成结果。

- [ ] **步骤 4：运行 protobuf 一致性检查**

运行：

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
make check-protobuf
```

预期：退出码为 `0`。如果输出 `Error: There are uncommitted protobuf changes`，说明生成文件未纳入工作区；检查并加入对应生成文件。

- [ ] **步骤 5：提交 CSE schema/proto metadata**

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
git add components/schema/src/schema.rs components/kvenginepb/src/fts.proto components/kvenginepb/src/fts.rs components/kvenginepb/src/changeset.rs
git commit -s -m "fts: add ngram metadata fields"
```

---

## 任务 5：CSE schema manager 复制和校验 gram 参数

**文件：**
- 修改：`/DATA/disk1/jinhelin/cloud-storage-engine/components/cloud_worker/src/schema_manager.rs`

- [ ] **步骤 1：定义 CSE 侧 gram 上限**

在 `parse_fulltext_indexes` 附近加入：

```rust
const FULLTEXT_NGRAM_V1: &str = "NGRAM_V1";
const MAX_FULLTEXT_NGRAM_GRAM: u32 = 8;
```

- [ ] **步骤 2：添加 fulltext 参数校验函数**

在 `parse_fulltext_indexes` 前新增：

```rust
fn validate_fulltext_gram_params(parser_type: &str, min_gram: u32, max_gram: u32) -> Result<()> {
    if parser_type != FULLTEXT_NGRAM_V1 {
        if min_gram != 0 || max_gram != 0 {
            return Err(SchemaError(format!(
                "fulltext parser {parser_type} does not support gram parameters"
            )));
        }
        return Ok(());
    }
    if min_gram == 0 || max_gram == 0 {
        return Err(SchemaError(
            "fulltext parser NGRAM_V1 requires min_gram and max_gram".to_string(),
        ));
    }
    if max_gram < min_gram {
        return Err(SchemaError(format!(
            "fulltext parser NGRAM_V1 requires max_gram >= min_gram, min_gram={min_gram}, max_gram={max_gram}"
        )));
    }
    if max_gram > MAX_FULLTEXT_NGRAM_GRAM {
        return Err(SchemaError(format!(
            "fulltext parser NGRAM_V1 requires max_gram <= {MAX_FULLTEXT_NGRAM_GRAM}, got {max_gram}"
        )));
    }
    Ok(())
}
```

- [ ] **步骤 3：让 `parse_fulltext_indexes` 返回 `Result`**

把函数签名改为：

```rust
fn parse_fulltext_indexes(
    ti_cols: &[ColumnInfo],
    idx_infos: Option<&Vec<IndexInfo>>,
) -> Result<Vec<kvenginepb::fts::FullTextIndexDef>> {
```

函数末尾改为：

```rust
    Ok(idxes)
}
```

- [ ] **步骤 4：复制 gram 字段并校验**

把 push 逻辑改为：

```rust
let min_gram = fts_info.min_gram.unwrap_or(0);
let max_gram = fts_info.max_gram.unwrap_or(0);
validate_fulltext_gram_params(&fts_info.parser_type, min_gram, max_gram)?;
idxes.push(kvenginepb::fts::FullTextIndexDef {
    index_id: idx_info.id,
    col_id,
    parser_type: fts_info.parser_type.clone(),
    min_gram,
    max_gram,
    ..Default::default()
});
```

- [ ] **步骤 5：更新调用点传播错误**

在 `table_info_to_schema` 中把：

```rust
let fulltext_indexes = parse_fulltext_indexes(ti_cols, ti.index_info.as_ref());
```

改为：

```rust
let fulltext_indexes = parse_fulltext_indexes(ti_cols, ti.index_info.as_ref())?;
```

- [ ] **步骤 6：格式化 CSE Rust 代码**

运行：

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
cargo fmt -p schema -p cloud_worker -p kvenginepb
```

预期：退出码为 `0`。

- [ ] **步骤 7：运行 CSE Rust 检查**

按顺序运行，不能并发运行 cargo 命令：

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
cargo check -p schema -p kvenginepb -p cloud_worker
make clippy
```

预期：两个命令退出码都为 `0`。如果 `make clippy` 报 warning，按 CSE 仓库规则修复 warning。

- [ ] **步骤 8：提交 CSE schema manager**

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
git add components/cloud_worker/src/schema_manager.rs
git commit -s -m "cloud_worker: pass fulltext ngram metadata"
```

---

## 任务 6：TiFlash schema JSON 兼容 Ngram metadata

**文件：**
- 修改：`/DATA/disk1/jinhelin/tiflash-1/dbms/src/TiDB/Schema/FullTextIndex.h`
- 修改：`/DATA/disk1/jinhelin/tiflash-1/dbms/src/TiDB/Schema/TiDB.cpp`

- [ ] **步骤 1：扩展 `FullTextIndexDefinition`**

在 `FullTextIndex.h` 中加入 include：

```cpp
#include <optional>
```

把结构体改为：

```cpp
struct FullTextIndexDefinition
{
    String parser_type = "INVALID";
    std::optional<UInt32> min_gram;
    std::optional<UInt32> max_gram;
};
```

- [ ] **步骤 2：更新 formatter**

把 formatter 改为 Ngram 带参数输出：

```cpp
if (vi.parser_type == "NGRAM_V1" && vi.min_gram && vi.max_gram)
{
    return fmt::format_to(
        ctx.out(),
        "PARSER_{}_MIN_{}_MAX_{}",
        vi.parser_type,
        *vi.min_gram,
        *vi.max_gram);
}
return fmt::format_to(ctx.out(), "PARSER_{}", vi.parser_type);
```

- [ ] **步骤 3：移除 schema 层对 Clara tokenizer registry 的依赖**

在 `TiDB.cpp` 中删除不再需要的 include：

```cpp
#include <clara_fts/src/tokenizer/mod.rs.h>
```

保留 `#if ENABLE_CLARA` 包裹的 fulltext schema 解析逻辑。

- [ ] **步骤 4：添加 TiFlash schema 层 parser 校验 helper**

在 `parseFullTextIndexFromJSON` 前新增：

```cpp
constexpr UInt32 MAX_FULLTEXT_NGRAM_GRAM = 8;

bool isSupportedFullTextParserType(const String & parser_type)
{
    return parser_type == "STANDARD_V1" || parser_type == "MULTILINGUAL_V1" || parser_type == "NGRAM_V1";
}

bool isNgramFullTextParserType(const String & parser_type)
{
    return parser_type == "NGRAM_V1";
}

void validateNgramFullTextParser(UInt32 min_gram, UInt32 max_gram)
{
    RUNTIME_CHECK_MSG(min_gram >= 1, "Invalid FullTextIndex definition, min_gram must be >= 1");
    RUNTIME_CHECK_MSG(
        max_gram >= min_gram,
        "Invalid FullTextIndex definition, max_gram {} must be >= min_gram {}",
        max_gram,
        min_gram);
    RUNTIME_CHECK_MSG(
        max_gram <= MAX_FULLTEXT_NGRAM_GRAM,
        "Invalid FullTextIndex definition, max_gram {} must be <= {}",
        max_gram,
        MAX_FULLTEXT_NGRAM_GRAM);
}
```

- [ ] **步骤 5：更新 `parseFullTextIndexFromJSON`**

把 Clara 校验替换为 schema 白名单和 gram 校验：

```cpp
RUNTIME_CHECK_MSG(
    isSupportedFullTextParserType(parser_type_field),
    "Invalid FullTextIndex definition, unsupported parser_type `{}`",
    parser_type_field);

const bool has_min_gram = json->has("min_gram");
const bool has_max_gram = json->has("max_gram");
if (isNgramFullTextParserType(parser_type_field))
{
    RUNTIME_CHECK_MSG(
        has_min_gram && has_max_gram,
        "Invalid FullTextIndex definition, NGRAM_V1 requires min_gram and max_gram");
    auto min_gram = json->getValue<UInt32>("min_gram");
    auto max_gram = json->getValue<UInt32>("max_gram");
    validateNgramFullTextParser(min_gram, max_gram);
    return std::make_shared<const FullTextIndexDefinition>(FullTextIndexDefinition{
        .parser_type = parser_type_field,
        .min_gram = min_gram,
        .max_gram = max_gram,
    });
}

RUNTIME_CHECK_MSG(
    !has_min_gram && !has_max_gram,
    "Invalid FullTextIndex definition, parser_type `{}` does not support gram parameters",
    parser_type_field);

return std::make_shared<const FullTextIndexDefinition>(FullTextIndexDefinition{
    .parser_type = parser_type_field,
});
```

- [ ] **步骤 6：更新 `fullTextIndexToJSON`**

把 Clara 校验替换为 schema 白名单，并只为 Ngram 写 gram 字段：

```cpp
RUNTIME_CHECK(isSupportedFullTextParserType(full_text_index->parser_type));

Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
json->set("parser_type", full_text_index->parser_type);
if (isNgramFullTextParserType(full_text_index->parser_type))
{
    RUNTIME_CHECK(full_text_index->min_gram.has_value());
    RUNTIME_CHECK(full_text_index->max_gram.has_value());
    validateNgramFullTextParser(*full_text_index->min_gram, *full_text_index->max_gram);
    json->set("min_gram", *full_text_index->min_gram);
    json->set("max_gram", *full_text_index->max_gram);
}
else
{
    RUNTIME_CHECK(!full_text_index->min_gram.has_value());
    RUNTIME_CHECK(!full_text_index->max_gram.has_value());
}
return json;
```

- [ ] **步骤 7：确认没有改 DeltaMerge FTS**

运行：

```bash
cd /DATA/disk1/jinhelin/tiflash-1
git diff -- dbms/src/Storages/DeltaMerge/Index/FullTextIndex
```

预期：无输出。

- [ ] **步骤 8：运行 TiFlash 格式和检查**

按仓库指令运行：

```bash
cd /DATA/disk1/jinhelin/tiflash-1
time (make format && make check)
```

预期：命令退出码为 `0`。

- [ ] **步骤 9：提交 TiFlash schema JSON 兼容**

```bash
cd /DATA/disk1/jinhelin/tiflash-1
git add dbms/src/TiDB/Schema/FullTextIndex.h dbms/src/TiDB/Schema/TiDB.cpp
git commit -m "columnar: parse fulltext ngram metadata"
```

---

## 任务 7：跨仓库收尾验证

**文件：**
- 检查：`/DATA/disk1/jinhelin/tidb-cse`
- 检查：`/DATA/disk1/jinhelin/cloud-storage-engine`
- 检查：`/DATA/disk1/jinhelin/tiflash-1`

- [ ] **步骤 1：检查 TiDB CSE 状态和验证**

运行：

```bash
cd /DATA/disk1/jinhelin/tidb-cse
git status --short
make parser_yacc
make parser_fmt
```

预期：`git status --short` 只显示预期未提交文件或为空；两个 make 命令退出码为 `0`。

- [ ] **步骤 2：检查 CSE 状态和验证**

运行：

```bash
cd /DATA/disk1/jinhelin/cloud-storage-engine
git status --short
make format
make check-protobuf
make clippy
```

预期：`git status --short` 只显示预期未提交文件或为空；三个 make 命令退出码为 `0`。不要并发运行 CSE cargo/make 检查。

- [ ] **步骤 3：检查 TiFlash 状态和验证**

运行：

```bash
cd /DATA/disk1/jinhelin/tiflash-1
git status --short
time (make format && make check)
```

预期：`git status --short` 只显示预期未提交文件或为空；`make format && make check` 退出码为 `0`。

- [ ] **步骤 4：确认范围没有漂移**

运行：

```bash
cd /DATA/disk1/jinhelin/tiflash-1
git diff HEAD -- dbms/src/Storages/DeltaMerge/Index/FullTextIndex contrib/cloud-storage-engine
```

预期：无输出。若有输出，撤掉这些不属于第一阶段的改动，除非已确认 TiFlash 编译必须同步内置 CSE metadata schema。

- [ ] **步骤 5：汇总提交**

记录三个仓库的提交：

```bash
git -C /DATA/disk1/jinhelin/tidb-cse log --oneline -3
git -C /DATA/disk1/jinhelin/cloud-storage-engine log --oneline -3
git -C /DATA/disk1/jinhelin/tiflash-1 log --oneline -3
```

预期：输出包含本计划每个仓库对应的提交，便于后续创建 PR 或推送分支。

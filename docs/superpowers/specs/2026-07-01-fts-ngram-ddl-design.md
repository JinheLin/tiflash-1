# FTS Ngram DDL 路径设计

## 范围

本文描述 Columnar FTS 支持 Ngram 的第一阶段：DDL 元数据链路。

本阶段目标是把 `NGRAM_V1`、`min_gram` 和 `max_gram` 从 TiDB schema 元数据传递到 Cloud Storage Engine（CSE），并保证 TiFlash 能兼容解析这些 schema 元数据。

本阶段不实现 Ngram 写入分词、Ngram 查询语义、误召回后置过滤，也不调整 score 语义。

## 设计决策

- SQL 语法支持显式 Ngram 参数：`WITH PARSER ngram (MIN_GRAM = 2, MAX_GRAM = 3)`。
- `NGRAM_V1` 固定为大小写不敏感语义，不提供大小写敏感相关的 DDL 参数。
- `NGRAM_V1` 必须显式指定 `MIN_GRAM` 和 `MAX_GRAM`。
- `STANDARD_V1` 和 `MULTILINGUAL_V1` 不允许携带 gram 参数。
- 第一版 `max_gram` 上限设为 `8`。
- 旧表中的 `STANDARD_V1` 和 `MULTILINGUAL_V1` 缺少 gram 字段时仍然合法。
- TiFlash 不通过 DeltaMerge FTS 执行这个链路。写请求走 CSE，读请求走 `StorageDisaggregated::readThroughColumnar`。
- TiFlash 只需要解析并回写 Ngram schema JSON，不修改 `dbms/src/Storages/DeltaMerge/Index/FullTextIndex/*`。
- 不注册会静默构建错误索引的假 `NGRAM_V1` Tantivy tokenizer。如果写路径在第二阶段前遇到 `NGRAM_V1`，应快速失败，而不是用错误语义建索引。

## TiDB CSE 修改

仓库：`/DATA/disk1/jinhelin/tidb-cse`

### Parser 模型

修改 `pkg/parser/model/index_full_text.go`：

- 新增 `FullTextParserTypeNgramV1 = "NGRAM_V1"`。
- `FullTextParserType.SQLName()` 对 `NGRAM_V1` 返回 `NGRAM`。
- `GetFullTextParserTypeBySQLName("ngram")` 返回 `FullTextParserTypeNgramV1`。
- 扩展 `FullTextIndexInfo`：
  - `MinGram *uint64 json:"min_gram,omitempty"`
  - `MaxGram *uint64 json:"max_gram,omitempty"`

这里使用指针字段保留字段是否存在的语义：旧 schema 反序列化后为 `nil`；合法的 Ngram schema 必须为非 `nil`。

### AST 与语法

修改 `pkg/parser/ast/ddl.go` 和 `pkg/parser/parser.y`：

- 在 `ast.IndexOption` 中增加一个小型 parser 参数结构，用于承载 fulltext parser 参数。
- 将现有 `WITH PARSER Identifier` grammar 扩展为支持可选括号参数列表。
- 参数名按 identifier 解析，并做大小写不敏感匹配。不要为 `MIN_GRAM` 或 `MAX_GRAM` 新增保留关键字。
- 支持参数任意顺序。
- DDL 校验阶段拒绝重复参数。
- AST 还原对 Ngram 参数使用固定顺序输出：`WITH PARSER ngram (MIN_GRAM = 2, MAX_GRAM = 3)`。
- `STANDARD` 和 `MULTILINGUAL` 的还原输出保持现有格式，不带括号参数。

修改 `parser.y` 后，需要按 TiDB CSE 现有 parser 生成流程重新生成 parser 代码。

### DDL 校验

修改 `pkg/planner/core/preprocess.go` 和 `pkg/ddl/index.go`：

- fulltext parser 名称校验接受 `NGRAM`。
- `buildFullTextInfoWithCheck` 保留现有 fulltext index 形态校验：
  - 只支持 1 个字符串列。
  - 不支持表达式。
  - 不支持 prefix length。
  - 不支持 DESC。
  - 同一列不允许重复创建 fulltext index。
- 对 `NGRAM_V1` 增加参数校验：
  - 必须同时指定 `MIN_GRAM` 和 `MAX_GRAM`。
  - `min_gram >= 1`。
  - `max_gram >= min_gram`。
  - `max_gram <= 8`。
  - 拒绝未知 fulltext parser 参数。
- 对 `STANDARD_V1` 和 `MULTILINGUAL_V1`：
  - 如果带 gram 参数，直接报错。
- 校验通过后，把 `ParserType`、`MinGram` 和 `MaxGram` 写入 `FullTextIndexInfo`。

### `SHOW CREATE TABLE`

修改 `pkg/executor/show.go`：

- 对 `NGRAM_V1` 输出 `WITH PARSER NGRAM (MIN_GRAM = <min>, MAX_GRAM = <max>)`。
- 对非 Ngram fulltext index 保持现有输出，例如 `WITH PARSER STANDARD` 或 `WITH PARSER MULTILINGUAL`。
- 对旧 metadata 不输出 gram 字段。

## Cloud Storage Engine 修改

仓库：`/DATA/disk1/jinhelin/cloud-storage-engine`

### TiDB Schema 反序列化

修改 `components/schema/src/schema.rs`：

- 扩展 `FullTextIndexInfo`：
  - `pub min_gram: Option<u32>`
  - `pub max_gram: Option<u32>`

Serde 默认行为可以保持旧 TiDB schema JSON 兼容：缺少字段时反序列化为 `None`。

### Protobuf 元数据

修改 `components/kvenginepb/src/fts.proto`：

- 扩展 `FullTextIndexDef`：
  - `uint32 min_gram = 11;`
  - `uint32 max_gram = 12;`

Proto3 primitive 字段使用 `0` 表示未设置。这个约定安全，因为合法 Ngram DDL 要求 `min_gram >= 1`。

修改 proto 后，通过 `kvenginepb` 的构建流程重新生成 Rust 代码。不要手动编辑生成文件。

### Schema 管理器

修改 `components/cloud_worker/src/schema_manager.rs`：

- 在 `parse_fulltext_indexes` 中，把 TiDB schema metadata 里的 gram 字段复制到 `kvenginepb::fts::FullTextIndexDef`。
- 缺失 gram 字段时写入 `0`。
- 增加防御性校验：
  - `NGRAM_V1` 要求 `min_gram` 和 `max_gram` 都非 `0`。
  - 非 Ngram parser 不允许携带非 `0` 的 gram 字段。

### CSE 运行时边界

现有 schema file 和 shard metadata 路径会复制并序列化 `FullTextIndexDef`。proto 重新生成后，新字段应自然完成持久化和恢复。

本阶段不实现 Ngram 写入或查询行为。特别是不要注册把 `NGRAM_V1` 映射到 `STANDARD_V1` 或固定 `2..3` 参数的占位 tokenizer。第二阶段会引入真正的 tokenizer 配置抽象和 canonical tokenizer name。

## TiFlash 修改

仓库：`/DATA/disk1/jinhelin/tiflash-1`

### Schema JSON 解析

修改 `dbms/src/TiDB/Schema/FullTextIndex.h`：

- 扩展 `TiDB::FullTextIndexDefinition`：
  - `std::optional<UInt32> min_gram`
  - `std::optional<UInt32> max_gram`
- 更新 formatter 输出，让 Ngram 定义在日志和调试信息中包含 gram 字段。

修改 `dbms/src/TiDB/Schema/TiDB.cpp`：

- 从 `full_text_index` JSON 中解析 `parser_type`、`min_gram` 和 `max_gram`。
- 使用 TiFlash schema 层自己的 parser 白名单：
  - `STANDARD_V1`
  - `MULTILINGUAL_V1`
  - `NGRAM_V1`
- schema 层校验不要依赖 `ClaraFTS::supports_tokenizer`。
- 对 `NGRAM_V1`，要求并校验 `min_gram/max_gram`，规则与 TiDB CSE 保持一致。
- 对非 Ngram parser，拒绝 gram 字段。
- 只有 `NGRAM_V1` 序列化时写回 `min_gram/max_gram`。

### Disaggregated 读路径

本阶段不需要修改 `StorageDisaggregated.cpp`。

`StorageDisaggregated::buildTableScanTiPB()` 已经直接转发 TiDB 下发的 table scan protobuf。`TiDBTableScan.cpp` 会从 `used_columnar_indexes` 中提取 `TypeFulltext` 的 `FTSQueryInfo`。Ngram 查询语义属于第三阶段。

### DeltaMerge FTS

不要修改：

- `dbms/src/Storages/DeltaMerge/Index/FullTextIndex/*`
- `FullTextIndexWriterInMemory`
- `FullTextIndexWriterOnDisk`
- DeltaMerge FTS reader/query 代码

这条路径对当前功能已经废弃。第一阶段修改它只会增加风险，不能帮助完成 DDL 元数据链路。

### 内置 CSE 说明

TiFlash 仓库中还有 `contrib/cloud-storage-engine`，它当前与 `/DATA/disk1/jinhelin/cloud-storage-engine` 不在同一个 commit。

以 `/DATA/disk1/jinhelin/cloud-storage-engine` 作为 CSE 修改的唯一来源。除非当前 TiFlash workspace 编译时使用 vendored CSE，否则不要修改 TiFlash 的 vendored CSE 目录。如果 TiFlash 编译确实依赖 vendored CSE，则只同步编译所需的 metadata schema 修改，不在 vendored 目录引入行为变化。

## 兼容性

- 旧 fulltext metadata 只有 `parser_type` 时仍然合法。
- 旧 `STANDARD_V1` 和 `MULTILINGUAL_V1` metadata 不会获得隐式 gram 默认值。
- `NGRAM_V1` metadata 缺少任意 gram 字段时非法。
- 非 Ngram metadata 携带 gram 字段时非法。
- CSE proto reader 可以加载旧文件；缺失 proto 字段会反序列化为 `0`。

## 不在本阶段处理

- Ngram analyzer 实现。
- `NGRAM_V1_2_3_CI` 这类 canonical tokenizer name。
- Delta cache 和 stable FTS writer 修改。
- FTS reader/query 修改。
- `fts_match_word_ngram`。
- tipb scalar enum 修改。
- false positive 后置过滤。
- score 语义。
- 监控和 dashboard 修改。

## 验证

第一阶段验证重点是格式化和编译检查，不要求跑完整单元测试套件。

- 在 `/DATA/disk1/jinhelin/tiflash-1` 运行 `time (make format && make check)`。
- 在 CSE 中运行触及 Rust/protobuf 文件所需的格式化、编译和 `kvenginepb` 生成检查。
- 在 TiDB CSE 中运行 parser 生成、格式化和 parser/DDL metadata 相关检查。

本阶段不要求运行完整单元测试。可以按实现风险补充 parser restore 或 schema round-trip 这类聚焦测试，但它们不是完成条件。

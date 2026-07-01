# FTS Ngram DDL Path Design

## Scope

This document describes phase one of Columnar FTS Ngram support: the DDL metadata path.

The goal is to pass `NGRAM_V1`, `min_gram`, and `max_gram` from TiDB schema metadata to Cloud Storage Engine (CSE), and to keep TiFlash schema parsing compatible with that metadata.

This phase does not implement Ngram tokenization for writes, Ngram query semantics, post-filtering, or score changes.

## Decisions

- SQL syntax supports explicit Ngram parameters:
  `WITH PARSER ngram (MIN_GRAM = 2, MAX_GRAM = 3)`.
- `NGRAM_V1` is always case-insensitive. There is no DDL parameter for case sensitivity.
- `MIN_GRAM` and `MAX_GRAM` are required for `NGRAM_V1`.
- `STANDARD_V1` and `MULTILINGUAL_V1` must not carry gram parameters.
- `max_gram` upper bound is `8` for phase one.
- Missing gram fields remain valid for old `STANDARD_V1` and `MULTILINGUAL_V1` metadata.
- TiFlash does not use DeltaMerge FTS for this flow. Writes go to CSE, and reads go through `StorageDisaggregated::readThroughColumnar`.
- TiFlash should only parse and round-trip schema JSON for Ngram metadata. It should not modify `dbms/src/Storages/DeltaMerge/Index/FullTextIndex/*`.
- Do not register a fake Tantivy tokenizer for `NGRAM_V1` that silently builds non-Ngram indexes. If a write path reaches `NGRAM_V1` before phase two, it should fail fast rather than build an index with incorrect semantics.

## TiDB CSE Changes

Repository: `/DATA/disk1/jinhelin/tidb-cse`

### Parser Model

Update `pkg/parser/model/index_full_text.go`:

- Add `FullTextParserTypeNgramV1 = "NGRAM_V1"`.
- Return `NGRAM` from `FullTextParserType.SQLName()`.
- Return `FullTextParserTypeNgramV1` from `GetFullTextParserTypeBySQLName("ngram")`.
- Extend `FullTextIndexInfo`:
  - `MinGram *uint64 json:"min_gram,omitempty"`
  - `MaxGram *uint64 json:"max_gram,omitempty"`

Pointer fields preserve presence: old schemas decode as nil, while valid Ngram schemas must have non-nil values.

### AST And Grammar

Update `pkg/parser/ast/ddl.go` and `pkg/parser/parser.y`:

- Add a small parser-parameter structure to `ast.IndexOption`, carrying fulltext parser options.
- Extend grammar from `WITH PARSER Identifier` to also accept an optional parenthesized parameter list.
- Parse parameter names as identifiers, case-insensitively. Do not add new reserved keywords for `MIN_GRAM` or `MAX_GRAM`.
- Support parameters in any order.
- Reject duplicate parameters during DDL validation.
- Restore Ngram parser options in canonical order:
  `WITH PARSER ngram (MIN_GRAM = 2, MAX_GRAM = 3)`.
- Keep existing restore output unchanged for `STANDARD` and `MULTILINGUAL`.

After modifying `parser.y`, regenerate parser output through the repository parser generation flow.

### DDL Validation

Update `pkg/planner/core/preprocess.go` and `pkg/ddl/index.go`:

- Accept `NGRAM` as a supported fulltext parser.
- In `buildFullTextInfoWithCheck`:
  - Validate fulltext index shape exactly as today: one string column, no expression, no prefix length, no descending order, no duplicate fulltext index on the same column.
  - For `NGRAM_V1`, require both `MIN_GRAM` and `MAX_GRAM`.
  - Require `min_gram >= 1`.
  - Require `max_gram >= min_gram`.
  - Require `max_gram <= 8`.
  - Reject unknown fulltext parser parameters.
  - Reject gram parameters for `STANDARD_V1` and `MULTILINGUAL_V1`.
  - Store `ParserType`, `MinGram`, and `MaxGram` in `FullTextIndexInfo`.

### SHOW CREATE TABLE

Update `pkg/executor/show.go`:

- For `NGRAM_V1`, append `WITH PARSER NGRAM (MIN_GRAM = <min>, MAX_GRAM = <max>)`.
- For non-Ngram fulltext indexes, keep the existing `WITH PARSER STANDARD` and `WITH PARSER MULTILINGUAL` output.
- Do not output gram fields for old metadata that does not carry them.

## Cloud Storage Engine Changes

Repository: `/DATA/disk1/jinhelin/cloud-storage-engine`

### TiDB Schema Deserialization

Update `components/schema/src/schema.rs`:

- Extend `FullTextIndexInfo`:
  - `pub min_gram: Option<u32>`
  - `pub max_gram: Option<u32>`

Serde default behavior keeps old TiDB schema JSON compatible: missing fields decode to `None`.

### Protobuf Metadata

Update `components/kvenginepb/src/fts.proto`:

- Extend `FullTextIndexDef`:
  - `uint32 min_gram = 11;`
  - `uint32 max_gram = 12;`

Proto3 primitive fields use `0` as unset. This is safe because valid Ngram DDL requires `min_gram >= 1`.

Regenerate protobuf Rust files through the kvenginepb build flow. Do not edit generated files by hand.

### Schema Manager

Update `components/cloud_worker/src/schema_manager.rs`:

- In `parse_fulltext_indexes`, copy gram fields from TiDB schema metadata to `kvenginepb::fts::FullTextIndexDef`.
- Use `0` for missing gram fields.
- Validate defensively:
  - `NGRAM_V1` requires non-zero `min_gram` and `max_gram`.
  - Non-Ngram parsers must not carry non-zero gram fields.

### CSE Runtime Boundary

Existing schema file and shard metadata paths clone and serialize `FullTextIndexDef`, so the new fields should naturally persist once protobuf is regenerated.

Do not implement Ngram writer/query behavior in this phase. In particular, do not register a placeholder `NGRAM_V1` tokenizer that maps to `STANDARD_V1` or fixed `2..3` behavior. Phase two will introduce a real tokenizer configuration abstraction and canonical tokenizer names.

## TiFlash Changes

Repository: `/DATA/disk1/jinhelin/tiflash-1`

### Schema JSON

Update `dbms/src/TiDB/Schema/FullTextIndex.h`:

- Extend `TiDB::FullTextIndexDefinition`:
  - `std::optional<UInt32> min_gram`
  - `std::optional<UInt32> max_gram`
- Update formatter output to include gram fields for Ngram definitions.

Update `dbms/src/TiDB/Schema/TiDB.cpp`:

- Parse `parser_type`, `min_gram`, and `max_gram` from `full_text_index` JSON.
- Use a TiFlash schema-layer parser whitelist:
  - `STANDARD_V1`
  - `MULTILINGUAL_V1`
  - `NGRAM_V1`
- Do not depend on `ClaraFTS::supports_tokenizer` for this schema-level check.
- For `NGRAM_V1`, require and validate `min_gram/max_gram` with the same rules as TiDB CSE.
- For non-Ngram parsers, reject gram fields.
- Serialize `min_gram/max_gram` only for `NGRAM_V1`.

### Disaggregated Read Path

No change is required in `StorageDisaggregated.cpp` for phase one.

`StorageDisaggregated::buildTableScanTiPB()` already forwards the TiDB table scan protobuf. `TiDBTableScan.cpp` extracts `FTSQueryInfo` from `used_columnar_indexes` for columnar fulltext reads. Ngram query semantics are phase three work.

### DeltaMerge FTS

Do not modify:

- `dbms/src/Storages/DeltaMerge/Index/FullTextIndex/*`
- `FullTextIndexWriterInMemory`
- `FullTextIndexWriterOnDisk`
- DeltaMerge FTS reader/query code

That path is deprecated for this feature. Touching it in phase one would add risk without serving the DDL metadata goal.

### Vendored CSE Note

TiFlash also contains `contrib/cloud-storage-engine`, which is currently at a different commit from `/DATA/disk1/jinhelin/cloud-storage-engine`.

Treat `/DATA/disk1/jinhelin/cloud-storage-engine` as the source of truth for CSE changes. Do not modify TiFlash's vendored CSE tree unless the TiFlash build in this workspace uses it for compilation. If the TiFlash build does use the vendored tree, mirror only the metadata-schema changes required for compilation and keep behavior changes out of the vendored tree in this phase.

## Compatibility

- Old fulltext metadata with only `parser_type` remains valid.
- Old `STANDARD_V1` and `MULTILINGUAL_V1` metadata does not gain implicit gram defaults.
- Ngram metadata without both gram fields is invalid.
- Non-Ngram metadata with gram fields is invalid.
- CSE proto readers can load old files; missing proto fields decode to `0`.

## Out Of Scope

- Ngram analyzer implementation.
- Canonical tokenizer names such as `NGRAM_V1_2_3_CI`.
- Delta cache and stable FTS writer changes.
- FTS reader/query changes.
- `fts_match_word_ngram`.
- tipb scalar enum changes.
- Post-filtering for false positives.
- Score semantics.
- Monitoring or dashboard changes.

## Verification

Phase one verification should focus on formatting and compile/check commands, not full unit test suites.

- In `/DATA/disk1/jinhelin/tiflash-1`, run `time (make format && make check)`.
- In CSE, run formatting and compile/check commands required by the touched Rust/protobuf files, including kvenginepb generation.
- In TiDB CSE, run parser generation and formatting/check commands required by parser and DDL metadata changes.

No full unit test suite is required for this phase. Focused parser restore or schema round-trip tests are optional implementation aids, not completion criteria.

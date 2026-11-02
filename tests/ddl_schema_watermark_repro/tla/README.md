# Schema read watermark 的 TLA+ / TLC 验证

本目录包含可独立运行的 TLA+ 模型、TLC 配置、执行器和证据复核脚本，只需要 Python 3.9+ 与 Java 11+，不需要启动 TiUP 或编译 TiFlash。2026-11-02 首次验证以 `dd85dc316868e3afc6f04b71e5e2a11c1814661f` 的实现为参考；下面的状态数和反例长度是该次记录，重跑不以固定状态数作为断言。

**结果：正确模型完整检查了 999,796 个不同状态，未违反安全不变量；3 个故障模型均找到误跳有效列的反例；6 个可达性检查确认关键路径实际可达。** 这是有限模型检查结果，不是 TLAPS 定理证明或 C++ 实现的 refinement 证明。

## 文件与重跑

- [SchemaWatermark.tla](SchemaWatermark.tla)：状态、动作、不变量和可达性哨兵。
- [correct.cfg](correct.cfg)：正确模型的有限边界及全部不变量。
- `no_clear.cfg`、`split_publish.cfg`、`min_commit.cfg`：三个故障版本。
- `witness_*.cfg`：可达性检查，故意对“该事件从不发生”设置不变量，预期找到违反它的路径。
- `<output>/summary.json`：本次 10 项检查的命令、返回码、状态数及模型/配置 SHA256。
- `<output>/<名称>/tlc.log`：原始 TLC 输出，故障日志包含完整逐状态反例；每项还保存了当时的模型和配置副本。
- [COUNTEREXAMPLES.md](COUNTEREXAMPLES.md)：三个反例的中文解释。
- `<output>/audit.json`、[audit_results.py](audit_results.py)：独立复核结果和脚本。复核使用结果目录中的模型/配置副本，不依赖当前 Git HEAD、工作区状态或最初机器的路径。
- `.tools/`、`results/` 为本地忽略目录，不提交 JAR、日志或运行结果。首次验证的全部原始材料已迁入本机 `results/20261102-original/`，包括早期迭代，保留作历史证据。

执行器默认使用官方 [v1.7.4 release](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4) 的 `tla2tools.jar`（TLC 2.19）。`--download-tools` 在 JAR 缺失时下载到 `.tools/`，写入前核对固定 SHA256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。也可通过 `--tlc-jar` 指定已有 JAR、`--java` 指定 Java。每次结果中的 `tool-manifest.json` 记录实际工具路径、SHA256、TLC 和 Java 版本。

从仓库根目录执行；默认结果写入本目录下全新的 `results/<UTC 时间戳>/`，也可显式指定新的输出目录：

```bash
python3 tests/ddl_schema_watermark_repro/tla/run_models.py --download-tools

# 离线使用已下载的 JAR，输出目录必须不存在。
python3 tests/ddl_schema_watermark_repro/tla/run_models.py \
  --tlc-jar /absolute/path/to/tla2tools.jar \
  --output /absolute/path/to/fresh-tlc-results

# 执行器结束时会自动复核，也可单独检查已保存的结果。
python3 tests/ddl_schema_watermark_repro/tla/audit_results.py \
  --output /absolute/path/to/fresh-tlc-results
```

执行器为正确模型配置 2 个 worker，其余模型 1 个 worker，JVM 堆上限 2 GiB。使用 BFS 完整状态搜索，没有使用随机模拟、搜索深度上限或 STATE CONSTRAINT。时间与对象数量的有限边界由下面的模型常量定义。

## 模型范围与环境前提

- 单个物理表，两个业务列 ID `a/b`；`a` 已在初始状态存在，`b` 可引入，任一列均可删除。
- Column ID 不复用；普通已提交行只能包含提交前已进入元数据的列 ID。允许延迟写入继续携带已删除 ID，是对历史写入的保守扩大。
- 全局逻辑时间为 `1..4`，初始时间为 1；后续 ADD、DROP、提交各推进一个时间点。请求、锁、持久化、解码动作不要求消耗新的时间点。
- 两个同步请求，各至多执行一次完整读取或局部更新，可任意交错；允许旧 RPC 晚返回、局部 TableInfo 在 alter lock 之前复制。
- 每次解码批次包含 1～2 条已提交记录，可以混合不同 commit_ts；记录数量由有限时间域限制。
- 完整 MVCC RPC 返回其 read TSO 对应的完整 schema，这是外部元数据服务的环境契约。模型不证明 TiKV MVCC 的实现。
- schema 的内存 ID 集合、持久化 ID 集合、watermark、缓存快照和解码快照分别建模。模型没有将 schema 与 watermark 的正确配对作为 ASSUME 或动作前置条件。
- 排他 alter lock 和共享解码锁显式建模；TLC 枚举更新步骤、异常、RPC 和解码之间的交错。
- 崩溃可发生在持锁更新、解码、持久化前后等任意阶段；重启从持久化 schema 恢复，watermark 和缓存水位归零，在途请求失效。

这里的 schema 是与解码有关的业务列 ID 集合。模型没有逐一模拟 C++ 的 ColumnsDescription、TableInfo、索引、类型和值对象，也不证明这些对象在所有异常后都可成功重建。若实际快照构造抛异常，这属于可用性/重试路径，需要已有故障单测补充；本模型的目标是“优化不得跳过有效列”。未建模 NOT NULL 缺列补值、RowV1/RowV2 字节解析、多表映射及跨时间域恢复。

## 动作与当前代码的对应关系

| 模型动作 | 实现对应 |
| --- | --- |
| `StartRead` / `CompleteRead` | [createSchemaGetter](../../../dbms/src/TiDB/Schema/TiDBSchemaSyncer.cpp#L38) 获取 TSO；SchemaGetter 使用同一快照读取 TableInfo。RPC 与读取 TSO 分开建模，允许其间发生 DDL。 |
| `CopyLocal` | [局部更新先复制本地 TableInfo](../../../dbms/src/TiDB/Schema/SchemaBuilder.cpp#L485)，最终以 `read_tso=0` 调用更新。 |
| `BeginAlter` / `IgnoreOlder` | [获取 alter lock](../../../dbms/src/TiDB/Schema/SchemaBuilder.cpp#L1760)，在锁内比较旧请求的 read TSO，并在修改前[清零 watermark](../../../dbms/src/Storages/StorageDeltaMerge.cpp#L1466)。 |
| `ApplyStore` | [更新底层列信息](../../../dbms/src/Storages/StorageDeltaMerge.cpp#L1488)，随后将缓存快照标记为需要重建。 |
| `Persist` / `Publish` | [先持久化元数据](../../../dbms/src/Storages/StorageDeltaMerge.cpp#L1508)，成功后才[设置 schema_read_tso](../../../dbms/src/Storages/StorageDeltaMerge.cpp#L1519)。 |
| `Fail` | 在 store 更新前、更新后、持久化后但发布 watermark 前抛异常，释放 alter lock；不自动回滚已改变的状态。 |
| `AcquireSnapshot` | [共享结构锁](../../../dbms/src/Storages/KVStore/Decode/PartitionStreams.cpp#L168) 保护解码；[按 dirty 状态复用或重建快照](../../../dbms/src/Storages/StorageDeltaMerge.cpp#L1248)。未改变的旧缓存可以继续保留旧的、正确配对的水位。 |
| `Decode` | [批次 watermark 判断](../../../dbms/src/Storages/KVStore/Decode/PartitionStreams.cpp#L82)：零水位不允许优化，否则使用整个批次的最大 commit_ts。 |
| `Restart` | 抽象进程崩溃和本地 metadata reload；清除在途状态，以零 watermark 重建。不是对实际磁盘崩溃一致性协议的建模。 |

## 不变量：检查目标而非预设结论

正确配置同时检查：

1. `TypeOK`：状态类型和边界正确。
2. `OrdinaryCommitOrder` / `HistoryLifecycle`：生成的普通写入与 ADD/DROP 历史满足元数据先引入、ID 不复用的契约。
3. `PublishedPair`：非零的本地 watermark 对应它所指向时刻的完整 schema。
4. `CachedPair`：正水位缓存快照保持自己的正确 schema/TSO 配对，即使它比当前状态旧。
5. `DecoderPair`：正在使用的正水位解码快照也保持配对。
6. `NoValidColumnSkipped`：所有已跳过列在解码快照的 schema_read_tso 前都已被删除，不能跳过仍有效或尚未引入的列。

`Decode` 的跳过条件只读取批次 commit_ts、快照 schema 和 watermark。引入/删除时间仅用于独立的错误判定，不用于阻止某个错误跳过动作发生。因此错误版本可以真实进入 `badSkipped={"b"}`，不是在模型中直接排除了错误状态。

时间戳规则的推导为：某列在行提交前已引入；若该行 `commit_ts <= schema_read_tso`，则该列在 schema_read_tso 前已引入。又因为此时完整 schema 中没有该 ID，且 ID 不复用，它只能已经被删除。实现模型额外检查“快照确实对应这个 schema_read_tso”这一步，而不是只复述这条推导。

## 最终结果

| 检查 | 实际结果 |
| --- | --- |
| 正确模型 | 3,942,832 个生成状态、999,796 个不同状态，队列剩余 0，完整状态图深度 19，7 个不变量通过。 |
| 不清零旧 watermark | 找到 15 个状态的误跳有效列反例。 |
| 新 watermark 在 schema 前对解码可见 | 找到 8 个状态的误跳有效列反例。 |
| 错用批次最小 commit_ts | 找到 12 个状态的混合批次误跳反例。 |
| 历史列实际跳过 | 正确模型能到达安全跳过动作，排除“完全没有开启优化所以通过”。 |
| 保守回退 | 正确模型能到达有额外列但不满足覆盖条件的回退。 |
| store 更新后失败 | 正确模型能到达该故障分支。 |
| 重启、更新中崩溃、解码中崩溃 | 三类事件在正确模型中均可达。 |

三个故障检查只要求 `NoValidColumnSkipped` 真正被违反，没有启用配对不变量去提前截断反例。六个 witness 检查是预期的 sentinel violation，不是发现了六个正确性问题。执行器按具体违反的不变量核验返回码，独立复核还检查了三个错误末状态中的 `b` 从未被删除。

有限对象和时间域内的 BFS 完成不等于对任意规模、无限时间或实际 C++ 程序的证明；没有运行 TLAPS，也没有完成实现到模型的机器检查映射。本次结果是可重复检查的有限安全模型和反例敏感性验证，应与实际代码 review、故障单测、TiUP 验证一起使用。

本目录随专用测试分支维护模型、配置、执行和复核脚本及说明。工具缓存、历史材料和新运行结果保存在本目录的忽略路径中，不随代码提交。

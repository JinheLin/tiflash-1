# 测试计划二：data 缺少 NOT NULL 列且没有 origin_default，导致重复同步 schema

## 目标与触发原理

复现“最终 schema 包含 NOT NULL 列，但某些积压行没有对应列 ID，且 `origin_default` 为空”。该问题不在本 PR 已实现的额外列优化范围内；预期是复现重复同步，并验证实际数据仍正确。

要连续执行三次需要 reorg 的 MODIFY，而不是只 ADD 一个带常量 DEFAULT 的列：

1. 暂停整个 TiFlash，依次 MODIFY 首列、中间列、末列。
2. 早期 reorg 记录写入时，后续 changing column ID 尚未生成，因此这些记录缺少最终 schema 中的部分列。
3. 恢复 TiFlash 后，第一次解码失败会取得最终完整 schema。随后即使批次满足 `max(commit_ts) <= schema_read_tso`，缺列检查仍可能失败。
4. 同步 schema 不会为积压 KV 增加缺失的值；`force_decode` 仅作用于当前批次，因此后续批次继续同步同一份 schema。

额外列和缺失列可以同时存在。可空列对照允许缺失值按原逻辑处理，用于区分本问题与 `PLAN_EXTRA_COLUMNS.md` 的额外列路径。

SQL 的 `DEFAULT` 用于新写入的省略列；`origin_default` 用于历史缺列补值。两者必须分别检查，不能根据 `SHOW CREATE TABLE` 中存在 `DEFAULT 0` 就认定存在 `origin_default`。

自动化入口为本目录 [run_missing_columns.py](run_missing_columns.py)，参数和版本约定见 [README](README.md)。下述手动步骤可独立执行。

## 环境与版本记录

- Linux，独立 TiUP Playground：1 PD、1 TiKV、1 TiDB、1 TiFlash。
- 主测试使用 `jinhe/schema-read-watermark-extra-columns` 分支在本轮执行前更新后的源码构建 TiFlash；旧版对照统一使用 `v8.5.7`。不固定某个 commit，各版本使用独立新集群和数据目录。
- TiDB、PD、TiKV 统一使用 `v8.5.7`。TiFlash 使用对应版本的完整安装目录，配置 `tcp_port=9000`、日志级别 `debug`。编译清除代理环境变量，不运行 `make format`。
- 保存版本来源、源码实际 commit（本地构建时）、工作区 diff、TiFlash 的 `version`、二进制/动态库 SHA256，以及 `information_schema.cluster_info` 的组件版本。不要将脚本当前 Git HEAD 当成二进制构建版本；同一轮测试不更换二进制，下轮重新更新分支并记录版本。
- TiKV 配置保留积压 Raft 日志。

终端 A 填写版本来源并替换两个绝对路径后前台运行，命令会在本轮新结果目录中生成配置。主测试的 `TIFLASH_TEST_REF` 为 `jinhe/schema-read-watermark-extra-columns`；旧版对照改为 `v8.5.7`，并切换对应完整安装目录：

```bash
export TIFLASH_TEST_REF=jinhe/schema-read-watermark-extra-columns
export TIFLASH_BIN=/absolute/path/to/install/tiflash
export REPRO_OUT=/absolute/path/to/new-missing-columns-run
mkdir -p "$REPRO_OUT"
printf '%s\n' "$TIFLASH_TEST_REF" > "$REPRO_OUT/tiflash-requested-ref.txt"
export LD_LIBRARY_PATH="$(dirname "$TIFLASH_BIN"):${LD_LIBRARY_PATH:-}"
cat > "$REPRO_OUT/tikv.toml" <<'TOML'
[raftstore]
raft-log-gc-tick-interval = "0s"
raft-log-gc-count-limit = 1000000
raft-log-gc-size-limit = "1GB"
TOML
cat > "$REPRO_OUT/tiflash.toml" <<'TOML'
tcp_port = 9000
capacity = 1073741824
[logger]
level = "debug"
TOML
tiup playground v8.5.7 --tag schema-missing-repro \
  --db 1 --pd 1 --kv 1 --tiflash 1 --without-monitor \
  --db.port 4000 --kv.config "$REPRO_OUT/tikv.toml" \
  --tiflash.config "$REPRO_OUT/tiflash.toml" \
  --tiflash.binpath "$TIFLASH_BIN"
```

新版 TiFlash 通过 `--tiflash.binpath` 覆盖，TiUP 的 `v8.5.7` 参数用于选择其余组件。如需通过 `--db.binpath`、`--pd.binpath`、`--kv.binpath` 指定本地组件，也必须选用 `v8.5.7` 并核验实际版本。历史成功记录使用过其他组件版本，不能代替本版本组合的复测。

终端 B 设置相同的二进制、输出目录及实际端口；TiUP 分配的端口以实际生成配置为准。

```bash
export TIDB_PORT=4000 TIDB_STATUS_PORT=10080
export TIFLASH_TCP_PORT=9000 TIFLASH_METRICS_PORT=8234
export TIFLASH_LOG=/absolute/path/to/this-playground/tiflash.log
export LD_LIBRARY_PATH="$(dirname "$TIFLASH_BIN"):${LD_LIBRARY_PATH:-}"
repro_mysql() { mysql --protocol=tcp -h127.0.0.1 -P"$TIDB_PORT" -uroot -N -B "$@"; }
repro_debug() { "$TIFLASH_BIN" client --host 127.0.0.1 --port "$TIFLASH_TCP_PORT" --query "$1"; }
"$TIFLASH_BIN" version > "$REPRO_OUT/tiflash-version.txt"
sha256sum "$TIFLASH_BIN" > "$REPRO_OUT/tiflash-sha256.txt"
repro_mysql -e 'SELECT TYPE,INSTANCE,VERSION,GIT_HASH FROM information_schema.cluster_info' > "$REPRO_OUT/cluster-info.tsv"
repro_debug "DBGInvoke enable_schema_sync_service('false')"
```

保存本集群 GC 原值，临时调整为 `1h`，结束时恢复：

```bash
repro_mysql -e "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_life_time'" > "$REPRO_OUT/gc-before.txt"
repro_mysql -e "UPDATE mysql.tidb SET VARIABLE_VALUE='1h' WHERE VARIABLE_NAME='tikv_gc_life_time'"
```

以下使用 RowV2，单个 reorg worker、较小 reorg batch，确保产生多个积压批次。

## 三组对照

每组都建新表，业务列使用同一约束，并在三条 MODIFY 中保留该约束。

| 组别 | 业务列约束 | 要验证的条件 |
| --- | --- | --- |
| A：主要复现 | `NOT NULL`，不声明 DEFAULT | 最终列缺少 origin_default 时重复同步 |
| B：可空对照 | `NULL DEFAULT NULL` | 相同 DDL、相同积压控制下，首次同步后不再重复同步 |
| C：排除 SQL 默认值干扰 | `NOT NULL DEFAULT 0` | 即使 SQL 有 default=0、没有 NoDefaultValue 标志，origin_default 为空仍能复现 |

先完成 A，随后分别以新表 `t_nullable`、`t_default_zero` 重复下列流程，替换表名和约束。一次只操作一张表，以便全局指标与目标表日志对应。

## 数据准备

```bash
repro_mysql <<'SQL'
SET GLOBAL tidb_row_format_version=2;
SET GLOBAL tidb_ddl_reorg_worker_cnt=1;
SET GLOBAL tidb_ddl_reorg_batch_size=256;
CREATE DATABASE schema_repro_missing;
CREATE TABLE schema_repro_missing.seed(n INT PRIMARY KEY);
INSERT INTO schema_repro_missing.seed VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE schema_repro_missing.t (
  c_first DECIMAL(18,2) NOT NULL,
  id BIGINT PRIMARY KEY CLUSTERED,
  c_middle DECIMAL(18,2) NOT NULL,
  c_last DECIMAL(18,2) NOT NULL
);
INSERT INTO schema_repro_missing.t
SELECT i/100, i, 3*i/100, (7*i+11)/100
FROM (SELECT 1+a.n+10*b.n+100*c.n+1000*d.n AS i
      FROM schema_repro_missing.seed a, schema_repro_missing.seed b,
           schema_repro_missing.seed c, schema_repro_missing.seed d) s;
ALTER TABLE schema_repro_missing.t SET TIFLASH REPLICA 1;
SQL
```

轮询 `information_schema.tiflash_replica`，等待该表 `AVAILABLE=1`，上限 240 秒。分别强制从 TiKV、TiFlash 读取 10,000 行全部列并 `ORDER BY id`，确认初始结果完全一致。保存 TABLE_ID、初始 TableInfo、DDL 历史。

```bash
repro_mysql -e "SELECT TABLE_ID,AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA='schema_repro_missing' AND TABLE_NAME='t'" > "$REPRO_OUT/table-id.tsv"
curl --noproxy '*' -fsS "http://127.0.0.1:$TIDB_STATUS_PORT/schema/schema_repro_missing/t" > "$REPRO_OUT/schema-before.json"
```

## 核心步骤：整个进程暂停期间连续三次 MODIFY

每组测量前，先等待相关指标连续三次、一秒间隔不再变化。从本次 Playground 的进程参数确认真实 TiFlash `server` PID；不要使用 tiup、shell wrapper 或 `tiflash client` 的 PID。将其赋给 `TIFLASH_PID` 后执行：

```bash
# 先设置 TIFLASH_PID 为本次集群实际 server PID。
: "${TIFLASH_PID:?set the actual TiFlash server PID}"
tr '\0' ' ' < "/proc/$TIFLASH_PID/cmdline" > "$REPRO_OUT/tiflash-cmdline.txt"
curl --noproxy '*' -fsS "http://127.0.0.1:$TIFLASH_METRICS_PORT/metrics" > "$REPRO_OUT/metrics-before.txt"
REPRO_LOG_START=$(wc -l < "$TIFLASH_LOG")
kill -STOP "$TIFLASH_PID"
cat "/proc/$TIFLASH_PID/status" > "$REPRO_OUT/stopped-process-status.txt"
```

确认 `/proc/<pid>/status` 中 State 为 `T` 后，在 TiDB 执行以下三条语句，必须全部完成后才恢复 TiFlash：

```bash
repro_mysql <<'SQL'
ALTER TABLE schema_repro_missing.t MODIFY COLUMN c_first DECIMAL(20,3) NOT NULL;
ALTER TABLE schema_repro_missing.t MODIFY COLUMN c_middle DECIMAL(20,3) NOT NULL;
ALTER TABLE schema_repro_missing.t MODIFY COLUMN c_last DECIMAL(20,3) NOT NULL;
SHOW CREATE TABLE schema_repro_missing.t;
ADMIN SHOW DDL JOBS 12;
SQL
curl --noproxy '*' -fsS "http://127.0.0.1:$TIDB_STATUS_PORT/schema/schema_repro_missing/t" > "$REPRO_OUT/schema-final.json"
```

此时从 **TiDB status 接口**保存最终元数据，TiFlash 进程仍保持暂停，不调用其 debug 或 metrics 接口。核对：

- 三次 DDL 都真实执行 reorg，最终列 ID 与对应原 ID 不同；历史记录是 `1/3/4 -> 5/6/7`，不将这些具体 ID 写成验收前提。
- A、C 组业务列均为 NOT NULL、public（记录中 state=5），`origin_default` 为空。
- C 组另外要求 `default=0`，并且 `Flag & 4096 == 0`（没有 NoDefaultValue 标志）；仅检查 SQL 中出现 DEFAULT 0 不够。

恢复同一个进程，让首次自然解码失败取得最终完整 schema：

```bash
kill -CONT "$TIFLASH_PID"
```

本轮不提前调用 `refresh_table_schema`。不要使用一次性的 `pause_before_apply_raft_cmd` 代替 SIGSTOP：历史第一轮正因后续 reorg apply 未被完全暂停而没有复现目标积压。

## 观察与数据验收

恢复后等待数据追平，相关指标连续三次、一秒间隔稳定后，保存指标和本阶段日志：

```bash
curl --noproxy '*' -fsS "http://127.0.0.1:$TIFLASH_METRICS_PORT/metrics" > "$REPRO_OUT/metrics-after.txt"
tail -n "+$((REPRO_LOG_START+1))" "$TIFLASH_LOG" > "$REPRO_OUT/tiflash-phase.log"
for REPRO_ENGINE in tikv tiflash; do
  repro_mysql -e "SET tidb_isolation_read_engines='$REPRO_ENGINE'; SET tidb_allow_mpp=1; SET tidb_enforce_mpp=$([ "$REPRO_ENGINE" = tiflash ] && printf 1 || printf 0); SELECT c_first,id,c_middle,c_last FROM schema_repro_missing.t ORDER BY id" > "$REPRO_OUT/$REPRO_ENGINE-rows.tsv"
done
diff -u "$REPRO_OUT/tikv-rows.tsv" "$REPRO_OUT/tiflash-rows.tsv"
```

新版 TiFlash 的判定标准：

1. `tiflash_schema_watermark_decode_count{type="covered"}` 增量大于 0。仅观察到 `no_watermark` 不能说明“已有水位仍重复同步”。
2. A、C 组 `tiflash_schema_trigger_count{type="raft_decode"}` 增量大于 1；目标 TABLE_ID 的 `sync schema cost` 次数与之对应。解析 `Update table_info` 中的 JSON，确认后续同步发布相同的最终 schema，排除真实 DDL 不断变化。
3. B 组只允许首次取得最终 schema 的同步，之后额外列能够跳过；若仍频繁同步，应先检查是否还有其他类型、主键或缺列条件。
4. 三组最终均为 10,000 行，全量逐列与 TiKV 一致，并独立核对 `c_first=id/100`、`c_middle=3*id/100`、`c_last=(7*id+11)/100`。不把零值填充、COUNT 相同当成正确性通过。
5. 观察阶段没有目标 Region 的新 snapshot apply；若积压未形成、由 snapshot 接替或只剩一个批次，记录为未覆盖，不能判定问题不存在。

`covered` 只代表时间戳覆盖；一次 covered 解码尝试仍可能因缺列失败。`skipped_extra_columns` 只统计实际跳过额外列且解码成功的批次，不与 `covered`、`raft_decode` 做互斥求和。

如执行旧版 `v8.5.7` 对照，仍运行相同三组 DDL 并校验全部实际值，但不要求 watermark 指标存在。旧版 B 组也可能因历史额外列重复同步，不能套用新版“只同步一次”的要求；缺列条件需结合最终元数据、积压行和同步日志确认。

## 历史结果、记录与清理

2026-10-29 的成功复现结果如下。具体次数取决于批次划分，新环境要求满足上述判定条件，不要求恰好等于 80。

| 组别 | 解码触发同步 | covered | 成功跳过额外列批次 | 最终数据 |
| --- | ---: | ---: | ---: | --- |
| A：NOT NULL，无 SQL DEFAULT | 80 | 119 | 40 | 10,000 行完全一致 |
| B：可空对照 | 1 | 119 | 119 | 10,000 行完全一致 |
| C：NOT NULL DEFAULT 0，origin_default 为空 | 80 | 119 | 40 | 10,000 行完全一致 |

版本边界：历史二进制基于当时的 PR 工作区，构建时存在 `StorageDeltaMerge.cpp` 中清零 watermark 的本地删除；实际 commit 和该差异完整保存在 build manifest。该次没有注入 schema 更新失败。这不是本计划当前版本组合的重跑结果，也不能将历史次数直接当成分支最新版本的测量值。

原始记录位于仓库根目录下：

- `build-schema-watermark/validation/tiup-not-null-modify-20261029/TEST_REPORT.txt`、`TEST_PLAN.txt`、`summary.json`、`build-manifest.json`。
- `attempt-3/not_null/`、`attempt-3/nullable_control/`：A、B 组证据。
- `attempt-5/not_null_default_zero/`：C 组证据。
- 同目录 `run_repro.py` 是历史自动化入口；依赖相邻 DDL matrix 脚本中的本机组件路径。异机可按本计划手动执行，或先调整这些路径再运行。

每组使用独立结果子目录，保存完整 SQL、真实 PID/停止状态、元数据、DDL 历史、指标前后快照、目标表日志、两份全量 TSV 和实际版本。不要覆盖失败尝试。

无论中途是否失败，先恢复本次暂停的 TiFlash，再恢复 GC、后台 schema sync。仅对本次实际 server PID 执行，确认其仍属于本次集群：

```bash
if [ -n "${TIFLASH_PID:-}" ] && kill -0 "$TIFLASH_PID" 2>/dev/null; then
  kill -CONT "$TIFLASH_PID"
fi
if [ -s "$REPRO_OUT/gc-before.txt" ]; then
  REPRO_GC_OLD=$(cat "$REPRO_OUT/gc-before.txt")
  repro_mysql -e "UPDATE mysql.tidb SET VARIABLE_VALUE='$REPRO_GC_OLD' WHERE VARIABLE_NAME='tikv_gc_life_time'"
fi
repro_debug "DBGInvoke enable_schema_sync_service('true')"
```

最后在终端 A 用 Ctrl+C 停止本次 Playground，确认实际子进程已退出。成功复现不表示该问题已修复。

## 2026-10-30 历史 PR 复现结果

当日曾在独立 TiUP 集群中复测当时的 PR 二进制，重新编译并核验了实际版本和 SHA256，没有复用上述更早的测试产物。该次使用的版本以报告和构建记录为准，不代表分支最新版本或本计划指定组件组合的复测结果。

三组分别触发 80、1、80 次解码 schema 同步，covered 均为 119。两组 NOT NULL 的同步均发布同一份最终 schema，各至少有 79 次同步发生在 watermark 已覆盖的批次中；DEFAULT 0 组最终 Flag=1、default=0、origin_default 为空。每组 10,000 行初始及最终数据均通过全量逐值比较和独立公式校验。

本次在每条 DDL 后及保存最终元数据时，均确认同一真实 TiFlash 进程处于 State T，三条 DDL 全部完成后才恢复；回放阶段没有新 snapshot apply。临时集群已清理，生产源码未修改。

完整流程与原始证据保留在`build-schema-watermark/validation/`（历史本机产物，不随测试分支提交）的 `tiup-missing-columns-*/RESULT_REPORT.md`；按报告日期找到对应运行，实际 commit 和 SHA256 以该次报告及构建记录为准。同目录包含实际命令、SQL、停止状态、列 ID 变化、元数据、指标、日志、全量 TSV 和独立复核结果。

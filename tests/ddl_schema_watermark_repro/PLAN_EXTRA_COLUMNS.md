# 测试计划一：data 多出历史列，导致按批次重复同步 schema

## 目标与触发原理

复现“数据包含当前 schema 已删除的列”，并对照验证 schema read watermark 优化。主用例采用 **DROP 可空列**，避免 NOT NULL 缺列检查干扰结论。

执行顺序必须是：

1. TiKV 提交包含旧列的 UPDATE，TiFlash 尚未 apply。
2. DROP 该列，并让 TiFlash 完整读取、应用删除后的 schema。
3. 释放旧数据积压。此时数据有旧列 ID，而解码 schema 没有它。

未修复版本遇到额外列会返回解码失败，触发同步；同步仍取得删除后的 schema，因此后续批次可能重复同步。优化版本中，若 `max(commit_ts) <= schema_read_tso`，普通 DDL 下可证明额外列已经被移除，直接跳过。

本计划与 `PLAN_MISSING_COLUMNS.md` 独立执行。额外列和缺失列也可能出现在同一行，但本用例只验证额外列路径。

自动化入口为本目录 [run_extra_columns.py](run_extra_columns.py)，参数和版本约定见 [README](README.md)。下述手动步骤可独立执行。

## 环境与版本记录

- Linux，独立 TiUP Playground：1 PD、1 TiKV、1 TiDB、1 TiFlash。
- 旧版 TiFlash 使用 `v8.5.7`；新版 TiFlash 使用 `jinhe/schema-read-watermark-extra-columns` 分支在本轮执行前更新后的源码构建，不固定某个 commit。
- 两版各准备一套完整安装目录，使用独立新集群和新数据目录；TiDB、PD、TiKV 统一使用 `v8.5.7`，其余测试配置保持一致。
- 每次执行记录版本来源、源码实际 commit（本地构建时）、工作区 diff、`tiflash version` 和二进制/动态库 SHA256。以待测二进制的构建记录核对实际版本，不用脚本所在仓库的 HEAD 代替；同一轮测试不更换二进制，下轮重新更新分支并记录版本。
- TiFlash 开启 `ENABLE_FAILPOINTS`，配置 `tcp_port=9000`、日志级别 `debug`。编译时移除代理环境变量，不执行 `make format`。
- TiKV 测试配置关闭本次集群的 Raft 日志 GC，保留积压供日志回放。

启动示例，在终端 A 前台运行。`TIFLASH_TEST_REF` 填本轮版本来源：旧版填 `v8.5.7`，新版填 `jinhe/schema-read-watermark-extra-columns`。替换两个绝对路径：`TIFLASH_BIN` 指向对应版本完整安装目录中的二进制，`REPRO_OUT` 是本轮新结果目录。需要调试接口或暂停点时，先确认安装包支持；如需自行构建旧版，使用 `v8.5.7` tag。

```bash
export TIFLASH_TEST_REF=jinhe/schema-read-watermark-extra-columns
export TIFLASH_BIN=/absolute/path/to/install/tiflash
export REPRO_OUT=/absolute/path/to/new-extra-columns-run
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
tiup playground v8.5.7 --tag schema-extra-repro \
  --db 1 --pd 1 --kv 1 --tiflash 1 --without-monitor \
  --db.port 4000 --kv.config "$REPRO_OUT/tikv.toml" \
  --tiflash.config "$REPRO_OUT/tiflash.toml" \
  --tiflash.binpath "$TIFLASH_BIN"
```

新版 TiFlash 通过 `--tiflash.binpath` 覆盖，TiUP 的 `v8.5.7` 参数用于选择其余组件。若通过 `--db.binpath`、`--kv.binpath`、`--pd.binpath` 覆盖本地组件，也必须选用 `v8.5.7` 并核验实际版本。历史记录使用过其他组件版本，只作为参考，不能代替本版本组合的复测。

终端 B 使用相同的 `TIFLASH_BIN`、`REPRO_OUT`，定义连接和实际端口。TiUP 可能调整冲突端口，应从启动输出、生成配置和进程参数确认。

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

## 数据准备

在终端 B 执行，保存实际 SQL。每次测试一个列位置，使用新表；下列步骤以删除首列为例。

```bash
repro_mysql <<'SQL'
SET GLOBAL tidb_row_format_version=2;
CREATE DATABASE schema_repro_extra;
CREATE TABLE schema_repro_extra.seed(n INT PRIMARY KEY);
INSERT INTO schema_repro_extra.seed VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE schema_repro_extra.t (
  c_first DECIMAL(18,2) NULL,
  id BIGINT PRIMARY KEY CLUSTERED,
  c_middle DECIMAL(18,2) NULL,
  c_last DECIMAL(18,2) NULL
);
INSERT INTO schema_repro_extra.t
SELECT IF(MOD(i,7)=0,NULL,i/100), i,
       IF(MOD(i,11)=0,NULL,3*i/100),
       IF(MOD(i,13)=0,NULL,(7*i+11)/100)
FROM (SELECT 1+a.n+10*b.n+100*c.n+1000*d.n AS i
      FROM schema_repro_extra.seed a, schema_repro_extra.seed b,
           schema_repro_extra.seed c, schema_repro_extra.seed d) s;
ALTER TABLE schema_repro_extra.t SET TIFLASH REPLICA 1;
SQL
```

轮询 `information_schema.tiflash_replica`，等待该表 `AVAILABLE=1`，上限 240 秒。然后强制分别从 TiKV、TiFlash 读取 `ORDER BY id` 的全部列，确认 10,000 行逐值一致；仅副本 AVAILABLE 或 COUNT 相等不足以代替这一步。

保存 TiDB 的 GC 原值，本次独立集群临时改为 `1h`，结束时恢复。保存初始 TABLE_ID 和 TableInfo：

```bash
repro_mysql -e "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_life_time'" > "$REPRO_OUT/gc-before.txt"
repro_mysql -e "UPDATE mysql.tidb SET VARIABLE_VALUE='1h' WHERE VARIABLE_NAME='tikv_gc_life_time'"
repro_mysql -e 'SHOW CREATE TABLE schema_repro_extra.t' > "$REPRO_OUT/show-create-before.txt"
repro_mysql -e "SELECT TABLE_ID,AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA='schema_repro_extra' AND TABLE_NAME='t'" > "$REPRO_OUT/table-id.tsv"
curl --noproxy '*' -fsS "http://127.0.0.1:$TIDB_STATUS_PORT/schema/schema_repro_extra/t" > "$REPRO_OUT/schema-before.json"
repro_debug 'DBGInvoke refresh_table_schema(schema_repro_extra, t)'
```

## 核心步骤：保留旧行，先应用删除后的 schema

先等待相关指标连续三次、每次间隔一秒保持不变，保存观察起点。

```bash
curl --noproxy '*' -fsS "http://127.0.0.1:$TIFLASH_METRICS_PORT/metrics" > "$REPRO_OUT/metrics-before.txt"
REPRO_LOG_START=$(wc -l < "$TIFLASH_LOG")
repro_debug 'DBGInvoke enable_fail_point(pause_before_apply_raft_cmd)'
repro_mysql <<'SQL'
UPDATE schema_repro_extra.t
SET c_first=c_first+1, c_middle=c_middle+1, c_last=c_last+1;
ALTER TABLE schema_repro_extra.t DROP COLUMN c_first;
SQL
repro_debug 'DBGInvoke refresh_table_schema(schema_repro_extra, t)'
curl --noproxy '*' -fsS "http://127.0.0.1:$TIDB_STATUS_PORT/schema/schema_repro_extra/t" > "$REPRO_OUT/schema-after.json"
repro_debug 'DBGInvoke disable_fail_point(pause_before_apply_raft_cmd)'
```

UPDATE 必须已经提交，完整 schema 刷新必须成功返回 `refreshed`，之后才释放暂停点。刷新后核对 `c_first` 的旧 ID 已从 schema 移除；不要只检查列名。

该暂停点用于单表 DROP 测试，不代表停止整个 TiFlash，也不能代替第二份计划的进程级暂停。如果数据提前 apply、未产生多个目标批次或修复版本没有实际跳过列，本轮记为“未覆盖”，扩大数据量或分批 UPDATE 后重做。

## 观察与验收

等待 TiFlash 全量结果与 TiKV 一致，相关指标连续三次、一秒间隔不再变化后保存：

```bash
curl --noproxy '*' -fsS "http://127.0.0.1:$TIFLASH_METRICS_PORT/metrics" > "$REPRO_OUT/metrics-after.txt"
tail -n "+$((REPRO_LOG_START+1))" "$TIFLASH_LOG" > "$REPRO_OUT/tiflash-phase.log"
for REPRO_ENGINE in tikv tiflash; do
  repro_mysql -e "SET tidb_isolation_read_engines='$REPRO_ENGINE'; SET tidb_allow_mpp=1; SET tidb_enforce_mpp=$([ "$REPRO_ENGINE" = tiflash ] && printf 1 || printf 0); SELECT id,c_middle,c_last FROM schema_repro_extra.t ORDER BY id" > "$REPRO_OUT/$REPRO_ENGINE-rows.tsv"
done
diff -u "$REPRO_OUT/tikv-rows.tsv" "$REPRO_OUT/tiflash-rows.tsv"
repro_mysql -e 'SHOW CREATE TABLE schema_repro_extra.t; ADMIN SHOW DDL JOBS 10' > "$REPRO_OUT/ddl.txt"
```

统计的是同一进程内的指标增量：

| 检查 | 未修复版本 | 优化版本 |
| --- | --- | --- |
| `tiflash_schema_trigger_count{type="raft_decode"}` | 多个带旧列批次可能反复触发同步 | 本用例已显式刷新完整 schema，预期增量为 0 |
| `tiflash_schema_watermark_decode_count{type="covered"}` | 可能无此指标 | 大于 0 |
| `tiflash_schema_watermark_decode_count{type="skipped_extra_columns"}` | 可能无此指标 | 大于 0，不能只看 covered |
| 10,000 行所有剩余列 | 与 TiKV 相同 | 与 TiKV 相同 |

同时检查：

- `sync schema cost` 日志按目标 TABLE_ID 统计，排除其他表同步。一次显式 refresh 单独记录，不算解码触发同步。
- 跳过采样日志中的 `read_tso` 对应 `schema_read_tso`；应满足 `max_commit_ts <= read_tso`，未知列 ID 为正且确为已删除的 ID。日志是采样的，不要求每个批次都有记录。
- 回放观察阶段没有目标 Region 的新 snapshot apply。若由 snapshot 接替了日志积压，不能据此判断普通 apply 优化有效。
- 比较所有值与 NULL，不只比较 COUNT。剩余列应满足原始公式加 1，原 NULL 仍为 NULL。
- 如果基线只有一个带额外列的批次，本轮只能验证解码行为，不能作为“重复同步”复现成功。

## 位置矩阵与扩展

在新表上分别 DROP `c_first`、`c_middle`、`c_last`，更新全量比较中的剩余列列表。另可用可空 DECIMAL 列做需要 reorg 的 MODIFY：暂停整个 TiFlash，执行 `DECIMAL(18,2) -> DECIMAL(20,3)`，再恢复，让首次自然失败同步最终 schema；后续历史额外列应不再逐批触发同步。

新增列反向检查：让 TiFlash 持有旧 schema 后暂停进程，ADD COLUMN 并 INSERT 一个该列值为 `424242` 的新行，再恢复。优化版本应走 `beyond_watermark` 和 schema-sync fallback，最终值不能丢失。

## 清理和已有证据

无论步骤是否成功，都释放暂停点、恢复 GC 原值和后台 schema sync，然后在终端 A 用 Ctrl+C 停止本次 Playground，并确认实际 TiFlash 子进程已退出。清理命令（仅对本次实例执行，GC 文件存在且非空时恢复）：

```bash
repro_debug 'DBGInvoke disable_fail_point(pause_before_apply_raft_cmd)'
if [ -s "$REPRO_OUT/gc-before.txt" ]; then
  REPRO_GC_OLD=$(cat "$REPRO_OUT/gc-before.txt")
  repro_mysql -e "UPDATE mysql.tidb SET VARIABLE_VALUE='$REPRO_GC_OLD' WHERE VARIABLE_NAME='tikv_gc_life_time'"
fi
repro_debug "DBGInvoke enable_schema_sync_service('true')"
```

保存版本、SHA256、SQL、TableInfo、指标前后快照、目标表日志和两份完整 TSV。

2026-10-29 的历史结果中，MODIFY 三个位置均为 40 次同步降至 1 次；DROP 三个位置在完整刷新后均为 0 次，数据一致。这些记录使用当时的 PR 版本和对照基线，旧版并非本计划指定的 `v8.5.7`，不能作为本轮版本组合的验收结果。原始记录保留在`build-schema-watermark/validation/`（历史本机产物，不随测试分支提交）：

- 在 `tiup-current-ddl-matrix-*/` 下查阅 `TEST_PLAN.txt`。
- 同目录 `RESULT_REPORT.txt`、`audit.json`、`run_matrix.py`。
- `baseline-attempt-1/`、`fixed-attempt-1/` 中各用例的完整结果。

异机可直接按上述步骤执行，不依赖历史脚本内的本机二进制路径；结果必须记录实际待测二进制版本。

## 2026-10-30 历史复测结果

当日曾在两套全新 TiUP 集群中对比当时的未修复基线和 PR 二进制。DROP 首列、中间列、末列时，基线分别出现 8、4、2 次解码触发同步；PR 均为 0 次，并分别实际跳过 4、1、9 个带额外列的批次。每个用例的 10,000 行剩余列值和 NULL 均通过全量比较，观察阶段没有 snapshot apply。这是历史记录，不是 `v8.5.7` 与分支最新版本的复测结果。

两版本的 ADD COLUMN 反向检查也通过，新增列值 `424242` 均正确；PR 确实走了 `beyond_watermark` 和同步回退。本轮共 8 个用例通过。可选 MODIFY、重启矩阵和独立的缺列计划未在本轮重跑。

完整执行流程和证据保留在`build-schema-watermark/validation/`（历史本机产物，不随测试分支提交）的 `tiup-extra-columns-*/RESULT_REPORT.md`；按报告日期找到对应运行，实际 commit 和 SHA256 以该次报告及构建记录为准。同目录保存实际命令、SQL、TableInfo、指标、日志、完整 TSV 和独立复核结果。两个临时集群均已退出。

补充控制限制：本次未确认全局一次性暂停点是否由目标 Region 命中，中间列和末列用例在 schema 刷新前已处理部分更新，所以不能宣称 10,000 行全部积压，也不属于固定批次数的严格对比。8、4、2 是实际观察到的重复同步次数，不是三种列位置的固定预期。它们证明问题发生，不能用于比较位置间的触发频率；详细依据见报告中的计数解释。

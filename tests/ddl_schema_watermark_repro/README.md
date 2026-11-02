# Schema watermark TiUP 测试

本目录在独立分支 `jinhe/schema-watermark-tiup-tests` 维护，基于核心优化分支 `jinhe/schema-read-watermark-extra-columns`。包含测试计划、复现脚本和 TLA+ 模型，运行结果、历史日志及二进制不提交到仓库。

## 计划与入口

| 计划 | 自动化入口 | 范围 |
| --- | --- | --- |
| [额外列复现](PLAN_EXTRA_COLUMNS.md) | [run_extra_columns.py](run_extra_columns.py) | DROP 首列、中间列、末列；实际跳过与完整值校验；ADD 新列反向检查 |
| [缺失列复现](PLAN_MISSING_COLUMNS.md) | [run_missing_columns.py](run_missing_columns.py) | 连续三次 MODIFY；NOT NULL、可空及 DEFAULT 0 三组；验证已有 watermark 下仍重复同步的独立问题 |
| [24 小时重启](PLAN_RESTART_SOAK_24H.md) | [run_restart_soak.py](run_restart_soak.py) | 仅验证新版 TiFlash；持续负载、真实重启、停机 DDL 和完整值校验 |
| 单轮 DDL/重启矩阵 | [run_matrix.py](run_matrix.py) | 新版执行普通、SIGTERM、SIGKILL × MODIFY/DROP × 首/中/末列；旧版执行普通 DDL 对照 |
| [TLA+ 有限模型验证](tla/README.md) | [tla/run_models.py](tla/run_models.py) | schema/watermark 配对、批次覆盖、失败和崩溃交错；三个故障反例和六个可达性见证 |

[restartable_tiflash.py](restartable_tiflash.py) 是共享 wrapper，通过 SIGUSR1 请求正常重启、SIGUSR2 请求强杀重启；存在 `restart.gate` 时不启动替代进程，允许在实际停机窗口写入和执行 DDL。

`run_matrix.py` 只有单轮矩阵，没有持续写入及 24 小时调度功能，不能通过重复调用就声称通过长跑计划。缺失列脚本的成功表示复现条件成立且数据正确，不表示已修复缺列问题。

重启长跑发现的 proxy Ready 分页调度停滞，其复现步骤、根因、最小候选修复及验证记录集中在 [PROXY_READY_RECOVERY_BUG.md](PROXY_READY_RECOVERY_BUG.md)。

## TLA+ / TLC

模型入口为 [tla/SchemaWatermark.tla](tla/SchemaWatermark.tla)，完整说明及历史验证结果见 [tla/README.md](tla/README.md)，反例解释见 [tla/COUNTEREXAMPLES.md](tla/COUNTEREXAMPLES.md)。独立运行只需要 Python 3.9+ 和 Java 11+，不需要下面的 TiUP 组件或 PyMySQL。

```bash
python3 tests/ddl_schema_watermark_repro/tla/run_models.py --download-tools
```

首次下载固定版本的官方 JAR 并校验 SHA256；离线可传 `--tlc-jar`。结果默认保存到 `tla/results/<UTC 时间戳>/`，也可用 `--output` 指定不存在的新目录。执行器会核对正确模型无错误、故障模型违反指定安全不变量、见证模型到达指定事件，并自动复核原始日志和文件哈希。有限模型通过不等于完成 C++ 实现的无界形式化证明。

## 版本与环境

- Linux；Python 3.9+、TiUP Playground 和 MySQL CLI 在 PATH 中。
- TiDB、PD、TiKV 的二进制路径显式传入。最初对照环境约定 `v8.5.7`，新版 TiFlash 应选择下述兼容组件组合；单轮脚本通过 `--component-version` 核对预期版本，长跑默认记录实际版本。
- 旧版 TiFlash 为 `v8.5.7`，使用 `--baseline` 选择旧版预期；新版使用 `jinhe/schema-read-watermark-extra-columns` 在执行前更新后的源码构建。TiFlash 使用完整安装目录及匹配的 proxy、运行库。
- 需要 TiFlash 调试接口和 failpoint；自行编译时移除代理环境变量，不执行 `make format`。
- 每次选择未使用的 TiUP tag、端口和全新结果目录；脚本启动自己的 1 PD/1 TiKV/1 TiDB/1 TiFlash 集群，不复用已有集群。
- 每次运行记录组件实际版本、二进制/运行库/脚本 SHA256、启动命令、实际 metrics 端口和 TiFlash 构建 commit。可用 `--expected-commit` 核对本轮构建记录，不在脚本中写死 commit，也不将脚本仓库的 HEAD 当成二进制版本。

上面的 `v8.5.7` 是最初对照环境约定。当前开发版 TiFlash 使用 PD `GetGCState`，PD `v8.5.7` 不提供该 RPC，实测会导致后台任务等待。长跑应选择兼容的组件组合；其入口默认只记录实际组件版本，不固定版本。其他脚本如改用兼容组件，应传 `--component-version` 对应实际版本（不要把 Nightly 分发别名当成 Release Version）。

## 运行示例

先准备好二进制并确认端口可用。下面的路径均需替换；结果目录不能预先创建，父目录可以存在。

```bash
REPRO_RUN_ID=$(date +%Y%m%d-%H%M%S)
REPRO_TIFLASH=/absolute/path/to/branch-install/tiflash
REPRO_TIDB=/absolute/path/to/v8.5.7/tidb-server
REPRO_TIKV=/absolute/path/to/v8.5.7/tikv-server
REPRO_PD=/absolute/path/to/v8.5.7/pd-server
REPRO_RESULTS=/absolute/path/to/results
REPRO_ARGS=(
  --tiflash "$REPRO_TIFLASH"
  --tidb "$REPRO_TIDB" --tikv "$REPRO_TIKV" --pd "$REPRO_PD"
  --offset 30000 --debug-port 49000
)

python3 tests/ddl_schema_watermark_repro/run_extra_columns.py \
  "${REPRO_ARGS[@]}" \
  --tag "schema-extra-$REPRO_RUN_ID" \
  --output "$REPRO_RESULTS/extra-$REPRO_RUN_ID"
```

旧版额外列对照：将 `--tiflash` 改为 `v8.5.7` 的完整安装目录中的二进制，再增加 `--baseline`，使用新的 tag 和结果目录。

上一轮集群清理完成后，可复用上述连接参数执行缺列复现或单轮矩阵：

```bash
python3 tests/ddl_schema_watermark_repro/run_missing_columns.py \
  "${REPRO_ARGS[@]}" \
  --tag "schema-missing-$REPRO_RUN_ID" \
  --output "$REPRO_RESULTS/missing-$REPRO_RUN_ID"

python3 tests/ddl_schema_watermark_repro/run_matrix.py \
  "${REPRO_ARGS[@]}" \
  --tag "schema-matrix-$REPRO_RUN_ID" \
  --output "$REPRO_RESULTS/matrix-$REPRO_RUN_ID"
```

缺失列自动化脚本只针对新版，因为其断言需要 watermark 指标。旧版缺列对照可按计划手动执行。`--help` 只展示参数，不启动集群。

## 24 小时重启执行器

安装 [requirements.txt](requirements.txt) 中的 PyMySQL。写入 worker 使用独立持久连接，确认标记与行变更在同一事务提交；客户端断线后通过唯一确认标记解决提交结果不明的问题。

恢复停滞调查可另运行 [monitor_apply_progress.py](monitor_apply_progress.py)，对齐 TiKV、proxy 的 `/region/{id}` 与 C++ `dump_all_region`。例如：

```bash
python3 tests/ddl_schema_watermark_repro/monitor_apply_progress.py \
  --tiflash /absolute/path/to/diagnostic-install/tiflash \
  --tikv http://127.0.0.1:26180 --proxy http://127.0.0.1:26292 \
  --debug-port 29000 --regions 143 150 --interval 2 --stall-seconds 20 \
  --output /absolute/path/to/new-diagnostic-output
```

结果保存在 `samples.jsonl` 和 `alerts.jsonl`。支持新增 `apply_diagnostics` 字段的诊断 proxy 可区分未投递、已投递未完成、RawNode 有 Ready 但 FSM 未轮询等状态；旧二进制仍能采样，但不能判断投递阶段。初始诊断版仅增加观测信息；后续 Ready 调度修复及验证结果见 [问题记录](PROXY_READY_RECOVERY_BUG.md)。运行时需记录对应 patch 与二进制 SHA256。`--pid-file` 可指定重启控制器保存的实际 TiFlash 子进程 PID 文件，避免跨重启累计停滞时间。接口不可用会中断连续停滞证据，commit 增长本身不会清除 apply 停滞计时。监控不写数据、不触发新 apply 任务；状态请求仍会经过 peer FSM，因此复现时须记录采样频率，不能假定观测完全不影响时序。

```bash
python3 -m pip install -r tests/ddl_schema_watermark_repro/requirements.txt

SOAK_RUN_ID=$(date +%Y%m%d-%H%M%S)
SOAK_ARGS=(
  --tiflash /absolute/path/to/branch-install/tiflash
  --tidb /absolute/path/to/compatible/tidb-server
  --tikv /absolute/path/to/compatible/tikv-server
  --pd /absolute/path/to/compatible/pd-server
  --offset 6000 --debug-port 29000
)

# 先预演四轮真实重启，包含第三轮清理旧表、第四轮重启。预演不算 24 小时验收。
python3 tests/ddl_schema_watermark_repro/run_restart_soak.py \
  "${SOAK_ARGS[@]}" --mode smoke \
  --tag "schema-soak-smoke-$SOAK_RUN_ID" \
  --output "/absolute/path/to/results/smoke-$SOAK_RUN_ID"

# 正式执行：默认 24 小时，使用全新 tag、结果目录。
python3 tests/ddl_schema_watermark_repro/run_restart_soak.py \
  "${SOAK_ARGS[@]}" --duration 24h \
  --build-manifest /absolute/path/to/build-manifest.json \
  --tag "schema-soak-$SOAK_RUN_ID" \
  --output "/absolute/path/to/results/soak-$SOAK_RUN_ID"
```

默认参数为 4 个 worker、启用时 200 行变更/秒、两张各 200,000 行的负载表、10,000 行 DDL 表；在停机期间写入并执行 DDL，每轮暂停 writer 后比较全部实际值、NULL 和独立预期。每 12 轮检查新增列，后台同步、进程身份、store_id、日志和资源按计划记录。

正式运行需传实际 TiFlash 构建清单：JSON 的 `head` 对应二进制实际 Git Commit Hash，`source_sha256` 保存构建时相关源码校验值；清单同时应保留构建时工作区 diff 或其证据。执行器核对 commit 并复制清单到结果目录，避免把测试脚本所在分支当成产品二进制的来源。短预演可不传构建清单。

`--warmup`、`--final-window`、`--interval`、`--round-timeout`、`--catchup-timeout` 接受 `s/m/h` 时长；资源阈值可通过 `--start-free-gib`、`--min-free-gib`、`--min-free-fraction`、`--max-log-gib` 设置。`--mode smoke` 自带小规模参数，`--mode soak --duration` 过短不会被报告为通过完整 24 小时验收。

启动前拒绝已有数据目录的 TiUP tag 和被占用的固定端口，避免误接已有集群。退出或失败时先停止写入、恢复暂停进程和测试配置，再关闭本次集群；保留数据、日志及失败证据。控制器收到 SIGTERM/SIGINT 时中止并清理，不自动从不确定阶段恢复。可按 `state.json`、`events.jsonl`、`operations.jsonl` 和 `failures/` 排障；下次运行必须选新目录，不盲目重放旧状态。

Linux 上还会拒绝落入 `/proc/sys/net/ipv4/ip_local_port_range` 的监听端口，避免停机期间被自动分配为出站连接端口。示例偏移适用于本机 `32768–60999` 范围；实际运行前检查当前范围和端口占用。启动后再次核对 TiUP 实际分配的 TiFlash 端口，包括 metrics。执行器不修改系统端口设置，也不终止其他集群。

结果查看 `FINAL_REPORT.md`、`result.json` 和 `audit.json`。正式验收必须 `passed=true` 且 `acceptance_24h=true`；短预演只能得到 `smoke_passed`。运行控制器回归测试：

```bash
python3 -m unittest discover -s tests/ddl_schema_watermark_repro -p test_restart_soak.py -v
```

## 结果与限制

查看进程退出码和结果目录的 `result.json`；要求 `passed=true`、GC 恢复成功、实际 TiFlash 子进程已退出，同时检查各用例完整行比较及指标/日志证据。清理失败不能当成整轮通过。不要覆盖失败结果。

DROP 用例沿用全局一次性的 `pause_before_apply_raft_cmd`：它不能保证所有 Region 的数据全部停住。没有多个目标积压批次或没有实际跳过时，不能声称覆盖了优化；不同位置的同步次数也不能直接比较。MODIFY/缺列用例使用整个实际进程的 SIGSTOP/SIGCONT，真实重启用例使用 SIGTERM/SIGKILL。

脚本从真实进程参数读取 metrics 端口。重启后计数器从新进程重新计算；恢复窗口的同步次数不要求固定为 0 或 1，后续受控 DDL 检查另行验证优化。

计划末尾的历史结果仅供理解复现方式。其完整报告保留在原测试机的本地结果目录，不包含在本分支；不能据此声称 `v8.5.7` 与当前分支的新版本组合已经复测。

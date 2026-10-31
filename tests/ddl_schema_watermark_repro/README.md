# Schema watermark TiUP 测试

本目录在独立分支 `jinhe/schema-watermark-tiup-tests` 维护，基于核心优化分支 `jinhe/schema-read-watermark-extra-columns`。包含测试计划和复现脚本，运行结果、历史日志及二进制不提交到仓库。

## 计划与入口

| 计划 | 自动化入口 | 范围 |
| --- | --- | --- |
| [额外列复现](PLAN_EXTRA_COLUMNS.md) | [run_extra_columns.py](run_extra_columns.py) | DROP 首列、中间列、末列；实际跳过与完整值校验；ADD 新列反向检查 |
| [缺失列复现](PLAN_MISSING_COLUMNS.md) | [run_missing_columns.py](run_missing_columns.py) | 连续三次 MODIFY；NOT NULL、可空及 DEFAULT 0 三组；验证已有 watermark 下仍重复同步的独立问题 |
| [24 小时重启](PLAN_RESTART_SOAK_24H.md) | 长跑执行器待实现 | 仅验证新版 TiFlash；持续负载、真实重启、停机 DDL 和完整值校验 |
| 单轮 DDL/重启矩阵 | [run_matrix.py](run_matrix.py) | 新版执行普通、SIGTERM、SIGKILL × MODIFY/DROP × 首/中/末列；旧版执行普通 DDL 对照 |

[restartable_tiflash.py](restartable_tiflash.py) 是共享 wrapper，通过 SIGUSR1 请求正常重启、SIGUSR2 请求强杀重启；存在 `restart.gate` 时不启动替代进程，允许在实际停机窗口写入和执行 DDL。

`run_matrix.py` 只有单轮矩阵，没有持续写入及 24 小时调度功能，不能通过重复调用就声称通过长跑计划。缺失列脚本的成功表示复现条件成立且数据正确，不表示已修复缺列问题。

## 版本与环境

- Linux；Python 3.9+、TiUP Playground 和 MySQL CLI 在 PATH 中。
- TiDB、PD、TiKV 使用 `v8.5.7`。所有组件二进制路径显式传入，脚本启动前检查实际版本。
- 旧版 TiFlash 为 `v8.5.7`，使用 `--baseline` 选择旧版预期；新版使用 `jinhe/schema-read-watermark-extra-columns` 在执行前更新后的源码构建。TiFlash 使用完整安装目录及匹配的 proxy、运行库。
- 需要 TiFlash 调试接口和 failpoint；自行编译时移除代理环境变量，不执行 `make format`。
- 每次选择未使用的 TiUP tag、端口和全新结果目录；脚本启动自己的 1 PD/1 TiKV/1 TiDB/1 TiFlash 集群，不复用已有集群。
- 每次运行记录组件实际版本、二进制/运行库/脚本 SHA256、启动命令、实际 metrics 端口和 TiFlash 构建 commit。可用 `--expected-commit` 核对本轮构建记录，不在脚本中写死 commit，也不将脚本仓库的 HEAD 当成二进制版本。

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

## 结果与限制

查看进程退出码和结果目录的 `result.json`；要求 `passed=true`、GC 恢复成功、实际 TiFlash 子进程已退出，同时检查各用例完整行比较及指标/日志证据。清理失败不能当成整轮通过。不要覆盖失败结果。

DROP 用例沿用全局一次性的 `pause_before_apply_raft_cmd`：它不能保证所有 Region 的数据全部停住。没有多个目标积压批次或没有实际跳过时，不能声称覆盖了优化；不同位置的同步次数也不能直接比较。MODIFY/缺列用例使用整个实际进程的 SIGSTOP/SIGCONT，真实重启用例使用 SIGTERM/SIGKILL。

脚本从真实进程参数读取 metrics 端口。重启后计数器从新进程重新计算；恢复窗口的同步次数不要求固定为 0 或 1，后续受控 DDL 检查另行验证优化。

计划末尾的历史结果仅供理解复现方式。其完整报告保留在原测试机的本地结果目录，不包含在本分支；不能据此声称 `v8.5.7` 与当前分支的新版本组合已经复测。

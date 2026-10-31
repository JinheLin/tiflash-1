# Schema watermark 真实重启 24 小时测试与验证计划

目标是在持续写入和反复重启中验证：TiFlash 能恢复停机期间的写入和 DDL，重新建立与解码 schema 对应的 `schema_read_tso`；历史额外列仍能跳过，新增有效列不会丢失，最终存储值始终正确。

本计划仅验证新版 TiFlash，使用 `jinhe/schema-read-watermark-extra-columns` 分支在本轮执行前更新后的源码构建，验证 PR #11168，不固定某个 commit。TiDB、PD、TiKV 统一使用 `v8.5.7`。正式执行时记录实际二进制版本和 SHA256，历史重启结果不能代替本轮验收。

这是一份执行规格。已有脚本提供单轮重启和校验能力，24 小时调度、持续负载及下述检查点需要由长跑执行器串接；不要向现有脚本传入未实现的 `--duration` 等参数。本次编写计划，不启动 24 小时集群。

## 环境和执行参数

使用独立新 TiUP Playground：1 PD、1 TiKV、1 TiDB、1 TiFlash。服务可部署在同一测试机；测试只重启真实 TiFlash server，TiDB、PD、TiKV 和 TiUP wrapper 持续运行。整个测试复用同一个 TiFlash 数据目录和 store_id。

| 参数 | 默认值或要求 |
| --- | --- |
| 运行时长 | 24 小时，使用单调时钟计算截止时间 |
| 阶段 | 预热 30 分钟，循环测试 23 小时，最终核验 30 分钟 |
| 重启节奏 | 每 10 分钟最多开始一轮，前一轮未完成不启动下一轮 |
| 最低覆盖 | 120 个完整成功轮次，12 个组合各至少 10 轮 |
| 退出方式 | SIGTERM 正常退出、SIGKILL 强制退出，各至少 60 轮 |
| 停机期间操作 | 单次 MODIFY 或 DROP，分别覆盖首列、中间列、末列 |
| 每张 DDL 表 | 10,000 行，三个可空 DECIMAL 业务列 |
| 持续写入 | 初始 200 行变更/秒，4 个写入 worker，固定随机种子 |
| 负载表 | 两张表，各限制在 200,000 个主键的范围内 |
| 编码 | RowV2；RowV1 已有单测，另加编码矩阵不作为本次前提 |
| reorg | worker=1，batch=256，执行后读取实际变量确认 |
| 单轮时间上限 | 8 分钟；超时停止进入新轮次并保留证据 |
| TiFlash 正常退出 | 45 秒内真实退出且返回码为 0 |
| TiFlash 启动就绪 | 新 PID 产生后 120 秒内 debug 和 metrics 可用 |
| 恢复追平 | 服务就绪后 180 秒内到达本轮明确的校验点 |

120 轮是覆盖下限，单纯运行满 24 小时不能算通过。主循环有约 138 个 10 分钟时隙；轮次失败不能通过增加其他组合的成功数抵消。

正式运行前做一次小规模预演，检查启动、两种信号、gate、数据导出及清理。预演使用独立 tag，结果不计入 24 小时覆盖。

## 构建和启动记录

保存以下信息到本轮独立结果目录，不覆盖历史运行：

- TiFlash 版本来源 `jinhe/schema-read-watermark-extra-columns`、源码实际 commit、工作区 diff、相关源码 SHA256；`tiflash version`、二进制和动态库 SHA256。源码版本从实际构建记录取得，不使用脚本所在仓库的 HEAD 代替。
- TiDB、TiKV、PD、proxy 的实际版本和二进制 SHA256，完整启动命令、实际端口和生成配置。
- run_id、固定随机种子、负载参数、开始/截止时间、预期组合次数。
- 真实 TiFlash PID、`/proc/<pid>/stat` 的 starttime、cmdline、cwd、数据路径、PD 中的 TiFlash store_id。

每轮测试执行前更新待测分支、构建并核验二进制，版本确认后才开始计时；这里的一轮指完整 24 小时测试，不是每次进程重启。需要编译时移除所有名称包含 proxy 的环境变量；不执行 `make format`。使用完整 TiFlash 安装目录，24 小时期间不更新分支、不替换二进制或依赖。下一次完整测试再更新分支并记录新的实际版本。

启动使用 `tiup playground v8.5.7`，通过 `--tiflash.binpath` 指定支持 gate 的 wrapper；wrapper 配置的真实 server 指向新版完整安装目录中的 TiFlash 二进制。若覆盖 `--db.binpath`、`--kv.binpath`、`--pd.binpath`，相应本地组件也须为 `v8.5.7`，并核验实际版本。

启用 TiFlash debug 日志和调试端口，固定端口前检查冲突。TiUP 可能重新分配 metrics 端口，启动后读取真实进程参数，不能只按偏移量推算。

本次独立 TiKV 关闭 Raft log GC，避免积压被 snapshot 替代；保存 SQL GC lifetime 原值，临时改为 `1h`。因关闭 Raft log GC，必须按下述资源规则限制写入和磁盘增长。正式计时前保存实际生效配置。

## 表和负载

### 持续写入表

两张负载表使用固定 schema，至少包含主键、`last_seq`、数值、可空列和可确定生成的字符串。INSERT、UPDATE、DELETE 的比例为 20%、60%、20%；更新使用由操作序号推导出的绝对值，避免重试使增量重复累加。

每个事务带唯一 `op_id`，在同一 TiKV 事务中更新数据并写入操作确认表。执行器保存操作日志和提交结果；遇到客户端超时，查询确认表解决提交状态不明的问题，不能将“超时”直接当作未提交并重复执行。

开始一个重启轮次时写入可核对的序号标记。SIGTERM/SIGKILL 发生时负载 worker 继续工作；确认 TiFlash 已退出后，TiDB 仍继续接收写入。这个过程用于验证真正的停机积压，不用 SIGSTOP 代替退出。

每轮最多保留当前及上一轮的 DDL 表；较早表在完整证据保存成功后清理。负载表主键范围固定，避免 24 小时内无限增加活动表和当前行数。

### 每轮 DDL 表

使用新表名，保留完整建表 SQL。字段顺序如下，`id` 固定在第二个物理位置：

```sql
CREATE TABLE t_round_N (
    c_first  DECIMAL(18,2) NULL,
    id       BIGINT PRIMARY KEY CLUSTERED,
    c_middle DECIMAL(18,2) NULL,
    c_last   DECIMAL(18,2) NULL
);
```

写入 `id=1..10000`。非 NULL 值分别为 `id/100`、`3*id/100`、`(7*id+11)/100`；三个业务列分别在 `id%7=0`、`id%11=0`、`id%13=0` 时为 NULL。初始设置 TiFlash replica=1，等待 AVAILABLE，并比较两种引擎的全部行后才允许重启。

业务列始终可空。NOT NULL 且缺少 origin_default 的重复同步问题使用独立的 [缺失列计划](PLAN_MISSING_COLUMNS.md)，不混入本次优化验收。

## 轮次矩阵和时间安排

完整矩阵为 2 种退出方式 × 2 种 DDL × 3 个列位置，共 12 个组合：

| 退出方式 | 停机期间 DDL | 目标列 |
| --- | --- | --- |
| SIGTERM | MODIFY | c_first、c_middle、c_last |
| SIGTERM | DROP | c_first、c_middle、c_last |
| SIGKILL | MODIFY | c_first、c_middle、c_last |
| SIGKILL | DROP | c_first、c_middle、c_last |

按固定种子打乱每一组 12 个组合，完成一组再开始下一组，防止随机调度长期遗漏某一项。最少完成 10 组。

- 00:00—00:30：预热负载、初始全量校验、确认资源和配置。预演已在正式计时前完成。
- 00:30—23:30：持续写入并执行上述矩阵；每轮重启后还执行一次优化检查。每完成 12 轮加一次 ADD COLUMN 反向检查。
- 23:30—24:00：不再启动重启和 DDL，停止写入并等待提交完成，执行最终全量校验、归档和清理。未完成的轮次不能挤占最终核验时间。

前 12 轮的恢复窗口关闭后台 schema sync，便于观察按需同步；后续恢复窗口开启后台 sync，覆盖日常并发。重启后进程参数不变，debug 可用时显式设置本轮期望状态并记录生效时点；不假定该开关跨进程保留。

所有确定性优化检查都暂时关闭后台 sync、暂停本次写入 worker 并排空其他表积压，避免把全局指标变化归到错误的目标表。检查完成后恢复后台 sync 和持续写入。

## 每轮真实重启和恢复

1. 新建本轮 DDL 表并完成初始全量校验，保存 TableInfo、table_id、列 ID、Region、PID 身份和当前日志偏移。
2. 创建 `restart.gate`，确认 wrapper 支持该 gate。请求 SIGTERM 或 SIGKILL 重启，让 wrapper 向真实子进程发送对应信号。
3. 等待 wrapper 记录旧 PID 的退出码，并确认旧进程已退出。SIGTERM 必须返回 0，SIGKILL 必须返回 -9；出现 `shutdown_timeout` 后被强杀不能算正常退出成功。gate 保持存在，确认没有新 server 提前启动。
4. 在 TiFlash 完全停机期间，对本轮 DDL 表提交所有业务列 `+1` 的 UPDATE，再执行目标 DDL。MODIFY 使用 `DECIMAL(18,2) -> DECIMAL(20,3) NULL DEFAULT NULL`，DROP 删除目标列。保存提交结果、DDL job 和 TiDB status 接口返回的最终 TableInfo。
5. 确认目标 DDL 真实完成。MODIFY 要求目标列 ID 已替换；DROP 要求旧 ID 已移除，其他列 ID 不变。继续累积 20—60 秒写入，实际停机窗口计入证据。
6. 释放 gate，记录新 PID、starttime、版本、参数、cwd、路径和 store_id。必须是新进程、同一安装目录、同一数据目录、同一 store_id；观察到自动删除 store 或改目录重建时，本轮失败。
7. 等待服务就绪和积压追平；此时恢复计时与等待过程中的 SQL 重试单独记录。不能把完整比较和采样时间算作纯恢复耗时。
8. 到达下面的数据校验点，比较本轮 DDL 表和持续负载表。验证本次恢复后的新增 UPDATE、INSERT、DELETE 也能落到 TiFlash。
9. 执行重启后的优化检查，再恢复常规负载，保存结果后进入下一个时隙。

恢复窗口的 schema-sync 次数不写死为 0 或 1：新进程初始化、后台同步和按需同步的时机不同。验收重点是数据正确、新 schema/read pair 能重新建立，以及后续受控优化检查通过。

## 重启后的优化和反向检查

### 每轮验证历史额外列仍能跳过

在新进程中另建一张 10,000 行的可空 DECIMAL 表，用相同初始公式及 NULL 分布。待数据、初始 schema 及所有相关指标稳定后，执行：

1. 关闭后台 sync，暂停写入 worker，确认之前各表积压已排空。
2. 刷新一次初始完整 schema，保存成功响应；记录目标表 ID、指标和日志起点。
3. SIGSTOP 真实 TiFlash server，并确认 State=T。执行一个目标位置的 MODIFY，保存修改后的 TableInfo，确认同一进程在 DDL 全程保持停止。
4. SIGCONT 恢复，让第一次自然解码失败触发完整 schema 同步，之后的积压批次继续解码。
5. 等待目标表全部行正确及指标连续三次、一秒间隔保持稳定。核对目标表日志与指标，并全量导出比较。

该检查必须满足：解码触发同步恰好 1 次、`covered>0`、`skipped_extra_columns>0`、剩余值和 NULL 全部正确。若没有多批次积压或没有实际跳过，记为未覆盖，不能算通过；固定数据规模不足时，应在预演阶段扩大规模并固定到本次配置中。

不得复用 `pause_before_apply_raft_cmd` 作为整个表的暂停机制：它是全局一次性暂停点，不能保证全部目标数据尚未 apply。

跳过日志中的 `read_tso` 即 `schema_read_tso`。每个出现的采样记录都必须满足 `0 < max_commit_ts <= read_tso`，未知列 ID 属于对应旧表定义且已从完整 schema 中移除。采样不是逐批日志；实际跳过由计数与正确数据共同确认，不强制每个表都有日志。每小时至少保留一条本次运行的有效采样证据。

重启后不要求首个解码一定是 NoWatermark：后台或初始化同步可能已先建立新 watermark。不能比较两个进程的指标绝对值；每次重启都重新读取新进程基线。

### 每完成一组 12 轮验证新增列不会被误跳

新建小表，插入初始行，等待 TiFlash 追平并完整刷新旧 schema，然后 SIGSTOP 真实进程，确认 State=T。ADD 一个可空列，INSERT 一行并为新列设置非零哨兵值，例如 `424242 + 轮次号`，再恢复。

要求 `beyond_watermark>0`、目标表发生 schema-sync fallback，新行哨兵值及全部其他值与 TiKV 完全一致。不能只检查行数或 SUM；保留完整逐行结果。该检查同样在暂停本次其他写入、关闭后台 sync 的隔离窗口执行。

## 数据校验点和独立预期

主校验点是每轮追平后；另外每小时和结束时执行全量校验。连续写入期间可以做健康采样，但不能把两个不同时间查询的结果直接相减判定丢值。

校验步骤必须是：暂停本次所有 writer 和 DDL 提交，等待在途事务返回并解决状态不明的提交；记录操作确认表中的截止序号；在数据不再变化的窗口，分别强制 TiKV、TiFlash 执行 `ORDER BY id` 全量查询，比较全部列及 NULL。核验完成才恢复写入。每小时的检查复核最近一个完整轮次的校验点；若该小时没有完整轮次，则单独建立校验点，不对仍在变化的数据作比较。

查询显式设置引擎，并保存 EXPLAIN，确认 TiFlash 查询没有回退 TiKV。等待 TiFlash 追平时允许短暂重试；到达恢复上限仍有差异则保留实际差异并判失败。若已经确认快照/校验点一致且存在真实差异，不能通过继续写入掩盖它。

两类独立预期必须同时保留：

- DDL 表按固定公式计算。停机 UPDATE 后非 NULL 值为原公式加 1，NULL 不变；DROP 忽略已删除列，MODIFY 只改变精度，不应改变数值。
- 负载表按固定种子、操作序号和已确认提交日志重建预期，确认 INSERT/UPDATE/DELETE 和哨兵值。两引擎一致之外，还要与这份预期比较。

保存完整排序 TSV、行数、NULL 个数、SHA256 和差异样例。每轮恢复表、优化检查表及两张负载表都保存完整结果，并对负载表生成分区摘要便于定位差异；摘要不替代全量逐值核验。大文件可压缩归档，哈希基于未压缩的规范化内容计算，并在归档后验证可解压和校验。

## 指标和资源监控

每 10 秒采样一次，记录进程代次和实际 metrics 端口：

- `tiflash_schema_trigger_count{type="raft_decode"}`。
- `tiflash_schema_watermark_decode_count` 的 no_watermark、beyond_watermark、covered、skipped_extra_columns。
- TiFlash 进程 RSS、CPU、打开文件数、线程数、数据目录大小和磁盘可用空间。
- 本次写入成功/失败/状态不明数、确认序号、查询错误、恢复阶段及耗时。

另统计 writer 实际运行时间和因校验暂停的时间；200 行变更/秒是 writer 启用时的目标速率。主循环中的有效写入时间至少占 50%，并报告全程实际提交量；若长时间都在等待或校验，应记为持续负载覆盖不足，不能只凭时长称为压力验证。

同步日志按目标 table_id 分组。后台同步开启期间不把全局同步数全部归给本轮表；受控优化检查才使用隔离后的指标增量做严格断言。

测试前建议为本次负载预留至少 300 GiB 空间，并记录所有日志/数据目录所在文件系统。磁盘可用空间低于 100 GiB、低于该文件系统容量的 10%，或低于“最近一小时本次测试增长量的两倍”中任一条件时，停止新增负载并进入取证。阈值在预演中确认；不依赖自动删除旧失败证据腾空间。

配置各组件日志轮转，文件总量设置上限，轮转后仍保证本轮及失败窗口可追溯；每轮及时复制目标日志窗口，避免被轮转覆盖。Raft 日志目录和日志归档单独计量。关闭 GC 下的空间增长是测试配置的预期成本，不能据此直接判 TiFlash 泄漏。

每次重启后的同一稳定阶段比较 RSS/FD；若连续 5 个相同阶段检查点都增长且总增幅超过 50%，记录资源异常并取证。最终判定结合缓存、数据量及进程代次，不能直接跨进程比较一个绝对值。

## 通过 失败和未覆盖的判定

整轮通过需要同时满足：实际运行满 24 小时，完成至少 120 个成功重启轮次，每个组合至少 10 轮，至少 10 次 ADD 反向检查通过，最终全量核验完成。

还必须满足：所有校验点无值差异；无非预期进程退出、hang 或 shutdown_timeout；原目录/store_id 全程不变；受控优化检查的同步和实际跳过断言全部通过；停机恢复阶段没有新的目标 Region snapshot apply 接替日志回放。

以下任一情况停止进入新轮次并取证：

- 真实数据值或 NULL 不一致，新增列哨兵丢失，删除后值/行不符合预期。
- metadata/schema snapshot 构造异常、持续 apply 失败、非预期内部 ERROR/FATAL、非预期退出。
- 正常退出超过 45 秒后被强杀、启动或追平超时、意外改数据目录或 store_id。
- 资源触及上述停止阈值。

出现新 snapshot、积压未形成、实际跳过为 0 或缺少必要证据时，该用例为“未覆盖”。即使最终数据正确，也不能计入优化和日志恢复的成功次数。环境容量不足导致中止，单独记录为容量受限，不声称功能测试通过或产品故障。

失败用例不自动清空目录重试来覆盖原结果。保存时间线、真实 PID、退出码、信号、版本、最后确认序号、DDL 状态、schema、指标及完整数据差异；停止 writer。如果进程仍处于 SIGSTOP，先核对身份再 SIGCONT，以便取证或正常清理。

预期停机窗口内的连接断开和暂时不可用单独记录，不能混同于数据差异或内部 apply 错误；恢复后仍不可用则按上述超时标准判失败。

## 证据目录和清理

建议目录为仓库的 `build-schema-watermark/validation/tiup-restart-soak-<commit>-<run_id>/`，计划和脚本在独立测试分支 `jinhe/schema-watermark-tiup-tests` 中维护，不加入核心修复 PR；运行产物保留在本地，不提交。

```text
manifest.json                     版本 哈希 配置 随机种子 进程身份
effective-configs/                各组件实际配置
events.jsonl                      全程状态机 信号 退出 重启和校验点
operations.jsonl                  操作及确认提交记录
metrics/                          带进程代次的周期采样
rounds/000001/                    每轮独立目录
  ddl.sql                         实际 DDL 和目标 ID
  process-before.json             旧进程身份
  process-after.json              新进程身份和相同路径/store_id 核对
  schemas/                        初始和最终 TableInfo
  recovery/                       退出 启动 追平阶段日志和计时
  post-restart-probe/              隔离优化检查及全量数据
  checks/                         数据快照 差异及独立预期
  result.json                     通过 失败或未覆盖及理由
hourly/                           每小时全量核验
failures/                         不覆盖的故障证据
FINAL_REPORT.md                   真实时长 各组合完成数 结果与限制
audit.json                        对原始证据重新复核的结果
```

最终汇总按组合统计实际完成数、退出码、启动/追平时间、schema-sync、实际跳过、新增列反向结果、值差异、snapshot、资源曲线和所有未覆盖项。恢复时间从新 PID 启动到校验点追平，正常退出耗时和停机写入窗口单列；不将 SQL 导出及稳定采样计入恢复性能。

结束或失败清理时，先停止本次 writer，释放 restart.gate，恢复本次暂停的真实进程，恢复 GC 原值和后台 schema sync，再停止本次 Playground 并确认其实际子进程全部退出。仅操作本次 tag，且在发送信号前匹配 PID 的 starttime、cmdline、cwd，避免误杀复用 PID 或其他集群。保留结果目录，数据目录在证据完整且不再用于排障后清理。

## 现有脚本复用和执行器要求

可复用已有代码中的组件启动、完整行比较和 `restart_case()`，但长跑需要增加调度与上述验证：

- 本目录 [run_matrix.py](run_matrix.py) 提供单轮 DDL、完整行比较和 `restart_case()`。
- 本目录 [restartable_tiflash.py](restartable_tiflash.py) 支持 `restart.gate`、真实子进程 PID 和信号/退出日志。
- 参数和运行示例见 [README](README.md)；当前没有 24 小时调度入口，单轮矩阵不能代替完整长跑。

执行前将实际使用的脚本复制到结果目录并记录哈希，配置指向这份固定副本。二进制路径通过参数指定，待测 commit 从真实二进制版本读取；不依赖历史结果目录或脚本仓库的 HEAD。禁止 24 小时期间热更新脚本或二进制。

执行器应实现持久化状态机：`Ready -> StopRequested -> ExitedAndGated -> OfflineWritesAndDDL -> Starting -> CatchingUp -> Checking -> OptimizationProbe -> Ready`。各阶段有超时及停止处理，写入结果以 `run_id + round_id + stage` 关联；调度器中断后先核对实际状态并取证，不自动重放不确定阶段。

单轮旧脚本中的全局一次性暂停点、固定 metrics 端口假设、跨进程计数相减，以及未核实真实子进程是否退出的清理方式，均不能带入长跑执行器。先完成预演，再运行正式 24 小时测试。

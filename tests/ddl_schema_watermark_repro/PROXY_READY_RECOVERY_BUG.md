# TiFlash 重启恢复时 Ready 分页调度停滞

本文记录重启测试中发现的 proxy Ready 调度问题，包括复现条件、操作步骤、根因、最小候选修复及验证结果。记录日期为 2026-11-02，时间均为 UTC+8。

**结论：proxy 已收到并持久化全部目标日志，但一次 Ready 和 LightReady 只投递了其中一部分。外层 `fsm.has_ready` 被清除后，apply 完成回调没有根据剩余 Ready 重新置位；learner 休眠后，剩余日志便无法继续投递。**

遗漏位于本次 schema watermark PR 未修改的 proxy 路径。本地候选已通过定向恢复及完整值校验，尚未提交或推送；正式 24 小时测试仍为失败，未重新启动。

## 现象和触发条件

TiFlash 重启后，启动流程等待 Region 的 applied index 达到 read-index 目标。在故障现场，少量 Region 长时间无法追平，查询 gRPC 服务尚未开放，最终 TiDB 查询报 `9012 TiFlash server timeout` 或连接拒绝。

触发本次问题需要同时出现以下条件：

1. 本地存在大量已提交、已持久化但需要重新 apply 的 Raft 日志，积压超过一次 Ready 加 LightReady 能取出的数量。
2. 后续处理进入 `HandleReadyResult::NoIoTask`，没有新写盘完成回调可以触发下一轮 Ready。
3. 已投递的 apply 全部完成，但还有已提交日志未投递。
4. learner 进入 Idle 并停止 Raft tick，之后没有新的 Raft 消息继续唤醒它。

本次长跑关闭了 TiKV 的 Raft log GC，以保留恢复所需日志。原故障重启时，Region 143 的持久化 applied 断点为 66，而 commit 已超过 247 万，因此需要重放大量历史日志。这里的断点是重启后的持久化恢复位置，不能据此认定此前内存中的 apply 一直没有推进。

表上发生的 DROP COLUMN 是当轮测试负载的一部分。Ready 调度遗漏本身位于 Raft 日志分页路径，候选修复没有修改 schema watermark 的判断逻辑。

## 关键状态的含义

| 状态 | 含义 |
| --- | --- |
| `commit` | 已提交日志的最大 index |
| `persisted` | proxy 已持久化日志的最大 index |
| `last_applying_index` | proxy 已投递给 apply 的最大 index |
| proxy `applied` | apply 完成结果已反馈到 proxy 的位置 |
| C++ `applied_index` | TiFlash C++ Region 当前应用位置 |
| `raw_node_has_ready` | raft-rs 内部是否还有待外部处理的 Ready |
| `fsm_has_ready` | proxy 外层 FSM 是否允许本轮调用 Ready 处理 |
| `raft_tick_scheduled` | 是否还安排了 Raft tick |

两个 `has_ready` 不是同一个变量。内部存在 Ready，并不保证外层一定会调用处理函数；这正是本次缺口。

`last_applying_index > applied` 表示已有工作在执行或等待完成。只有在 `last_applying_index == applied < commit` 时，才能把剩余差距归为尚未继续投递的日志。index 差值是日志条目数量，不是 SQL 行数。

## 复现步骤

### 从新 TiUP 集群构造积压

测试分支为 `jinhe/schema-watermark-tiup-tests`。准备未包含本地 Ready 修复的 TiFlash 完整安装目录，以及兼容的 TiDB、PD、TiKV 二进制。每次记录实际版本、源码 diff 和二进制指纹，后续复现不要求固定代码 commit。

本次使用 `v9.0.0-beta.2.pre-nightly` 分发目录中的 TiDB、PD、TiKV；实际版本以运行结果中的 `versions` 为准。该目录名不能代替实际版本。开发版 TiFlash 所需的 PD RPC 与 `v8.5.7` 不兼容，不能直接把组件全部换成 `v8.5.7`。本次未执行旧版 TiFlash 对照。

在仓库根目录执行，替换以下绝对路径，选择未使用的 tag、端口及新输出目录：

```bash
python3 -m pip install -r tests/ddl_schema_watermark_repro/requirements.txt

READY_REPRO_ID=$(date +%Y%m%d-%H%M%S)
READY_REPRO_RESULTS=/absolute/path/to/results

python3 tests/ddl_schema_watermark_repro/run_restart_soak.py \
  --tiflash /absolute/path/to/unfixed-install/tiflash \
  --tidb /absolute/path/to/compatible/tidb-server \
  --tikv /absolute/path/to/compatible/tikv-server \
  --pd /absolute/path/to/compatible/pd-server \
  --build-manifest /absolute/path/to/build-manifest.json \
  --mode soak --duration 24h \
  --warmup 30m --final-window 30m --interval 10m \
  --round-timeout 8m --catchup-timeout 3m --offline-seconds 30 \
  --load-rows 200000 --ddl-rows 10000 --workers 4 --rate 200 \
  --seed 20261031 --offset 6000 --debug-port 29000 \
  --tag "ready-repro-$READY_REPRO_ID" \
  --output "$READY_REPRO_RESULTS/ready-repro-$READY_REPRO_ID"
```

执行器通过 TiUP 部署 1 PD、1 TiKV、1 TiDB、1 TiFlash，创建两张负载表，持续执行带事务确认标记的 INSERT、UPDATE、DELETE，并在 TiFlash 停机窗口执行 DDL。参数及环境要求还可查阅同目录的 `PLAN_RESTART_SOAK_24H.md`。

原运行在约 4 小时 14 分钟、第 23 轮 `crash + DROP c_first` 时超时。这个轮次和耗时是已观察结果，不是每次新集群都能在相同位置触发的保证。若要缩短排查周期，使用下一节保存的持久化现场。

### 使用已保存现场复现

当前测试机上保留了以下目录。其他机器需要先取得现场数据、结果清单和诊断脚本，并调整路径及 Region ID。

| 项目 | 路径 |
| --- | --- |
| 原运行及结果根目录 | `/DATA/disk1/jinhelin/tiflash-1/build-schema-watermark/validation/tiup-restart-soak-20261101-180432` |
| 已停止的原始集群数据 | `/DATA/disk1/jinhelin/.tiup/data/schema-soak-20261101-180432` |
| 诊断脚本及二进制目录 | 原运行根目录下 `error-analysis/instrumented` |
| 未修复的诊断版 | 诊断目录下 `install/tiflash` |
| 候选修复版 | 诊断目录下 `candidate-fix/install/tiflash` |

诊断版增加了状态字段和 C++ 启动等待日志。初始 `proxy.patch` 不含 Ready 调度修复，候选功能补丁独立保存在 `proxy-ready-fix.patch`。

复现过程如下：

1. 确认原集群已停止，源数据不再变化，测试端口未被其他进程监听。
2. 使用普通复制或 reflink 复制 PD、TiKV、TiFlash 数据和配置，改写副本配置中的路径。避免硬链接；所有进程只打开副本。
3. 在副本上启动 PD、TiKV、未修复诊断版 TiFlash，等待首次恢复。
4. 保持 PD、TiKV 运行，仅 SIGKILL 并重启 TiFlash。恢复诊断阶段不启动 TiDB，不增加 SQL 写入，不用 GDB 暂停进程。
5. 每 2 秒采样 TiKV、proxy 和 C++ index。20 秒无 apply 推进时记录告警，取得至少 60 秒连续有效停滞证据后结束取证并清理进程。

本机可以使用现存脚本执行上述操作。输出目录必须不存在；下列命令会创建新副本，不打开原始数据库：

```bash
READY_REPRO_ROOT=/DATA/disk1/jinhelin/tiflash-1/build-schema-watermark/validation/tiup-restart-soak-20261101-180432
READY_REPRO_DIAG="$READY_REPRO_ROOT/error-analysis/instrumented"
READY_REPRO_ID=$(date +%Y%m%d-%H%M%S)

python3 -u "$READY_REPRO_DIAG/reproduce.py" \
  --tiflash "$READY_REPRO_DIAG/install/tiflash" \
  --restart-count 10 \
  --output "$READY_REPRO_DIAG/repro-$READY_REPRO_ID"
```

`reproduce.py` 从原运行的 manifest 和 result 读取源数据路径、组件路径及启动参数，并针对 Region 143、150 采样。它是该现场的本地诊断脚本，不是任意集群通用入口，也未随测试分支提交。它会使用 PD 8379/8380、TiKV 26160/26180、proxy 26170/26292、TiFlash 9930/14234/29000。

复现成功应同时满足：

```text
TiKV applied/commit == proxy commit/last/persisted
proxy last_applying_index == proxy applied == C++ applied_index
proxy applied < commit

raw_node_has_ready = true
fsm_has_ready = false
raft_tick_scheduled = false
group_state = Idle
stopped = pending_remove = wait_data = pending_snapshot = false
```

这一组合持续至少 60 秒，且 leader、term 保持稳定。只有一次瞬时采样或单独的 SQL 超时，不足以确认同一问题。接口断连应中断连续停滞计时，commit 增长本身不能当作 apply 恢复。

### 手动采样和 DEBUG 日志

在本机现场副本上可读取：

```bash
curl --noproxy '*' http://127.0.0.1:26180/region/143
curl --noproxy '*' http://127.0.0.1:26292/region/143

LD_LIBRARY_PATH="$READY_REPRO_DIAG/install" \
  "$READY_REPRO_DIAG/install/tiflash" client \
  --host 127.0.0.1 --port 29000 \
  --query 'DBGInvoke dump_all_region(0, false, true)'
```

Region 150 采用相同方式。proxy 的 `apply_diagnostics` 字段来自本地观测补丁，原始二进制没有这些字段。AccessPeer 状态请求也会经过 peer FSM，因此需保留采样频率和脚本；不能假定观测完全不影响时序。

需要历史事件时，在原 TiFlash server 启动参数的配置覆盖区增加 `--flash.proxy.log-level=debug`，关注 `handle raft message`、`handle raft ready`、`async apply finish`、`stop ticking`、`follower hibernates`。DEBUG 会逐条记录大量 apply，需保留轮转日志。本次还在 INFO 日志配置下复现过同类停滞。

## 实际复现证据

2026-11-02 的 DEBUG 取证轮，首次冷启动约 56.6 秒恢复，第一次强杀重启后两个 Region 均停滞。

| 指标 | Region 143 | Region 150 |
| --- | ---: | ---: |
| TiKV commit/applied | 2,480,067 | 2,477,889 |
| proxy commit/last/persisted | 2,480,067 | 2,477,889 |
| proxy last_applying/applied | 2,380,008 | 2,379,981 |
| C++ applied | 2,380,008 | 2,379,981 |
| 未投递日志条数 | 100,059 | 97,908 |
| 连续有效停滞 | 约 61.6 秒 | 约 61.6 秒 |

两个 Region 的事件顺序一致：

| 时间 | Region 143 | Region 150 |
| --- | --- | --- |
| 09:31:01.704 / .705 | 最后一条 MsgHeartbeat，随后最后一次 Ready 处理 | 相同 |
| 09:31:09.093 / .103 | follower hibernates | 相同 |
| 09:31:12.656 / .944 | 最后一次 apply 完成回调 | 最后一次 apply 完成回调 |
| 后续约 61.6 秒 | applied 不再推进，内部仍有 Ready | 相同 |

这说明已投递的 apply 能够完成，剩余缺口在继续投递阶段。TiKV 已追平，proxy 也已持久化全部目标日志；同期 leader、term 稳定，没有快照或待删除状态。原始查询超时是启动等待这些 Region 的后果。

## 根因分析

### 外层门控先被清除

`PeerFsmDelegate::collect_ready()` 先读取并清除外层标志：

```rust
let has_ready = self.fsm.has_ready;
self.fsm.has_ready = false;
if !has_ready || self.fsm.stopped {
    return false;
}
```

因此，内部 `RawNode::has_ready()` 为 true 时，也必须有下一次外层调度，才能调用 `handle_raft_ready_append()`。

### 一次 Ready 和 LightReady 无法保证取完积压

proxy 配置 `MAX_COMMITTED_SIZE_PER_READY = 16 * 1024 * 1024`。raft-rs 的 `ready()` 取得一批 committed entries，`advance_append()` 返回的 LightReady 再取得一批；两批都受字节上限限制。

在 `HandleReadyResult::NoIoTask` 分支，没有在途持久化任务时，proxy 直接调用 `advance_append(ready)` 并投递 LightReady 中的日志。执行后仍可能存在更多 Ready。

本次重启时日志已经持久化，term/commit 稳定，加上 Ready 的任务构造逻辑，可推导后续分页走到这一分支。原有 DEBUG 日志没有直接打印 `NoIoTask` 枚举；实际观察到的调度状态与下面的依赖库实验互相印证。

### 普通 apply 回调没有补上下一轮

有异步写盘任务时，`on_persisted_msg()` 会重新设置 `fsm.has_ready=true`。不需要写盘的分页不能依赖这条回调。

apply 完成后，`on_apply_res()` 执行：

```rust
self.fsm.has_ready |= self.fsm.peer.post_apply(...);
```

修复前的 `Peer::post_apply()` 只有在 pending snapshot 可以继续处理时返回 true。普通日志 apply 完成，即使 RawNode 还有下一批 committed entries，也不会重新置位。这是已存在的 apply 完成回调没有推进下一批的具体遗漏。

### 休眠使缺口持续暴露

follower 的休眠判断不要求上述已提交日志全部 apply。进入 Idle 后最终停止 tick，而新 Raft 消息本来会重新置位外层标志。没有新消息时，内部剩余 Ready 就持续停留。

因此，“漏了 Ready”指分批处理结束后没有安排下一轮消费，并非网络通知丢失。简单延长启动超时，不能补齐这条推进路径。

### 适合 review 的调用链

以下位置均在 `contrib/tiflash-proxy/components/raftstore/src/store/` 下，行号会随源码更新变化，review 时以函数名为准：

| 文件和函数 | 需要检查的行为 |
| --- | --- |
| `fsm/store.rs` 的 `handle_normal()` | 处理 peer 消息后调用 `collect_ready()` |
| `fsm/peer.rs` 的 `collect_ready()` | 消费并清除外层 `has_ready` |
| `peer.rs` 的 `handle_raft_ready_append()` | 调用 RawNode，处理 `NoIoTask` 和 LightReady |
| `peer_storage.rs` 的 `handle_raft_ready()` | 根据实际写盘数据生成 NoIoTask/IO 任务 |
| `fsm/peer.rs` 的 `on_apply_res()` | 将 `post_apply()` 返回值合并到外层门控 |
| `peer.rs` 的 `post_apply()` | 更新 apply 状态后决定是否继续处理 Ready |
| `peer.rs` 的 `check_after_tick()`、`fsm/peer.rs` 的 `on_raft_base_tick()` | follower 进入 Idle 并停止 tick |

## 依赖库最小实验

为确认一次 Ready/advance 后仍可有剩余 Ready，独立实验直接链接诊断版 proxy 实际编译的 raft-rs 库。它模拟外围 FSM 门控，不是完整 proxy 集成测试。

构造一个 learner，在 MemStorage 中预存 12 条已提交日志，每条 payload 为 5 MiB，应用位置为 0，采用与 proxy 相同的 16 MiB 分批上限。没有新消息、新写盘或 snapshot。

```text
第一次：Ready=[1,2,3]，LightReady=[4,5,6]
        applied=6，commit=12，fsm_has_ready=false，RawNode.has_ready=true

显式重新调度后：Ready=[7,8,9]，LightReady=[10,11,12]
              applied=12，commit=12，RawNode.has_ready=false
```

第一步证明取两批后仍有工作；第二步说明显式继续消费可以排空。完整程序和构建参数在诊断目录的 `ready_pagination_probe.rs`、`ready-pagination-build-receipt.json`，输出在 `ready-pagination-result.txt`。实际依赖版本以该次 Cargo.lock 和构建清单为准。

## 最小候选修复

修改 `Peer::post_apply()` 末尾的返回条件，保留已有 snapshot 判定，并检查 RawNode 是否仍有 Ready：

```diff
-        has_ready
+        // Ready and LightReady both limit committed entries by size. Applying one
+        // batch can leave another page to dispatch even without new Raft messages
+        // or persistence callbacks, particularly when the follower is hibernating.
+        has_ready || self.raft_group.has_ready()
```

apply 完成后，`on_apply_res()` 因返回 true 而恢复外层标志；同轮 `handle_normal()` 末尾的 `collect_ready()` 随后继续取下一批。`has_ready()` 只检查状态，不直接消费 Ready，也不在该函数内循环重放。

编译候选时清理所有名称中含 `proxy` 的环境变量，不执行 `make format`。候选完整安装目录与未修复版分开保存，核对实际加载的 `libtiflash_proxy.so` 及其 SHA256，不能只看未改变的版本字符串。

## 修复验证方法与结果

### 恢复推进

在原失败数据的新副本上执行一次冷启动、8 次 SIGKILL 重启、2 次正常退出重启，先执行强杀场景。矩阵期间不启动 TiDB，没有新 SQL 写入、GDB 或 Ready 注入；保留休眠配置及原分批上限。

每轮要求 proxy/C++ applied 达到 TiKV 目标、gRPC 可用，随后至少 10 秒持续追平，最终两个 Region 进入 Idle、内部 Ready 清空且 Raft tick 停止。

11 次启动均满足上述条件，未捕获持续停滞。首次完全追平及 gRPC 可用耗时约 41.4～50.7 秒；追平后观测 CPU 约 6.0%～6.6%。采样观察到进程峰值 RSS 约 7.97 GiB；没有完整性能或内存对照，不能据此宣称没有额外资源开销。

### 完整值校验

矩阵结束后才启动 TiDB，在保留的同一测试副本上完成全量查询。两张负载表与原测试 2,522,516 次已确认事务的独立重放结果逐行比较，并核对 worker 提交序列及事务 marker 数量。其他表逐行比较 TiKV 与 TiFlash 的全部列，导出时将主键 `id` 放在首列。

TiFlash 查询强制使用 MPP，并保留执行计划；导出的全部实际值及 NULL 保存为压缩文件，重新读取归档校验 SHA256。

| 表 | 行数 | 比较依据 | 结果 |
| --- | ---: | --- | --- |
| load_a | 198,940 | 独立事务重放与 TiKV、TiFlash | 一致 |
| load_b | 198,602 | 独立事务重放与 TiKV、TiFlash | 一致 |
| r21_ddl、r21_probe、r22_ddl、r22_probe、r23_ddl | 每表 10,000 | TiKV 与 TiFlash 全部字段 | 一致 |
| 合计 | 447,542 | 7 张表 | 一致 |

补充验证最终 TiDB、TiFlash、TiKV、PD 退出码均为 0，源数据文件大小和 mtime 核对一致。TiFlash 日志中的 4,816 条旧表 mapping 消息，全部精确对应原测试已成功 DROP 并登记的默认 keyspace 表 ID，单独保留；未发现其他 TiFlash/proxy ERROR 或 panic。

### 校验脚本修正和验收范围

恢复矩阵完成后，校验阶段依次修正了旧进程检查、TiDB SQL 端口参数（应为 `--P`）、归档器的主键列位置假设，以及 TIME_WAIT 导致的端口探测误判。原始错误和重试结果均保留，没有把原 `status=error` 改写为通过。最终验证摘要合并恢复矩阵与补充全值校验的证据。

本轮验证确认候选能消除已复现的调度停滞，并通过上述数据检查。尚未完成候选的 24 小时重跑、proxy 完整集成测试和性能对照。原正式长跑的 `passed=false`、`acceptance_24h=false` 保持不变。

## 证据索引

以下文件均位于本机诊断目录：

`/DATA/disk1/jinhelin/tiflash-1/build-schema-watermark/validation/tiup-restart-soak-20261101-180432/error-analysis/instrumented`

这些运行产物未提交到仓库；对外转交本文时，需要一并转交所需日志、脚本和数据。

| 内容 | 相对诊断目录的路径 |
| --- | --- |
| 未修复版构建清单、观测补丁 | `diagnostic-build-manifest.json`、`tiflash.patch`、`proxy.patch` |
| INFO 配置复现结果 | `run-1/diagnostic-result.json` |
| DEBUG 配置复现结果 | `run-2-debug/diagnostic-result.json` |
| 最终停滞采样 | `run-2-debug/cycle-01-sample-044.json` |
| 原日志位置及精确行号 | `ready-event-trace.txt`、`ready-event-trace.json` |
| 分页机制及状态证据 | `ready-pagination-proof.json` |
| 独立最小实验 | `ready_pagination_probe.rs`、`ready-pagination-result.txt`、`ready-pagination-build-receipt.json` |
| 最小功能补丁 | `proxy-ready-fix.patch` |
| 候选构建清单 | `candidate-fix/build-manifest.json` |
| 候选实际加载的 proxy | `candidate-fix/loaded-proxy-proof.json` |
| 11 次启动的原始矩阵结果 | `candidate-fix/run-1/diagnostic-result.json`，保留后续控制器错误 |
| 该矩阵实际使用的脚本 | `candidate-fix/reproduce_candidate-used.py`、`candidate-fix/harness-sha256.json` |
| 最终补充全量值结果 | `candidate-fix/data-check-resume-4/result.json` |
| 全部实际值及查询计划 | `candidate-fix/data-check-resume-4/data-check/` |
| 已知旧表错误与其他错误分类 | `candidate-fix/log-error-check.json` |
| 合并后的验证摘要 | `candidate-fix/verification-summary.json` |
| 详细验证记录 | `candidate-fix/VERIFICATION.md` |

原始 TiUP 失败结果另保存在原运行根目录的 `run/result.json`；TiKV、PD、proxy 交叉分析保存在 `error-analysis/REGION143_CROSS_COMPONENT_ANALYSIS.md`。

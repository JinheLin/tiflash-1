# 三个故障版本找到的误跳反例

以下均是故意修改模型规则后产生的反例，不代表当前实现存在这些错误。完整原始状态序列分别保存在 由 `--output` 指定的结果目录下的 `<名称>/tlc.log`。列 `b` 在三个反例中均从未被删除，`NoValidColumnSkipped` 在最后的 Decode 动作被违反。

## 1. 省略更新前清零：旧局部副本 + read_tso=0 + 持久化前失败

配置：[no_clear.cfg](no_clear.cfg)。完整轨迹有 15 个状态。

1. 初始本地 schema 为 `{a}`，watermark=1。
2. TiDB 在时间 2 引入有效列 `b`，一条包含 `b` 的行在 commit_ts=3 提交。
3. 完整同步请求读取时间 3 的 `{a,b}`；另一个局部更新请求在其应用前复制本地旧 schema `{a}`，并携带 read_tso=0。
4. 完整同步成功应用 `{a,b}`，持久化并发布 watermark=3。
5. 旧的局部请求取得 alter lock。故障版本没有先清零 watermark，更新了内存列为 `{a}`，随后在持久化前失败。
6. 锁释放后，新的解码快照变成 `{a}, watermark=3`。
7. 回放 commit_ts=3、包含 `b` 的行。因为 `3 <= 3`，错误地将有效列 `b` 当作历史额外列跳过。

最终状态：本地内存列为 `{a}`，磁盘 schema 仍为 `{a,b}`，快照水位为 3，`badSkipped={b}`。当前规则在第 5 步把 watermark 清为 0，本轨迹中重建得到的将是 `{a},0`，无法获得跳过资格。尚未失效而能继续复用的缓存则保持自身原来的正确配对，不会嫁接新的 watermark。

## 2. watermark 先于 schema 对解码可见

配置：[split_publish.cfg](split_publish.cfg)。完整轨迹有 8 个状态。

1. 本地 `{a}, watermark=1`；TiDB 引入 `b`，包含 `b` 的行在 commit_ts=3 提交。
2. 完整读取已取得 `{a,b}` 和 read_tso=3。
3. 故障版本先把 watermark=3 暴露给解码，标记缓存应重建，却尚未应用 `{a,b}`，也没有继续持有阻止解码的排他锁。
4. 解码重建出 `{a},3`；回放 commit_ts=3 的行，误跳有效 `b`。

这里刻意模拟的是“错误配对已经可见”，不只是把 C++ 中的一行赋值前移。在正确的排他锁内仅调整行顺序，未必会让解码观察到中间状态；是否发生异常、何时释放锁和是否重建缓存也必须一起分析。

## 3. 用 min(commit_ts) 代替 max(commit_ts)

配置：[min_commit.cfg](min_commit.cfg)。完整轨迹有 12 个状态。

1. 一条只含 `a` 的行在 commit_ts=2 提交，完整同步取得时间 2 的 schema `{a}`。
2. 列 `b` 在时间 3 引入，另一条包含 `b` 的行在 commit_ts=4 提交。
3. 延迟的完整读取成功应用 `{a}, watermark=2`，这是正确配对。
4. 解码批次同时包含 commit_ts=2 和 commit_ts=4 两条行。
5. 故障版本使用 `min=2 <= watermark=2` 放行整个批次，于是跳过新行中有效的 `b`。

正确版本使用 `max=4 > watermark=2`，拒绝这次批次级优化并保留同步回退。此反例说明“schema/TSO 配对正确”与“批次覆盖条件正确”是两个独立的安全要求。

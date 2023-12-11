// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{
SegmentReadTaskScheduler::SegmentReadTaskScheduler()
    : stop(false)
    , log(Logger::get())
{
    sched_thread = std::thread(&SegmentReadTaskScheduler::schedLoop, this);
}

SegmentReadTaskScheduler::~SegmentReadTaskScheduler()
{
    setStop();
    sched_thread.join();
}

void SegmentReadTaskScheduler::add(const SegmentReadTaskPoolPtr & pool, const LoggerPtr & req_log)
{
    Stopwatch sw_add;
    // `add_lock` is only used in this function to make all threads calling `add` to execute serially.
    std::lock_guard add_lock(add_mtx);
    // `lock` is used to protect data.
    std::lock_guard lock(mtx);
    Stopwatch sw_do_add;
    read_pools.push_back(pool);

    const auto & tasks = pool->getTasks();
    for (const auto & [seg_id, task] : tasks)
    {
        merging_segments[seg_id].push_back(pool->pool_id);
    }
    LOG_DEBUG(
        req_log,
        "Added, pool_id={} block_slots={} segment_count={} pool_count={} cost={:.3f}us do_add_cost={:.3f}us", //
        pool->pool_id,
        pool->getFreeBlockSlots(),
        tasks.size(),
        read_pools.size(),
        sw_add.elapsed() / 1000.0,
        sw_do_add.elapsed() / 1000.0);
}

MergedTaskPtr SegmentReadTaskScheduler::scheduleMergedTask(SegmentReadTaskPoolPtr & pool)
{
    // If pool->valid(), read blocks.
    // If !pool->valid(), read path will clean it.
    auto merged_task = merged_task_pool.pop(pool->pool_id);
    if (merged_task != nullptr)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_from_cache).Increment();
        return merged_task;
    }

    if (!pool->valid())
    {
        return nullptr;
    }

    auto segment = scheduleSegmentUnlock(pool);
    if (!segment)
    {
        // The number of active segments reaches the limit.
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_segment).Increment();
        return nullptr;
    }

    RUNTIME_CHECK(!segment->second.empty());
    auto pools = getPoolsUnlock(segment->second);
    if (pools.empty())
    {
        // Maybe SegmentReadTaskPools are expired because of upper threads finish the request.
        return nullptr;
    }
    RUNTIME_CHECK(pools.size() == segment->second.size(), pools.size(), segment->second.size());

    std::vector<MergedUnit> units;
    units.reserve(pools.size());
    for (auto & pool : pools)
    {
        units.emplace_back(pool, pool->getTask(segment->first));
    }
    GET_METRIC(tiflash_storage_read_thread_counter, type_sche_new_task).Increment();

    return std::make_shared<MergedTask>(segment->first, std::move(units));
}

SegmentReadTaskPools SegmentReadTaskScheduler::getPoolsUnlock(const std::vector<uint64_t> & pool_ids)
{
    SegmentReadTaskPools pools;
    pools.reserve(pool_ids.size());
    for (const auto & pool : read_pools)
    {
        if (std::find(pool_ids.begin(), pool_ids.end(), pool->pool_id) != pool_ids.end())
        {
            pools.push_back(pool);
        }
    }
    return pools;
}

bool SegmentReadTaskScheduler::needScheduleToRead(const SegmentReadTaskPoolPtr & pool)
{
    if (pool->getFreeBlockSlots() <= 0)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_slot).Increment();
        return false;
    }

    if (pool->isRUExhausted())
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_ru).Increment();
        return false;
    }

    // Check if there are segments that can be scheduled:
    // 1. There are already activated segments.
    if (merged_task_pool.has(pool->pool_id))
    {
        return true;
    }
    // 2. Not reach limitation, we can activate a segment.
    if (pool->getFreeActiveSegments() > 0 && pool->getPendingSegmentCount() > 0)
    {
        return true;
    }

    if (pool->getFreeActiveSegments() <= 0)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_active_segment_limit).Increment();
    }
    else
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_segment).Increment();
    }
    return false;
}

bool SegmentReadTaskScheduler::needSchedule(const SegmentReadTaskPoolPtr & pool)
{
    // If !pool->valid(), schedule it for clean MergedTaskPool.
    return pool != nullptr && (needScheduleToRead(pool) || !pool->valid());
}

std::optional<std::pair<GlobalSegmentID, std::vector<UInt64>>> SegmentReadTaskScheduler::scheduleSegmentUnlock(
    const SegmentReadTaskPoolPtr & pool)
{
    auto expected_merge_seg_count = std::min(read_pools.size(), 2); // Not accurate.

    std::optional<std::pair<GlobalSegmentID, std::vector<uint64_t>>> result;
    auto target = pool->scheduleSegment(merging_segments, expected_merge_seg_count);
    if (target != merging_segments.end())
    {
        if (MergedTask::getPassiveMergedSegments() < 100 || target->second.size() == 1)
        {
            result = *target;
            merging_segments.erase(target);
        }
        else
        {
            result = std::pair{target->first, std::vector<uint64_t>(1, pool->pool_id)};
            auto itr = std::find(target->second.begin(), target->second.end(), pool->pool_id);
            *itr = target->second
                       .back(); // SegmentReadTaskPool::scheduleSegment ensures `pool->poolId` must exists in `target`.
            target->second.resize(target->second.size() - 1);
        }
    }
    return result;
}

void SegmentReadTaskScheduler::setStop()
{
    stop.store(true, std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::isStop() const
{
    return stop.load(std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::schedule()
{
    Stopwatch sw_sched_total;
    std::lock_guard lock(mtx);
    Stopwatch sw_do_sched;

    auto pool_count = read_pools.size();
    UInt64 erased_pool_count = 0;
    UInt64 sched_null_count = 0;
    UInt64 sched_succ_count = 0;

    for (auto itr = read_pools.begin(); itr != read_pools.end(); /**/)
    {
        Stopwatch sw_sched_once;
        if (itr->use_count() == 1)
        {
            ++erased_pool_count;
            itr = read_pools.erase(itr);
            continue;
        }
        auto & pool = *itr;
        ++itr;

        if (!needSchedule(pool))
        {
            ++sched_null_count;
            continue;
        }

        auto merged_task = scheduleMergedTask(pool);
        if (merged_task == nullptr)
        {
            ++sched_null_count;
            continue;
        }
        ++sched_succ_count;
        SegmentReaderPoolManager::instance().addTask(std::move(merged_task));
        if (auto elapsed_ms = sw_sched_once.elapsedMilliseconds(); elapsed_ms >= 10)
        {
            LOG_INFO(
                log,
                "scheduleMergedTask merged_task=<{}> cost={}ms pool_count={}",
                merged_task->toString(),
                elapsed_ms,
                pool_count);
        }
    }

    if (read_pools.empty())
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_sche_no_pool).Increment();
    }

    auto total_ms = sw_sched_total.elapsedMilliseconds();
    if (total_ms >= 100)
    {
        LOG_INFO(
            log,
            "schedule pool_count={} erased_pool_count={} sched_null_count={} sched_succ_count={} cost={}ms "
            "do_sched_cost={}ms",
            pool_count,
            erased_pool_count,
            sched_null_count,
            sched_succ_count,
            total_ms,
            sw_do_sched.elapsedMilliseconds());
    }
    return sched_succ_count > 0;
}

void SegmentReadTaskScheduler::schedLoop()
{
    while (!isStop())
    {
        if (!schedule())
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(2ms);
        }
    }
}

} // namespace DB::DM

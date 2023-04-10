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
#pragma once

#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <Storages/DeltaMerge/ReadThread/MergedTask.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <absl/synchronization/mutex.h>

#include <memory>
namespace DB::DM
{
using SegmentReadTaskPoolList = CircularScanList<SegmentReadTaskPool>;
// SegmentReadTaskScheduler is a global singleton.
// All SegmentReadTaskPool will be added to it and be scheduled by it.

// 1. DeltaMergeStore::read/readRaw will call SegmentReadTaskScheduler::add to add a SegmentReadTaskPool object to the `read_pools` list and
// index segments information into `merging_segments`.
// 2. A schedule-thread will scheduling read tasks:
//   a. It scans the read_pools list and choosing a SegmentReadTaskPool.
//   b. Chooses a segment of the SegmentReadTaskPool and build a MergedTask.
//   c. Sends the MergedTask to read threads(SegmentReader).
class SegmentReadTaskScheduler
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    ~SegmentReadTaskScheduler();
    DISALLOW_COPY_AND_MOVE(SegmentReadTaskScheduler);

    // Add SegmentReadTaskPool to `read_pools` and index segments into merging_segments.
    void add(const SegmentReadTaskPoolPtr & pool) ABSL_LOCKS_EXCLUDED(absl_add_mtx, absl_mtx);

    void pushMergedTask(const MergedTaskPtr & p) { merged_task_pool.push(p); }

private:
    SegmentReadTaskScheduler();

    void setStop();
    bool isStop() const;
    bool needScheduleToRead(const SegmentReadTaskPoolPtr & pool);

    void schedLoop() ABSL_LOCKS_EXCLUDED(absl_mtx);
    bool schedule() ABSL_LOCKS_EXCLUDED(absl_mtx);
    // Choose segment to read.
    // Returns <MergedTaskPtr, run_next_schedule_immediately>
    std::pair<MergedTaskPtr, bool> scheduleMergedTask() ABSL_EXCLUSIVE_LOCKS_REQUIRED(absl_mtx);
    SegmentReadTaskPoolPtr scheduleSegmentReadTaskPoolUnlock() ABSL_EXCLUSIVE_LOCKS_REQUIRED(absl_mtx);
    // <seg_id, pool_ids>
    std::optional<std::pair<GlobalSegmentID, std::vector<UInt64>>> scheduleSegmentUnlock(
        const SegmentReadTaskPoolPtr & pool) ABSL_EXCLUSIVE_LOCKS_REQUIRED(absl_mtx);
    SegmentReadTaskPools getPoolsUnlock(const std::vector<uint64_t> & pool_ids) ABSL_EXCLUSIVE_LOCKS_REQUIRED(absl_mtx);

    // To restrict the instantaneous concurrency of `add` and avoid `schedule` from always failing to acquire the lock.
    absl::Mutex absl_add_mtx ABSL_ACQUIRED_BEFORE(absl_mtx);

    absl::Mutex absl_mtx;
    SegmentReadTaskPoolList read_pools ABSL_GUARDED_BY(absl_mtx);
    // GlobalSegmentID -> pool_ids
    MergingSegments merging_segments ABSL_GUARDED_BY(absl_mtx);

    MergedTaskPool merged_task_pool;

    std::atomic<bool> stop;
    std::thread sched_thread;

    LoggerPtr log;
};
} // namespace DB::DM

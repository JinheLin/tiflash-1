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

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Remote/RNWorkerFetchPages.h>

#include <ext/scope_guard.h>

using namespace std::chrono_literals;

namespace DB::DM::Remote
{

SegmentReadTaskPtr RNWorkerFetchPages::doWork(const SegmentReadTaskPtr & seg_task)
{
    MemoryTrackerSetter setter(true, fetch_pages_mem_tracker.get());
    Stopwatch watch_work{CLOCK_MONOTONIC_COARSE};
    SCOPE_EXIT({
        // This metric is per-segment.
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_worker_fetch_page)
            .Observe(watch_work.elapsedSeconds());
    });

    auto occupy_result = seg_task->blockingOccupySpaceForTask();
    auto req = seg_task->buildFetchPagesRequest(occupy_result.pages_not_in_cache);
    {
        auto cftiny_total = seg_task->extra_remote_info->remote_page_ids.size();
        auto cftiny_fetch = occupy_result.pages_not_in_cache.size();
        LOG_DEBUG(
            log,
            "Ready to fetch pages, seg_task={} page_hit_rate={} pages_not_in_cache={}",
            seg_task,
            cftiny_total == 0 ? "N/A" : fmt::format("{:.2f}%", 100.0 - 100.0 * cftiny_fetch / cftiny_total),
            occupy_result.pages_not_in_cache);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_read).Increment(cftiny_total);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_fetch).Increment(cftiny_fetch);
    }

    const size_t max_retry_times = 3;
    std::exception_ptr last_exception;

    // TODO: Maybe don't need to re-fetch all pages when retry.
    for (size_t i = 0; i < max_retry_times; ++i)
    {
        try
        {
            seg_task->fetchPages(req);
            seg_task->initColumnFileDataProvider(occupy_result.pages_guard);

            // We finished fetch all pages for this seg task, just return it for downstream
            // workers. If we have met any errors, page guard will not be persisted.
            return seg_task;
        }
        catch (const pingcap::Exception & e)
        {
            last_exception = std::current_exception();
            LOG_WARNING(
                log,
                "Meet RPC client exception when fetching pages: {}, will be retried. seg_task={}",
                e.displayText(),
                seg_task);
            std::this_thread::sleep_for(1s);
        }
        catch (...)
        {
            LOG_ERROR(log, "{}: {}", seg_task, getCurrentExceptionMessage(true));
            throw;
        }
    }

    // Still failed after retry...
    RUNTIME_CHECK(last_exception);
    std::rethrow_exception(last_exception);
}
} // namespace DB::DM::Remote

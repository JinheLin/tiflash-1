#pragma once

#include <Storages/DeltaMerge/UncommittedZone/UncommittedZone.h>

namespace DB::DM
{

    void UncommittedZone::ingest(const RowKeyRange & range, UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id)
    {
        auto uncommitted_file = UncommittedFile::create(range, start_ts, generation, parent_path, file_id);

        auto opt_uncommitted_range = ranges.find(rowKeyRangeToInterval(range));
        if (opt_uncommitted_range)
        {
            opt_uncommitted_range.value()->insert(uncommitted_file);
        }
        else
        {
            auto uncommitted_range = std::make_shared<UncommittedRange>(range, start_ts, parent_path, file_id);
            ranges.insert({uncommitted_range->getStart(), uncommitted_range->getEnd(), uncommitted_range});
            // Flush prev
        }
    }

    void UncommittedZone::commit(const RowKeyRange & range, UInt64 start_ts, UInt64 commit_ts)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->commit(range, start_ts, commit_ts);
        }
    }

    void UncommittedZone::remove(const RowKeyRange & range, UInt64 start_ts)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->remove(range, start_ts);
        }
    }

    void UncommittedZone::remove(const RowKeyRange & range)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->remove(range);
        }
    }

    void UncommittedZone::flush(const RowKeyRange & range) {}

} // namespace DB::DM
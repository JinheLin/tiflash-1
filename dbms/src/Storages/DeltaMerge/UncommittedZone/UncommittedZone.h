#pragma once

#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <Storages/DeltaMerge/UncommittedZone/UncommittedFile.h>
#include <Storages/DeltaMerge/UncommittedZone/UncommittedRange.h>

namespace DB::DM
{
class UncommittedZone
{
public:
    void ingest(const RowKeyRange & range, UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id)
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

    void commit(const RowKeyRange & range, UInt64 start_ts, UInt64 commit_ts)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->commit(range, start_ts, commit_ts);
        }
    }

    void remove(const RowKeyRange & range, UInt64 start_ts)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->remove(range, start_ts);
        }
    }

    void remove(const RowKeyRange & range)
    {
        auto uncommitted_ranges = ranges.findOverlapIntervals({range.getStart(), range.getEnd()});
        for (auto & uncommitted_range : uncommitted_ranges)
        {
            uncommitted_range->remove(range);
        }
    }

    void flush(const RowKeyRange & range) {}

private:
    std::mutex mtx;
    IntervalTree<std::string_view, UncommittedRangePtr> ranges;

    using Interval = IntervalTree<std::string_view, UncommittedRangePtr>::Interval;
    static Interval rowKeyRangeToInterval(const RowKeyRange & range)
    { 
        return Interval{*(range.start.value), *(range.end.value)};
    }
};
} // namespace DB::DM
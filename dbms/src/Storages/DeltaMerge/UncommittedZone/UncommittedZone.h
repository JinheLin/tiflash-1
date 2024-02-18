#pragma once

#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <Storages/DeltaMerge/UncommittedZone/UncommittedFile.h>
#include <Storages/DeltaMerge/UncommittedZone/UncommittedRange.h>

namespace DB::DM
{
class UncommittedZone
{
public:
    void ingest(const RowKeyRange & range, UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id);
    void commit(const RowKeyRange & range, UInt64 start_ts, UInt64 commit_ts);
    void remove(const RowKeyRange & range, UInt64 start_ts);
    void remove(const RowKeyRange & range);
    void flush(const RowKeyRange & range);

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
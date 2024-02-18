#include <Storages/DeltaMerge/UncommittedZone/UncommittedZone.h>

namespace DB::DM
{

void UncommittedZone::ingest(
    const RowKeyRange & range,
    UInt64 start_ts,
    UInt64 generation,
    const String & parent_path,
    PageIdU64 file_id)
{
    auto uncommitted_file = UncommittedFile::create(start_ts, generation, parent_path, file_id, {range});

    auto opt_uncommitted_range = ranges.find(rowKeyRangeToInterval(range));
    if (opt_uncommitted_range)
    {
        opt_uncommitted_range.value()->addFile(uncommitted_file);
    }
    else
    {
        // TODO: generate range_id
        UInt64 range_id = 0;
        auto uncommitted_range = UncommittedRange::create(range_id, range, uncommitted_file);
        const auto & range = uncommitted_range->getRange();
        ranges.insert({*(range.start.value), *(range.end.value), uncommitted_range});
        // TODO: Flush uncommitted_range
        // TODO: Update and flush prev uncommitted_range
    }
}
/*
void UncommittedZone::commit(const RowKeyRange & range, UInt64 start_ts, UInt64 commit_ts)
{
    auto uncommitted_ranges = ranges.findOverlappingIntervals(rowKeyRangeToInterval(range), false);
    for (auto & uncommitted_range : uncommitted_ranges)
    {
        uncommitted_range.value->commit(range, start_ts, commit_ts);
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
*/
} // namespace DB::DM
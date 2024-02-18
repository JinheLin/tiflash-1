#include <Storages/DeltaMerge/UncommittedZone/UncommittedFile.h>
#include <Storages/DeltaMerge/UncommittedZone/Proto/uncommitted.pb.h>
#include <Common/Exception.h>
#include "common/types.h"

namespace DB::DM
{

UncommittedFilePtr UncommittedFile::create(UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id, const RowKeyRanges & valid_ranges)
{
    return std::make_shared<UncommittedFile>(start_ts, generation, parent_path, file_id, valid_ranges);
}

UncommittedFile::UncommittedFile(UInt64 start_ts_, UInt64 generation_, const String & parent_path_, PageIdU64 file_id_, const RowKeyRanges & valid_ranges_)
    : start_ts(start_ts_)
    , generation(generation_)
    , parent_path(parent_path_)
    , file_id(file_id_)
    , valid_ranges(valid_ranges_)
{
    RUNTIME_CHECK(!valid_ranges.empty());
}

void UncommittedFile::deleteRange(const RowKeyRange & delete_range)
{
    RowKeyRanges new_valid_ranges;
    for (const auto & range : valid_ranges)
    {
        if (range.intersect(delete_range))
        {
            auto intersection = range.shrink(delete_range);
            if (range.start < delete_range.start)
            {
                // [range.start, delete_range.start)
                new_valid_ranges.emplace_back(range.start, delete_range.start, range.is_common_handle, range.rowkey_column_size);
            }
            if (range.end > delete_range.end)
            {
                // [delete_range.end, range.end)
                new_valid_ranges.emplace_back(delete_range.end, range.end, range.is_common_handle, range.rowkey_column_size);
            }
        }
        else
        {
            new_valid_ranges.push_back(range);
        }
    }
    valid_ranges = new_valid_ranges.empty() ? RowKeyRanges{RowKeyRange::newNone(valid_ranges.front().is_common_handle, valid_ranges.front().rowkey_column_size)} : new_valid_ranges;
}

void UncommittedFile::commit(const RowKeyRange & /*commit_range*/, UInt64 /*commit_ts*/)
{
    // TODO
    // Generate commit_ts column file.
    // Ingest with range.
    // Calcute new valid ranges.
}

} // namespace DB::DM
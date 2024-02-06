#pragma once

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <Storages/Page/PageDefinesBase.h>
#include <common/types.h>

namespace DB::DM
{

class UncommittedFile
{
public:
    UncommittedFile(const String & parent_path_, PageIdU64 file_id_, UInt64 sequence_)
        : parent_path(parent_path_)
        , file_id(file_id_)
        , sequence(sequence_)
    {}

    void remove(const RowKeyRange & range);

private:
    String parent_path;
    PageIdU64 file_id;
    UInt64 sequence;
    RowKeyRanges valid_ranges; // Empty means all.
};

using UncommittedFiles = std::vector<UncommittedFile>;

class UncommittedRange
{
public:
    explicit UncommittedRange(UInt64 range_id_)
        : range_id(range_id_)
    {
        // Restore
    }

    UncommittedRange(UInt64 range_id_, const RowKeyRange & range_, UInt64 start_ts, const UncommittedFile & file)
        : range_id(range_id_)
        , range(range_)
    {
        // Create
    }

private:
    std::mutex mtx;
    const UInt64 range_id;
    UInt64 next_range_id = 0;
    RowKeyRange range;
    std::unordered_map<UInt64, UncommittedFiles> files;
};

using UncommittedRangePtr = std::shared_ptr<UncommittedRange>;

class UncommittedZone
{
public:
    void ingest(const RowKeyRange & range, UInt64 start_ts, const String & parent_path, PageIdU64 file_id)
    {
        auto opt_uncommitted_range = ranges.find(range);
        if (opt_uncommitted_range)
        {
            auto & uncommitted_range = *opt_uncommitted_range;
            uncommitted_range->insert(start_ts, parent_path, file_id);
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
    IntervalTree<RowKeyValueRef, UncommittedRangePtr> ranges;
};
} // namespace DB::DM
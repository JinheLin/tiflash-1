#pragma once

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/UncommittedZone/Proto/uncommitted.pb.h>
#include <Storages/Page/PageDefinesBase.h>
#include <common/types.h>

namespace DB::DM
{

class UncommittedFile;
using UncommittedFilePtr = std::shared_ptr<UncommittedFile>;
using UncommittedFiles = std::vector<UncommittedFilePtr>;

class UncommittedFile
{
public:
    static UncommittedFilePtr create(
        UInt64 start_ts,
        UInt64 generation,
        const String & parent_path,
        PageIdU64 file_id,
        const RowKeyRanges & valid_ranges);
    static UncommittedFilePtr restore(const uncommitted_zone::UncommittedFile & proto);

    UncommittedFile(
        UInt64 start_ts_,
        UInt64 generation_,
        const String & parent_path_,
        PageIdU64 file_id_,
        const RowKeyRanges & valid_ranges_);
    void deleteRange(const RowKeyRange & delete_range);
    UInt64 getStartTS() const { return start_ts; }
    void commit(const RowKeyRange & commit_range, UInt64 commit_ts);
    uncommitted_zone::UncommittedFile toProto() const;

private:
    // `start_ts` is the unique identifiers for a transaction.
    UInt64 start_ts;
    // `generation` is used to indicate the order of data flushing.
    // Mainly used to distinguish between old and new versions of the same key that have been flushed multiple times.
    [[maybe_unused]] UInt64 generation;
    String parent_path;
    [[maybe_unused]] PageIdU64 file_id;
    RowKeyRanges valid_ranges;
};
} // namespace DB::DM
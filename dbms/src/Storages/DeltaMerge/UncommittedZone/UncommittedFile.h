#pragma once

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Page/PageDefinesBase.h>
#include <common/types.h>

namespace DB::DM
{

struct UncommittedFile;
using UncommittedFilePtr = std::shared_ptr<UncommittedFile>;
using UncommittedFiles = std::vector<UncommittedFilePtr>;

struct UncommittedFile
{
    static UncommittedFilePtr create(const RowKeyRange & /*range*/, UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id);

    UncommittedFile(UInt64 start_ts_, UInt64 generation_, const String & parent_path_, PageIdU64 file_id_);
    
    void remove(const RowKeyRange & range);

    // `start_ts` is the unique identifiers for a transaction.
    UInt64 start_ts;
    // `generation` is used to indicate the order of data flushing.
    // Mainly used to distinguish between old and new versions of the same key that have been flushed multiple times.
    UInt64 generation;
    String parent_path;
    PageIdU64 file_id;
    RowKeyRanges valid_ranges; // Empty means all.
};
} // namespace DB::DM
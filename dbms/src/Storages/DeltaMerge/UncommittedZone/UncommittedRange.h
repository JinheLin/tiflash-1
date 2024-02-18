#pragma once

#include <Storages/DeltaMerge/UncommittedZone/UncommittedFile.h>

namespace DB::DM
{

class UncommittedRange;
using UncommittedRangePtr = std::shared_ptr<UncommittedRange>;

class UncommittedRange
{
public:
    static UncommittedRangePtr create(UInt64 range_id, const RowKeyRange & range, const UncommittedFilePtr & file);
    static UncommittedRangePtr restore(UInt64 range_id);

    // Use for restore.
    explicit UncommittedRange(UInt64 range_id_);
    // Use for create.
    UncommittedRange(UInt64 range_id_, const RowKeyRange & range_, UInt64 start_ts, const UncommittedFilePtr & file);
    void insert(UInt64 start_ts, const UncommittedFilePtr & file);

private:
    std::mutex mtx;
    const UInt64 range_id;
    UInt64 next_range_id = 0;
    RowKeyRange range;
    std::unordered_map<UInt64, UncommittedFiles> files;
};

} // namespace DB::DM
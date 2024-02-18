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
    UncommittedRange(UInt64 range_id_, const RowKeyRange & range_, const UncommittedFilePtr & file);
    void addFile(const UncommittedFilePtr & file);
    const RowKeyRange & getRange() const;
private:
    std::mutex mtx;
    [[maybe_unused]]const UInt64 range_id;
    [[maybe_unused]]UInt64 next_range_id = 0;
    [[maybe_unused]]RowKeyRange range;
    std::unordered_map<UInt64, UncommittedFiles> files;
};

} // namespace DB::DM
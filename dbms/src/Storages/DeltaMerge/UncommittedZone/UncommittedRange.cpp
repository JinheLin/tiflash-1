#pragma once

#include <Storages/DeltaMerge/UncommittedZone/UncommittedRange.h>

namespace DB::DM
{

UncommittedRangePtr UncommittedRange::create(UInt64 range_id, const RowKeyRange & range, const UncommittedFilePtr & file)
{
    // TODO
    return nullptr;
}

UncommittedRangePtr UncommittedRange::restore(UInt64 range_id)
{
    // TODO
    return nullptr;
}

UncommittedRange::UncommittedRange(UInt64 range_id_)
    : range_id(range_id_)
{
    // TODO
}


UncommittedRange::UncommittedRange(UInt64 range_id_, const RowKeyRange & range_, UInt64 start_ts, const UncommittedFilePtr & file)
    : range_id(range_id_)
    , range(range_)
{
    files[start_ts].push_back(file);
}

void UncommittedRange::insert(UInt64 start_ts, const UncommittedFilePtr & file)
{
    // TODO
}

} // namespace DB::DM
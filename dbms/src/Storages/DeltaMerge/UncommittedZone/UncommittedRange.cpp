#include <Storages/DeltaMerge/UncommittedZone/UncommittedRange.h>

namespace DB::DM
{

UncommittedRangePtr UncommittedRange::create([[maybe_unused]]UInt64 range_id, [[maybe_unused]] const RowKeyRange & range, [[maybe_unused]]const UncommittedFilePtr & file)
{
    return std::make_shared<UncommittedRange>(range_id, range, file);
}

UncommittedRangePtr UncommittedRange::restore([[maybe_unused]]UInt64 range_id)
{
    // TODO
    return nullptr;
}

UncommittedRange::UncommittedRange(UInt64 range_id_)
    : range_id(range_id_)
{
    // TODO
}

UncommittedRange::UncommittedRange(UInt64 range_id_, const RowKeyRange & range_, const UncommittedFilePtr & file)
    : range_id(range_id_)
    , range(range_)
{
    files[file->getStartTS()].push_back(file);
}

void UncommittedRange::addFile(const UncommittedFilePtr & file)
{
    files[file->getStartTS()].push_back(file);
}

const RowKeyRange & UncommittedRange::getRange() const
{
    return range;
}

} // namespace DB::DM
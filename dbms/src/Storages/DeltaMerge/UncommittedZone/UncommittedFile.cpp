#include <Storages/DeltaMerge/UncommittedZone/UncommittedFile.h>

namespace DB::DM
{

UncommittedFilePtr UncommittedFile::create(const RowKeyRange & /*range*/, UInt64 start_ts, UInt64 generation, const String & parent_path, PageIdU64 file_id)
{
    // TODO: verify range of DMFile.
    return std::make_shared<UncommittedFile>(start_ts, generation, parent_path, file_id);
}

UncommittedFile::UncommittedFile(UInt64 start_ts_, UInt64 generation_, const String & parent_path_, PageIdU64 file_id_)
    : start_ts(start_ts_)
    , generation(generation_)
    , parent_path(parent_path_)
    , file_id(file_id_)
{}

void UncommittedFile::remove(const RowKeyRange & /*range*/)
{
    // TODO
}

} // namespace DB::DM
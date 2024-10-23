#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeMyTimeBase.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <ext/scope_guard.h>
namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::DM
{

using ColumnStreams = DMFileWriter::ColumnStreams;

namespace
{
ColumnStreams createOutputStreams(
    const Context & context,
    const DMFilePtr & dmfile,
    ColId original_col_id,
    const DataTypePtr & type)
{
    const auto & settings = context.getSettingsRef();
    auto compression_settings = CompressionSettings(settings.dt_compression_method, settings.dt_compression_level);
    auto max_compress_block_size = settings.max_compress_block_size;
    const auto & file_provider = context.getFileProvider();
    const auto & write_limiter = context.getWriteLimiter();
    ColumnStreams column_streams;

    DMFileWriter::addStreams(
        getInvertedIndexColumnID(original_col_id),
        type,
        true,
        dmfile,
        compression_settings,
        max_compress_block_size,
        file_provider,
        write_limiter,
        true,
        column_streams);


    DMFileWriter::addStreams(
        getInvertedRowIDColumnID(original_col_id),
        getInvertedRowIDDataType(),
        false,
        dmfile,
        compression_settings,
        max_compress_block_size,
        file_provider,
        write_limiter,
        true,
        column_streams);

    return column_streams;
}

Block readBlock(IBlockInputStream & input_stream, ColId original_col_id, UInt32 rows)
{
    auto block = input_stream.read();
    RUNTIME_CHECK(block.rows() == rows, block.rows(), rows);
    RUNTIME_CHECK(block.columns() == 1, block.columns());

    auto rid_type = getInvertedRowIDDataType();
    auto rid_col = rid_type->createColumn();
    auto * col_u32 = typeid_cast<ColumnUInt32 *>(rid_col.get());
    RUNTIME_CHECK(col_u32 != nullptr);
    auto & vec = col_u32->getData();
    vec.resize(rows);
    std::iota(vec.begin(), vec.end(), 0);
    block.insert(ColumnWithTypeAndName(std::move(rid_col), rid_type, getInvertedRowIDColumnName(original_col_id)));
    return block;
}

void writeColumn(
    const ColumnStreams & column_streams,
    const IDataType & type,
    const IColumn & column,
    [[maybe_unused]]size_t min_compress_block_size,
    ColId col_id)
{
    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(stream_name);

            // Write Min-Max Index
            if (stream->minmaxes)
                stream->minmaxes->addPack(column, nullptr);

            // Write Mark
            stream->compressed_buf->next();
            auto offset_in_compressed_block = stream->compressed_buf->offset();
            writeIntBinary(stream->plain_file->count(), *stream->mark_file);
            writeIntBinary(offset_in_compressed_block, *stream->mark_file);
        },
        {});

    type.serializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(stream_name);
            return &(*stream->compressed_buf);
        },
        0,
        column.size(),
        true,
        {});

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(stream_name);
            stream->compressed_buf->nextIfAtEnd();
        },
        {});
}

ColumnPtr copyColumn(const IDataType & type, const IColumn & from_column, size_t offset, size_t rows)
{
    auto to_column = type.createColumn();
    to_column->insertRangeFrom(from_column, offset, rows);
    return to_column;
}

void writeColumn(
    const DMFilePtr & dmfile,
    const ColumnStreams & column_streams,
    const IDataType & type,
    const IColumn & column,
    size_t min_compress_block_size,
    ColId col_id)
{
    const auto & pack_stats = dmfile->getPackStats();
    size_t offset = 0;
    for (const auto & pack : pack_stats)
    {
        auto pack_column = copyColumn(type, column, offset, pack.rows);
        offset += pack.rows;
        writeColumn(column_streams, type, *pack_column, min_compress_block_size, col_id);
    }
}

[[nodiscard]] std::optional<ext::ScopeGuard<std::function<void()>>> prepare(DMFile & dmfile, ColId col_id)
{
    if (!dmfile.isColumnExist(col_id))
    {
        LOG_ERROR(dmfile.getLogger(), "ColId {} not exist", col_id);
        return std::nullopt;
    }

    const auto & col_stat = dmfile.getColumnStat(col_id);

    auto type = removeNullable(col_stat.type);
    if (!type->isInteger() && !type->isDateOrDateTime())
    {
        LOG_ERROR(
            dmfile.getLogger(),
            "ColId={} Type={} FullType={} is not supported",
            col_id,
            type->getName(),
            col_stat.type->getName());
        return std::nullopt;
    }

    constexpr auto force_build = true;

    auto fnames = dmfile.getInvertedFileNames(col_id);
    if (!force_build && std::all_of(fnames.cbegin(), fnames.cend(), [&dmfile](const String & fname) {
            return std::filesystem::exists(dmfile.subFilePath(fname));
        }))
        return std::nullopt;

    // Avoid build inverted index concurrently
    // Maybe allow build inverted index of different columns concurrently
    {
        std::unique_lock lock(dmfile.building_inverted_index_mutex);
        if (dmfile.building_inverted_index)
            return std::nullopt;
        dmfile.building_inverted_index = true;
    }
    std::function<void()> func = [&dmfile]() {
        dmfile.building_inverted_index = false;
    };
    auto res = ext::make_scope_guard(std::move(func));

    for (const auto & fname : fnames)
    {
        auto full_fname = dmfile.subFilePath(fname);
        auto temp_fname = full_fname + ".tmp";
        std::filesystem::remove(full_fname);
        std::filesystem::remove(temp_fname);
    }

    return std::move(res);
}

void writeBlock(
    const Context & context,
    const DMFilePtr & dmfile,
    const Block & block,
    const ColumnStreams & output_streams,
    size_t min_compress_block_size)
{
    const auto & index_column = block.getByPosition(0);
    auto original_col_id = index_column.column_id;
    writeColumn(
        dmfile,
        output_streams,
        *index_column.type,
        *index_column.column,
        min_compress_block_size,
        getInvertedIndexColumnID(original_col_id));

    const auto & rowid_column = block.getByPosition(1);
    writeColumn(
        dmfile,
        output_streams,
        *rowid_column.type,
        *rowid_column.column,
        min_compress_block_size,
        getInvertedRowIDColumnID(original_col_id));

    const auto & file_provider = context.getFileProvider();
    const auto & write_limiter = context.getWriteLimiter();

    auto index_stream_name = DMFile::getFileNameBase(getInvertedIndexColumnID(original_col_id));
    auto & stream = output_streams.at(index_stream_name);
    RUNTIME_CHECK(stream->minmaxes);

    auto buf = ChecksumWriteBufferBuilder::build(
        dmfile->getConfiguration().has_value(),
        file_provider,
        dmfile->colIndexPath(index_stream_name),
        dmfile->encryptionIndexPath(index_stream_name),
        false,
        write_limiter,
        detail::getAlgorithmOrNone(*dmfile),
        detail::getFrameSizeOrDefault(*dmfile));
    stream->minmaxes->write(*index_column.type, *buf);
    buf->sync();

    for (const auto & [name, stream] : output_streams)
    {
        stream->compressed_buf->next();
        stream->plain_file->next();
        stream->plain_file->sync();
        stream->mark_file->sync();
    }
}

ColumnStat buildColumnStat(
    const DMFilePtr & dmfile,
    ColId col_id,
    const DataTypePtr & type,
    size_t avg_size,
    bool has_index)
{
    ColumnStat col_stat;
    col_stat.col_id = col_id;
    col_stat.type = type;
    col_stat.avg_size = avg_size;
    {
        auto fname_base = DMFile::getFileNameBase(col_id);
        col_stat.data_bytes = std::filesystem::file_size(dmfile->subFilePath(colDataFileName(fname_base)));
        col_stat.mark_bytes = std::filesystem::file_size(dmfile->subFilePath(colMarkFileName(fname_base)));
        if (has_index)
            col_stat.index_bytes = std::filesystem::file_size(dmfile->subFilePath(colIndexFileName(fname_base)));
        col_stat.serialized_bytes += col_stat.data_bytes + col_stat.mark_bytes + col_stat.index_bytes;
    }
    if (type->isNullable())
    {
        auto null_fname_base
            = DMFile::getFileNameBase(col_id, {IDataType::Substream{IDataType::Substream::Type::NullMap}});
        col_stat.nullmap_data_bytes = std::filesystem::file_size(dmfile->subFilePath(colDataFileName(null_fname_base)));
        col_stat.nullmap_mark_bytes = std::filesystem::file_size(dmfile->subFilePath(colMarkFileName(null_fname_base)));
        col_stat.serialized_bytes += col_stat.nullmap_data_bytes + col_stat.nullmap_mark_bytes;
    }
    return col_stat;
}

void updateColumnStats(const Context & context, const DMFilePtr & dmfile, ColId col_id)
{
    const auto & col_stat = dmfile->getColumnStat(col_id);
    auto index_col_stat
        = buildColumnStat(dmfile, getInvertedIndexColumnID(col_id), col_stat.type, col_stat.avg_size, true);
    auto rowid_col_stat
        = buildColumnStat(dmfile, getInvertedRowIDColumnID(col_id), getInvertedRowIDDataType(), sizeof(UInt32), false);

    auto & column_stats = dmfile->getMeta().getColumnStats();
    column_stats.emplace(getInvertedIndexColumnID(col_id), index_col_stat);
    column_stats.emplace(getInvertedRowIDColumnID(col_id), rowid_col_stat);

    auto meta_path = dmfile->getMeta().metaPath();
    auto meta_tmp_path = meta_path + ".tmp";
    auto write_buffer = WriteBufferFromWritableFileBuilder::buildPtr(
        context.getFileProvider(),
        meta_tmp_path,
        dmfile->getMeta().encryptionMetaPath(),
        /*create_new_encryption_info*/ false,
        context.getWriteLimiter(),
        DMFileMetaV2::meta_buffer_size);
    dmfile->getMeta().finalize(*write_buffer, context.getFileProvider(), context.getWriteLimiter());
    write_buffer->sync();
    std::filesystem::rename(meta_tmp_path, meta_path);
}

} // namespace

Strings DMFile::getInvertedIndexFileNames(ColId original_col_id) const
{
    Strings fnames;
    auto cb = [&](const IDataType::SubstreamPath & substream_path) {
        auto base = DMFile::getFileNameBase(getInvertedIndexColumnID(original_col_id), substream_path);
        fnames.push_back(colDataFileName(base));
        fnames.push_back(colMarkFileName(base));
        if (substream_path.empty())
            fnames.push_back(colIndexFileName(base));
    };
    getColumnStat(original_col_id).type->enumerateStreams(cb, {});
    return fnames;
}

Strings DMFile::getInvertedRowIDFileNames(ColId original_col_id) const
{
    Strings fnames;
    auto cb = [&](const IDataType::SubstreamPath & substream_path) {
        auto base = DMFile::getFileNameBase(getInvertedRowIDColumnID(original_col_id), substream_path);
        fnames.push_back(colDataFileName(base));
        fnames.push_back(colMarkFileName(base));
    };
    getInvertedRowIDDataType()->enumerateStreams(cb, {});
    return fnames;
}

Strings DMFile::getInvertedFileNames(ColId original_col_id) const
{
    auto fnames = getInvertedIndexFileNames(original_col_id);
    auto v = getInvertedRowIDFileNames(original_col_id);
    fnames.insert(fnames.end(), v.begin(), v.end());
    return fnames;
}

void DMFile::buildInvertedIndex(const Context & context, const DMFilePtr & dmfile, ColId col_id)
{
    auto guard = prepare(*dmfile, col_id);
    if (!guard)
        return;

    auto rows = dmfile->getRows();
    RUNTIME_CHECK(rows <= std::numeric_limits<UInt32>::max());
    DMFileBlockInputStreamBuilder builder(context);
    builder.setRowsThreshold(rows);
    const auto & col_stat = dmfile->getColumnStat(col_id);
    auto cd = ColumnDefine{col_stat.col_id, "", col_stat.type};
    auto input_stream = builder.build(dmfile, {cd}, /*rowkey_ranges*/ {}, std::make_shared<ScanContext>());
    auto block = readBlock(*input_stream, col_id, rows);

    auto desc = SortColumnDescription{
        0, // sort by the first column
        1, // asc
        1 // null in the last
    };
    sortBlock(block, {desc});
    /*
    const auto * index = toColumnVectorDataPtr<UInt64>(
        static_cast<const ColumnNullable &>(*block.getByPosition(0).column).getNestedColumnPtr());
    const auto * rowid = toColumnVectorDataPtr<UInt32>(block.getByPosition(1).column);
*/
    RUNTIME_CHECK(!block.getByPosition(0).column->isNullAt(rows - 1)); // TODO: support null

    auto output_streams = createOutputStreams(context, dmfile, col_id, cd.type);
    writeBlock(context, dmfile, block, output_streams, context.getSettingsRef().min_compress_block_size);

    updateColumnStats(context, dmfile, col_id);

    LOG_INFO(dmfile->getLogger(), "Done: {}", dmfile->path());
}

} // namespace DB::DM

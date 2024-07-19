// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/formatReadable.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <iostream>
namespace bpo = boost::program_options;

using namespace DB;
using namespace DB::DM;

namespace DTTool::Analyze
{
MinMaxIndexPtr loadIndex(const DMFilePtr & dmfile, const FileProviderPtr & file_provider, ColId col_id)
{
    auto logger = DB::Logger::get("DTToolInspect");
    if (!dmfile->isColIndexExist(col_id))
    {
        LOG_INFO(logger, "Index of col {} is not exist", col_id);
        return nullptr;
    }
    const auto & type = dmfile->getColumnStat(col_id).type;
    LOG_INFO(logger, "Type: {}", type->getName());
    const auto file_name_base = DMFile::getFileNameBase(col_id);
    const auto index_file_size = dmfile->colIndexSize(col_id);
    if (index_file_size == 0)
        return std::make_shared<MinMaxIndex>(*type);

    auto scan_context = std::make_shared<ScanContext>();
    auto index_guard = S3::S3RandomAccessFile::setReadFileInfo({
        .size = dmfile->getReadFileSize(col_id, colIndexFileName(file_name_base)),
        .scan_context = scan_context,
    });

    if (!dmfile->getConfiguration()) // v1
    {
        auto index_buf = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            dmfile->colIndexPath(file_name_base),
            dmfile->encryptionIndexPath(file_name_base),
            std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), index_file_size));
        return MinMaxIndex::read(*type, index_buf, index_file_size);
    }
    else if (dmfile->useMetaV2()) // v3
    {
        const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
        assert(dmfile_meta != nullptr);
        auto info = dmfile_meta->merged_sub_file_infos.find(colIndexFileName(file_name_base));
        if (info == dmfile_meta->merged_sub_file_infos.end())
        {
            throw Exception(
                fmt::format("Unknown index file {}", dmfile->colIndexPath(file_name_base)),
                ErrorCodes::LOGICAL_ERROR);
        }

        auto file_path = dmfile->meta->mergedPath(info->second.number);
        auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
        auto offset = info->second.offset;
        auto data_size = info->second.size;

        auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            file_path,
            encryp_path,
            dmfile->getConfiguration()->getChecksumFrameLength());
        buffer.seek(offset);

        String raw_data;
        raw_data.resize(data_size);

        buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

        auto buf = ChecksumReadBufferBuilder::build(
            std::move(raw_data),
            dmfile->colDataPath(file_name_base),
            dmfile->getConfiguration()->getChecksumFrameLength(),
            dmfile->getConfiguration()->getChecksumAlgorithm(),
            dmfile->getConfiguration()->getChecksumFrameLength());

        auto header_size = dmfile->getConfiguration()->getChecksumHeaderLength();
        auto frame_total_size = dmfile->getConfiguration()->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);

        return MinMaxIndex::read(*type, *buf, index_file_size - header_size * frame_count);
    }
    else
    { // v2
        auto index_buf = ChecksumReadBufferBuilder::build(
            file_provider,
            dmfile->colIndexPath(file_name_base),
            dmfile->encryptionIndexPath(file_name_base),
            index_file_size,
            /*read_limiter*/ nullptr,
            dmfile->getConfiguration()->getChecksumAlgorithm(),
            dmfile->getConfiguration()->getChecksumFrameLength());
        auto header_size = dmfile->getConfiguration()->getChecksumHeaderLength();
        auto frame_total_size = dmfile->getConfiguration()->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);
        return MinMaxIndex::read(*type, *index_buf, index_file_size - header_size * frame_count);
    }
}

void run(DB::Context & context, const String & workdir, size_t file_id, [[maybe_unused]] size_t col_id)
{
    // from this part, the base daemon is running, so we use logger instead
    auto logger = DB::Logger::get("DTToolInspect");

    // Open the DMFile at `workdir/dmf_<file-id>`
    auto fp = context.getFileProvider();
    auto dmfile = DB::DM::DMFile::restore(fp, file_id, 0, workdir, DB::DM::DMFileMeta::ReadMode::all());

    LOG_INFO(logger, "bytes on disk: {}", dmfile->getBytesOnDisk());

    auto minmax_index = loadIndex(dmfile, fp, col_id);

    if (minmax_index == nullptr)
        return;

    LOG_INFO(logger, "MinMax: {}", minmax_index->byteSize());

    size_t pack_count = minmax_index->getPackCount();
    LOG_INFO(logger, "count: {}", pack_count);
    for (size_t i = 0; i < pack_count; ++i)
    {
        auto [min, max] = minmax_index->getUInt64MinMax(i);
        LOG_INFO(logger, "[{}, {}]", MyDate{min}.toString(), MyDate{max}.toString());
    }
}

void entry(const std::vector<std::string> & opts)
{
    bpo::variables_map vm;
    bpo::options_description options{"Delta Merge Analyze"};
    options.add_options() //
        ("help", "Print help message and exit.") //
        ("workdir",
         bpo::value<std::string>()->required(),
         "Target directory. Will inpsect the delta-tree file ${workdir}/dmf_${file-id}/") //
        ("file-id", bpo::value<size_t>()->required(), "Target DTFile ID.") //
        ("col-id", bpo::value<size_t>()->required(), "Target Column ID.");

    bpo::store(
        bpo::command_line_parser(opts)
            .options(options)
            .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
            .run(),
        vm);

    try
    {
        if (vm.count("help"))
        {
            options.print(std::cerr);
            return;
        }
        bpo::notify(vm);
        auto workdir = vm["workdir"].as<std::string>();
        auto file_id = vm["file-id"].as<size_t>();
        auto col_id = vm["col-id"].as<size_t>();
        auto env = detail::ImitativeEnv{workdir};
        run(*env.getContext(), workdir, file_id, col_id);
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl;
        options.print(std::cerr);
        throw;
    }
    catch (DB::Exception &)
    {
        DB::tryLogCurrentException(DB::Logger::get("DTToolAnalyze"));
        throw;
    }
}
} // namespace DTTool::Analyze

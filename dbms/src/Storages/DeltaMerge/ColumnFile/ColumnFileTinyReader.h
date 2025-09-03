// Copyright 2024 PingCAP, Inc.
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

#pragma once

#include <IO/FileProvider/FileProvider_fwd.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <span>

namespace DB::DM
{

class ColumnFileTinyReader : public ColumnFileReader
{
private:
    const ColumnFileTiny & tiny_file;
    const IColumnFileDataProviderPtr data_provider;
    const ColumnDefinesPtr col_defs;

    // PK & Version column cache.
    Columns cols_data_cache;
    bool read_done = false;

    ScanContextPtr scan_context;
    ReadTag read_tag;
    std::optional<LACBytesCollector> lac_bytes_collector;
public:
    ColumnFileTinyReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_,
        const Columns & cols_data_cache_,
        const ScanContextPtr & scan_context,
        ReadTag read_tag)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
        , scan_context(scan_context)
        , read_tag(read_tag)
        , lac_bytes_collector(scan_context ? scan_context->newLACBytesCollector(read_tag) : std::nullopt)
    {}

    ColumnFileTinyReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_,
        const ScanContextPtr & scan_context,
        ReadTag read_tag)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , col_defs(col_defs_)
        , scan_context(scan_context)
        , read_tag(read_tag)
        , lac_bytes_collector(scan_context ? scan_context->newLACBytesCollector(read_tag) : std::nullopt)
    {}

    /// This is a ugly hack to fast return PK & Version column.
    std::pair<ColumnPtr, ColumnPtr> getPKAndVersionColumns();

    std::pair<size_t, size_t> readRows(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range) override;

    Columns readFromDisk(
        const IColumnFileDataProviderPtr & data_provider,
        const std::span<const ColumnDefine> & column_defines);

    Block readNextBlock() override;

    size_t skipNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag read_tag_) override;
};

} // namespace DB::DM

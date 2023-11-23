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
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageUtil.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>
#include <tipb/expression.pb.h>

#include <ext/scope_guard.h>
#include <memory>
#include <tuple>

namespace DB
{
WNFetchPagesStreamWriterPtr WNFetchPagesStreamWriter::build(
    const DM::Remote::SegmentPagesFetchTask & task,
    const PageIdU64s & read_page_ids,
    const Settings & settings)
{
    return std::unique_ptr<WNFetchPagesStreamWriter>(
        new WNFetchPagesStreamWriter(task.seg_task, task.column_defines, read_page_ids, settings));
}

std::tuple<DM::RemotePb::RemotePage, size_t> WNFetchPagesStreamWriter::getPersistedRemotePage(UInt64 page_id)
{
    auto page = seg_task->read_snapshot->delta->getPersistedFileSetSnapshot()->getDataProvider()->readTinyData(page_id);
    DM::RemotePb::RemotePage remote_page;
    remote_page.set_page_id(page_id);
    remote_page.mutable_data()->assign(page.data.begin(), page.data.end());
    const auto field_sizes = PageUtil::getFieldSizes(page.field_offsets, page.data.size());
    for (const auto field_sz : field_sizes)
    {
        remote_page.add_field_sizes(field_sz);
    }
    return {remote_page, page.data.size()};
}

std::tuple<disaggregated::PagesPacket, size_t> WNFetchPagesStreamWriter::getMemTableSet()
{
    const auto & mem_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
    auto mem_size_before = mem_tracker_wrapper.size;
    auto cfs = DM::Remote::Serializer::serializeColumnFileSet(mem_snap, mem_tracker_wrapper, true);
    auto mem_size_delta = mem_tracker_wrapper.size > mem_size_before ? mem_tracker_wrapper.size - mem_size_before : 0;
    disaggregated::PagesPacket packet;
    for (const auto & cf : cfs)
    {
        packet.mutable_chunks()->Add(cf.SerializeAsString());
    }
    return std::make_tuple(std::move(packet), mem_size_delta);
}

std::tuple<Int64, Int64> WNFetchPagesStreamWriter::sendMemTableSet(SyncPagePacketWriter * sync_writer)
{
    if (!enable_delta_cache_streaming)
    {
        return std::make_tuple(-1, -1);
    }
    auto t = getMemTableSet();
    SCOPE_EXIT({ mem_tracker_wrapper.free(std::get<1>(t)); });

    const auto & [packet, data_size] = t;
    sync_writer->Write(packet);
    return std::make_tuple(packet.chunks_size(), data_size);
}

void WNFetchPagesStreamWriter::pipeTo(SyncPagePacketWriter * sync_writer)
{
    Stopwatch send_mem_sw;
    auto [mem_count, mem_size] = sendMemTableSet(sync_writer);
    auto send_mem_ns = send_mem_sw.elapsed();

    disaggregated::PagesPacket packet;
    UInt64 total_pages_data_size = 0;
    UInt64 packet_count = 0;
    UInt64 pending_pages_data_size = 0;
    UInt64 read_page_ns = 0;
    UInt64 send_page_ns = 0;
    for (const auto page_id : read_page_ids)
    {
        Stopwatch sw_packet;
        auto [remote_page, page_size] = getPersistedRemotePage(page_id);
        total_pages_data_size += page_size;
        pending_pages_data_size += page_size;
        packet.mutable_pages()->Add(remote_page.SerializeAsString());
        mem_tracker_wrapper.alloc(page_size);
        read_page_ns += sw_packet.elapsedFromLastTime();

        if (pending_pages_data_size > packet_limit_size)
        {
            ++packet_count;
            sync_writer->Write(packet);
            send_page_ns += sw_packet.elapsedFromLastTime();
            pending_pages_data_size = 0;
            packet.clear_pages(); // Only set pages field before.
            mem_tracker_wrapper.freeAll();
        }
    }

    if (packet.pages_size() > 0)
    {
        Stopwatch sw;
        ++packet_count;
        sync_writer->Write(packet);
        send_page_ns += sw.elapsedFromLastTime();
    }

    LOG_DEBUG(
        log,
        "mem_count={} mem_size={} send_mem_ms={} pages={} pages_size={} packets={} read_page_ms={} send_page_ms={}",
        mem_count,
        mem_size,
        send_mem_ns / 1000000,
        read_page_ids.size(),
        total_pages_data_size,
        packet_count,
        read_page_ns / 1000000,
        send_page_ns / 1000000);
}


} // namespace DB

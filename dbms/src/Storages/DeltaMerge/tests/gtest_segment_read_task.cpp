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

#include <Common/Logger.h>
#include <Core/BlockUtils.h>
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreMock.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <grpcpp/server_builder.h>

#include <algorithm>
#include <atomic>
#include <thread>

using namespace DB::tests;

namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
extern const int FETCH_PAGES_ERROR;
extern const int TIMEOUT_EXCEEDED;
} // namespace DB::ErrorCodes

namespace DB::FailPoints
{
extern const char skip_check_segment_update[];
extern const char disable_flush_cache[];
} // namespace DB::FailPoints

namespace DB::DM::tests
{

namespace
{
class FetchPagesService : public tikvpb::Tikv::Service
{
public:
    grpc::Status FetchDisaggPages(
        grpc::ServerContext *,
        const disaggregated::FetchDisaggPagesRequest * request,
        grpc::ServerWriter<disaggregated::PagesPacket> * writer) override
    {
        const bool fail = requests.fetch_add(1) < failures;
        const size_t packet_count = fail ? (send_packet_before_error ? 1 : 0) : packets.size();
        for (size_t i = 0; i < packet_count; ++i)
        {
            auto packet = packets[i];
            packet.clear_pages();
            for (const auto & data : packets[i].pages())
            {
                RemotePb::RemotePage page;
                if (!page.ParseFromString(data))
                    return {grpc::StatusCode::INTERNAL, "Invalid test page"};
                if (std::find(request->page_ids().begin(), request->page_ids().end(), page.page_id())
                    != request->page_ids().end())
                    packet.add_pages(data);
            }
            if (!writer->Write(packet))
                return {grpc::StatusCode::CANCELLED, "Client stopped reading pages"};
        }
        return fail ? error : grpc::Status::OK;
    }

    std::vector<disaggregated::PagesPacket> packets;
    size_t failures = 0;
    grpc::Status error{grpc::StatusCode::UNAVAILABLE, "WN is restarting"};
    bool send_packet_before_error = false;
    std::atomic<size_t> requests{0};
};
} // namespace

class SegmentReadTaskTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentReadTaskTest");


    void initInputStream()
    {
        // write stable
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10000, 0);
        mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
        // split into 2 segment
        auto segment_id = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
        ASSERT_TRUE(segment_id.has_value());
        // write delta
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, 0);
        writeSegment(*segment_id, 1000, 8000);

        // Init delta index
        {
            auto [first, first_snap] = getSegmentForRead(DELTA_MERGE_FIRST_SEGMENT_ID);
            first->placeDeltaIndex(*dm_context, first_snap);
        }
        auto [first, first_snap] = getSegmentForRead(DELTA_MERGE_FIRST_SEGMENT_ID);
        LOG_DEBUG(log, "First: {}", first_snap->delta->getSharedDeltaIndex()->toString());

        {
            auto [second, second_snap] = getSegmentForRead(*segment_id);
            second->placeDeltaIndex(*dm_context, second_snap);
        }
        auto [second, second_snap] = getSegmentForRead(*segment_id);
        LOG_DEBUG(log, "Second: {}", second_snap->delta->getSharedDeltaIndex()->toString());

        // Create a wrong delta index for first segment.
        auto [placed_rows, placed_deletes] = first_snap->delta->getSharedDeltaIndex()->getPlacedStatus();
        auto broken_delta_index = std::make_shared<DeltaIndex>(
            second_snap->delta->getSharedDeltaIndex()->getDeltaTree(),
            placed_rows,
            placed_deletes);

        // hack to change the "immutable" delta-index on delta-snapshot for testing
        (*const_cast<DeltaIndexPtr *>(&first_snap->delta->getSharedDeltaIndex())) = broken_delta_index;

        // Always use delta index in this case.
        auto guard = disableVersionChainTemporary(db_context->getGlobalContext().getSettingsRef());
        // read the left segment after split
        auto task = std::make_shared<DM::SegmentReadTask>(
            first,
            first_snap,
            createDMContext(),
            RowKeyRanges{first->getRowKeyRange()});

        const auto & column_defines = *tableColumns();
        ASSERT_FALSE(task->doInitInputStreamWithErrorFallback(
            column_defines,
            0,
            nullptr,
            ReadMode::Bitmap,
            DEFAULT_BLOCK_SIZE,
            true));

        try
        {
            [[maybe_unused]] auto succ = task->doInitInputStreamWithErrorFallback(
                column_defines,
                0,
                nullptr,
                ReadMode::Bitmap,
                DEFAULT_BLOCK_SIZE,
                false);
            FAIL() << "Should not come here.";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::DT_DELTA_INDEX_ERROR);
        }

        task->initInputStream(
            column_defines,
            std::numeric_limits<UInt64>::max(),
            nullptr,
            ReadMode::Bitmap,
            DEFAULT_BLOCK_SIZE,
            true);
        auto stream = task->getInputStream();
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, 5000))}));
    }
};

TEST_F(SegmentReadTaskTest, InitInputStream)
try
{
    initInputStream();
}
CATCH

class DMStoreForSegmentReadTaskTest : public DeltaMergeStoreTest
{
public:
    void SetUp() override
    {
        DeltaMergeStoreTest::SetUp();
        initReadNodePageCacheIfUninitialized();
    }

    auto getWNReadSnapshot()
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto snap = store->writeNodeBuildRemoteReadSnapshot(
            *db_context,
            db_context->getSettingsRef(),
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            1,
            "req_id",
            {},
            scan_context);
        snap->column_defines = std::make_shared<ColumnDefines>(store->getTableColumns());
        return snap;
    }

    [[nodiscard]] auto setMockRemoteDataStore()
    {
        db_context->getSharedContextDisagg()->remote_data_store
            = std::make_shared<DM::Remote::DataStoreMock>(db_context->getFileProvider());
        return ext::make_scope_guard([&]() { db_context->getSharedContextDisagg()->remote_data_store = nullptr; });
    }

    [[nodiscard]] static auto disableFlushCache()
    {
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::skip_check_segment_update);
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::disable_flush_cache);
        return ext::make_scope_guard([]() {
            DB::FailPointHelper::disableFailPoint(DB::FailPoints::skip_check_segment_update);
            DB::FailPointHelper::disableFailPoint(DB::FailPoints::disable_flush_cache);
        });
    }

    auto getRemoteSegment(const Remote::DisaggPhysicalTableReadSnapshotPtr & snap, bool need_mem_data, int idx)
    {
        MemTrackerWrapper mem_tracker_wrapper(nullptr);
        auto remote_table_pb = Remote::Serializer::serializePhysicalTable(
            snap,
            /*task_id*/ {},
            mem_tracker_wrapper,
            need_mem_data);
        RUNTIME_CHECK(remote_table_pb.segments_size() > idx, remote_table_pb.segments_size(), idx);
        auto remote_seg = remote_table_pb.segments(idx);
        auto guard = setMockRemoteDataStore();
        auto scan_context = std::make_shared<ScanContext>();
        return std::make_shared<SegmentReadTask>(
            Logger::get(),
            *db_context,
            scan_context,
            remote_seg,
            DisaggTaskId{},
            /*store_id*/ 1,
            /*store_address*/ "127.0.0.1",
            store->keyspace_id,
            store->physical_table_id,
            /*pk_col_id*/ 0,
            /*is_same_zone*/ false,
            /*establish_disagg_task_resp_size*/ 0);
    }

    void initReadNodePageCacheIfUninitialized()
    {
        if (db_context->getSharedContextDisagg()->rn_page_cache == nullptr)
        {
            db_context->getSharedContextDisagg()->initReadNodePageCache(
                db_context->getPathPool(),
                getTemporaryPath() + "/page_cache",
                1 * 1024 * 1024 * 1024);
        }
    }

    std::tuple<SegmentReadTaskPtr, SegmentReadTaskPtr> getRemoteAndLocalSegmentReadTasks(bool need_mem_data, int idx)
    {
        auto snap = getWNReadSnapshot();
        auto remote_seg = getRemoteSegment(snap, need_mem_data, idx);
        auto local_seg = snap->popTask(remote_seg->segment->segmentId());
        RUNTIME_CHECK(remote_seg->extra_remote_info.has_value());
        RUNTIME_CHECK(!local_seg->extra_remote_info.has_value());
        return std::make_tuple(remote_seg, local_seg);
    }

    void fetchPages(const SegmentReadTaskPtr & remote_seg, const SegmentReadTaskPtr & local_seg)
    {
        WorkQueue<disaggregated::PagesPacket> q;

        // Send pages.
        WNFetchPagesStreamWriter writer(
            [&q](const disaggregated::PagesPacket & packet) { q.push(packet, nullptr); },
            local_seg,
            remote_seg->extra_remote_info->remote_page_ids,
            db_context->getSettingsRef());
        writer.syncWrite();
        q.finish();

        // Receive pages.
        auto ds_guard = setMockRemoteDataStore();
        auto occupied_result = remote_seg->blockingOccupySpaceForTask();
        remote_seg->doFetchPagesImpl(
            [&q](disaggregated::PagesPacket & packet) { return q.pop(packet); },
            std::unordered_set<UInt64>(
                remote_seg->extra_remote_info->remote_page_ids.begin(),
                remote_seg->extra_remote_info->remote_page_ids.end()));
        ASSERT_EQ(q.size(), 0);

        remote_seg->initColumnFileDataProvider(occupied_result.pages_guard);
        checkInputStreams(remote_seg, local_seg);
    }

    static void checkInputStreams(const SegmentReadTaskPtr & remote_seg, const SegmentReadTaskPtr & local_seg)
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        remote_seg->initInputStream(*table_column_defines, 0, nullptr, ReadMode::Bitmap, DEFAULT_BLOCK_SIZE, false);
        auto remote_stream = remote_seg->getInputStream();

        Blocks remote_blks;
        while (true)
        {
            auto b = remote_stream->read();
            if (b)
            {
                remote_blks.push_back(std::move(b));
            }
            else
            {
                break;
            }
        }

        local_seg->initInputStream(*table_column_defines, 0, nullptr, ReadMode::Bitmap, DEFAULT_BLOCK_SIZE, false);
        auto local_stream = local_seg->getInputStream();

        ASSERT_INPUTSTREAM_BLOCKS(local_stream, remote_blks);
    }

    void withFetchPagesService(
        FetchPagesService & service,
        const std::function<void(const SegmentReadTaskPtr &, const SegmentReadTaskPtr &)> & check,
        bool restart_server = false)
    {
        auto fp_guard = disableFlushCache();
        const auto tiny_rows = db_context->getSettingsRef().dt_segment_delta_cache_limit_rows;
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, tiny_rows, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        block = DMTestEnv::prepareSimpleWriteBlock(tiny_rows, tiny_rows + 64, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(false, 0);
        ASSERT_FALSE(remote_seg->extra_remote_info->remote_page_ids.empty());
        ASSERT_TRUE(remote_seg->needFetchMemTableSet());

        WNFetchPagesStreamWriter writer(
            [&service](const disaggregated::PagesPacket & packet) { service.packets.push_back(packet); },
            local_seg,
            remote_seg->extra_remote_info->remote_page_ids,
            db_context->getSettingsRef());
        writer.syncWrite();
        ASSERT_FALSE(service.packets.empty());

        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        ASSERT_NE(server, nullptr);
        SCOPE_EXIT({
            if (server)
            {
                server->Shutdown();
                server->Wait();
            }
        });

        auto address = fmt::format("127.0.0.1:{}", port);
        remote_seg->extra_remote_info->store_address = address;
        const auto & rpc_client = db_context->getTMTContext().getKVCluster()->rpc_client;
        auto old_connections = rpc_client->getConnArray(address);
        SCOPE_EXIT({ rpc_client->removeConn(address); });
        std::thread restart_thread;
        SCOPE_EXIT({
            if (restart_thread.joinable())
                restart_thread.join();
        });
        if (restart_server)
        {
            server->Shutdown();
            server->Wait();
            server.reset();
            // Put every pooled channel in TRANSIENT_FAILURE before the page RPC starts.
            const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
            for (const auto & connection : old_connections->vec)
                connection->channel->GetState(true);
            for (const auto & connection : old_connections->vec)
            {
                auto state = connection->channel->GetState(false);
                while (state != GRPC_CHANNEL_TRANSIENT_FAILURE)
                {
                    ASSERT_TRUE(connection->channel->WaitForStateChange(state, deadline));
                    state = connection->channel->GetState(false);
                }
            }
            restart_thread = std::thread([&] {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                // Waiting for readiness must not fail and replace the connection pool.
                EXPECT_EQ(rpc_client->getConnArray(address), old_connections);
                grpc::ServerBuilder restart_builder;
                restart_builder.AddListeningPort(address, grpc::InsecureServerCredentials());
                restart_builder.RegisterService(&service);
                server = restart_builder.BuildAndStart();
                EXPECT_NE(server, nullptr);
            });
        }
        auto ds_guard = setMockRemoteDataStore();
        check(remote_seg, local_seg);
        if (service.error.error_code() == grpc::StatusCode::UNAVAILABLE && service.failures > 0)
            EXPECT_NE(rpc_client->getConnArray(address), old_connections);
    }

    static void checkDMFile(const DMFilePtr & dmfile_wn, const DMFilePtr & dmfile_cn)
    {
        ASSERT_EQ(dmfile_wn->fileId(), dmfile_cn->fileId());
        ASSERT_EQ(dmfile_wn->page_id, dmfile_cn->page_id);
        ASSERT_EQ(dmfile_wn->version(), dmfile_cn->version());
        ASSERT_EQ(dmfile_wn->parentPath(), dmfile_cn->parentPath());
        ASSERT_EQ(dmfile_wn->getStatus(), dmfile_cn->getStatus());

        ASSERT_TRUE(dmfile_wn->getConfiguration().has_value());
        ASSERT_TRUE(dmfile_cn->getConfiguration().has_value());
        ASSERT_EQ(
            dmfile_wn->getConfiguration()->getChecksumFrameLength(),
            dmfile_cn->getConfiguration()->getChecksumFrameLength());
        ASSERT_EQ(
            dmfile_wn->getConfiguration()->getChecksumHeaderLength(),
            dmfile_cn->getConfiguration()->getChecksumHeaderLength());
        ASSERT_EQ(
            dmfile_wn->getConfiguration()->getChecksumAlgorithm(),
            dmfile_cn->getConfiguration()->getChecksumAlgorithm());
        ASSERT_EQ(
            dmfile_wn->getConfiguration()->getEmbeddedChecksum(),
            dmfile_cn->getConfiguration()->getEmbeddedChecksum());
        ASSERT_EQ(dmfile_wn->getConfiguration()->getDebugInfo(), dmfile_cn->getConfiguration()->getDebugInfo());

        ASSERT_EQ(dmfile_wn->getPackStats().size(), dmfile_cn->getPackStats().size());
        for (size_t j = 0; j < dmfile_wn->getPackStats().size(); j++)
        {
            ASSERT_EQ(dmfile_wn->getPackStats()[j].toDebugString(), dmfile_cn->getPackStats()[j].toDebugString());
        }

        ASSERT_EQ(dmfile_wn->getPackProperties().property_size(), dmfile_cn->getPackProperties().property_size());
        for (int j = 0; j < dmfile_wn->getPackProperties().property_size(); j++)
        {
            ASSERT_EQ(
                dmfile_wn->getPackProperties().property(j).ShortDebugString(),
                dmfile_cn->getPackProperties().property(j).ShortDebugString());
        }

        ASSERT_EQ(dmfile_wn->getColumnStats().size(), dmfile_cn->getColumnStats().size());
        for (const auto & [col_id, col_stat_wn] : dmfile_wn->getColumnStats())
        {
            auto itr = dmfile_cn->getColumnStats().find(col_id);
            ASSERT_NE(itr, dmfile_cn->getColumnStats().end());
            const auto & col_stat_cn = itr->second;
            WriteBufferFromOwnString wb_wn;
            col_stat_wn.serializeToBuffer(wb_wn);
            WriteBufferFromOwnString wb_cn;
            col_stat_cn.serializeToBuffer(wb_cn);
            ASSERT_EQ(wb_wn.str(), wb_cn.str());
        }

        ASSERT_EQ(dmfile_wn->getColumnIndices(), dmfile_cn->getColumnIndices());

        const auto * dmfile_meta_wn = typeid_cast<const DMFileMetaV2 *>(dmfile_wn->meta.get());
        const auto * dmfile_meta_cn = typeid_cast<const DMFileMetaV2 *>(dmfile_cn->meta.get());
        ASSERT_EQ(dmfile_meta_cn->merged_files.size(), dmfile_meta_wn->merged_files.size());
        for (size_t j = 0; j < dmfile_meta_wn->merged_files.size(); ++j)
        {
            const auto & merged_file_wn = dmfile_meta_wn->merged_files[j];
            const auto & merged_file_cn = dmfile_meta_cn->merged_files[j];
            ASSERT_EQ(merged_file_wn.number, merged_file_cn.number);
            ASSERT_EQ(merged_file_wn.size, merged_file_cn.size);
        }

        ASSERT_EQ(dmfile_meta_wn->merged_sub_file_infos.size(), dmfile_meta_cn->merged_sub_file_infos.size());
        for (const auto & [fname, sub_files_wn] : dmfile_meta_wn->merged_sub_file_infos)
        {
            auto itr = dmfile_meta_cn->merged_sub_file_infos.find(fname);
            ASSERT_NE(itr, dmfile_meta_cn->merged_sub_file_infos.end());
            const auto & sub_files_cn = itr->second;
            WriteBufferFromOwnString wb_wn;
            sub_files_wn.serializeToBuffer(wb_wn);
            WriteBufferFromOwnString wb_cn;
            sub_files_cn.serializeToBuffer(wb_cn);
            ASSERT_EQ(wb_wn.str(), wb_cn.str());
        }
    }


    void basicMemTableSet()
    {
        auto fp_guard = disableFlushCache();

        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);

        auto get_remote_segment = [&](bool need_mem_data) {
            auto snap = getWNReadSnapshot();
            return getRemoteSegment(snap, need_mem_data, 0);
        };

        // cf delete range
        {
            HandleRange range(0, 128);
            store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
            auto check = [&](bool need_mem_data) {
                auto seg_task = get_remote_segment(need_mem_data);
                ASSERT_NE(seg_task, nullptr);
                ASSERT_FALSE(seg_task->needFetchMemTableSet());
                auto mem_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
                ASSERT_EQ(mem_snap->getColumnFileCount(), 1);
                ASSERT_EQ(mem_snap->getColumnFiles()[0]->getType(), ColumnFile::Type::DELETE_RANGE);
            };
            check(true);
            check(false);
        }

        // cf tiny in memtableset
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            auto check = [&](bool need_mem_data) {
                auto seg_task = get_remote_segment(need_mem_data);
                ASSERT_NE(seg_task, nullptr);
                ASSERT_FALSE(seg_task->needFetchMemTableSet());
                auto mem_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
                ASSERT_EQ(mem_snap->getColumnFileCount(), 2);
                ASSERT_EQ(mem_snap->getColumnFiles()[0]->getType(), ColumnFile::Type::DELETE_RANGE);
                ASSERT_EQ(mem_snap->getColumnFiles()[1]->getType(), ColumnFile::Type::TINY_FILE);
            };
            check(true);
            check(false);
        }

        // cf mem
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            auto check = [&](bool need_mem_data) {
                auto seg_task = get_remote_segment(need_mem_data);
                ASSERT_NE(seg_task, nullptr);
                ASSERT_NE(seg_task->needFetchMemTableSet(), need_mem_data);
                auto mem_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
                ASSERT_EQ(mem_snap->getColumnFileCount(), 3);
                ASSERT_EQ(mem_snap->getColumnFiles()[0]->getType(), ColumnFile::Type::DELETE_RANGE);
                ASSERT_EQ(mem_snap->getColumnFiles()[1]->getType(), ColumnFile::Type::TINY_FILE);
                ASSERT_EQ(mem_snap->getColumnFiles()[2]->getType(), ColumnFile::Type::INMEMORY_FILE);
            };
            check(true);
            check(false);
        }

        // cf big
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
            auto check = [&](bool need_mem_data) {
                auto seg_task = get_remote_segment(need_mem_data);
                ASSERT_NE(seg_task, nullptr);
                ASSERT_NE(seg_task->needFetchMemTableSet(), need_mem_data);
                auto mem_snap = seg_task->read_snapshot->delta->getMemTableSetSnapshot();
                ASSERT_EQ(mem_snap->getColumnFileCount(), 4);
                ASSERT_EQ(mem_snap->getColumnFiles()[0]->getType(), ColumnFile::Type::DELETE_RANGE);
                ASSERT_EQ(mem_snap->getColumnFiles()[1]->getType(), ColumnFile::Type::TINY_FILE);
                ASSERT_EQ(mem_snap->getColumnFiles()[2]->getType(), ColumnFile::Type::INMEMORY_FILE);
                ASSERT_EQ(mem_snap->getColumnFiles()[3]->getType(), ColumnFile::Type::BIG_FILE);
            };
            check(true);
            check(false);
        }
    }

    void fetchPagesNoTinyNoInMem()
    {
        auto fp_guard = disableFlushCache();
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);

        // stable
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            store->mergeDeltaAll(*db_context);
        }

        // cf delete range
        {
            HandleRange range(0, 128);
            store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
        }

        // cf big
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
        }

        // cf tiny
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }

        auto check = [&](bool need_mem_data) {
            auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(need_mem_data, 0);
            ASSERT_FALSE(remote_seg->extra_remote_info->remote_page_ids.empty());
            ASSERT_FALSE(remote_seg->needFetchMemTableSet());
        };
        check(true);
        check(false);
    }

    void fetchPagesTinyNoInMem()
    {
        auto fp_guard = disableFlushCache();
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);

        // stable
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            store->mergeDeltaAll(*db_context);
        }

        // cf delete range
        {
            HandleRange range(0, 128);
            store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
        }

        // cf big
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
        }

        // cf tiny
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }

        auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(true, 0);
        ASSERT_FALSE(remote_seg->needFetchMemTableSet());
        fetchPages(remote_seg, local_seg);
    }


    void fetchPagesNoTinyInMem()
    {
        auto fp_guard = disableFlushCache();
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);

        // stable
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            store->mergeDeltaAll(*db_context);
        }

        // cf delete range
        {
            HandleRange range(0, 128);
            store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
        }

        // cf big
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
        }

        // cf in mem
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows / 5,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }

        {
            auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(true, 0);
            ASSERT_FALSE(remote_seg->needFetchMemTableSet());
            fetchPages(remote_seg, local_seg);
        }

        {
            auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(false, 0);
            ASSERT_TRUE(remote_seg->needFetchMemTableSet());
            fetchPages(remote_seg, local_seg);
        }
    }

    void fetchPagesTinyInMem()
    {
        auto fp_guard = disableFlushCache();
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);

        // stable
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
            store->write(*db_context, db_context->getSettingsRef(), block);
            store->mergeDeltaAll(*db_context);
        }

        // cf delete range
        {
            HandleRange range(0, 128);
            store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
        }

        // cf big
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
        }

        // cf tiny
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }

        // cf in mem
        {
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                0,
                db_context->getSettingsRef().dt_segment_delta_cache_limit_rows / 5,
                false);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }

        {
            auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(true, 0);
            ASSERT_FALSE(remote_seg->needFetchMemTableSet());
            fetchPages(remote_seg, local_seg);
        }

        {
            auto [remote_seg, local_seg] = getRemoteAndLocalSegmentReadTasks(false, 0);
            ASSERT_TRUE(remote_seg->needFetchMemTableSet());
            fetchPages(remote_seg, local_seg);
        }
    }
};

TEST_F(DMStoreForSegmentReadTaskTest, DisaggReadSnapshot)
try
{
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    // stable
    {
        // skip_check_segment_update prevents write() from scheduling background
        // flush/merge tasks, avoiding a race where background placeDeltaIndex or
        // merge delta holds is_updating and causes mergeDeltaAll() to fail silently.
        FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
        auto fp_guard
            = ext::make_scope_guard([]() { FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update); });

        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->mergeDeltaAll(*db_context);
    }

    // cf delete range
    {
        HandleRange range(0, 128);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
    }

    // cf big
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto [range, file_ids] = genDMFile(*dm_context, block);
        store->ingestFiles(dm_context, range, file_ids, false);
    }

    // cf tiny
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    auto fp_guard = disableFlushCache();

    // cf mem
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    auto scan_context = std::make_shared<ScanContext>();
    auto snap = store->writeNodeBuildRemoteReadSnapshot(
        *db_context,
        db_context->getSettingsRef(),
        {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
        1,
        "req_id",
        {},
        scan_context);

    snap->column_defines = std::make_shared<ColumnDefines>(store->getTableColumns());

    MemTrackerWrapper mem_tracker_wrapper(nullptr);
    auto remote_table_pb
        = Remote::Serializer::serializePhysicalTable(snap, /*task_id*/ {}, mem_tracker_wrapper, /*need_mem_data*/ true);

    ASSERT_GT(remote_table_pb.segments_size(), 0);

    auto guard = setMockRemoteDataStore();
    for (const auto & remote_seg : remote_table_pb.segments())
    {
        auto seg_task = std::make_shared<SegmentReadTask>(
            Logger::get(),
            *db_context,
            scan_context,
            remote_seg,
            DisaggTaskId{},
            /*store_id*/ 1,
            /*store_address*/ "127.0.0.1",
            store->keyspace_id,
            store->physical_table_id,
            /*pk_col_id*/ 0,
            /*is_same_zone*/ false,
            /*establish_disagg_task_resp_size*/ 0);

        auto seg_id = seg_task->segment->segmentId();

        auto itr = store->id_to_segment.find(seg_id);
        ASSERT_NE(itr, store->id_to_segment.end()) << seg_id;

        auto seg = itr->second;
        ASSERT_NE(seg, nullptr) << seg_id;
        auto delta_wn = seg->getDelta();
        ASSERT_NE(delta_wn, nullptr) << seg_id;
        auto stable_wn = seg->getStable();
        ASSERT_NE(stable_wn, nullptr) << seg_id;

        ASSERT_NE(seg_task->segment, nullptr) << seg_id;
        auto delta_cn = seg->getDelta();
        ASSERT_NE(delta_cn, nullptr) << seg_id;
        auto stable_cn = seg->getStable();
        ASSERT_NE(stable_cn, nullptr) << seg_id;

        // Check Delta
        ASSERT_EQ(delta_wn->getDeltaIndexEpoch(), delta_cn->getDeltaIndexEpoch());
        ASSERT_EQ(delta_wn->simpleInfo(), delta_cn->simpleInfo());
        ASSERT_EQ(delta_wn->info(), delta_cn->info());
        ASSERT_EQ(delta_wn->getId(), delta_cn->getId());

        // cf mem set
        auto mem_set_wn = delta_wn->getMemTableSet();
        auto mem_set_cn = delta_cn->getMemTableSet();
        ASSERT_EQ(mem_set_wn->getColumnFileCount(), 1);
        ASSERT_EQ(mem_set_cn->getColumnFileCount(), 1);
        for (size_t i = 0; i < mem_set_wn->getColumnFileCount(); i++)
        {
            auto * cf_wn = mem_set_wn->column_files[i]->tryToInMemoryFile();
            ASSERT_NE(cf_wn, nullptr);
            auto * cf_cn = mem_set_cn->column_files[i]->tryToInMemoryFile();
            ASSERT_NE(cf_cn, nullptr);
            ASSERT_EQ(cf_wn->toString(), cf_cn->toString());
            String msg;
            ASSERT_TRUE(blockEqual(cf_wn->getCache()->block, cf_cn->getCache()->block, msg));
        }


        // cf persist set
        auto persist_set_wn = delta_wn->getPersistedFileSet();
        auto persist_set_cn = delta_cn->getPersistedFileSet();
        ASSERT_EQ(persist_set_wn->getColumnFileCount(), 4);
        ASSERT_EQ(persist_set_cn->getColumnFileCount(), 4);
        ASSERT_EQ(persist_set_wn->detailInfo(), persist_set_cn->detailInfo());
        for (size_t i = 0; i < persist_set_wn->getColumnFileCount(); i++)
        {
            auto cf_wn = persist_set_wn->getFiles()[i];
            auto cf_cn = persist_set_cn->getFiles()[i];

            if (i == 0)
            {
                auto * cf_del_wn = cf_wn->tryToDeleteRange();
                ASSERT_NE(cf_del_wn, nullptr);
                auto * cf_del_cn = cf_cn->tryToDeleteRange();
                ASSERT_NE(cf_del_cn, nullptr);
                ASSERT_EQ(cf_del_wn->getDeleteRange(), cf_del_cn->getDeleteRange());
            }
            else if (i == 1)
            {
                auto * cf_big_wn = cf_wn->tryToBigFile();
                ASSERT_NE(cf_big_wn, nullptr);
                auto * cf_big_cn = cf_cn->tryToBigFile();
                ASSERT_NE(cf_big_cn, nullptr);
                checkDMFile(cf_big_wn->getFile(), cf_big_cn->getFile());
            }
            else
            {
                auto * cf_tiny_wn = cf_wn->tryToTinyFile();
                ASSERT_NE(cf_tiny_wn, nullptr);
                auto * cf_tiny_cn = cf_cn->tryToTinyFile();
                ASSERT_NE(cf_tiny_cn, nullptr);
                ASSERT_EQ(cf_tiny_wn->getDataPageId(), cf_tiny_cn->getDataPageId());
                ASSERT_EQ(cf_tiny_wn->getDataPageSize(), cf_tiny_cn->getDataPageSize());
            }
        }

        // Check Stable
        ASSERT_EQ(stable_wn->getId(), stable_cn->getId());
        ASSERT_EQ(stable_wn->getRows(), stable_cn->getRows());
        ASSERT_EQ(stable_wn->getBytes(), stable_cn->getBytes());
        ASSERT_EQ(stable_wn->getDMFiles().size(), 1);
        ASSERT_EQ(stable_cn->getDMFiles().size(), 1);
        for (size_t i = 0; i < stable_wn->getDMFiles().size(); i++)
        {
            const auto & dmfile_wn = stable_wn->getDMFiles()[i];
            const auto & dmfile_cn = stable_cn->getDMFiles()[i];
            checkDMFile(dmfile_wn, dmfile_cn);
        }
    }
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, MemTableSetWithCFTiny)
try
{
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    auto fp_guard = disableFlushCache();
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(
            0,
            db_context->getSettingsRef().dt_segment_delta_cache_limit_rows,
            false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    auto scan_context = std::make_shared<ScanContext>();
    auto snap = store->writeNodeBuildRemoteReadSnapshot(
        *db_context,
        db_context->getSettingsRef(),
        {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
        1,
        "req_id",
        {},
        scan_context);

    snap->column_defines = std::make_shared<ColumnDefines>(store->getTableColumns());

    MemTrackerWrapper mem_tracker_wrapper(nullptr);
    auto remote_table_pb
        = Remote::Serializer::serializePhysicalTable(snap, /*task_id*/ {}, mem_tracker_wrapper, /*need_mem_data*/ true);
    ASSERT_EQ(remote_table_pb.segments_size(), 1);

    auto guard = setMockRemoteDataStore();
    const auto & remote_seg = remote_table_pb.segments(0);
    auto seg_task = std::make_shared<SegmentReadTask>(
        Logger::get(),
        *db_context,
        scan_context,
        remote_seg,
        DisaggTaskId{},
        /*store_id*/ 1,
        /*store_address*/ "127.0.0.1",
        store->keyspace_id,
        store->physical_table_id,
        /*pk_col_id*/ 0,
        /*is_same_zone*/ false,
        /*establish_disagg_task_resp_size*/ 0);
    const auto & cfs = seg_task->read_snapshot->delta->getMemTableSetSnapshot()->getColumnFiles();
    ASSERT_EQ(cfs.size(), 1);
    const auto & cf = cfs.front();
    ASSERT_NE(cf->tryToTinyFile(), nullptr);
    ASSERT_EQ(seg_task->extra_remote_info->remote_page_ids.size(), 1);
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, BasicMemTableSet)
try
{
    basicMemTableSet();
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesRetryUnavailable)
try
{
    FetchPagesService service;
    service.failures = 2;
    withFetchPagesService(service, [&](const auto & remote_seg, const auto & local_seg) {
        remote_seg->fetchPages();
        EXPECT_EQ(service.requests.load(), 3);
        checkInputStreams(remote_seg, local_seg);
    });
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesWaitForReady)
try
{
    FetchPagesService service;
    withFetchPagesService(
        service,
        [&](const auto & remote_seg, const auto & local_seg) {
            remote_seg->fetchPages();
            EXPECT_EQ(service.requests.load(), 1);
            checkInputStreams(remote_seg, local_seg);
        },
        /*restart_server*/ true);
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesUnavailableRetryLimit)
try
{
    FetchPagesService service;
    service.failures = 3;
    withFetchPagesService(service, [&](const auto & remote_seg, const auto &) {
        try
        {
            remote_seg->fetchPages();
            FAIL() << "Unavailable WN should fail after three attempts";
        }
        catch (const pingcap::Exception & e)
        {
            EXPECT_EQ(e.code(), pingcap::GRPCErrorCode);
        }
        EXPECT_EQ(service.requests.load(), 3);
    });
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesNoRetryAfterPacket)
try
{
    FetchPagesService service;
    service.failures = 1;
    service.send_packet_before_error = true;
    withFetchPagesService(service, [&](const auto & remote_seg, const auto &) {
        EXPECT_THROW(remote_seg->fetchPages(), DB::Exception);
        EXPECT_EQ(service.requests.load(), 1);
    });
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesNoRetryNonTransientError)
try
{
    FetchPagesService service;
    service.failures = 1;
    service.error = {grpc::StatusCode::INTERNAL, "Failed to read page"};
    withFetchPagesService(service, [&](const auto & remote_seg, const auto &) {
        EXPECT_THROW(remote_seg->fetchPages(), DB::Exception);
        EXPECT_EQ(service.requests.load(), 1);
    });
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesNoRetryTimeout)
try
{
    FetchPagesService service;
    service.failures = 1;
    service.error = {grpc::StatusCode::DEADLINE_EXCEEDED, "Fetch pages timed out"};
    withFetchPagesService(service, [&](const auto & remote_seg, const auto &) {
        try
        {
            remote_seg->fetchPages();
            FAIL() << "Fetch pages timeout should be propagated";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::TIMEOUT_EXCEEDED);
        }
        EXPECT_EQ(service.requests.load(), 1);
    });
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesNoTinyNoInMem)
try
{
    fetchPagesNoTinyNoInMem();
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesTinyNoInMem)
try
{
    fetchPagesTinyNoInMem();
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesNoTinyInMem)
try
{
    fetchPagesNoTinyInMem();
}
CATCH

TEST_F(DMStoreForSegmentReadTaskTest, FetchPagesTinyInMem)
try
{
    fetchPagesTinyInMem();
}
CATCH
} // namespace DB::DM::tests

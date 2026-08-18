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

#include <Flash/Coprocessor/ExecutionSummary.h>
#include <Flash/Coprocessor/FTSExecutionContext.h>
#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <kvproto/resource_manager.pb.h>

namespace DB
{
namespace tests
{
TEST(FTSExecutionContextTest, MergeSerializeAndDeserialize)
{
    FTSRuntimeStats stats{
        .index_query_time_sum_ns = 1,
        .shard_count = 2,
        .shard_query_time_sum_ns = 3,
        .full_fallback_shard_count = 4,
        .full_fallback_query_time_sum_ns = 5,
        .tantivy_index_count = 6,
        .merge_mvcc_time_sum_ns = 7,
        .merge_mvcc_output_count = 8,
        .index_hit_count = 9,
        .unindexed_read_count = 10,
        .unindexed_read_time_sum_ns = 11,
        .fallback_eval_count = 12,
        .fallback_eval_time_sum_ns = 13,
        .fallback_match_count = 14,
        .fts_candidate_count = 15,
        .row_lookup_time_sum_ns = 16,
        .used_row_lookup = 1,
    };

    FTSExecutionContext context;
    context.merge(stats);
    const auto pb = context.serialize();
    ASSERT_EQ(pb.index_query_time_sum_ns(), 1);
    ASSERT_EQ(pb.shard_count(), 2);
    ASSERT_EQ(pb.shard_query_time_sum_ns(), 3);
    ASSERT_EQ(pb.full_fallback_shard_count(), 4);
    ASSERT_EQ(pb.full_fallback_query_time_sum_ns(), 5);
    ASSERT_EQ(pb.tantivy_index_count(), 6);
    ASSERT_EQ(pb.merge_mvcc_time_sum_ns(), 7);
    ASSERT_EQ(pb.merge_mvcc_output_count(), 8);
    ASSERT_EQ(pb.index_hit_count(), 9);
    ASSERT_EQ(pb.unindexed_read_count(), 10);
    ASSERT_EQ(pb.unindexed_read_time_sum_ns(), 11);
    ASSERT_EQ(pb.fallback_eval_count(), 12);
    ASSERT_EQ(pb.fallback_eval_time_sum_ns(), 13);
    ASSERT_EQ(pb.fallback_match_count(), 14);
    ASSERT_EQ(pb.fts_candidate_count(), 15);
    ASSERT_EQ(pb.row_lookup_time_sum_ns(), 16);
    ASSERT_TRUE(pb.used_row_lookup());

    tipb::ExecutorExecutionSummary remote;
    remote.mutable_fts()->CopyFrom(pb);
    ExecutionSummary summary;
    summary.init(remote);
    ASSERT_NE(summary.fts_execution_context, nullptr);
    const auto roundtrip = summary.fts_execution_context->serialize();
    ASSERT_EQ(roundtrip.SerializeAsString(), pb.SerializeAsString());
}

class ExecutionSummaryTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            {"test_db", "test_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>(
                 "s1",
                 {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
             toNullableVec<String>(
                 "s2",
                 {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});

        context.addExchangeReceiver(
            "test_exchange",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>(
                 "s1",
                 {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
             toNullableVec<String>(
                 "s2",
                 {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "empty_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {}), toNullableVec<String>("s2", {})});
    }

    static constexpr size_t concurrency = 10;

#define WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN                                      \
    std::vector<DAGRequestType> type{DAGRequestType::tree, DAGRequestType::list}; \
    std::vector<bool> pipeline_bools{false, true};                                \
    for (auto enable_pipeline : pipeline_bools)                                   \
    {                                                                             \
        enablePipeline(enable_pipeline);                                          \
        for (const auto t : type)                                                 \
        {
#define WRAP_FOR_EXCUTION_SUMMARY_TEST_END \
    }                                      \
    }
};

TEST_F(ExecutionSummaryTestRunner, testBasic)
try
{
    WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context, t);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"selection_1", {4, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    {
        auto request
            = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).limit(2).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {not_check_rows, concurrency}},
            {"limit_2",
             {2, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, limit can be executed in parallel.

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").limit(5).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"limit_1",
             {5, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, limit can be executed in parallel.
        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").topN("s1", true, 5).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1",
             {not_check_rows,
              enable_pipeline ? concurrency : 1}}}; // for pipeline mode, topn can be executed in parallel.
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .topN("s1", true, 12)
                           .build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {4, concurrency}},
            {"topn_2", {4, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({col("s2")}, {col("s2")})
                           .topN("s2", true, 12)
                           .build(context, t);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            {"topn_2", {3, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }

    WRAP_FOR_EXCUTION_SUMMARY_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, join)
try
{
    {
        auto t1 = context.scan("test_db", "test_table");
        auto t2 = context.scan("test_db", "test_table");
        auto request = t1.join(t2, tipb::JoinType::TypeInnerJoin, {col("s1")}).build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"table_scan_1", {12, concurrency}},
            {"Join_2", {64, concurrency}}};
        testForExecutionSummary(request, expect);
    }
}
CATCH

TEST_F(ExecutionSummaryTestRunner, genMPPTaskExecutionInfoWithoutSetRUInfo)
try
{
    auto request = context.scan("test_db", "test_table").limit(1).exchangeSender(tipb::PassThrough).build(context);
    request->set_collect_execution_summaries(true);

    mpp::TaskMeta meta;
    meta.set_gather_id(1);
    meta.set_task_id(1);
    meta.set_query_ts(1);
    meta.set_local_query_id(1);
    meta.set_server_id(1);
    meta.set_start_ts(1);

    DAGContext dag_context(*request, meta, false);
    // ExchangeSenderStatistics reads tunnel_set while initializing the mock MPP DAG.
    dag_context.tunnel_set = std::make_shared<MPPTunnelSet>("test-host");
    MPPTaskStatistics task_statistics(MPPTaskId(meta), "test-host");
    task_statistics.initializeExecutorDAG(&dag_context);

    auto execution_info = task_statistics.genTiFlashExecutionInfo();
    const auto root_executor_id = dag_context.dag_request.rootExecutorID();
    const tipb::ExecutorExecutionSummary * root_summary = nullptr;
    for (const auto & summary : execution_info.execution_summaries())
    {
        if (summary.executor_id() == root_executor_id)
        {
            root_summary = &summary;
            break;
        }
    }

    ASSERT_NE(root_summary, nullptr);
    ASSERT_TRUE(root_summary->has_ru_consumption());
    resource_manager::Consumption ru_consumption;
    ASSERT_TRUE(ru_consumption.ParseFromString(root_summary->ru_consumption()));
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.r_r_u());
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.read_bytes());
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.total_cpu_time_ms());
}
CATCH

TEST_F(ExecutionSummaryTestRunner, treeBased)
try
{
    WRAP_FOR_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .limit(2)
                           .project({col("s1")})
                           .build(context);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {not_check_rows, concurrency}},
            {"limit_2",
             {2, enable_pipeline ? concurrency : 1}}, // for pipeline mode, limit can be executed in parallel.
            {"project_3", {2, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table").topN("s1", true, 5).project({col("s2")}).build(context);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1",
             {not_check_rows,
              enable_pipeline ? concurrency : 1}}, // for pipeline mode, topn can be executed in parallel.
            {"project_2", {not_check_rows, concurrency}}};
        Expect expect_pipeline{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1", {not_check_rows, concurrency}},
            {"project_2", {not_check_rows, concurrency}}};

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").project({col("s2")}).build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"project_1", {12, concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table").project({col("s2")}).project({col("s2")}).build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"project_1", {12, concurrency}},
            {"project_2", {12, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "empty_table").project({col("s2")}).build(context);
        Expect expect{{"table_scan_0", {0, concurrency}}, {"project_1", {0, concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "empty_table").project({col("s2")}).topN("s2", true, 12).build(context);
        Expect expect{
            {"table_scan_0", {0, concurrency}},
            {"project_1", {0, concurrency}},
            {"topn_2", {0, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, topn can be executed in parallel.
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({col("s2")}, {col("s2")})
                           .project({col("s2")})
                           .build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            // for pipeline mode, the concurrency of agg convergent is determined by the amount of data. Because the amount of data is small, the concurrency here is 1.
            {"project_2", {3, enable_pipeline ? 1 : concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({}, {col("s2")})
                           .project({col("s2")})
                           .limit(2)
                           .build(context);

        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            // for pipeline mode, the concurrency of agg convergent is determined by the amount of data. Because the amount of data is small, the concurrency here is 1.
            {"project_2", {not_check_rows, enable_pipeline ? 1 : concurrency}},
            {"limit_3", {2, 1}}};

        testForExecutionSummary(request, expect);
    }

    WRAP_FOR_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, expand)
try
{
    WRAP_FOR_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table")
                           .expand(MockVVecColumnNameVec{
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"s1"},
                               },
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"s2"},
                               },
                           })
                           .build(context);

        Expect expect{{"table_scan_0", {12, concurrency}}, {"expand_1", {24, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WRAP_FOR_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, agg)
try
{
    WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table").aggregation({col("s2")}, {col("s2")}).build(context, t);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"aggregation_1", {3, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WRAP_FOR_EXCUTION_SUMMARY_TEST_END
}
CATCH

#undef WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
#undef WRAP_FOR_EXCUTION_SUMMARY_TEST_END

} // namespace tests
} // namespace DB

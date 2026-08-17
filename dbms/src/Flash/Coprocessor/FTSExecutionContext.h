// Copyright 2026 PingCAP, Inc.
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

#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <atomic>

namespace DB
{
class FTSExecutionContext
{
public:
    std::atomic<UInt64> index_query_time_sum_ns{0};
    std::atomic<UInt64> shard_count{0};
    std::atomic<UInt64> shard_query_time_sum_ns{0};
    std::atomic<UInt64> full_fallback_shard_count{0};
    std::atomic<UInt64> full_fallback_query_time_sum_ns{0};
    std::atomic<UInt64> tantivy_index_count{0};
    std::atomic<UInt64> merge_mvcc_time_sum_ns{0};
    std::atomic<UInt64> merge_mvcc_output_count{0};
    std::atomic<UInt64> index_hit_count{0};
    std::atomic<UInt64> unindexed_read_count{0};
    std::atomic<UInt64> fallback_eval_count{0};
    std::atomic<UInt64> fallback_eval_time_sum_ns{0};
    std::atomic<UInt64> fallback_match_count{0};
    std::atomic<UInt64> fts_candidate_count{0};
    std::atomic<UInt64> row_lookup_time_sum_ns{0};
    std::atomic<bool> used_row_lookup{false};

    void deserialize(const tipb::FTSExecutionSummary & pb)
    {
        index_query_time_sum_ns = pb.index_query_time_sum_ns();
        shard_count = pb.shard_count();
        shard_query_time_sum_ns = pb.shard_query_time_sum_ns();
        full_fallback_shard_count = pb.full_fallback_shard_count();
        full_fallback_query_time_sum_ns = pb.full_fallback_query_time_sum_ns();
        tantivy_index_count = pb.tantivy_index_count();
        merge_mvcc_time_sum_ns = pb.merge_mvcc_time_sum_ns();
        merge_mvcc_output_count = pb.merge_mvcc_output_count();
        index_hit_count = pb.index_hit_count();
        unindexed_read_count = pb.unindexed_read_count();
        fallback_eval_count = pb.fallback_eval_count();
        fallback_eval_time_sum_ns = pb.fallback_eval_time_sum_ns();
        fallback_match_count = pb.fallback_match_count();
        fts_candidate_count = pb.fts_candidate_count();
        row_lookup_time_sum_ns = pb.row_lookup_time_sum_ns();
        used_row_lookup = pb.used_row_lookup();
    }

    tipb::FTSExecutionSummary serialize() const
    {
        tipb::FTSExecutionSummary pb;
        pb.set_index_query_time_sum_ns(index_query_time_sum_ns.load());
        pb.set_shard_count(shard_count.load());
        pb.set_shard_query_time_sum_ns(shard_query_time_sum_ns.load());
        pb.set_full_fallback_shard_count(full_fallback_shard_count.load());
        pb.set_full_fallback_query_time_sum_ns(full_fallback_query_time_sum_ns.load());
        pb.set_tantivy_index_count(tantivy_index_count.load());
        pb.set_merge_mvcc_time_sum_ns(merge_mvcc_time_sum_ns.load());
        pb.set_merge_mvcc_output_count(merge_mvcc_output_count.load());
        pb.set_index_hit_count(index_hit_count.load());
        pb.set_unindexed_read_count(unindexed_read_count.load());
        pb.set_fallback_eval_count(fallback_eval_count.load());
        pb.set_fallback_eval_time_sum_ns(fallback_eval_time_sum_ns.load());
        pb.set_fallback_match_count(fallback_match_count.load());
        pb.set_fts_candidate_count(fts_candidate_count.load());
        pb.set_row_lookup_time_sum_ns(row_lookup_time_sum_ns.load());
        pb.set_used_row_lookup(used_row_lookup.load());
        return pb;
    }

    void merge(const FTSExecutionContext & other)
    {
        index_query_time_sum_ns += other.index_query_time_sum_ns.load();
        shard_count += other.shard_count.load();
        shard_query_time_sum_ns += other.shard_query_time_sum_ns.load();
        full_fallback_shard_count += other.full_fallback_shard_count.load();
        full_fallback_query_time_sum_ns += other.full_fallback_query_time_sum_ns.load();
        tantivy_index_count += other.tantivy_index_count.load();
        merge_mvcc_time_sum_ns += other.merge_mvcc_time_sum_ns.load();
        merge_mvcc_output_count += other.merge_mvcc_output_count.load();
        index_hit_count += other.index_hit_count.load();
        unindexed_read_count += other.unindexed_read_count.load();
        fallback_eval_count += other.fallback_eval_count.load();
        fallback_eval_time_sum_ns += other.fallback_eval_time_sum_ns.load();
        fallback_match_count += other.fallback_match_count.load();
        fts_candidate_count += other.fts_candidate_count.load();
        row_lookup_time_sum_ns += other.row_lookup_time_sum_ns.load();
        mergeUsedRowLookup(other.used_row_lookup.load());
    }

    void merge(const tipb::FTSExecutionSummary & other)
    {
        index_query_time_sum_ns += other.index_query_time_sum_ns();
        shard_count += other.shard_count();
        shard_query_time_sum_ns += other.shard_query_time_sum_ns();
        full_fallback_shard_count += other.full_fallback_shard_count();
        full_fallback_query_time_sum_ns += other.full_fallback_query_time_sum_ns();
        tantivy_index_count += other.tantivy_index_count();
        merge_mvcc_time_sum_ns += other.merge_mvcc_time_sum_ns();
        merge_mvcc_output_count += other.merge_mvcc_output_count();
        index_hit_count += other.index_hit_count();
        unindexed_read_count += other.unindexed_read_count();
        fallback_eval_count += other.fallback_eval_count();
        fallback_eval_time_sum_ns += other.fallback_eval_time_sum_ns();
        fallback_match_count += other.fallback_match_count();
        fts_candidate_count += other.fts_candidate_count();
        row_lookup_time_sum_ns += other.row_lookup_time_sum_ns();
        mergeUsedRowLookup(other.used_row_lookup());
    }

    void merge(const FTSRuntimeStats & other)
    {
        index_query_time_sum_ns += other.index_query_time_sum_ns;
        shard_count += other.shard_count;
        shard_query_time_sum_ns += other.shard_query_time_sum_ns;
        full_fallback_shard_count += other.full_fallback_shard_count;
        full_fallback_query_time_sum_ns += other.full_fallback_query_time_sum_ns;
        tantivy_index_count += other.tantivy_index_count;
        merge_mvcc_time_sum_ns += other.merge_mvcc_time_sum_ns;
        merge_mvcc_output_count += other.merge_mvcc_output_count;
        index_hit_count += other.index_hit_count;
        unindexed_read_count += other.unindexed_read_count;
        fallback_eval_count += other.fallback_eval_count;
        fallback_eval_time_sum_ns += other.fallback_eval_time_sum_ns;
        fallback_match_count += other.fallback_match_count;
        fts_candidate_count += other.fts_candidate_count;
        row_lookup_time_sum_ns += other.row_lookup_time_sum_ns;
        mergeUsedRowLookup(other.used_row_lookup != 0);
    }

private:
    void mergeUsedRowLookup(bool value)
    {
        if (value)
            used_row_lookup = true;
    }
};
} // namespace DB

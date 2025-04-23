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

#pragma once

#include <Common/LRUCache.h>
#include <Storages/DeltaMerge/Remote/RNMVCCIndexCache_fwd.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

namespace DB::DM
{
class DeltaIndex;
using DeltaIndexPtr = std::shared_ptr<DeltaIndex>;
} // namespace DB::DM

namespace DB::DM::Remote
{
/**
 * A LRU cache that holds delta-tree indexes from different remote write nodes.
 * Delta-tree indexes are used as much as possible when same segments are accessed multiple times.
 */
template <typename T>
class RNMVCCIndexCache : private boost::noncopyable
{
public:
    using TPtr = std::shared_ptr<T>;
    explicit RNMVCCIndexCache(size_t max_cache_size)
        : cache(max_cache_size)
    {}

    struct CacheKey
    {
        UInt64 store_id;
        Int64 table_id;
        UInt64 segment_id;
        UInt64 segment_epoch;
        UInt64 delta_index_epoch;
        KeyspaceID keyspace_id;

        bool operator==(const CacheKey & other) const
        {
            return store_id == other.store_id && keyspace_id == other.keyspace_id && table_id == other.table_id
                && segment_id == other.segment_id && segment_epoch == other.segment_epoch
                && delta_index_epoch == other.delta_index_epoch;
        }
    };

    TPtr get(const CacheKey & key);
    void set(const CacheKey & key, const TPtr & value);

    size_t getCacheWeight() const { return cache.weight(); }
    size_t getCacheCount() const { return cache.count(); }

private:
    static size_t getBytes(const TPtr & ptr) const;
    
    struct CacheValue
    {
        TPtr value;
        size_t bytes;
    };

    struct CacheKeyHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, k.store_id);
            boost::hash_combine(seed, k.table_id);
            boost::hash_combine(seed, k.segment_id);
            boost::hash_combine(seed, k.segment_epoch);
            boost::hash_combine(seed, k.delta_index_epoch);
            boost::hash_combine(seed, k.keyspace_id);
            return seed;
        }
    };

    struct CacheValueWeight
    {
        size_t operator()(const CacheKey & key, const CacheValue & v) const { return sizeof(key) + v.bytes; }
    };

private:
    std::mutex mtx;
    LRUCache<CacheKey, CacheValue, CacheKeyHasher, CacheValueWeight> cache;
};

} // namespace DB::DM::Remote

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

#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaIndex/DeltaIndex.h>
#include <Storages/DeltaMerge/Remote/RNMVCCIndexCache.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>
namespace CurrentMetrics
{
extern const Metric DT_DeltaIndexCacheSize;
} // namespace CurrentMetrics

namespace DB::DM::Remote
{

namespace
{
void reportCacheHitStats(bool miss)
{
    if (miss)
        GET_METRIC(tiflash_storage_mvcc_index_cache, type_miss).Increment();
    else
        GET_METRIC(tiflash_storage_mvcc_index_cache, type_hit).Increment();
}
} // namespace

template <typename T>
std::shared_ptr<T> RNMVCCIndexCache::get(const CacheKey & key)
{
    auto [value, miss] = cache.getOrSet(key, [] {
        return std::make_shared<CacheValue>(std::make_shared<T>(), 0);
    });
    reportCacheHitStats(miss);
    return value->value;
}

template <typename T>
void RNMVCCIndexCache::set(const CacheKey & key, const TPtr & new_value)
{
    std::lock_guard lock(mtx);
    if (auto cached_value = cache.get(key); cached_value)
    {
        cache.set(key, std::make_shared<CacheValue>(new_value, getBytes(new_value)));
        CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, cache.weight());
    }
}

template <typename T>
size_t RNMVCCIndexCache::getBytes(const TPtr & ptr) const
{
    if constexpr (std::is_same_v<T, DeltaIndex>)
        return ptr->getBytes();
    else if constexpr (std::is_same_v<T, VersionChain>)
        return getVersionChainBytes(ptr);
    else
        static_assert(false, "Unsupported type for getBytes");
}


template class RNMVCCIndexCache<DeltaIndex>;
template class RNMVCCIndexCache<VersionChain>;
} // namespace DB::DM::Remote

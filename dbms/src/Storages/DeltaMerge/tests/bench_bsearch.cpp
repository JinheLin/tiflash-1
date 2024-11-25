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


#include <benchmark/benchmark.h>
#include <absl/container/btree_set.h>
#include <random>

namespace
{

enum class SearchType
{
    Scan = 0,
    LowerBound = 1,
    StdSet = 2,
    AbslSet = 3,
};

template <typename... Args>
void alwaysFound(benchmark::State & state, Args &&... args)
{
    const auto [search_type] = std::make_tuple(std::move(args)...);
    std::vector<long> v(8192);
    std::iota(v.begin(), v.end(), 0);
    std::random_device rd;
    std::mt19937 g(rd());

    auto gen = [&]() { return g() % 8192; };

    if (search_type == SearchType::LowerBound)
    {
        for (auto _ : state)
        {
            auto itr = std::lower_bound(v.begin(), v.end(), gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::Scan)
    {
        for (auto _ : state)
        {
            auto itr = std::find(v.begin(), v.end(), gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::StdSet)
    {
        std::set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::AbslSet)
    {
        absl::btree_set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
}

BENCHMARK_CAPTURE(alwaysFound, Scan, SearchType::Scan);
BENCHMARK_CAPTURE(alwaysFound, LowerBound, SearchType::LowerBound);
BENCHMARK_CAPTURE(alwaysFound, StdSet, SearchType::StdSet);
BENCHMARK_CAPTURE(alwaysFound, AbslSet, SearchType::AbslSet);
} // namespace

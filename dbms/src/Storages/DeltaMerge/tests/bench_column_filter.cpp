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


#include <Columns/ColumnVector.h>
#include <benchmark/benchmark.h>

#include <random>

using namespace DB;

namespace
{
IColumn::Filter createRandomFilter(size_t n, size_t set_n)
{
    assert(n >= set_n);

    IColumn::Filter filter(set_n, 1);
    filter.resize_fill_zero(n, 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(filter.begin(), filter.end(), g);
    return filter;
}

template <typename... Args>
void columnFilter(benchmark::State & state, Args &&... args)
{
    auto [version, n, set_percent] = std::make_tuple(std::move(args)...);
    auto col = ColumnVector<Int64>::create();
    auto & v = col->getData();
    v.resize(n);
    std::iota(v.begin(), v.end(), 0);
    auto set_n = n * set_percent;
    auto filter = createRandomFilter(n, set_n);

    if (version == 0)
    {
        for (auto _ : state)
        {
            auto t = col->filter(filter, set_n * sizeof(Int64));
            benchmark::DoNotOptimize(t);
        }
    }
    else
    {
        for (auto _ : state)
        {
            auto t = col->filterNew(filter, set_n * sizeof(Int64));
            benchmark::DoNotOptimize(t);
        }
    }
}

BENCHMARK_CAPTURE(columnFilter, v0_00, 0, DEFAULT_BLOCK_SIZE, 0.0);
BENCHMARK_CAPTURE(columnFilter, v0_01, 0, DEFAULT_BLOCK_SIZE, 0.1);
BENCHMARK_CAPTURE(columnFilter, v0_02, 0, DEFAULT_BLOCK_SIZE, 0.2);
BENCHMARK_CAPTURE(columnFilter, v0_03, 0, DEFAULT_BLOCK_SIZE, 0.3);
BENCHMARK_CAPTURE(columnFilter, v0_04, 0, DEFAULT_BLOCK_SIZE, 0.4);
BENCHMARK_CAPTURE(columnFilter, v0_05, 0, DEFAULT_BLOCK_SIZE, 0.5);
BENCHMARK_CAPTURE(columnFilter, v0_06, 0, DEFAULT_BLOCK_SIZE, 0.6);
BENCHMARK_CAPTURE(columnFilter, v0_07, 0, DEFAULT_BLOCK_SIZE, 0.7);
BENCHMARK_CAPTURE(columnFilter, v0_08, 0, DEFAULT_BLOCK_SIZE, 0.8);
BENCHMARK_CAPTURE(columnFilter, v0_09, 0, DEFAULT_BLOCK_SIZE, 0.9);
BENCHMARK_CAPTURE(columnFilter, v0_10, 0, DEFAULT_BLOCK_SIZE, 1.0);

BENCHMARK_CAPTURE(columnFilter, v1_00, 1, DEFAULT_BLOCK_SIZE, 0.0);
BENCHMARK_CAPTURE(columnFilter, v1_01, 1, DEFAULT_BLOCK_SIZE, 0.1);
BENCHMARK_CAPTURE(columnFilter, v1_02, 1, DEFAULT_BLOCK_SIZE, 0.2);
BENCHMARK_CAPTURE(columnFilter, v1_03, 1, DEFAULT_BLOCK_SIZE, 0.3);
BENCHMARK_CAPTURE(columnFilter, v1_04, 1, DEFAULT_BLOCK_SIZE, 0.4);
BENCHMARK_CAPTURE(columnFilter, v1_05, 1, DEFAULT_BLOCK_SIZE, 0.5);
BENCHMARK_CAPTURE(columnFilter, v1_06, 1, DEFAULT_BLOCK_SIZE, 0.6);
BENCHMARK_CAPTURE(columnFilter, v1_07, 1, DEFAULT_BLOCK_SIZE, 0.7);
BENCHMARK_CAPTURE(columnFilter, v1_08, 1, DEFAULT_BLOCK_SIZE, 0.8);
BENCHMARK_CAPTURE(columnFilter, v1_09, 1, DEFAULT_BLOCK_SIZE, 0.9);
BENCHMARK_CAPTURE(columnFilter, v1_10, 1, DEFAULT_BLOCK_SIZE, 1.0);

}
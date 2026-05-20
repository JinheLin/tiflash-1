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

#include <common/types.h>

#include <tuple>

namespace DB
{
std::tuple<uint64_t *, uint64_t *> getAllocDeallocPtr();

struct ProcessMemoryUsage
{
    UInt64 resident_bytes; // Raw VmRSS in bytes.
    UInt64 rss_file_bytes;
    UInt64 memory_control_rss_bytes;
    UInt64 cur_virt_bytes;
    Int64 cur_proc_num_threads;
    bool valid_rss_file; // Whether rss_file_bytes was sampled from process metrics.
};
ProcessMemoryUsage get_process_mem_usage(bool exclude_rss_file_from_memory_control = false);

} // namespace DB

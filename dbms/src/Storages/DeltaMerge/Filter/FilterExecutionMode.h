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

namespace DB
{
// The execution mode of DAGQueryInfo::filters.
enum class FilterExecutionMode
{
    // Executed in computational layer.
    Computation = 0,

    // Executed in storage layer, but not for late materialization.
    // Only in this mode will the filters use min-max index to reduce computation.
    Storage = 1,

    // Merged with DAGQueryInfo::pushed_down_filters for late materialization.
    LM = 2,
};


} // namespace DB

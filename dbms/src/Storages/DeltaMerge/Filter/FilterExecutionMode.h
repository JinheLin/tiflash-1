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
// Computation - DAGQueryInfo::filters are executed in threads of computation.
// Storage - DAGQueryInfo::filters are executed in threads of storage, but not for late materialization.
// LM - DAGQueryInfo::filters are merged with DAGQueryInfo::pushed_down_filters, for late materialization.
enum class FilterExecutionMode
{
    Computation = 0,
    Storage = 1,
    LM = 2,
};


} // namespace DB

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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM
{

struct Attr
{
    String col_name;
    ColId col_id;
    DataTypePtr type;
};
using Attrs = std::vector<Attr>;

class RSResult
{
public:
    enum class HitState : UInt8
    {
        Unknown = 0,
        None = 1,
        Some = 2,
        All = 3,
    };

    bool has_null;

    HitState hit_state;

    RSResult(HitState hit_state_, bool has_null_)
        : has_null(has_null_)
        , hit_state(hit_state_)
    {}

    RSResult operator||(const RSResult & other) const
    {
        RUNTIME_CHECK(hit_state != HitState::Unknown || other.hit_state != HitState::Unknown);

        if (hit_state == HitState::All || other.hit_state == HitState::All)
            return RSResult(HitState::All, has_null || other.has_null);

        if (hit_state == HitState::Some || other.hit_state == HitState::Some)
            return RSResult(HitState::Some, has_null || other.has_null);

        return RSResult(HitState::None, has_null || other.has_null);
    }

    RSResult operator&&(const RSResult & other) const
    {
        RUNTIME_CHECK(hit_state != HitState::Unknown || other.hit_state != HitState::Unknown);

        if (hit_state == HitState::None || other.hit_state == HitState::None)
            return RSResult(HitState::None, has_null || other.has_null);

        if (hit_state == HitState::All && other.hit_state == HitState::All)
            return RSResult(HitState::All, has_null || other.has_null);

        return RSResult(HitState::Some, has_null || other.has_null);
    }

    RSResult operator!() const
    {
        switch (hit_state)
        {
        case HitState::Some:
            return RSResult(HitState::Some, has_null);
        case HitState::None:
            return RSResult(HitState::All, has_null);
        case HitState::All:
            return RSResult(HitState::None, has_null);
        default:
            throw Exception("Unexpected Unknown");
        }
    }

    String toString() const
    {
        switch (hit_state)
        {
        case HitState::None:
            return has_null ? "NoneNull" : "None";
        case HitState::Some:
            return has_null ? "SomeNull" : "Some";
        case HitState::All:
            return has_null ? "AllNull" : "All";
        default:
            return "Unknown";
        }
    }

    bool operator==(const RSResult & other) const { return hit_state == other.hit_state && has_null == other.has_null; }

    bool needToRead() const { return hit_state != HitState::None; }

    bool needToFilter() const { return hit_state == HitState::All && !has_null; }

    void setNotNeedToRead() { hit_state = HitState::None; }
};

inline RSResult::HitState operator||(RSResult::HitState v0, RSResult::HitState v1)
{
    RUNTIME_CHECK(v0 != RSResult::HitState::Unknown || v1 != RSResult::HitState::Unknown);
    if (v0 == RSResult::HitState::All || v1 == RSResult::HitState::All)
        return RSResult::HitState::All;
    if (v0 == RSResult::HitState::Some || v1 == RSResult::HitState::Some)
        return RSResult::HitState::Some;
    return RSResult::HitState::None;
}

inline RSResult::HitState operator&&(RSResult::HitState v0, RSResult::HitState v1)
{
    RUNTIME_CHECK(v0 != RSResult::HitState::Unknown || v1 != RSResult::HitState::Unknown);
    if (v0 == RSResult::HitState::None || v1 == RSResult::HitState::None)
        return RSResult::HitState::None;
    if (v0 == RSResult::HitState::All && v1 == RSResult::HitState::All)
        return RSResult::HitState::All;
    return RSResult::HitState::Some;
}

inline RSResult::HitState operator!(RSResult::HitState v)
{
    switch (v)
    {
    case RSResult::HitState::Some:
        return RSResult::HitState::Some;
    case RSResult::HitState::None:
        return RSResult::HitState::All;
    case RSResult::HitState::All:
        return RSResult::HitState::None;
    default:
        throw Exception("Unexpected Unknown");
    }
}

namespace RSResultConst
{

// All values meet requirements NOT has null, need to read and no need perform filtering
static const RSResult All = RSResult(RSResult::HitState::All, false);
// Some values meet requirements and NOT has null, need to read and perform filtering
static const RSResult Some = RSResult(RSResult::HitState::Some, false);
// No value meets requirements and NOT has null, no need to read
static const RSResult None = RSResult(RSResult::HitState::None, false);
// All values meet requirements and has null, need to read and perform filtering
static const RSResult AllNull = RSResult(RSResult::HitState::All, true);
// Some values meet requirements and has null, need to read and perform filtering
static const RSResult SomeNull = RSResult(RSResult::HitState::Some, true);
// No value meets requirements and has null, no need to read
static const RSResult NoneNull = RSResult(RSResult::HitState::None, true);

} // namespace RSResultConst

using RSResults = std::vector<RSResult>;

} // namespace DB::DM

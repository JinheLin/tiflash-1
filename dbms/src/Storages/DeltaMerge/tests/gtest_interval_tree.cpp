#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <list>
#include <random>

namespace DB::DM::tests
{
template <typename IntervalType, typename ValueType, typename ToValue, typename ToIntervalType>
void testSimple(ToValue to_value, ToIntervalType to_interval_type)
{
    IntervalTree<IntervalType, ValueType> tree;
    {
        auto v = to_value(1, 10);
        tree.insert({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v)), v});
    }
    {
        auto v = to_value(10, 20);
        tree.insert({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v)), v});
    }
    {
        auto v = to_value(20, 30);
        tree.insert({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v)), v});
    }

    {
        auto v = to_value(1, 10);
        auto r = tree.find({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(*r, v);
    }
    {
        auto v = to_value(20, 30);
        auto r = tree.find({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(*r, v);
    }
    {
        auto v = to_value(5, 15);
        auto r = tree.find({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
        ASSERT_FALSE(r.has_value());
    }

    {
        // Find overlap with [10, 20)
        auto v = to_value(10, 20);
        auto overlaps = tree.findOverlappingIntervals(
            {to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))},
            false);
        ASSERT_EQ(overlaps.size(), 1);
        ASSERT_EQ(overlaps.front().value, v);
    }
    {
        // Find overlap with [10, 10)
        auto v = to_value(10, 10);
        auto overlaps = tree.findOverlappingIntervals(
            {to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))},
            false);
        ASSERT_TRUE(overlaps.empty());
    }
    {
        // Find overlap with [30, 30)
        auto v = to_value(30, 30);
        auto overlaps = tree.findOverlappingIntervals(
            {to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))},
            false);
        ASSERT_TRUE(overlaps.empty());
    }
}

TEST(IntervalTree_test, Simple)
{
    {
        auto to_value = [](int i, int j) {
            return std::make_tuple(i, j);
        };
        auto to_interval_type = [](int i) {
            return i;
        };
        testSimple<int, std::tuple<int, int>>(to_value, to_interval_type);
    }

    {
        auto to_row_key_value = [](int i) {
            WriteBufferFromOwnString ss;
            DB::EncodeInt64(i, ss);
            return RowKeyValue(false, std::make_shared<String>(ss.releaseStr()), i);
        };
        auto to_value = [&](int i, int j) {
            return std::make_tuple(to_row_key_value(i), to_row_key_value(j));
        };
        auto to_interval_type = [](const RowKeyValue & key) {
            return std::string_view(*(key.value));
        };
        testSimple<std::string_view, std::tuple<RowKeyValue, RowKeyValue>>(to_value, to_interval_type);
    }
}

template <typename T, typename ValueType>
class SequenceInterval
{
public:
    bool insert(T interval)
    {
        if (!find(interval))
        {
            intervals.push_back(std::move(interval));
            return true;
        }
        return false;
    }

    std::optional<ValueType> find(const T & interval) const
    {
        auto itr = std::find(intervals.cbegin(), intervals.cend(), interval);
        return itr != intervals.cend() ? std::make_optional<ValueType>(itr->value) : std::nullopt;
    }

    std::vector<T> findOverlappingIntervals(const T & interval, bool boundary) const
    {
        std::vector<T> out;
        std::copy_if(
            intervals.cbegin(),
            intervals.cend(),
            std::back_inserter(out),
            [&interval, boundary](const auto & a) {
                return boundary ? closedIntersecting(interval, a) : rightOpenIntersecting(interval, a);
            });
        return out;
    }

    bool remove(const T & interval)
    {
        auto itr = std::find(intervals.cbegin(), intervals.cend(), interval);
        if (itr != intervals.cend())
        {
            intervals.erase(itr);
            return true;
        }
        return false;
    }

    size_t size() const { return intervals.size(); }

private:
    static bool rightOpenIntersecting(const T & a, const T & b) { return a.low < b.high && b.low < a.high; }
    static bool closedIntersecting(const T & a, const T & b) { return a.low <= b.high && b.low <= a.high; }

    std::list<T> intervals;
};

void setUpDisjointRanges(std::vector<std::tuple<int, int>> & ranges, int count)
{
    constexpr auto range_max_step_length = 10000;
    ranges.reserve(count);
    std::default_random_engine e;
    int low = 0;
    for (int i = 0; i < count; i++)
    {
        int high = low + e() % range_max_step_length + 1;
        ranges.emplace_back(low, high);
        low = high;
    }
}

void setUpSplitRanges(std::vector<std::tuple<int, int>> & ranges, int count)
{
    std::default_random_engine e;
    for (int i = 0; i < count; i++)
    {
        auto t = e() % ranges.size();
        auto [low, high] = ranges[t];
        auto mid = (low + high) / 2;
        ranges.emplace_back(low, high);
        ranges.emplace_back(mid, high);
    }
}

template <typename IntervalType, typename ValueType, typename ToValue, typename ToIntervalType>
void testRandom(ToValue to_value, ToIntervalType to_interval_type)
{
    Stopwatch sw;
    constexpr auto min_ranges_count = 5000;
    std::default_random_engine e;
    int ranges_count = e() % min_ranges_count + min_ranges_count;
    std::vector<std::tuple<int, int>> random_ranges;
    random_ranges.reserve(ranges_count);
    setUpDisjointRanges(random_ranges, ranges_count);
    setUpSplitRanges(random_ranges, e() % min_ranges_count);
    auto setup_seconds = sw.elapsedSecondsFromLastTime();

    auto insert = [&](auto & t) {
        for (auto [l, h] : random_ranges)
        {
            auto v = to_value(l, h);
            t.insert({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v)), v});
        }
    };

    IntervalTree<IntervalType, ValueType> tree;
    SequenceInterval<typename IntervalTree<IntervalType, ValueType>::Interval, ValueType> seq;
    insert(seq);
    insert(tree);
    ASSERT_EQ(tree.size(), seq.size());
    auto insert_seconds = sw.elapsedSecondsFromLastTime();

    auto find_overlap = [&]() {
        for (auto [l, h] : random_ranges)
        {
            auto v = to_value(l, h);
            auto seq_overlaps = seq.findOverlappingIntervals(
                {to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))},
                false);
            auto tree_overlaps = tree.findOverlappingIntervals(
                {to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))},
                false);
            ASSERT_EQ(seq_overlaps.size(), tree_overlaps.size());
            for (const auto & interval : seq_overlaps)
            {
                auto itr = std::find(tree_overlaps.cbegin(), tree_overlaps.cend(), interval);
                ASSERT_NE(itr, tree_overlaps.cend());
                ASSERT_EQ(itr->value, interval.value);
            }
        }
    };

    auto find = [&]() {
        for (auto [l, h] : random_ranges)
        {
            auto v = to_value(l, h);
            auto seq_v = seq.find({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
            auto tree_v = tree.find({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
            ASSERT_EQ(seq_v, tree_v);
            if (tree_v)
            {
                ASSERT_EQ(*tree_v, v);
            }
        }
    };

    auto remove_random = [&]() {
        auto i = e() % random_ranges.size();
        auto [l, h] = random_ranges[i];
        auto v = to_value(l, h);
        auto r1 = seq.remove({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
        auto r2 = tree.remove({to_interval_type(std::get<0>(v)), to_interval_type(std::get<1>(v))});
        RUNTIME_CHECK(r1 == r2);
        return r1;
    };

    auto remove_count = 0;
    auto find_overlap_seconds = 0.0;
    auto find_seconds = 0.0;
    for (int i = 0; i < 10; i++)
    {
        find_overlap();
        find_overlap_seconds += sw.elapsedSecondsFromLastTime();
        find();
        find_seconds += sw.elapsedSecondsFromLastTime();

        remove_count += remove_random();
    }

    LOG_INFO(
        Logger::get(),
        "setup_seconds={}, insert_seconds={}, find_overlap_seconds={}, find_seconds={}, remove_count={}",
        setup_seconds,
        insert_seconds,
        find_overlap_seconds,
        find_seconds,
        remove_count);
}

TEST(IntervalTree_test, Random)
{
    {
        auto to_value = [](int i, int j) {
            return std::make_tuple(i, j);
        };
        auto to_interval_type = [](int i) {
            return i;
        };
        testRandom<int, std::tuple<int, int>>(to_value, to_interval_type);
    }

    {
        auto to_row_key_value = [](int i) {
            WriteBufferFromOwnString ss;
            DB::EncodeInt64(i, ss);
            return RowKeyValue(false, std::make_shared<String>(ss.releaseStr()), i);
        };
        auto to_value = [&](int i, int j) {
            return std::make_tuple(to_row_key_value(i), to_row_key_value(j));
        };
        auto to_interval_type = [](const RowKeyValue & key) {
            return std::string_view(*(key.value));
        };
        testRandom<std::string_view, std::tuple<RowKeyValue, RowKeyValue>>(to_value, to_interval_type);
    }
}

} // namespace DB::DM::tests
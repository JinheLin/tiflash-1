#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <list>
#include <random>

namespace DB::DM::tests
{

using IntervalType = int;
using ValueType = int;
using ITree = IntervalTree<IntervalType, ValueType>;
using Interval = ITree::Interval;
using Intervals = ITree::Intervals;

TEST(IntervalTree_test, FindOverlap)
{
    ITree tree;
    // [1, 10), [10, 20), [20, 30)
    tree.insert({1, 10});
    tree.insert({10, 20});
    tree.insert({20, 30});

    {
        // Find overlap with [10, 20)
        auto overlaps = tree.findOverlappingIntervals({10, 20}, false);
        ASSERT_EQ(overlaps.size(), 1);
        ASSERT_EQ(overlaps.front(), (Interval{10, 20}));
    }

    {
        // Find overlap with [10, 10)
        auto overlaps = tree.findOverlappingIntervals({10, 10}, false);
        ASSERT_TRUE(overlaps.empty());
    }

    {
        // Find overlap with [30, 30)
        auto overlaps = tree.findOverlappingIntervals({30, 30}, false);
        ASSERT_TRUE(overlaps.empty());
    }
}

TEST(IntervalTree_test, Find)
{
    ITree tree;
    tree.insert({1, 10, 110});
    tree.insert({20, 30, 2030});

    auto v1 = tree.find({1, 10});
    ASSERT_TRUE(v1.has_value());
    ASSERT_EQ(*v1, 110);
    auto v2 = tree.find({20, 30});
    ASSERT_TRUE(v2.has_value());
    ASSERT_EQ(*v2, 2030);
    auto v3 = tree.find({5, 15});
    ASSERT_FALSE(v3.has_value());
}

class SequenceInterval
{
public:
    bool insert(Interval interval)
    {
        if (!find(interval))
        {
            intervals.push_back(std::move(interval));
            return true;
        }
        return false;
    }

    std::optional<ValueType> find(const Interval & interval) const
    {
        auto itr = std::find(intervals.cbegin(), intervals.cend(), interval);
        return itr != intervals.cend() ? std::make_optional<ValueType>(itr->value) : std::nullopt;
    }

    Intervals findOverlappingIntervals(const Interval & interval, bool boundary) const
    {
        Intervals out;
        std::copy_if(
            intervals.cbegin(),
            intervals.cend(),
            std::back_inserter(out),
            [&interval, boundary](const auto & a) {
                return boundary ? closedIntersecting(interval, a) : rightOpenIntersecting(interval, a);
            });
        return out;
    }

    bool remove(const Interval & interval)
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
    static bool rightOpenIntersecting(const Interval & a, const Interval & b)
    {
        return a.low < b.high && b.low < a.high;
    }
    static bool closedIntersecting(const Interval & a, const Interval & b)
    {
        return a.low <= b.high && b.low <= a.high;
    }

    std::list<Interval> intervals;
};

void setUpDisjointRanges(std::vector<std::tuple<int, int, int>> & ranges, int count)
{
    constexpr auto range_max_step_length = 10000;
    ranges.reserve(count);
    std::default_random_engine e;
    int low = 0;
    for (int i = 0; i < count; i++)
    {
        int high = low + e() % range_max_step_length + 1;
        ranges.emplace_back(low, high, e());
        low = high + 1;
    }
}

void setUpSplitRanges(std::vector<std::tuple<int, int, int>> & ranges, int count)
{
    std::default_random_engine e;
    for (int i = 0; i < count; i++)
    {
        auto t = e() % ranges.size();
        auto [low, high, value] = ranges[t];
        auto mid = (low + high) / 2;
        ranges.emplace_back(low, high, value);
        ranges.emplace_back(mid, high, value);
    }
}

TEST(IntervalTree_test, RandomTest)
{
    Stopwatch sw;
    constexpr auto min_ranges_count = 10000;
    std::default_random_engine e;
    int ranges_count = e() % min_ranges_count + min_ranges_count;
    std::vector<std::tuple<int, int, int>> random_ranges;
    random_ranges.reserve(ranges_count);
    setUpDisjointRanges(random_ranges, ranges_count);
    setUpSplitRanges(random_ranges, e() % min_ranges_count);
    auto setup_seconds = sw.elapsedSecondsFromLastTime();

    auto insert = [&](auto & t) {
        for (auto [l, h, v] : random_ranges)
        {
            t.insert({l, h, v});
        }
    };

    SequenceInterval seq;
    insert(seq);
    ITree tree;
    insert(tree);
    ASSERT_EQ(tree.size(), seq.size());
    auto insert_seconds = sw.elapsedSecondsFromLastTime();

    auto find_overlap = [&]() {
        for (auto [l, h, v] : random_ranges)
        {
            auto seq_overlaps = seq.findOverlappingIntervals({l, h}, false);
            auto tree_overlaps = tree.findOverlappingIntervals({l, h}, false);
            ASSERT_EQ(seq_overlaps.size(), tree_overlaps.size());
            for (const auto & interval : seq_overlaps)
            {
                auto itr = std::find(tree_overlaps.cbegin(), tree_overlaps.cend(), interval);
                ASSERT_NE(itr, tree_overlaps.cend());
                ASSERT_EQ(itr->value, v);
            }
        }
    };

    auto find = [&]() {
        for (auto [l, h, v] : random_ranges)
        {
            auto seq_v = seq.find({l, h});
            auto tree_v = tree.find({l, h});
            ASSERT_EQ(seq_v, tree_v);
            if (tree_v)
            {
                ASSERT_EQ(*tree_v, v);
            }
        }
    };

    auto remove_random = [&]() {
        auto i = e() % random_ranges.size();
        auto [l, h, v] = random_ranges[i];
        auto r1 = seq.remove({l, h});
        auto r2 = tree.remove({l, h});
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

} // namespace DB::DM::tests
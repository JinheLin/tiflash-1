#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <gtest/gtest.h>
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

    size_t size() const
    {
        return intervals.size();
    }
private:
    static bool rightOpenIntersecting(const Interval & a, const Interval & b)
    {
        return a.low < b.high && b.low < a.high;
    }
    static bool closedIntersecting(const Interval & a, const Interval & b)
    {
        return a.low <= b.high && b.low <= a.high;
    }
    
    Intervals intervals;
};

void setUpDisjointRanges(std::vector<std::pair<int, int>> & ranges, int count)
{
    constexpr auto range_max_step_length = 10000;
    ranges.reserve(count);
    std::default_random_engine e;
    int left = 0;
    for (int i = 0; i < count; i++) {
        int right = left + e() % range_max_step_length + 1;
        ranges.emplace_back(left, right);
        left = right + 1;
    }
}

void setUpSplitRanges(std::vector<std::pair<int, int>> & ranges, int count)
{
    std::default_random_engine e;
    for (int i = 0; i < count; i++) {
        auto t = e() % ranges.size();
        auto [left, right] = ranges[t];
        auto mid = (left + right) / 2;
        ranges.emplace_back(left, mid);
        ranges.emplace_back(mid, right);
    }
}

template<typename T>
void insert(T & t, const std::vector<std::pair<int, int>> & ranges)
{
    for (auto [l, h] : ranges)
    {
        t.insert({l, h});
    }
}

TEST(IntervalTree_test, RandomTest)
{
    std::default_random_engine e;
    int ranges_count = e() % 10000 + 10000;
    std::vector<std::pair<int, int>> random_ranges;
    random_ranges.reserve(ranges_count);
    setUpDisjointRanges(random_ranges, ranges_count);
    setUpSplitRanges(random_ranges, e() % 10000);

    SequenceInterval seq;
    insert(seq, random_ranges);
    ITree tree;
    insert(tree, random_ranges);
    ASSERT_EQ(tree.size(), seq.size());

    for (auto [l, h] : random_ranges)
    {
        auto seq_overlaps = seq.findOverlappingIntervals({l, h}, false);
        auto tree_overlaps = tree.findOverlappingIntervals({l, h}, false);
        ASSERT_EQ(seq_overlaps.size(), tree_overlaps.size());
        for (const auto & interval : seq_overlaps)
        {
            ASSERT_NE(std::find(tree_overlaps.cbegin(), tree_overlaps.cend(), interval), tree_overlaps.cend());
        }
    }
}

} // namespace DB::DM::tests
#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <gtest/gtest.h>

#include <iostream>
namespace DB::DM::tests
{
TEST(IntervalTree_test, FindOverlap)
{
    IntervalTree<int, int> tree;
    // [1, 10), [10, 20), [20, 30)
    tree.insert({1, 10});
    tree.insert({10, 20});
    tree.insert({20, 30});

    {
        IntervalTree<int, int>::Intervals overlaps;
        // Find overlap with [10, 20)
        tree.findOverlappingIntervals({10, 20}, overlaps, false);
        ASSERT_EQ(overlaps.size(), 1);
        ASSERT_EQ(overlaps.front(), (IntervalTree<int, int>::Interval{10, 20}));
    }
}

TEST(IntervalTree_test, Find)
{
    IntervalTree<int, int> tree;
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


} // namespace DB::DM::tests
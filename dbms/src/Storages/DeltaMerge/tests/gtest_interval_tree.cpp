#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <gtest/gtest.h>

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
        // Find overlap with [10, 20)
        auto overlaps = tree.findOverlappingIntervals({10, 20}, false);
        ASSERT_EQ(overlaps.size(), 1);
        ASSERT_EQ(overlaps.front(), (IntervalTree<int, int>::Interval{10, 20}));
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
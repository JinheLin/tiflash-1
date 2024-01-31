#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <gtest/gtest.h>

#include <iostream>
namespace DB::DM::tests
{
TEST(IntervalTree_test, FindOverlap)
{
    IntervalTree<int, int> tree;
    tree.insert({1, 10});
    tree.insert({20, 30});
    {
        IntervalTree<int, int>::Intervals overlaps;
        tree.findOverlappingIntervals({10, 20}, overlaps, true);
        for (const auto & o : overlaps)
        {
            std::cout << o << std::endl;
        }
        std::cout << std::endl;
    }

    {
        IntervalTree<int, int>::Intervals overlaps;
        tree.findOverlappingIntervals({10, 20}, overlaps, false);
        for (const auto & o : overlaps)
        {
            std::cout << o << std::endl;
        }
        std::cout << std::endl;
    }
}

TEST(IntervalTree_test, Find)
{
    IntervalTree<int, int> tree;
    tree.insert({1, 10, 110});
    tree.insert({20, 30, 2030});
    
    auto v1 = tree.find({1, 10});
    ASSERT_TRUE(v1.has_value());
    auto v2 = tree.find({20, 30});
    ASSERT_TRUE(v2.has_value());
    auto v3 = tree.find({5, 15});
    ASSERT_FALSE(v3.has_value());
}


} // namespace DB::DM::tests
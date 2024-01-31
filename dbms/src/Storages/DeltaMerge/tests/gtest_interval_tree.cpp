#include <Storages/DeltaMerge/UncommittedZone/IntervalTree.h>
#include <gtest/gtest.h>

#include <iostream>
namespace DB::DM::tests
{
TEST(IntervalTree_test, FindOverlap)
{
    IntervalTree<int> tree;
    tree.insert({1, 10});
    tree.insert({20, 30});
    {
        IntervalTree<int>::Intervals overlaps;
        tree.findOverlappingIntervals({10, 20}, overlaps, true);
        for (const auto & o : overlaps)
        {
            std::cout << o << std::endl;
        }
        std::cout << std::endl;
    }

    {
        IntervalTree<int>::Intervals overlaps;
        tree.findOverlappingIntervals({10, 20}, overlaps, false);
        for (const auto & o : overlaps)
        {
            std::cout << o << std::endl;
        }
        std::cout << std::endl;
    }
}

} // namespace DB::DM::tests
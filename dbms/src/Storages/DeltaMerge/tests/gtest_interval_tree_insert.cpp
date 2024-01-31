#include <Storages/DeltaMerge/SpilledZone/IntervalTree.h>
#include <Storages/DeltaMerge/SpilledZone/IntervalTypes.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <functional>
#include <list>
#include <random>
#include <vector>

using namespace lib_interval_tree;

template <typename ContainedT>
struct IntervalTypes
{
    using value_type = ContainedT;
    using interval_type = lib_interval_tree::interval<ContainedT, lib_interval_tree::right_open>;
    using tree_type = lib_interval_tree::interval_tree<interval_type>;
    using iterator_type = typename tree_type::iterator;
};

/**
 *  Warning this function is very expensive.
 */
template <typename TreeT>
void testRedBlackPropertyViolation(TreeT const & tree)
{
    using namespace lib_interval_tree;

    // root is always black.
    EXPECT_EQ(tree.root().color(), rb_color::black);

    // check that all nodes have red or black coloring. (seems obvious, but is not on bug)
    for (auto i = std::begin(tree); i != std::end(tree); ++i)
    {
        EXPECT_EQ(true, i.color() == rb_color::black || i.color() == rb_color::red);
    }

    // check for (red children = black) property:
    for (auto i = std::begin(tree); i != std::end(tree); ++i)
    {
        auto nodeColor = i.color();
        if (nodeColor == rb_color::red)
        {
            if (i.left() != std::end(tree))
            {
                EXPECT_EQ(i.left().color(), rb_color::black);
            }
            if (i.right() != std::end(tree))
            {
                EXPECT_EQ(i.right().color(), rb_color::black);
            }
        }
    }

    auto leafCollector = [&](typename TreeT::const_iterator root) {
        std::list<typename TreeT::const_iterator> leaves{};
        std::function<void(typename std::list<typename TreeT::const_iterator>::iterator)> recursiveLeafFinder;
        recursiveLeafFinder = [&](typename std::list<typename TreeT::const_iterator>::iterator self) {
            if (self->left() != std::end(tree))
            {
                recursiveLeafFinder(leaves.insert(self, self->left()));
            }
            if (self->right() != std::end(tree))
            {
                *self = self->right();
                recursiveLeafFinder(self);
            }
        };
        leaves.push_back(root);
        recursiveLeafFinder(leaves.begin());
        return leaves;
    };

    // Test that for every node, on the path to its leaves, has the same number of black nodes.
    for (auto i = std::cbegin(tree); i != std::cend(tree); ++i)
    {
        auto leaves = leafCollector(i);
        int comparisonCounter{0};
        for (auto const & leaf : leaves)
        {
            auto p = leaf;
            int counter{0};
            do
            {
                if (p.color() == rb_color::black)
                    ++counter;
                p = p.parent();
            } while (p != i && p != std::end(tree));
            if (comparisonCounter == 0)
                comparisonCounter = counter;
            else
            {
                EXPECT_EQ(comparisonCounter, counter);
            }
        }
    }
}

template <typename TreeT>
void testMaxProperty(TreeT const & tree)
{
    for (auto i = std::begin(tree); i != std::end(tree); ++i)
    {
        if (i->left())
        {
            EXPECT_LE(i->left()->max(), i->max());
        }
        if (i->right())
        {
            EXPECT_LE(i->right()->max(), i->max());
        }
        EXPECT_GE(i->max(), i->interval().high());
    }
}

template <typename TreeT>
void testTreeHeightHealth(TreeT const & tree)
{
    int treeSize = tree.size();

    auto maxHeight{0};
    for (auto i = std::begin(tree); i != std::end(tree); ++i)
        maxHeight = std::max(maxHeight, i->height());

    EXPECT_LE(maxHeight, 2 * std::log2(treeSize + 1));
}

template <typename numerical_type, typename interval_kind_ = lib_interval_tree::closed>
struct multi_join_interval
{
public:
    using value_type = numerical_type;
    using interval_kind = interval_kind_;

    constexpr multi_join_interval(value_type low, value_type high)
        : low_{low}
        , high_{high}
    {
        if (low > high)
            throw std::invalid_argument("Low border is not lower or equal to high border.");
    }

    virtual ~multi_join_interval() = default;
    friend bool operator==(multi_join_interval const & lhs, multi_join_interval const & other)
    {
        return lhs.low_ == other.low_ && lhs.high_ == other.high_;
    }
    friend bool operator!=(multi_join_interval const & lhs, multi_join_interval const & other)
    {
        return lhs.low_ != other.low_ || lhs.high_ != other.high_;
    }
    value_type low() const { return low_; }
    value_type high() const { return high_; }
    bool overlaps(value_type l, value_type h) const { return low_ <= h && l <= high_; }
    bool overlaps_exclusive(value_type l, value_type h) const { return low_ < h && l < high_; }
    bool overlaps(multi_join_interval const & other) const { return overlaps(other.low_, other.high_); }
    bool overlaps_exclusive(multi_join_interval const & other) const
    {
        return overlaps_exclusive(other.low_, other.high_);
    }
    bool within(value_type value) const { return interval_kind::within(low_, high_, value); }
    bool within(multi_join_interval const & other) const { return low_ <= other.low_ && high_ >= other.high_; }
    value_type operator-(multi_join_interval const & other) const
    {
        if (overlaps(other))
            return 0;
        if (high_ < other.low_)
            return other.low_ - high_;
        else
            return low_ - other.high_;
    }
    value_type size() const { return high_ - low_; }
    std::vector<multi_join_interval> join(multi_join_interval const & other) const
    {
        const auto min = std::min(low_, other.low_);
        const auto max = std::max(high_, other.high_);
        const auto avg = (min + max) / 2;
        return {
            {min, avg},
            {avg, max},
        };
    }

protected:
    value_type low_;
    value_type high_;
};

class IntervalTreeInsertTests : public ::testing::Test
{
public:
    using types = IntervalTypes<int>;

protected:
    IntervalTypes<int>::tree_type tree;
    std::default_random_engine gen;
    std::uniform_int_distribution<int> distSmall{-500, 500};
    std::uniform_int_distribution<int> distLarge{-50000, 50000};
};

TEST_F(IntervalTreeInsertTests, InsertIntoEmpty1)
{
    auto inserted_interval = types::interval_type{0, 16};

    tree.insert(inserted_interval);
    EXPECT_EQ(*tree.begin(), inserted_interval);
    EXPECT_EQ(tree.size(), 1);
}

TEST_F(IntervalTreeInsertTests, InsertIntoEmpty2)
{
    auto inserted_interval = types::interval_type{-45, 16};

    tree.insert(inserted_interval);
    EXPECT_EQ(*tree.begin(), inserted_interval);
    EXPECT_EQ(tree.size(), 1);
}

TEST_F(IntervalTreeInsertTests, InsertMultipleIntoEmpty)
{
    auto firstInterval = types::interval_type{0, 16};
    auto secondInterval = types::interval_type{5, 13};

    tree.insert(firstInterval);
    tree.insert(secondInterval);

    EXPECT_EQ(tree.size(), 2);

    EXPECT_EQ(*tree.begin(), firstInterval);
    EXPECT_EQ(*(++tree.begin()), secondInterval);
}

TEST_F(IntervalTreeInsertTests, TreeHeightHealthynessTest)
{
    constexpr int amount = 100'000;

    for (int i = 0; i != amount; ++i)
        tree.insert(lib_interval_tree::make_safe_interval<int, right_open>(distSmall(gen), distSmall(gen)));

    testTreeHeightHealth(tree);
}

TEST_F(IntervalTreeInsertTests, MaxValueTest1)
{
    constexpr int amount = 100'000;

    for (int i = 0; i != amount; ++i)
        tree.insert(lib_interval_tree::make_safe_interval<int, right_open>(distSmall(gen), distSmall(gen)));

    testMaxProperty(tree);
}

TEST_F(IntervalTreeInsertTests, RBPropertyInsertTest)
{
    constexpr int amount = 1000;

    for (int i = 0; i != amount; ++i)
        tree.insert(lib_interval_tree::make_safe_interval<int, right_open>(distSmall(gen), distSmall(gen)));

    testRedBlackPropertyViolation(tree);
}

TEST_F(IntervalTreeInsertTests, IntervalsMayReturnMultipleIntervalsForJoin)
{
    using interval_type = multi_join_interval<int>;
    using tree_type = lib_interval_tree::interval_tree<interval_type>;

    auto multiJoinTree = tree_type{};

    multiJoinTree.insert({0, 1});
    multiJoinTree.insert_overlap({0, 2});

    EXPECT_EQ(multiJoinTree.size(), 2);
    EXPECT_EQ(*multiJoinTree.begin(), (interval_type{0, 1}))
        << multiJoinTree.begin()->low() << multiJoinTree.begin()->high();
    EXPECT_EQ(*++multiJoinTree.begin(), (interval_type{1, 2}));
}


class OverlapFindTests
    : public ::testing::Test
{
public:
    using types = IntervalTypes <int>;
protected:
    IntervalTypes <int>::tree_type tree;
};

TEST_F(OverlapFindTests, WillReturnEndIfTreeIsEmpty)
{
    EXPECT_EQ(tree.overlap_find({2, 7}), std::end(tree));
}

TEST_F(OverlapFindTests, WillNotFindOverlapWithRootIfItDoesntOverlap)
{
    tree.insert({0, 1});
    EXPECT_EQ(tree.overlap_find({2, 7}), std::end(tree));
}

TEST_F(OverlapFindTests, WillFindOverlapWithRoot)
{
    tree.insert({2, 4});
    EXPECT_EQ(tree.overlap_find({2, 7}), std::begin(tree));
}

TEST_F(OverlapFindTests, WillFindOverlapWithRootOnConstTree)
{
    tree.insert({2, 4});
    [](auto const& tree) {
        EXPECT_EQ(tree.overlap_find({2, 7}), std::begin(tree));
    }(tree);
}

TEST_F(OverlapFindTests, WillFindOverlapWithRootIfMatchingExactly)
{
    tree.insert({2, 7});
    EXPECT_EQ(tree.overlap_find({2, 7}), std::begin(tree));
}

TEST_F(OverlapFindTests, WillFindOverlapWithRootIfTouching)
{
    tree.insert({2, 7});
    EXPECT_EQ(tree.overlap_find({7, 9}), std::begin(tree));
}

TEST_F(OverlapFindTests, WillNotFindOverlapIfNothingOverlaps)
{
    tree.insert({0, 5});
    tree.insert({5, 10});
    tree.insert({10, 15});
    tree.insert({15, 20});
    EXPECT_EQ(tree.overlap_find({77, 99}), std::end(tree));
}

TEST_F(OverlapFindTests, WillNotFindOverlapOnBorderIfExclusive)
{
    tree.insert({0, 5});
    tree.insert({5, 10});
    tree.insert({10, 15});
    tree.insert({15, 20});
    EXPECT_EQ(tree.overlap_find({5, 5}, true), std::end(tree));
}

TEST_F(OverlapFindTests, WillFindMultipleOverlaps)
{
    tree.insert({0, 5});
    tree.insert({5, 10});
    tree.insert({10, 15});
    tree.insert({15, 20});

    std::vector <decltype(tree)::interval_type> intervals;
    tree.overlap_find_all({5, 5}, [&intervals](auto iter) {
        intervals.push_back(*iter);
        return true;
    });
    //using ::testing::UnorderedElementsAre;
    //ASSERT_THAT(intervals, UnorderedElementsAre(decltype(tree)::interval_type{0, 5}, decltype(tree)::interval_type{5, 10}));
}

TEST_F(OverlapFindTests, FindAllWillFindNothingIfEmpty)
{
    int findCount = 0;
    tree.overlap_find_all({2, 7}, [&findCount](auto){
        ++findCount;
        return true;
    });
    EXPECT_EQ(findCount, 0);
}

TEST_F(OverlapFindTests, FindAllWillFindNothingIfNothingOverlaps)
{
    tree.insert({16, 21});
    tree.insert({8, 9});
    tree.insert({25, 30});
    tree.insert({5, 8});
    tree.insert({15, 23});
    int findCount = 0;
    tree.overlap_find_all({1000, 2000}, [&findCount](auto){
        ++findCount;
        return true;
    });
    EXPECT_EQ(findCount, 0);
}

TEST_F(OverlapFindTests, FindAllWillFindAllWithLotsOfDuplicates)
{
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});

    int findCount = 0;
    tree.overlap_find_all({2, 3}, [&findCount](decltype(tree)::iterator iter){
        ++findCount;
        EXPECT_EQ(*iter, (decltype(tree)::interval_type{0, 5}));
        return true;
    });
    EXPECT_EQ(findCount, tree.size());
}

TEST_F(OverlapFindTests, CanExitPreemptivelyByReturningFalse)
{
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});
    tree.insert({0, 5});

    int findCount = 0;
    tree.overlap_find_all({5, 8}, [&findCount](decltype(tree)::iterator ){
        ++findCount;
        return true;
    });
    EXPECT_EQ(findCount, 0);
}

TEST_F(OverlapFindTests, WillFindSingleOverlapInBiggerTree)
{
    tree.insert({16, 21});
    tree.insert({8, 9});
    tree.insert({25, 30});
    tree.insert({5, 8});
    tree.insert({15, 23});
    tree.insert({17, 19});
    tree.insert({26, 26});
    tree.insert({1000, 2000});
    tree.insert({6, 10});
    tree.insert({19, 20});
    auto iter = tree.overlap_find({1000, 1001});
    EXPECT_NE(iter, std::end(tree));
    EXPECT_EQ(iter->low(), 1000);
    EXPECT_EQ(iter->high(), 2000);
}

class FindTests
    : public ::testing::Test
{
public:
    using types = IntervalTypes <int>;
protected:
    IntervalTypes <int>::tree_type tree;
    std::default_random_engine gen;
    std::uniform_int_distribution <int> distLarge{-50000, 50000};
};

TEST_F(FindTests, WillReturnEndIfTreeIsEmpty)
{
    EXPECT_EQ(tree.find({2, 7}), std::end(tree));
}

TEST_F(FindTests, WillNotFindRootIfItIsntTheSame)
{
    tree.insert({0, 1});
    EXPECT_EQ(tree.find({2, 7}), std::end(tree));
}

TEST_F(FindTests, WillFindRoot)
{
    tree.insert({0, 1});
    EXPECT_EQ(tree.find({0, 1}), std::begin(tree));
}

TEST_F(FindTests, WillFindRootOnConstTree)
{
    tree.insert({0, 1});
    [](auto const& tree)
    {
        EXPECT_EQ(tree.find({0, 1}), std::begin(tree));
    }(tree);
}

TEST_F(FindTests, WillFindInBiggerTree)
{
    tree.insert({16, 21});
    tree.insert({8, 9});
    tree.insert({25, 30});
    tree.insert({5, 8});
    tree.insert({15, 23});
    tree.insert({17, 19});
    tree.insert({26, 26});
    tree.insert({0, 3});
    tree.insert({6, 10});
    tree.insert({19, 20});
    auto iter = tree.find({15, 23});
    EXPECT_NE(iter, std::end(tree));
    EXPECT_EQ(iter->low(), 15);
    EXPECT_EQ(iter->high(), 23);
}

TEST_F(FindTests, WillFindAllInTreeWithDuplicates)
{
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    int findCount = 0;
    tree.find_all({5, 8}, [&findCount](decltype(tree)::iterator iter){
        ++findCount;
        EXPECT_EQ(*iter, (decltype(tree)::interval_type{5, 8}));
        return true;
    });
    EXPECT_EQ(findCount, tree.size());
}

TEST_F(FindTests, WillFindAllCanExitPreemptively)
{
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    tree.insert({5, 8});
    int findCount = 0;
    tree.find_all({5, 8}, [&findCount](decltype(tree)::iterator iter){
        ++findCount;
        EXPECT_EQ(*iter, (decltype(tree)::interval_type{5, 8}));
        return findCount < 3;
    });
    EXPECT_EQ(findCount, 3);
}

TEST_F(FindTests, CanFindAllElementsBack)
{
    constexpr int amount = 10'000;

    std::vector <decltype(tree)::interval_type> intervals;
    intervals.reserve(amount);
    for (int i = 0; i != amount; ++i)
    {
        const auto interval = lib_interval_tree::make_safe_interval<int, right_open>(distLarge(gen), distLarge(gen));
        intervals.emplace_back(interval);
        tree.insert(interval);
    }
    for (auto const& ival : intervals)
    {
        ASSERT_NE(tree.find(ival), std::end(tree));
    }
}

TEST_F(FindTests, CanFindAllElementsBackInStrictlyAscendingNonOverlappingIntervals)
{
    constexpr int amount = 10'000;

    std::vector <decltype(tree)::interval_type> intervals;
    intervals.reserve(amount);
    for (int i = 0; i != amount; ++i)
    {
        const auto interval = lib_interval_tree::make_safe_interval<int, right_open>(i * 2,  i * 2 + 1);
        intervals.emplace_back(interval);
        tree.insert(interval);
    }
    for (auto const& ival : intervals)
    {
        ASSERT_NE(tree.find(ival), std::end(tree));
    }
}

TEST_F(FindTests, CanFindAllElementsBackInStrictlyAscendingOverlappingIntervals)
{
    constexpr int amount = 10'000;

    std::vector <decltype(tree)::interval_type> intervals;
    intervals.reserve(amount);
    for (int i = 0; i != amount; ++i)
    {
        const auto interval = lib_interval_tree::make_safe_interval<int, right_open>(i - 1,  i + 1);
        intervals.emplace_back(interval);
        tree.insert(interval);
    }
    for (auto const& ival : intervals)
    {
        ASSERT_NE(tree.find(ival), std::end(tree));
    }
}

TEST_F(FindTests, CanFindAllOnConstTree)
{
    const auto targetInterval = lib_interval_tree::make_safe_interval<int, right_open>(16, 21);
    tree.insert(targetInterval);
    tree.insert({8, 9});
    tree.insert({25, 30});
    std::vector <decltype(tree)::interval_type> intervals;
    auto findWithConstTree = [&intervals, &targetInterval](auto const& tree)
    {
        tree.find_all(targetInterval, [&intervals](auto const& iter) {
            intervals.emplace_back(*iter);
            return true;
        });
    };
    findWithConstTree(tree);

    ASSERT_EQ(intervals.size(), 1);
    EXPECT_EQ(intervals[0], targetInterval);
}

TEST_F(FindTests, CanOverlapFindAllOnConstTree)
{
    const auto targetInterval = lib_interval_tree::make_safe_interval<int, right_open>(16, 21);
    tree.insert(targetInterval);
    tree.insert({8, 9});
    tree.insert({25, 30});
    std::vector <decltype(tree)::interval_type> intervals;
    auto findWithConstTree = [&intervals, &targetInterval](auto const& tree)
    {
        tree.overlap_find_all(targetInterval, [&intervals](auto const& iter) {
            intervals.emplace_back(*iter);
            return true;
        });
    };
    findWithConstTree(tree);

    ASSERT_EQ(intervals.size(), 1);
    EXPECT_EQ(intervals[0], targetInterval);
}
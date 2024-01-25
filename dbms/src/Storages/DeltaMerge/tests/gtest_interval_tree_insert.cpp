#include <Storages/DeltaMerge/SpilledZone/IntervalTree.h>
#include <Storages/DeltaMerge/SpilledZone/IntervalTypes.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <list>
#include <random>
#include <vector>

template <typename ContainedT>
struct IntervalTypes
{
    using value_type = ContainedT;
    using interval_type = lib_interval_tree::interval<ContainedT>;
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
        tree.insert(lib_interval_tree::make_safe_interval(distSmall(gen), distSmall(gen)));

    testTreeHeightHealth(tree);
}

TEST_F(IntervalTreeInsertTests, MaxValueTest1)
{
    constexpr int amount = 100'000;

    for (int i = 0; i != amount; ++i)
        tree.insert(lib_interval_tree::make_safe_interval(distSmall(gen), distSmall(gen)));

    testMaxProperty(tree);
}

TEST_F(IntervalTreeInsertTests, RBPropertyInsertTest)
{
    constexpr int amount = 1000;

    for (int i = 0; i != amount; ++i)
        tree.insert(lib_interval_tree::make_safe_interval(distSmall(gen), distSmall(gen)));

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
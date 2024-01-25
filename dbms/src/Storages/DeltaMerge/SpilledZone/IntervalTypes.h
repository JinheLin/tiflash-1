#pragma once

#include <algorithm>
#include <stdexcept>

namespace lib_interval_tree
{
// (]
struct left_open
{
    template <typename numerical_type>
    static inline bool within(numerical_type b, numerical_type e, numerical_type p)
    {
        return (b < p) && (p <= e);
    }
};
// [)
struct right_open
{
    template <typename numerical_type>
    static inline bool within(numerical_type b, numerical_type e, numerical_type p)
    {
        return (b <= p) && (p < e);
    }
};
// []
struct closed
{
    template <typename numerical_type>
    static inline bool within(numerical_type b, numerical_type e, numerical_type p)
    {
        return (b <= p) && (p <= e);
    }
};
// ()
struct open
{
    template <typename numerical_type>
    static inline bool within(numerical_type b, numerical_type e, numerical_type p)
    {
        return (b < p) && (p < e);
    }
};

template <typename numerical_type, typename interval_kind_ = closed>
struct interval
{
public:
    using value_type = numerical_type;
    using interval_kind = interval_kind_;

    /**
         *  Constructs an interval. low MUST be smaller than high.
         */
    constexpr interval(value_type low, value_type high)
        : low_{low}
        , high_{high}
    {
        if (low > high)
            throw std::invalid_argument("Low border is not lower or equal to high border.");
    }

    virtual ~interval() = default;

    /**
         *  Returns if both intervals equal.
         */
    friend bool operator==(interval const & lhs, interval const & other)
    {
        return lhs.low_ == other.low_ && lhs.high_ == other.high_;
    }

    /**
         *  Returns if both intervals are different.
         */
    friend bool operator!=(interval const & lhs, interval const & other)
    {
        return lhs.low_ != other.low_ || lhs.high_ != other.high_;
    }

    /**
         *  Returns the lower bound of the interval
         */
    value_type low() const { return low_; }

    /**
         *  Returns the upper bound of the interval
         */
    value_type high() const { return high_; }

    /**
         *  Returns whether the intervals overlap.
         *  For when both intervals are closed.
         */
    bool overlaps(value_type l, value_type h) const { return low_ <= h && l <= high_; }

    /**
         *  Returns whether the intervals overlap, excluding border.
         *  For when at least one interval is open (l&r).
         */
    bool overlaps_exclusive(value_type l, value_type h) const { return low_ < h && l < high_; }

    /**
         *  Returns whether the intervals overlap
         */
    bool overlaps(interval const & other) const { return overlaps(other.low_, other.high_); }

    /**
         *  Returns whether the intervals overlap, excluding border.
         */
    bool overlaps_exclusive(interval const & other) const { return overlaps_exclusive(other.low_, other.high_); }

    /**
         *  Returns whether the given value is in this.
         */
    bool within(value_type value) const { return interval_kind::within(low_, high_, value); }

    /**
         *  Returns whether the given interval is in this.
         */
    bool within(interval const & other) const { return low_ <= other.low_ && high_ >= other.high_; }

    /**
         *  Calculates the distance between the two intervals.
         *  Overlapping intervals have 0 distance.
         */
    value_type operator-(interval const & other) const
    {
        if (overlaps(other))
            return 0;
        if (high_ < other.low_)
            return other.low_ - high_;
        else
            return low_ - other.high_;
    }

    /**
         *  Returns the size of the interval.
         */
    value_type size() const { return high_ - low_; }

    /**
         *  Creates a new interval from this and other, that contains both intervals and whatever
         *  is between.
         */
    interval join(interval const & other) const { return {std::min(low_, other.low_), std::max(high_, other.high_)}; }

protected:
    value_type low_;
    value_type high_;
};

/**
     *  Creates a safe interval that puts the lower bound left automatically.
     */
template <typename numerical_type, typename interval_kind_ = closed>
constexpr interval<numerical_type, interval_kind_> make_safe_interval(numerical_type lhs, numerical_type rhs)
{
    return interval<numerical_type, interval_kind_>{std::min(lhs, rhs), std::max(lhs, rhs)};
}

} // namespace lib_interval_tree
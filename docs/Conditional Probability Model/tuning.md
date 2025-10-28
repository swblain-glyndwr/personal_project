# Conditional Probability Recommender Tuning Parameters

### Code summary

The `task_conditional_probability_recs.py` script contains several weights, filters and parameters that change the behaviour of the final scoring metrics. This document is meant to capture these levers which can be tweaked to change the behaviour of the recommender.



```python
# Current defaults parameters
REFERENCE_DATE = F.current_date() 
BASKETS_LOOKBACK_DAYS = jobparser.get_arg('--baskets_lookback_days') or 365
VIEWS_LOOKBACK_DAYS = jobparser.get_arg('--views_lookback_days') or 28
PURCHASE_WEIGHT = jobparser.get_arg('--purchase_weight') or 10
CART_WEIGHT = jobparser.get_arg('--cart_weight') or 2
VIEW_WEIGHT = jobparser.get_arg('--view_weight') or 1
TIME_DECAY_FACTOR = jobparser.get_arg('--time_decay_factor') or -0.1
```


## 1. Interaction Time Windows (Interaction Lookback Periods)

### A. Purchase Lookback
Location: [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) TABLE 4
```python
"baskets": {"lookback": BASKETS_LOOKBACK_DAYS}  # Currently 1 year
```

**Impact:**
- **Increase (e.g., → 730 days):**
  - More historical data = stronger sequential patterns
  - Captures seasonal purchase cycles (e.g., "bought winter coat → bought boots 11 months later")
  - Older patterns may not reflect current trends
  - Larger computation (more data to process)

- **Decrease (e.g., → 180 days):**
  - More recent patterns (trend-focused)
  - Faster computation
  - Miss long-term sequential patterns (furniture → decor after 6 months)
  - Less data = weaker statistical signals for niche themes

### B. Views Lookback
**Location:** [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) TABLE 2
```python
"views": {"lookback": VIEWS_LOOKBACK_DAYS}  # Currently 28 days
```

**Impact:**
- **Increase (e.g., → 180 days):**
  - Captures browsing intent over longer window ("viewed in summer, bought in winter")
  - Views from 6 months ago less relevant (stale intent)
  - Massive data volume (views >> purchases)
  - Old views pollute recent intent signals

- **Decrease (e.g., → 7 days):**
  - Only recent high-intent views (conversion-focused)
  - Smaller data footprint
  - Miss consideration phase (research → buy later)


## 2. Iteractions Type Weights

Location: [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) line 68 TABLE 2
```python
CASE
  WHEN interaction_type = 'purchase' THEN PURCHASE_WEIGHT
  ELSE VIEW_WEIGHT
END
```

**Current Ratio:** `Purchase : Cart : View = 10 : 2 : 1`

**Impact:**
- **Increase purchase weight (e.g., → 20):**
  - Recommendations heavily favor proven purchase patterns
  - Ignores browsing signals (miss discovery opportunities)
  - Biased toward existing customers (fewer recs for browsers)

- **Increase view weight (e.g., → 5):**
  - Captures interest signals earlier (better for prospecting)
  - More diverse recommendations
  - Noisy signals (casual browsing vs intent)
  - Views can overwhelm purchases (see your math: view needs 23+ days advantage to beat purchase)


## 3. Time Decay On Weights

**Location:** [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) TABLE 2
```python
exp(TIME_DECAY_FACTOR * days_ago)  # Decay factor = 0.1
```

**Current Behavior:**
- **10 days ago:** 37% weight remaining
- **23 days ago:** 10% (crossover point where view > old purchase)
- **46 days ago:** 1%

**Impact:**
- **Increase decay (e.g., → 0.2):**
  - Stronger recency bias (only last 2 weeks matter)
  - Miss seasonal patterns
  - Unfair to infrequent shoppers

- **Decrease decay (e.g., → 0.05):**
  - Long-term affinities valued more
  - Smoother weight decay
  - Old interactions dilute recent intent
  - 23-day crossover becomes 46 days


## 4. Theme Associations Filters

### A. Minimum Frequency Threshold
**Location:** [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) line 209
```python
WHERE freq12 > 100  # Strong patterns only
   OR (freq12 > 5 AND confidence > 0.01)  # Weak but high-confidence
```

**Impact:**
- **Increase (e.g., → 500):**
  - Only proven, high-volume patterns
  - Reduces noise
  - Miss niche/emerging themes (e.g., "sustainable fashion")
  - Fewer recommendations overall

- **Decrease (e.g., → 10):**
  - More recommendations (especially for niche themes)
  - Discover new patterns
  - Noisy associations (spurious correlations)
  - Computational overhead (more pairs to evaluate)

### B. Top-N Associations per Theme
**Location:** [`task_conditional_probability_recs.py`](task_conditional_probability_recs.py) line 215
```python
QUALIFY ROW_NUMBER() ... <= 20  # Top 20 per seed theme
```

**Impact:**
- **Increase (e.g., → 50):**
  - More diverse recommendations
  - Lower-quality patterns included
  - 2.5× more data in TABLE 4

- **Decrease (e.g., → 10):**
  - Only strongest patterns
  - Smaller table
  - Less diversity (same recs for everyone)
  - Miss secondary preferences

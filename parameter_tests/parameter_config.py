# Score thresholds
SCORE_THRESHOLDS = [-1, -0.5, 0, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35]

# Equal investment options (True = same dollar amount per stock, False = same number of shares)
EQUAL_INVESTMENT_OPTIONS = [True, False]

# Technical/Sentiment weight combinations
WEIGHT_COMBINATIONS = [
    (0.0, 1.0),
    (0.2, 0.8),
    (0.4, 0.6),
    (0.6, 0.4),
    (0.7, 0.3),
    (0.75, 0.25),
    (0.8, 0.2),
    (0.85, 0.15),
    (0.9, 0.1),
    (0.95, 0.05),
    (1.0, 0.0)
]

# Market cap combinations (min_market_cap, max_market_cap) in dollars
MARKET_CAP_COMBINATIONS = [
    (1, 100_000_000_000_000),              # all companies
    (1_000_000, 100_000_000_000_000),      # $1M to $100T
    (10_000_000, 100_000_000_000_000),     # $10M to $100T
    (10_000_000, 1_000_000_000),           # $10M to $1B (small-mid cap)
    (50_000_000, 10_000_000_000),          # $50M to $10B
    (100_000_000, 1_000_000_000_000),      # $100M to $1T (mid-large cap)
    (1_000_000_000, 100_000_000_000_000),  # $1B to $100T (large cap only)
]

# Fixed parameters during testing (modify as needed)
FIXED_PARAMETERS = {
    'INVESTMENT_PER_STOCK': '100',
    'DATA_FETCH_PADDING_DAYS': '30',
    'RSI_PERIOD': '18',
    'VOLUME_TREND_PERIOD': '9', 
    'VOLUME_AVERAGE_PERIOD': '26',
    'VOLUME_ACCUMULATION_DISTRIBUTION_PERIOD': '26',
    'LOG_LEVEL': 'WARNING',
    'SCRAPING_DELAY': '1',
    'MAX_NEWS_0_1_DAYS': '12',
    'MAX_NEWS_2_4_DAYS': '6',
    'MAX_NEWS_5_7_DAYS': '6'
}

# CSV output configuration
CSV_COLUMNS = [
    'equal_investment', 'min_market_cap', 'max_market_cap', 
    'min_score_threshold', 'technical_weight', 'sentiment_weight',
    'total_operations', 'roi', 'avg_profit_per_op', 'win_rate'
]
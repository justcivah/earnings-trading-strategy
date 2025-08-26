import pandas as pd
import itertools
from parameter_tests.parameter_config import *

jan = pd.read_csv("./simulation_results/simulation_results_january_2025.csv")
mar = pd.read_csv("./simulation_results/simulation_results_march_2025.csv")
apr = pd.read_csv("./simulation_results/simulation_results_april_2025.csv")
jun = pd.read_csv("./simulation_results/simulation_results_june_2025.csv")

# Generate all combinations from config
all_combinations = list(
    itertools.product(
        SCORE_THRESHOLDS,
        EQUAL_INVESTMENT_OPTIONS,
        WEIGHT_COMBINATIONS,
        MARKET_CAP_COMBINATIONS,
    )
)

final = []

for combination in all_combinations:
    min_score_threshold, equal_investment, weight_comb, market_cap_comb = combination
    technical_weight, sentiment_weight = weight_comb
    min_market_cap, max_market_cap = market_cap_comb

    jan_row = jan[
        (jan["equal_investment"] == equal_investment)
        & (jan["min_market_cap"] == min_market_cap)
        & (jan["max_market_cap"] == max_market_cap)
        & (jan["min_score_threshold"] == min_score_threshold)
        & (jan["technical_weight"] == technical_weight)
        & (jan["sentiment_weight"] == sentiment_weight)
    ].iloc[0]

    mar_row = mar[
        (jan["equal_investment"] == equal_investment)
        & (jan["min_market_cap"] == min_market_cap)
        & (jan["max_market_cap"] == max_market_cap)
        & (jan["min_score_threshold"] == min_score_threshold)
        & (jan["technical_weight"] == technical_weight)
        & (jan["sentiment_weight"] == sentiment_weight)
    ].iloc[0]
    
    apr_row = apr[
        (jan["equal_investment"] == equal_investment)
        & (jan["min_market_cap"] == min_market_cap)
        & (jan["max_market_cap"] == max_market_cap)
        & (jan["min_score_threshold"] == min_score_threshold)
        & (jan["technical_weight"] == technical_weight)
        & (jan["sentiment_weight"] == sentiment_weight)
    ].iloc[0]
    
    jun_row = jun[
        (jan["equal_investment"] == equal_investment)
        & (jan["min_market_cap"] == min_market_cap)
        & (jan["max_market_cap"] == max_market_cap)
        & (jan["min_score_threshold"] == min_score_threshold)
        & (jan["technical_weight"] == technical_weight)
        & (jan["sentiment_weight"] == sentiment_weight)
    ].iloc[0]

    total_ops = jan_row.total_operations + mar_row.total_operations + apr_row.total_operations + jun_row.total_operations
    avg_profit = (jan_row.avg_profit_per_op*jan_row.total_operations + mar_row.avg_profit_per_op*mar_row.total_operations + apr_row.avg_profit_per_op*apr_row.total_operations + jun_row.avg_profit_per_op*jun_row.total_operations) / total_ops
    roi = (jan_row.roi*jan_row.total_operations + mar_row.roi*mar_row.total_operations + apr_row.roi*apr_row.total_operations + jun_row.roi*jun_row.total_operations) / total_ops
    win_rate = (jan_row.win_rate*jan_row.total_operations + mar_row.win_rate*mar_row.total_operations + apr_row.win_rate*apr_row.total_operations + jun_row.win_rate*jun_row.total_operations) / total_ops

    final.append({
        "score_threshold": min_score_threshold,
        "equal_investment": equal_investment,
        "technical_weight": technical_weight,
        "sentiment_weight": sentiment_weight,
        "min_market_cap": min_market_cap,
        "max_market_cap": max_market_cap,
        "total_operations": total_ops,
        "roi": roi,
        "avg_profit_per_op": avg_profit,
        "win_rate": win_rate
    })

final_df = pd.DataFrame(final)
final_df.to_csv("aggregated_results.csv", index=False)


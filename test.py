import yfinance as yf
import pandas as pd

# Define the stock ticker and date range
ticker_symbol = "AAPL"  # Example: Apple Inc.
start_date = "2025-07-01"
end_date = "2025-07-10"

# Download historical data
data = yf.download(
    ticker_symbol,
    start=start_date,
    end=end_date,
    interval="1d"  # Daily data
)



# Print the result
print(data.to_dict(orient="records"))
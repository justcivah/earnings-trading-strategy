import os
from datetime import datetime, timedelta
from utils.logging_utils import get_logger
from database.repositories import StockPriceRepository

class RSI:
    def __init__(self):
        self.logger = get_logger(__name__)
          
    def compute_rsi(self, symbol, date):
        """Calculate RSI for a given symbol and date"""

        period = int(os.getenv("RSI_PERIOD"))

        # Get historical prices
        start_date = date - timedelta(period)
        end_date = date
        
        prices = StockPriceRepository.get_prices_for_symbol(symbol, start_date, end_date)
        
        self.logger.debug(f"RSI - Period: {period}, Fetched {len(prices)} records")

        if len(prices) * 1.55 < period:
            self.logger.warning(f"Insufficient data for RSI calculation for {symbol}")
            return 0.0
        
        prices.sort(key=lambda x: x["date"])
        
        # Calculate price changes
        price_changes = []
        for i in range(1, len(prices)):
            change = prices[i]["close"] - prices[i-1]["close"]
            price_changes.append(change)
        
        if len(price_changes) > period:
            price_changes = price_changes[-period:]
        
        # Separate gains and losses
        gains = [change if change > 0 else 0 for change in price_changes]
        losses = [-change if change < 0 else 0 for change in price_changes]
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        # Calculate RSI
        if avg_loss == 0:
            return 1.0
        
        rs = avg_gain / avg_loss

        rsi = 100 - (100 / (1 + rs))
        # RSI: +1: oversold, -1 overbought. Normalized, original range [0, 100]
        normalized_rsi = (rsi - 50) / 50
        return round(normalized_rsi, 4)
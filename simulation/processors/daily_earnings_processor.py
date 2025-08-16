import os
from datetime import datetime, timedelta
from utils.logging_utils import get_logger
from database.repositories import EarningsRepository
from database.repositories import CompanyRepository
from database.repositories import StockPriceRepository

class DailyEarningsProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)

    def compute(self, day):
        self.logger.info(f"Starting processing earnings for {day}...")

        min_market_cap = int(os.getenv("MIN_MARKET_CAP"))
        max_market_cap = int(os.getenv("MAX_MARKET_CAP"))

        all_earnings = EarningsRepository.get_earnings_for_date(day)
        filtered_earnings = [] 
        operations = {}

        for earnings in all_earnings:
            company = CompanyRepository.get_company(earnings["symbol"])

            # Filtering companies by market cap
            if company["market_cap"] is not None and min_market_cap <= company["market_cap"] <= max_market_cap:
                filtered_earnings.append(earnings)
                
        if len(all_earnings) == 0 or len(filtered_earnings) == 0:
            self.logger.info("No valid earnings for that day")
            
        self.logger.info("Earnings succesfully processed")

        return filtered_earnings
    
    def calculate_rsi(self, symbol, current_date, period):
        """Calculate RSI for a given symbol and date"""

        # Get historical prices
        start_date = current_date - timedelta(period)
        end_date = current_date
        
        prices = StockPriceRepository.get_prices_for_symbol(symbol, start_date, end_date)
        
        if len(prices) < period + 1:
            self.logger.warning(f"Insufficient data for RSI calculation for {symbol}")
            # Return neural score
            return 50.0
        
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
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)
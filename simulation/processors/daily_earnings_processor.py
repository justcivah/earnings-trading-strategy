import os
from datetime import datetime, timedelta
from utils.logging_utils import get_logger
from database.repositories import EarningsRepository
from database.repositories import CompanyRepository
from database.repositories import StockPriceRepository
from simulation.indexes.rsi import RSI
from simulation.indexes.volume import Volume
from simulation.indexes.sentiment import Sentiment

class DailyEarningsProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)
        
        self.rsi = RSI()
        self.volume = Volume()
        self.sentiment = Sentiment()

    def compute(self, date):
        self.logger.info(f"Starting processing earnings for {date}...")

        min_market_cap = int(os.getenv("MIN_MARKET_CAP"))
        max_market_cap = int(os.getenv("MAX_MARKET_CAP"))

        all_earnings = EarningsRepository.get_earnings_for_date(date)
        filtered_earnings = []
        processed_earnings = []
        operations = []

        for earnings in all_earnings:
            company = CompanyRepository.get_company(earnings["symbol"])

            # Filter companies
            if company["market_cap"] is not None and min_market_cap <= company["market_cap"] <= max_market_cap:
                filtered_earnings.append(earnings)
                
        if len(all_earnings) == 0 or len(filtered_earnings) == 0:
            self.logger.warning("No valid earnings for that date")
            return []

        # Computing company score
        for earnings in filtered_earnings:
            technical_weight = float(os.getenv("TECHNICAL_WEIGHT"))
            sentiment_weight = float(os.getenv("SENTIMENT_WEIGHT"))

            company = CompanyRepository.get_company(earnings["symbol"])
            self.logger.info(f"Processing data for company {company['name']} ({earnings['symbol']})")
        
            rsi = self.rsi.compute_rsi(earnings["symbol"], date)
            volume_trend = self.volume.compute_volume_trend(earnings["symbol"], date)
            volume_average = -self.volume.compute_volume_average(earnings["symbol"], date)
            volume_ac = -self.volume.compute_volume_accumulation_distribution(earnings["symbol"], date)
            sentiment = self.sentiment.compute(earnings["symbol"], date)
            technical = (rsi + volume_trend + volume_average + volume_ac) / 4

            final_score = technical * technical_weight + sentiment * sentiment_weight 

            processed_earnings.append({
                "symbol": earnings["symbol"],
                "rsi": rsi,
                "volume_trend": volume_trend,
                "volume_average": volume_average,
                "volume_ac": volume_ac,
                "sentiment": sentiment,
                "technical": technical,
                "final_score": final_score
            })

        self.logger.debug("Completed earnings processing")

        min_score_threshold = float(os.getenv("MIN_SCORE_THRESHOLD"))

        # Considering only stocks with final score greater than MIN_SCORE_THRESHOLD
        for earnings in processed_earnings:
            if (earnings["final_score"] >= min_score_threshold):
                buy_data = self.__get_stock_data_before_date(earnings["symbol"], date)
                sell_data = self.__get_stock_data_after_date(earnings["symbol"], date)

                if buy_data is None or sell_data is None:
                    self.logger.warning(f"Couldn't get stock data for {earnings['symbol']} on {date}")

                    earnings["profit_loss"] = 0
                    earnings["buy_price"] = 0
                    earnings["sell_price"] = 0
                    continue

                # Decide whether to invest the same amount in all companies, or buy one stock per company
                if os.getenv("EQUAL_INVESTMENT").lower() in ["true"]:
                    investment_amount = int(os.getenv("INVESTMENT_PER_STOCK"))

                    earnings["profit_loss"] = (investment_amount / buy_data["open"]) * (sell_data["close"] - buy_data["open"])
                    earnings["buy_price"] = investment_amount
                    earnings["sell_price"] = investment_amount + earnings["profit_loss"]

                else:
                    earnings["profit_loss"] = sell_data["close"] - buy_data["open"]
                    earnings["buy_price"] = buy_data["open"]
                    earnings["sell_price"] = sell_data["close"]
                
                company = CompanyRepository.get_company(earnings["symbol"])
                self.logger.info(f"Company {company['name']} ({earnings['symbol']})")

                self.logger.debug("Scores:")
                self.logger.debug(f"\tRSI: {earnings['rsi']}")
                self.logger.debug(f"\tVolume trend: {earnings['volume_trend']}")
                self.logger.debug(f"\tVolume average: {earnings['volume_average']}")
                self.logger.debug(f"\tVolume accumulation/distribution: {earnings['volume_ac']}")
                self.logger.debug(f"\tSentiment: {earnings['sentiment']}")
                self.logger.debug(f"\tTechnical indexes average: {earnings['technical']}")
                self.logger.debug(f"\tFinal score: {earnings['final_score']}")

                self.logger.debug("Prices:")
                self.logger.debug(f"\tBuy price: {earnings['buy_price']}")
                self.logger.debug(f"\tSell price: {earnings['sell_price']}")
                self.logger.debug(f"\tProfit/Loss: {earnings['profit_loss']}")

                operations.append(earnings)
            
        self.logger.info("All earnings succesfully processed")

        return operations
    
    def __get_stock_data_before_date(self, symbol, date):
        """Get stock data for the first valid day before (including) the given date"""

        padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))
        
        for i in range(1, padding_days + 1):
            search_date = date - timedelta(days=i)
            stock_data = StockPriceRepository.get_price_on_date(symbol, search_date)
            if stock_data:
                return stock_data
        
        return None
    
    def __get_stock_data_after_date(self, symbol, date):
        """Get stock data for the first valid day after (including) the given date"""

        padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))
        
        for i in range(1, padding_days + 1):
            search_date = date + timedelta(days=i)
            stock_data = StockPriceRepository.get_price_on_date(symbol, search_date)
            if stock_data:
                return stock_data
        
        return None
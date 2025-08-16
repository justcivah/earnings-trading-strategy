import os
import datetime
from dotenv import load_dotenv
from utils.logging_utils import get_logger

load_dotenv()

class ConfigDataValidator:
    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_config(self):
        """Check whether config data is correct"""

        self.logger.debug("Starting configuration data validation...")

        # Date validation
        start_date = datetime.datetime.strptime(os.getenv("START_DATE"), "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(os.getenv("END_DATE"), "%Y-%m-%d").date()
        if start_date > end_date:
            self.logger.critical(f"Invalid dates: START_DATE={start_date}, END_DATE={end_date}")
            raise ValueError("Start date must be before the end date")
        self.logger.debug("START_DATE and END_DATE validated")

        # Initial capital validation
        initial_capital = int(os.getenv("INITIAL_CAPITAL"))
        if initial_capital <= 0:
            self.logger.critical(f"Invalid INITIAL_CAPITAL: {initial_capital}")
            raise ValueError("Initial capital must be greater than zero")
        self.logger.debug("INITIAL_CAPITAL validated")

        # Minimum score threshold validation
        min_score = float(os.getenv("MIN_SCORE_THRESHOLD"))
        if min_score < 0:
            self.logger.critical(f"Invalid MIN_SCORE_THRESHOLD: {min_score}")
            raise ValueError("Minimum score threshold must be greater than or equal to zero")
        self.logger.debug("MIN_SCORE_THRESHOLD validated")

        # Max positions validation
        max_positions = int(os.getenv("MAX_POSITIONS"))
        if max_positions < 0:
            self.logger.critical(f"Invalid MAX_POSITIONS: {max_positions}")
            raise ValueError("Maximum positions must be greater than or equal to zero")
        self.logger.debug("MAX_POSITIONS validated")

        # Scraping delay validation
        scraping_delay = int(os.getenv("SCRAPING_DELAY"))
        if scraping_delay <= 0:
            self.logger.critical(f"Invalid SCRAPING_DELAY: {scraping_delay}")
            raise ValueError("Scraping delay must be greater than zero")
        self.logger.debug("SCRAPING_DELAY validated")
        
		# Scraping delay validation
        data_fetch_padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))
        if data_fetch_padding_days < 0:
            self.logger.critical(f"Invalid DATA_FETCH_PADDING_DAYS: {data_fetch_padding_days}")
            raise ValueError("Data fetch padding days must be greater than or equal to zero")
        self.logger.debug("DATA_FETCH_PADDING_DAYS validated")

        # Weights validation
        sentiment_weight = float(os.getenv("SENTIMENT_WEIGHT"))
        technical_weight = float(os.getenv("TECHNICAL_WEIGHT"))
        total_weight = sentiment_weight + technical_weight
        if not 0.999 <= total_weight <= 1.001:
            self.logger.critical(f"Invalid weights: SENTIMENT_WEIGHT={sentiment_weight}, TECHNICAL_WEIGHT={technical_weight}")
            raise ValueError("Weights must sum up to 1")
        self.logger.debug("SENTIMENT_WEIGHT and TECHNICAL_WEIGHT validated")

        # RSI period validation
        rsi_period = int(os.getenv("RSI_PERIOD"))
        if rsi_period <= 0:
            self.logger.critical(f"Invalid RSI_PERIOD: {rsi_period}")
            raise ValueError("RSI period must be greater than zero")
        self.logger.debug("RSI_PERIOD validated")

        # Market cap validation
        min_cap = int(os.getenv("MIN_MARKET_CAP"))
        max_cap = int(os.getenv("MAX_MARKET_CAP"))
        if min_cap > max_cap:
            self.logger.critical(f"Invalid market caps: MIN_MARKET_CAP={min_cap}, MAX_MARKET_CAP={max_cap}")
            raise ValueError("Minimum market cap must be less than the maximum market cap")
        self.logger.debug("MIN_MARKET_CAP and MAX_MARKET_CAP validated")

        # Log level validation
        log_level = os.getenv("LOG_LEVEL")
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            self.logger.critical(f"Invalid LOG_LEVEL: {log_level}")
            raise ValueError("Log level is not a valid value")
        self.logger.debug("LOG_LEVEL validated")

        self.logger.info("Configuration data successfully validated")
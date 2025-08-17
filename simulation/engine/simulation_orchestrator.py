import os
import pandas as pd
from simulation.processors.daily_earnings_processor import DailyEarningsProcessor
from utils.logging_utils import get_logger

class SimulationOrchestrator:
    def __init__(self):
        self.logger = get_logger(__name__)
        
        self.daily_earnings_processor = DailyEarningsProcessor()

    def run_full_simulation(self):
        """Simulates the whole system"""

        self.logger.info("=== SIMULATION STARTED ===")
        
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        
        results = {}

        for date in pd.date_range(start=start_date, end=end_date).date:
            results[date] = self.daily_earnings_processor.compute(date)
            
        print(results)
        self.logger.info("=== SIMULATION COMPLETED ===")
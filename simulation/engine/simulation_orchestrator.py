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
        
        all_operations = []
        daily_investments = []
        
        for date in pd.date_range(start=start_date, end=end_date).date:
            daily_operations = self.daily_earnings_processor.compute(date)
            all_operations.extend(daily_operations)
            
            # Calculate daily investment amount
            daily_investment = sum(op.get('buy_price', 0) for op in daily_operations if op.get('buy_price') is not None)
            daily_investments.append(daily_investment)
        
        # Calculate and display overall statistics
        self._display_simulation_statistics(all_operations, daily_investments, start_date, end_date)
        
        self.logger.info("=== SIMULATION COMPLETED ===")

    def _display_simulation_statistics(self, all_operations, daily_investments, start_date, end_date):
        """Calculate and display overall simulation statistics"""
        
        if not all_operations:
            self.logger.warning("No operations found in the simulation period")
            return
        
        # Basic statistics
        total_operations = len(all_operations)
        total_profit_loss = sum(op.get('profit_loss', 0) for op in all_operations if op.get('profit_loss') is not None)
        
        # Investment statistics
        total_invested = sum(op.get('buy_price', 0) for op in all_operations if op.get('buy_price') is not None)
        avg_daily_investment = sum(daily_investments) / len(daily_investments) if daily_investments else 0
        trading_days = len([inv for inv in daily_investments if inv > 0])
        avg_investment_on_trading_days = sum([inv for inv in daily_investments if inv > 0]) / trading_days if trading_days > 0 else 0
        
        # Profitable vs losing operations
        profitable_ops = [op for op in all_operations if op.get('profit_loss', 0) > 0]
        losing_ops = [op for op in all_operations if op.get('profit_loss', 0) < 0]
        break_even_ops = [op for op in all_operations if op.get('profit_loss', 0) == 0]
        
        win_rate = (len(profitable_ops) / total_operations) * 100 if total_operations > 0 else 0
        avg_profit_per_operation = total_profit_loss / total_operations if total_operations > 0 else 0
        
        # Return on investment
        roi_percentage = (total_profit_loss / total_invested) * 100 if total_invested > 0 else 0
        
        # Additional statistics
        avg_profitable_gain = sum(op['profit_loss'] for op in profitable_ops) / len(profitable_ops) if profitable_ops else 0
        avg_losing_loss = sum(op['profit_loss'] for op in losing_ops) / len(losing_ops) if losing_ops else 0
        
        # Score statistics
        avg_final_score = sum(op.get('final_score', 0) for op in all_operations) / total_operations if total_operations > 0 else 0
        
        # Display results
        self.logger.info("=== SIMULATION STATISTICS ===")
        self.logger.info(f"Simulation Period: {start_date} to {end_date}")
        self.logger.info(f"Total Operations: {total_operations}")
        self.logger.info(f"Total Amount Invested: ${total_invested:.2f}")
        self.logger.info(f"Average Daily Investment: ${avg_daily_investment:.2f}")
        self.logger.info(f"Average Investment on Trading Days: ${avg_investment_on_trading_days:.2f}")
        self.logger.info(f"Trading Days (with operations): {trading_days}")
        self.logger.info(f"Total Profit/Loss: ${total_profit_loss:.2f}")
        self.logger.info(f"ROI: {roi_percentage:.2f}%")
        self.logger.info(f"Average Profit per Operation: ${avg_profit_per_operation:.2f}")
        self.logger.info(f"Win Rate: {win_rate:.2f}%")
        self.logger.info(f"Profitable Operations: {len(profitable_ops)} (avg: ${avg_profitable_gain:.2f})")
        self.logger.info(f"Losing Operations: {len(losing_ops)} (avg: ${avg_losing_loss:.2f})")
        self.logger.info(f"Break-even Operations: {len(break_even_ops)}")
        self.logger.info(f"Average Final Score: {avg_final_score:.4f}")
        
        # Console output for easy reading
        print("\n" + "="*60)
        print("SIMULATION RESULTS SUMMARY")
        print("="*60)
        print(f"Period: {start_date} to {end_date}")
        print(f"Total Operations: {total_operations}")
        print(f"Total Amount Invested: ${total_invested:.2f}")
        print(f"Average Daily Investment: ${avg_daily_investment:.2f}")
        print(f"Average Investment on Trading Days: ${avg_investment_on_trading_days:.2f}")
        print(f"Trading Days: {trading_days}")
        print(f"Total Profit/Loss: ${total_profit_loss:.2f}")
        print(f"ROI: {roi_percentage:.2f}%")
        print(f"Average Profit per Operation: ${avg_profit_per_operation:.2f}")
        print(f"Win Rate: {win_rate:.2f}%")
        print(f"Profitable: {len(profitable_ops)} | Losing: {len(losing_ops)} | Break-even: {len(break_even_ops)}")
        print(f"Avg Profitable Gain: ${avg_profitable_gain:.2f}")
        print(f"Avg Losing Loss: ${avg_losing_loss:.2f}")
        print(f"Average Final Score: {avg_final_score:.4f}")
        print("="*60 + "\n")
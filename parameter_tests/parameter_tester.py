import os
import csv
import itertools
from datetime import datetime
import pandas as pd
from simulation.engine.simulation_orchestrator import SimulationOrchestrator
from utils.logging_utils import setup_logging, get_logger
from parameter_tests.parameter_config import *

class ParameterTester:
    def __init__(self):
        setup_logging()
        self.logger = get_logger(__name__)
        self.results = []
        
    def run_single_simulation(self, params):
        """Run a single simulation with given parameters"""
        try:
            self.set_environment_variables(params)
            
            # Create a modified orchestrator that returns results
            orchestrator = ModifiedSimulationOrchestrator()
            results = orchestrator.run_full_simulation_with_results()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in simulation with params {params}: {e}")
            return None
    
    def set_environment_variables(self, params):
        """Set environment variables for the current parameter combination"""
        
        # Set the test parameters
        os.environ['MIN_SCORE_THRESHOLD'] = str(params['min_score_threshold'])
        os.environ['EQUAL_INVESTMENT'] = str(params['equal_investment'])
        os.environ['TECHNICAL_WEIGHT'] = str(params['technical_weight'])
        os.environ['SENTIMENT_WEIGHT'] = str(params['sentiment_weight'])
        os.environ['MIN_MARKET_CAP'] = str(params['min_market_cap'])
        os.environ['MAX_MARKET_CAP'] = str(params['max_market_cap'])
        
        # Set fixed parameters
        for key, value in FIXED_PARAMETERS.items():
            os.environ[key] = value
    
    def run_parameter_sweep(self):
        """Run simulations for all parameter combinations"""
        
        # Generate all combinations from config
        all_combinations = list(itertools.product(
            SCORE_THRESHOLDS,
            EQUAL_INVESTMENT_OPTIONS,
            WEIGHT_COMBINATIONS,
            MARKET_CAP_COMBINATIONS
        ))
        
        total_combinations = len(all_combinations)
        self.logger.info(f"Starting parameter sweep with {total_combinations} combinations")
        print(f"Starting parameter sweep with {total_combinations} combinations")
        
        successful_runs = 0
        failed_runs = 0
        
        for i, (threshold, equal_inv, weights, market_caps) in enumerate(all_combinations):
            tech_weight, sent_weight = weights
            min_cap, max_cap = market_caps
            
            params = {
                'min_score_threshold': threshold,
                'equal_investment': equal_inv,
                'technical_weight': tech_weight,
                'sentiment_weight': sent_weight,
                'min_market_cap': min_cap,
                'max_market_cap': max_cap
            }
            
            print(f"\nRunning simulation {i+1}/{total_combinations}")
            print(f"Parameters: Threshold={threshold}, Equal_Inv={equal_inv}, "f"Tech/Sent={tech_weight}/{sent_weight}, MarketCap={min_cap/1e6:.0f}M-{max_cap/1e12:.0f}T")
            
            results = self.run_single_simulation(params)
            
            if results:
                results.update(params)
                self.results.append(results)
                successful_runs += 1
                
                print(f"ROI: {results.get('roi', 0):.2f}%, Win Rate: {results.get('win_rate', 0):.1f}%, Ops: {results.get('total_operations', 0)}")
            
            else:
                failed_runs += 1
                print(f"Simulation failed")
        
        print(f"\nParameter sweep completed!")
        print(f"Successful runs: {successful_runs}")
        print(f"Failed runs: {failed_runs}")
    
    def save_results_to_csv(self, filename=None):
        """Save all results to CSV file"""
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"simulation_results_{timestamp}.csv"
        
        if not self.results:
            self.logger.warning("No results to save")
            return
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            
            for result in self.results:
                # Convert boolean to 0/1 for equal_investment
                row = result.copy()
                row['equal_investment'] = 1 if result['equal_investment'] else 0
                
                # Ensure all required columns are present
                filtered_row = {col: row.get(col, '') for col in CSV_COLUMNS}
                writer.writerow(filtered_row)
        
        self.logger.info(f"Results saved to {filename}")
        print(f"Results saved to {filename}")
        
        return filename

class ModifiedSimulationOrchestrator(SimulationOrchestrator):
    """Modified orchestrator that returns results instead of just logging"""
    
    def run_full_simulation_with_results(self):
        """Run simulation and return results dictionary"""
        
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
        
        # Calculate statistics and return as dictionary
        return self._calculate_simulation_statistics(all_operations, daily_investments)
    
    def _calculate_simulation_statistics(self, all_operations, daily_investments):
        """Calculate statistics and return as dictionary"""
        
        if not all_operations:
            return {
                'total_operations': 0,
                'roi': 0,
                'avg_profit_per_op': 0,
                'win_rate': 0
            }
        
        # Basic statistics
        total_operations = len(all_operations)
        total_profit_loss = sum(op.get('profit_loss', 0) for op in all_operations if op.get('profit_loss') is not None)
        
        # Investment statistics  
        total_invested = sum(op.get('buy_price', 0) for op in all_operations if op.get('buy_price') is not None)
        
        # Profitable vs losing operations
        profitable_ops = [op for op in all_operations if op.get('profit_loss', 0) > 0]
        
        # Calculate key metrics
        win_rate = (len(profitable_ops) / total_operations) * 100 if total_operations > 0 else 0
        avg_profit_per_operation = total_profit_loss / total_operations if total_operations > 0 else 0
        roi_percentage = (total_profit_loss / total_invested) * 100 if total_invested > 0 else 0
        
        return {
            'total_operations': total_operations,
            'roi': round(roi_percentage, 2),
            'avg_profit_per_op': round(avg_profit_per_operation, 2),
            'win_rate': round(win_rate, 2)
        }

def run_full_test():
    """Run the full parameter sweep"""
    
    print("Running full parameter sweep...")
    
    tester = ParameterTester()
    tester.run_parameter_sweep()
    filename = tester.save_results_to_csv()
    
    return filename

def main():
    """Main function to run parameter testing"""
    
    print("Automated Parameter Testing for Trading Simulation")
    print("="*50)
    print(f"Simulation period: {os.getenv('START_DATE')} to {os.getenv('END_DATE')}")
    print(f"Score thresholds: {len(SCORE_THRESHOLDS)} values")
    print(f"Weight combinations: {len(WEIGHT_COMBINATIONS)} combinations")
    print(f"Market cap ranges: {len(MARKET_CAP_COMBINATIONS)} ranges")
    print(f"Equal investment options: {len(EQUAL_INVESTMENT_OPTIONS)} options")
    
    total_combinations = (len(SCORE_THRESHOLDS) * len(EQUAL_INVESTMENT_OPTIONS) * len(WEIGHT_COMBINATIONS) * len(MARKET_CAP_COMBINATIONS))
    print(f"Total combinations: {total_combinations}")
    print("\nParameter testing completed!")

if __name__ == "__main__":
    main()
import os
import math
from datetime import datetime, timedelta
from utils.logging_utils import get_logger
from database.repositories import StockPriceRepository

class Volume:
    def __init__(self):
        self.logger = get_logger(__name__)
          
    def compute_volume_trend(self, symbol, date):
        """Calculate if volume is trending up or down"""

        volume_trend_period = int(os.getenv("VOLUME_TREND_PERIOD"))
        start_date = date - timedelta(days=volume_trend_period + 2)
        
        prices_last_days = StockPriceRepository.get_prices_for_symbol(symbol, start_date, date)
        if len(prices_last_days) * 1.25 < volume_trend_period:
            self.logger.warning(f"Insufficient data for volume trend calculation for {symbol}")
            return 0.0
        
        prices_last_days.sort(key=lambda x: x['date'])
        prices_last_days = prices_last_days[-volume_trend_period:]  # Get last N days
        
        trend_scores = []
        for i in range(1, len(prices_last_days)):
            current_vol = prices_last_days[i]['volume']
            previous_vol = prices_last_days[i-1]['volume']
            if current_vol > previous_vol:
                trend_scores.append(1)
            elif current_vol < previous_vol:
                trend_scores.append(-1)
            else:
                trend_scores.append(0)
        
        if len(trend_scores) == 0:
            return 0.0
        
        # Average trend: +1 = consistently increasing, -1 = consistently decreasing
        return sum(trend_scores) / len(trend_scores)
    
    def compute_volume_average(self, symbol, date):
        """Compare recent volume to baseline average"""
        volume_average_period = int(os.getenv("VOLUME_AVERAGE_PERIOD"))
        volume_trend_period = int(os.getenv("VOLUME_TREND_PERIOD"))
        
        start_date = date - timedelta(days=volume_average_period + volume_trend_period + 5)
        prices = StockPriceRepository.get_prices_for_symbol(symbol, start_date, date)
        
        if len(prices) * 1.25 < volume_average_period:
            self.logger.warning(f"Insufficient data for volume average calculation for {symbol}")
            return 1.0
        
        prices.sort(key=lambda x: x['date'])
        
        # Baseline average volume
        baseline_period = prices[:-volume_trend_period] if len(prices) > volume_average_period + volume_trend_period else prices[:volume_average_period]
        avg_volume_baseline = sum(p['volume'] for p in baseline_period) / len(baseline_period)
        
        # Recent period average volume
        recent_days = prices[-volume_trend_period:]
        recent_avg_volume = sum(p['volume'] for p in recent_days) / len(recent_days)
        
        volume_ratio = recent_avg_volume / avg_volume_baseline if avg_volume_baseline > 0 else 1.0
        
        # Volume ratio: +1 = very low volumes (bearish), -1 = very high volumes (bullish). Normalized, original range [0, 100]
        return self.__normalize_volume_ratio(volume_ratio)
    
    def __normalize_volume_ratio(self, volume_ratio):
        """Normalize volume ratio to [-1, +1] using logarithmic scaling"""

        if volume_ratio <= 0:
            return -1.0
        
        log_ratio = math.log(volume_ratio)
        normalized = log_ratio / math.log(3.0)
        
        # Clamp to [-1, +1] range
        return max(-1.0, min(1.0, normalized))

    def compute_volume_accumulation_distribution(self, symbol, date):
        """Calculate if volume favors up days or down days"""
        volume_ad_period = int(os.getenv("VOLUME_ACCUMULATION_DISTRIBUTION_PERIOD"))
        
        # Get data for the specified period plus buffer for trading days
        start_date = date - timedelta(days=volume_ad_period + 5)
        prices = StockPriceRepository.get_prices_for_symbol(symbol, start_date, date)
        
        if len(prices) * 1.25 < volume_ad_period:
            self.logger.warning(f"Insufficient data for volume accumulation/distribution calculation for {symbol}")
            return 0
        
        prices.sort(key=lambda x: x['date'])
        # Get last N trading days
        prices = prices[-volume_ad_period:]
        
        up_day_volume = 0
        down_day_volume = 0
        
        for i in range(1, len(prices)):
            current_close = prices[i]['close']
            previous_close = prices[i-1]['close']
            volume = prices[i]['volume']
            
            if current_close > previous_close:
                up_day_volume += volume
            elif current_close < previous_close:
                down_day_volume += volume
        
        total_directional_volume = up_day_volume + down_day_volume
        if total_directional_volume == 0:
            return 0
        
        # A/D ratio: +1 = all volume on up days, -1 = all volume on down days
        return (up_day_volume - down_day_volume) / total_directional_volume
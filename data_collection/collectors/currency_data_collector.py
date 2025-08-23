import os
import yfinance as yf
from database.repositories import CompanyRepository, CurrenciesConversionsRepository
from utils.logging_utils import get_logger
from datetime import timedelta, datetime
from typing import Set

class CurrencyDataCollector:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.final_currency = os.getenv("FINAL_CURRENCY", "EUR")
    
    def collect(self):
        self.logger.info("Starting currency data collection...")
        
        currencies = self._get_all_currencies()
        
        if not currencies:
            self.logger.warning("No currencies found in companies")
            return
            
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        data_fetch_padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))
        
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        extended_start = start_date - timedelta(days=data_fetch_padding_days)
        extended_end = end_date + timedelta(days=data_fetch_padding_days)
        
        for currency in currencies:
            if currency == self.final_currency:
                # Same currency = rate 1.0
                self.logger.debug(f"Currency {currency} is same as final currency, setting rate to 1.0")
                self._save_identical_currency_data(currency, extended_start, extended_end)
            else:
                self.logger.debug(f"Fetching {currency} to {self.final_currency} conversion rates for period {extended_start} - {extended_end}...")
                self._fetch_and_save_currency_data(currency, extended_start, extended_end)
        
        self.logger.info("Currency data successfully collected")
    
    def _get_all_currencies(self) -> Set[str]:
        currencies = CompanyRepository.get_all_currencies()
        self.logger.info(f"Found {len(currencies)} unique currencies: {currencies}")
        return set(filter(None, currencies))
    
    def _save_identical_currency_data(self, currency: str, start_date, end_date):
        conversion_data = []
        current_date = start_date
        
        while current_date <= end_date:
            conversion_data.append({
                "date": current_date,
                "currency_name": currency,
                "conversion_rate": 1.0
            })
            current_date += timedelta(days=1)
        
        if conversion_data:
            CurrenciesConversionsRepository.save_currency_conversions(conversion_data)
            self.logger.debug(f"Saved {len(conversion_data)} identical currency records for {currency}")
    
    def _fetch_and_save_currency_data(self, currency: str, start_date, end_date):
        try:
            # yfinance format: "USDEUR=X"
            currency_pair = f"{currency}{self.final_currency}=X"
            
            self.logger.debug(f"Fetching currency pair: {currency_pair}")
            
            currency_data = yf.download(
                currency_pair, 
                start=start_date, 
                end=end_date + timedelta(days=1),
                interval="1d", 
                auto_adjust=True, 
                progress=False
            )
            
            if not currency_data.empty:
                self._save_currency_data(currency_data, currency)
                self.logger.debug(f"Currency data for {currency} successfully saved")
            else:
                self.logger.warning(f"No currency data found for {currency_pair}")
                
        except Exception as e:
            self.logger.error(f"Error fetching currency data for {currency}: {str(e)}")
    
    def _save_currency_data(self, currency_data, currency: str):
        if currency_data.columns.nlevels > 1:
            currency_data.columns = currency_data.columns.droplevel(1)
        
        currency_data = currency_data.reset_index()
        
        conversion_data = []
        for _, row in currency_data.iterrows():
            conversion_data.append({
                "date": row["Date"].date(),
                "currency_name": currency,
                # Use close price as conversion rate
                "conversion_rate": float(row["Close"]) 
            })
        
        if conversion_data:
            CurrenciesConversionsRepository.save_currency_conversions(conversion_data)
            self.logger.debug(f"Saved {len(conversion_data)} conversion records for {currency}")
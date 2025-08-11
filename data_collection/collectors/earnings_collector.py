import os
import time
import pandas as pd
from io import StringIO
from playwright.sync_api import sync_playwright
from shared.utils.logging_utils import get_logger
from database.repositories import EarningsRepository
from playwright.sync_api import sync_playwright, TimeoutError

class EarningsCollector:
    def __init__(self):
        self.logger = get_logger(__name__)

    def collect(self):
        self.logger.info("Starting earnings collection...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            start_date = os.getenv("START_DATE")
            end_date = os.getenv("END_DATE")

            for datetime in pd.date_range(start=start_date, end=end_date):
                date = datetime.date()
                self.logger.debug(f"Getting earnings for date {date}")

                offset = 0
                size = 100

                while True:
                    url = (
                        f"https://finance.yahoo.com/calendar/earnings"
                        f"?day={date}&offset={offset}&size={size}"
                    )
                    self.logger.debug(f"URL used to fetch data: {url}")

                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=10000)

                        # Handle cookie consent
                        try:
                            accept_button = page.wait_for_selector(
                                'button:has-text("Accept all"), button:has-text("Accept"),  button:has-text("Accetta tutto"), '
                                'button[name="agree"], button[id*="accept"]',
                                timeout=5000
                            )
                            if accept_button:
                                accept_button.click()
                                self.logger.debug("Accepted Yahoo cookies")
                                page.wait_for_timeout(2500)
                        except:
                            self.logger.debug("No cookie consent found or already accepted")

                        # Wait for the earnings table to load
                        page.wait_for_selector("table", timeout=10000)
                        content = page.content()

                        # Parse with pandas
                        tables = pd.read_html(StringIO(content))
                        if tables and not tables[0].empty:
                            earnings = tables[0]
                            processed_data = self.__save_earnings_data(earnings, date)

                            # If less than requested size, no more pages
                            if processed_data < size:
                                break
                            else:
                                offset += size
                                time.sleep(int(os.getenv("SCRAPING_DELAY")))
                        else:
                            break

                    except TimeoutError:
                        self.logger.warning(f"No earnings found for date {date}")
                        break
                    except Exception as e:
                        self.logger.error(f"Error fetching data for {date} offset {offset}: {e}")
                        break

                time.sleep(int(os.getenv("SCRAPING_DELAY")))

            browser.close()
        
        self.logger.info("Earnings succesfully collected")
            
    def __save_earnings_data(self, earnings, date):
        # Renaming columns
        earnings = earnings.rename(columns={
            "Symbol": "symbol",
            "EPS Estimate": "eps_estimate",
            "Reported EPS": "eps_actual",
            "Surprise (%)": "surprise"
        })

        # Removing columns
        earnings = earnings.drop(columns=["Company", "Event Name", "Earnings Call Time", "Market Cap", "Follow"])

        # Adding date
        earnings["date"] = date

        # Removing last row (always null)
        earnings = earnings.iloc[:-1]

        # Converting numerical values to float
        earnings["eps_estimate"] = pd.to_numeric(earnings["eps_estimate"], errors="coerce")
        earnings["eps_actual"] = pd.to_numeric(earnings["eps_actual"], errors="coerce")
        earnings["surprise"] = pd.to_numeric(earnings["surprise"], errors="coerce")

        # Remove duplicates
        earnings.drop_duplicates(subset=["symbol", "date"], inplace=True)

        self.logger.debug(f"Fetched {len(earnings)} earnings for date {date}")

        if not earnings.empty:
            EarningsRepository.save_earnings_dates(earnings.to_dict(orient="records"))
            self.logger.debug(f"Earnings for date {date} succesfully saved in the database")

        return len(earnings) if not earnings.empty else 0
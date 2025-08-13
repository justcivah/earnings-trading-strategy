import os
import json
import time
import pandas as pd
from openai import OpenAI
from shared.utils.logging_utils import get_logger
from database.repositories import NewsRepository
from database.repositories import CompanyRepository
from datetime import timedelta
from datetime import datetime


class SentimentProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)
        
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL")
        
        self.base_dir = "batch_files"
        self.input_dir = os.path.join(self.base_dir, "input")
        self.output_dir = os.path.join(self.base_dir, "output")

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def process(self):
        self.logger.info("Starting news batch sentiment processing...")

        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        data_fetch_padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))

        # Convert to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Extend the date range
        extended_start = start_date - timedelta(days=data_fetch_padding_days)
        extended_end = end_date + timedelta(days=data_fetch_padding_days)
        
        batch_ids = {}

        # Create and submit one batch per day
        for date in pd.date_range(start=extended_start, end=extended_end):
            date = date.date()
            self.logger.debug(f"Fetching news from {date}")
            articles = NewsRepository.get_articles_for_date(date)

            if not articles:
                self.logger.debug(f"No articles found for {date}, skipping.")
                continue

            # Save files in dedicated folders
            jsonl_filename = os.path.join(self.input_dir, f"batchinput_{date}.jsonl")
            output_filename = os.path.join(self.output_dir, f"batchoutput_{date}.jsonl")

            self.__create_jsonl_file(articles, jsonl_filename)
            file_id = self.__upload_file(jsonl_filename)
            batch_id = self.__create_batch(file_id, f"Sentiment analysis for {date}")

            batch_ids[batch_id] = output_filename

            self.logger.info(f"Submitted batch for {date} with ID {batch_id}")

        # Wait for all batches to complete
        results = self.__wait_for_all_batches(list(batch_ids.keys()), 300)

        # Download, process, and clean up
        for batch_id, output_file_id in results.items():
            output_filename = batch_ids[batch_id]

            if output_file_id:
                self.__download_results(output_file_id, output_filename)
                self.__process_results(output_filename)

        self.logger.info("News batches successfully processed.")

    def __build_prompt(self, article_content, symbol, company):
        return f"""
            You are a Financial Sentiment Analysis Expert specializing in stock market sentiment evaluation.
            Analyze the provided company news and predict the likely impact on the company’s stock performance after earnings reports.

            The article text may contain information about other companies. Only consider the symbol {symbol} of the company {company} when forming your analysis.
            The article was automatically scraped from the web, so it may include irrelevant or unrelated information — ignore anything that does not pertain to {company}.

            Output Requirements:
            Return ONLY a JSON object with exactly these fields:

            - "reasoning_process": Brief explanation (1–2 sentences) of your analysis.
            - "sentiment_score": A number between -1 (extremely bad choice) and 1 (must buy right now), with 0 being neutral.
            - The sentiment_score MUST be a numeric value (e.g., 0.75, -0.4, 0), not text, not words, and without any extra symbols or formatting.

            Example Outputs:
            {{
            "reasoning_process": "Company reported strong revenue growth and raised guidance, indicating positive market momentum.",
            "sentiment_score": 0.85
            }}
            {{
            "reasoning_process": "CEO resignation amid fraud investigation is likely to damage investor confidence.",
            "sentiment_score": -0.9
            }}
            {{
            "reasoning_process": "Earnings met expectations but guidance was conservative due to market uncertainties, though the product pipeline remains strong.",
            "sentiment_score": 0.1
            }}

            Article:
            {article_content}
            """

    def __create_jsonl_file(self, articles, filename):
        with open(filename, "w", encoding="utf-8") as f:
            for article in articles:
                company_data = CompanyRepository.get_company(article["symbol"])
                company_name = company_data["name"] if company_data and company_data["name"] else article["symbol"]
                
                prompt = self.__build_prompt(
                    article_content=article["content"],
                    symbol=article["symbol"],
                    company=company_name
                )
                
                request = {
                    "custom_id": f"article-{article['id']}",
                    "method": "POST",
                    "url": "/v1/responses",
                    "body": {
                        "model": self.model,
                        "input": prompt,
                        "temperature": 1,
                        "text": {
                            "format": {
                                "type": "json_schema",
                                "name": "sentiment_analysis",
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "reasoning_process": {"type": "string"},
                                        "sentiment_score": {"type": "number", "minimum": -1, "maximum": 1}
                                    },
                                    "required": ["reasoning_process", "sentiment_score"],
                                    "additionalProperties": False
                                },
                                "strict": True
                            }
                        },
                        "reasoning": {"effort": "low"}
                    },
                }
                f.write(json.dumps(request) + "\n")
        return filename

    def __upload_file(self, filename):
        file_obj = self.client.files.create(file=open(filename, "rb"), purpose="batch")
        return file_obj.id

    def __create_batch(self, file_id, description):
        batch = self.client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/responses",
            completion_window="24h",
            metadata={"description": description},
        )
        return batch.id

    def __wait_for_all_batches(self, batch_ids, poll_interval):
        completed = {}
        total_batches = len(batch_ids)

        while len(completed) < total_batches:
            self.logger.info("Fetching batches states...")

            for idx, batch_id in enumerate(batch_ids, start=1):
                if batch_id in completed:
                    continue

                status_obj = self.client.batches.retrieve(batch_id)
                status = status_obj.status

                self.logger.info(f"[{idx}/{total_batches}] Batch {batch_id} status: {status}")

                if status == "completed":
                    completed[batch_id] = status_obj.output_file_id

                elif status in ["failed", "expired", "cancelled"]:
                    self.logger.error(f"[{idx}/{total_batches}] Batch {batch_id} ended with status: {status}")
                    completed[batch_id] = None

            if len(completed) < total_batches:
                time.sleep(poll_interval)

        return completed

    def __download_results(self, file_id, filename):
        file_response = self.client.files.content(file_id)
        with open(filename, "wb") as f:
            f.write(file_response.read())
        return filename

    def __process_results(self, filename):
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)

                try:
                    if data["response"]["status_code"] != 200:
                        self.logger.error(f"Request failed for {data.get('custom_id')}: {data['response']}")
                        continue
                    
                    content = data["response"]["body"]["output"][1]["content"][0]["text"]

                    parsed = json.loads(content)
                    sentiment_score = parsed["sentiment_score"]
                    sentiment_reasoning = parsed["reasoning_process"]
                    article_id = int(data["custom_id"].split("-")[1])

                    NewsRepository.update_article_sentiment(article_id, sentiment_score, sentiment_reasoning)

                except Exception as e:
                    self.logger.error(f"Error parsing result for {data.get('custom_id')}: {e}")
                    self.logger.error(f"Response structure: {data.get('response', {})}")
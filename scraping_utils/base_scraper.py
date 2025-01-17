"""
Base scraper module for creating job board scrapers.

Example usage:

    from typing import Optional
    from datetime import datetime

    from prefect import flow, task
    from pydantic import BaseModel

    from scraping_utils.base_scraper import JobBoardBaseScraper, ScraperConfig
    

    # Define your job posting model (the data you'll get for each one)
    class JobPosting(BaseModel):
        url: str
        title: str
        company: str
        location: str
        posted_date: datetime
        description: str

    CATEGORIES = ()  # List all the categories as they appear in URLs

    # Create your scraper by inheriting from JobBoardBaseScraper
    class MyJobBoardScraper(CurlCffiJobBoardBaseScraper):
        def __init__(self, config):
            super().__init__(config)

        def fetch_job_details(self, posting_tree, posting_url):
            # Implement parsing logic for a single job posting
            return JobPosting(
                url=posting_url,
                title=posting_tree.xpath('//h1/text()')[0],
                company=posting_tree.xpath('//div[@class="company"]/text()')[0],
                location=posting_tree.xpath('//div[@class="location"]/text()')[0],
                posted_date=datetime.now(),
                description=posting_tree.xpath('//div[@class="description"]//text()')[0]
            )

    # Overload process methods to manage it with Prefect
    # @task(task_run_name='{job_link}', persist_result=False)
    # def process_job(self, db, session, proxy_url, job_link, referer):
    #     return super().process_job(db, session, proxy_url, job_link, referer)
    
    # @flow(flow_run_name='{category} - {page}')
    # def process_page(self, db, session, proxy_url, category, page):
    #     return super().process_page(db, session, proxy_url, category, page)
    
    # @flow(flow_run_name='{category}')
    # def process_category(self, db, category):
    #     return super().process_category(db, category)

    # Configure and run your scraper
    # @flow(my-scraper)  # for running with Prefect
    def run_my_scraper(categories: list[str], proxy_urls: list[str]):
        config = ScraperConfig(
            name="my_job_board",
            main_url="https://example.com",
            categories=categories,
            jobs_links_xpath="//div[@class='job-card']//a/@href",
            posting_validation_xpath="//div[@class='job-details']",
            page_url="https://example.com/jobs/{category}?page={page}",
            posting_url="https://example.com{posting_link}",
            proxy_urls=proxy_urls
        )
        scraper = MyJobBoardScraper(config)
        scraper.main()

    if __name__ == "__main__":
        run_my_scraper(CATEGORIES, ['http://user:pass@ip:port'])

The base scraper handles:
- Proxy rotation and validation
- Session management
- Error handling and retries
- Metrics collection
- Rate limiting
- Database connections
- Logging
"""
import time
import random
import traceback
from logging import Logger
from itertools import cycle
from datetime import datetime
from urllib.parse import quote
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Any, Generator

from lxml import html
from lxml.html import HtmlElement
from curl_cffi import requests
from pydantic import BaseModel, field_validator

from scraping_utils.mongodb_client import MongoDBConnection
from scraping_utils.core_utils import setup_logging, retry, check_proxies


class ScraperConfig(BaseModel):
    name: str
    main_url: str
    categories: tuple[str, ...]
    jobs_links_xpath: str
    posting_validation_xpath: str
    page_url: str
    posting_url: str
    proxy_urls: list[str]
    use_prefect: bool = True
    wait_times: tuple[int, int] = (4, 8)
    skip_after_failed: int = 5

    @field_validator('wait_times')
    @classmethod
    def validate_wait_times(cls, v: tuple[int, int]) -> tuple[int, int]:
        if len(v) != 2 or v[0] > v[1]:
            raise ValueError('wait_times must be a tuple of (min, max) where min <= max')
        return v

    @field_validator('proxy_urls')
    @classmethod
    def validate_proxy_urls(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError('At least one proxy URL must be provided')
        return v


@dataclass
class ScrapingMetrics:
    total_jobs_found: int = 0
    successful_scrapes: int = 0
    failed_scrapes: int = 0
    total_pages_scraped: int = 0
    errors_by_type: dict[str, int] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    processing_times: list[float] = field(default_factory=list)

    def get_summary(self) -> dict[str, Any]:
        duration = datetime.now() - self.start_time
        avg_processing_time = (
            sum(self.processing_times) / len(self.processing_times)
            if self.processing_times else 0
        )
        return {
            "duration_seconds": duration.total_seconds(),
            "total_jobs_found": self.total_jobs_found,
            "successful_scrapes": self.successful_scrapes,
            "failed_scrapes": self.failed_scrapes,
            "total_pages_scraped": self.total_pages_scraped,
            "success_rate": (
                self.successful_scrapes / (self.successful_scrapes + self.failed_scrapes)
                if self.successful_scrapes + self.failed_scrapes > 0 else 0
            ),
            "average_processing_time": avg_processing_time,
            "errors_by_type": self.errors_by_type
        }

    def add_error(self, error: Exception) -> None:
        error_type = type(error).__name__
        self.errors_by_type[error_type] = self.errors_by_type.get(error_type, 0) + 1


class CurlCffiJobBoardBaseScraper(ABC):
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = setup_logging(log_file_name=f"{config.name}.log", use_prefect=self.config.use_prefect)
        self.metrics = ScrapingMetrics()
        self.recent_postings = set()
        self.failures = 0
        self._proxy_cycle = cycle(check_proxies(
            self.config.main_url,
            self.config.proxy_urls,
            self.logger,
            self.metrics
        ))

    def get_logger(self) -> Logger:
        return self.logger

    @contextmanager
    def session_scope(self) -> Generator[tuple[requests.Session, str], None, None]:
        session = requests.Session()
        proxy_url = next(self._proxy_cycle)
        try:
            yield session, proxy_url
        finally:
            session.close()

    @contextmanager
    def db_scope(self) -> Generator[MongoDBConnection, None, None]:
        db = MongoDBConnection(self.config.name, 'job_ads', 'mongodb-jobads-uri', self.logger)
        try:
            yield db
        finally:
            db.close_connection()

    @abstractmethod
    def fetch_job_details(self, posting_tree: HtmlElement, posting_url: str) -> BaseModel:
        ...

    def construct_page_url(self, category: str, page: int) -> str:
        encoded_category = quote(category, safe=':/?=&')
        return self.config.page_url.format(category=encoded_category, page=page)

    def construct_job_posting_url(self, posting_url: str) -> str:
        encoded_url = quote(posting_url, safe=':/?=&')
        return self.config.posting_url.format(posting_link=encoded_url)

    @retry(max_retries=2, delay=5, logger_func=get_logger)
    def fetch_jobs_links(
        self,
        session: requests.Session,
        proxy_url: str,
        page_url: str,
        referer: Optional[str] = None
    ) -> set[str]:
        self.logger.info(f"Visiting page {page_url}")
        if referer:
            session.headers.update({"referer": referer})
        
        response = session.get(
            page_url,
            impersonate="chrome",
            proxies={"http": proxy_url, "https": proxy_url}
        )
        tree = html.fromstring(response.content)
        hrefs = set(tree.xpath(self.config.jobs_links_xpath))
        
        self.metrics.total_jobs_found += len(hrefs)
        self.logger.info(
            f"Fetched {len(hrefs)} job links\nexample: {next(iter(hrefs)) if hrefs else 'None'}"
        )
        return hrefs
    
    @retry(max_retries=2, delay=5)
    def get_job_posting_tree(
        self,
        session: requests.Session,
        proxy_url: str,
        posting_url: str,
        referer: Optional[str]
    ) -> HtmlElement:
        if referer:
            session.headers.update({"referer": referer})
        
        response = session.get(
            posting_url,
            impersonate="chrome",
            proxies={"http": proxy_url, "https": proxy_url}
        )
        tree = html.fromstring(response.content)
        
        if not tree.xpath(self.config.posting_validation_xpath):
            raise ValueError("Page has unexpected format or hasn't loaded")
        return tree

    def process_job(
        self,
        db: MongoDBConnection,
        session: requests.Session,
        proxy_url: str,
        job_link: str,
        referer: Optional[str]
    ) -> bool:
        start_time = time.time()
        posting_url = self.construct_job_posting_url(job_link)

        if posting_url in self.recent_postings:
            self.logger.info(f"{posting_url} already in the database")
            return True

        try:
            self.logger.info(f"Getting job ad {posting_url}")
            posting_tree = self.get_job_posting_tree(session, proxy_url, posting_url, referer)
            job_details = self.fetch_job_details(posting_tree, posting_url)
            
            if not job_details:
                self.metrics.failed_scrapes += 1
                return False
            
            db.insert_to_mongodb(job_details.model_dump())
            self.recent_postings.add(posting_url)
            self.metrics.successful_scrapes += 1
            
            processing_time = time.time() - start_time
            self.metrics.processing_times.append(processing_time)
            
            time.sleep(random.uniform(*self.config.wait_times))
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing {posting_url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.metrics.failed_scrapes += 1
            self.metrics.add_error(e)
            return False

    def process_page(
        self,
        db: MongoDBConnection,
        session: requests.Session,
        proxy_url: str,
        category: str,
        page: int
    ) -> bool:
        page_url = self.construct_page_url(category, page)
        try:
            jobs_links = self.fetch_jobs_links(session, proxy_url, page_url, referer=self.config.main_url)
            if not jobs_links and page < 3:
                self.logger.warning(f"No jobs found for category {category} on page {page_url}")
                return False
            if not jobs_links:
                self.logger.info(f"No more jobs found for category {category} on page {page_url}")
                return False

            unsuccessful = 0
            for job_link in jobs_links:
                if not self.process_job(db, session, proxy_url, job_link, referer=page_url):
                    unsuccessful += 1
                    if unsuccessful >= self.config.skip_after_failed:
                        self.logger.error(f"Couldn't fetch job posting {unsuccessful} times")
                        return False
                    wait_a, wait_b = self.config.wait_times
                    time.sleep(random.uniform(wait_a*2, wait_b*2))
            
            self.metrics.total_pages_scraped += 1
            self.logger.info(f"Completed page {page} for category {category}")
            time.sleep(random.uniform(*self.config.wait_times))
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing page {page} of category {category}: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.metrics.add_error(e)
            return False

    def process_category(self, db: MongoDBConnection, category: str) -> None:
        self.logger.info(f"Starting scraping for category {category}")
        already_scraped = len(self.recent_postings)
        
        with self.session_scope() as (session, proxy_url):
            self.logger.info(f"Using proxy: {proxy_url}")
            page = 1
            while self.process_page(db, session, proxy_url, category, page):
                page += 1
                
        self.logger.info(f"Completed scrape for category: {category}")
        self.logger.info(
            f"Scraped {len(self.recent_postings) - already_scraped} postings for this category"
        )
        time.sleep(random.uniform(*self.config.wait_times))

    def main(self) -> None:
        try:
            with self.db_scope() as db:
                self.recent_postings = set(db.get_recent_urls())
                already_scraped = len(self.recent_postings)
                
                for category in self.config.categories:
                    self.process_category(db, category)
                
                new_postings = len(self.recent_postings) - already_scraped
                self.logger.info(f"Scraped {new_postings} postings")
                self.logger.info("Metrics summary:")
                self.logger.info(self.metrics.get_summary())

                if new_postings == 0:
                    raise Exception("No new postings were scraped. This likely indicates a problem with the scraper.")
                
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            self.metrics.add_error(e)
            raise
                
        finally:
            self.logger.info("Scraping ended")

import time
import random
import traceback
from dataclasses import dataclass
from abc import ABC, abstractmethod

from lxml import html
from curl_cffi import requests

from scraping_utils.core import setup_logging, retry
from scraping_utils.mongodb import MongoDBConnection


@dataclass
class ScraperConfig:
    name: str
    main_url: str
    categories: tuple[str]
    jobs_links_xpath: str
    posting_validation_xpath: str
    page_url: str
    posting_url: str
    use_prefect: bool = False


class JobBoardBaseScraper(ABC):
    def __init__(self, config):
        self.name = config.name
        self.main_url = config.main_url
        self.categories = config.categories
        self.jobs_links_xpath = config.jobs_links_xpath
        self.posting_validation_xpath = config.posting_validation_xpath
        self.page_url = config.page_url
        self.posting_url = config.posting_url
        self.use_prefect = config.use_prefect
        self.logger = setup_logging(f"{config.name}.log", self.use_prefect)
        self.recent_postings = []

    def get_logger(self):
        return self.logger

    @abstractmethod
    def fetch_job_details(self, posting_tree, posting_url):
        ...

    def construct_page_url(self, category, page):
        return self.page_url.format(category=category, page=page)

    def construct_job_posting_url(self, posting_url):
        return self.posting_url.format(posting_link=posting_url)

    @retry(max_retries=2, delay=5, logger_func=get_logger)
    def fetch_jobs_links(self, session, page_url, referer=None):
        self.logger.info(f"Visiting page {page_url}")
        if referer:
            session.headers.update({"referer": referer})
        response = session.get(page_url, impersonate="chrome")
        tree = html.fromstring(response.content)
        hrefs = set(tree.xpath(self.jobs_links_xpath))
        self.logger.info(f"Fetched {len(hrefs)} job links\nexample: {next(iter(hrefs)) if hrefs else 'None'}")
        return hrefs
    
    @retry(max_retries=2, delay=5)
    def get_job_posting_tree(self, session, posting_url, referer):
        if referer:
            session.headers.update({"referer": referer})
        response = session.get(posting_url, impersonate="chrome")
        tree = html.fromstring(response.content)
        if not tree.xpath(self.posting_validation_xpath):
            raise ValueError("Page has unexpected format or hasn't loaded")
        return tree

    def process_job(self, session, db, job_link, referer):
        posting_url = self.construct_job_posting_url(job_link)

        if posting_url in self.recent_postings:
            self.logger.info(f"{posting_url} already in the database")
            return True

        try:
            self.logger.info(f"Getting job ad {posting_url}")
            posting_tree = self.get_job_posting_tree(session, posting_url, referer)
        except Exception as e:
            self.logger.error(f"Error getting {posting_url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        
        try:
            job_details = self.fetch_job_details(posting_tree, posting_url)
        except Exception as e:
            self.logger.error(f"Error fetching job details for {posting_url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        
        if not job_details:
            return False
        
        db.insert_to_mongodb(job_details.model_dump())
        self.recent_postings.append(posting_url)
        time.sleep(random.randint(3, 7))
        return True

    def process_page(self, session, db, category, page):
        page_url = self.construct_page_url(category, page)
        try:
            jobs_links = self.fetch_jobs_links(session, page_url, referer=self.main_url)
            if not jobs_links:
                self.logger.info(f"No more jobs found for category {category} on page {page_url}")
                return False
            
            unsuccessful = 0
            for job_link in jobs_links:
                if not self.process_job(session, db, job_link, referer=page_url):
                    unsuccessful += 1
                    if unsuccessful > 1:
                        break
            
            self.logger.info(f"Completed page {page} for category {category}")
            time.sleep(random.randint(3, 7))
            return True
        except Exception as e:
            self.logger.error(f"Error processing page {page} of category {category}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True

    def process_category(self, session, db, category):
        self.logger.info(f"Starting scrape for category: {category}")
        page = 1
        while self.process_page(session, db, category, page):
            page += 1
        self.logger.info(f"Completed scrape for category: {category}")
        time.sleep(random.randint(5, 10))

    def main(self):
        db = None
        try:
            db = MongoDBConnection(self.name, 'job_ads', 'mongodb-jobads-uri', self.logger)
            self.recent_postings = db.get_recent_urls()

            with requests.Session() as session:
                for category in self.categories:
                    self.process_category(session, db, category)
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            self.logger.error(traceback.format_exc())
        finally:
            if db:
                db.close_connection()
            self.logger.info("Scraping ended")

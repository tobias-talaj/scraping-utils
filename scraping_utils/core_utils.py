import time
import logging
import traceback
from pathlib import Path
from functools import wraps
from typing import Callable, Optional, Any
from logging.handlers import RotatingFileHandler
from logging import Logger, Formatter, StreamHandler

import requests
from lxml import etree
from prefect import get_run_logger
from prefect.logging import get_run_logger


def setup_logging(
    log_file_name: str = 'scraper.log',
    log_level: int = logging.INFO,
    use_prefect: bool = False
) -> Logger | Any:
    if use_prefect:
        return get_run_logger()
    
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    log_file_path = log_dir / log_file_name
    
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
    
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(log_format)
    file_handler.setFormatter(log_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


def retry(
    max_retries: int = 3,
    delay: int = 5,
    logger_func: Optional[Callable[[], Logger]] = None
) -> Callable:
    def decorator(func):
        wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if logger_func:
                        logger = logger_func()
                        logger.error(f"Attempt {attempts} failed: {str(e)}")
                        logger.error(traceback.format_exc())
                    if attempts == max_retries:
                        raise
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


def save_failed_html(tree: etree._Element, filename: str) -> None:
    html_string = etree.tostring(tree, pretty_print=True, method="html", encoding="unicode")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_string)


def check_proxies(
    main_url: str,
    proxy_urls: list[str],
    logger: Logger,
    metrics: Any
) -> list[str]:
    ok_proxies: list[str] = []
    for proxy_url in proxy_urls:
        try:
            response = requests.get(
                main_url,
                impersonate='chrome',
                proxies={"http": proxy_url, "https": proxy_url}
            )
            if response.status_code == 200:
                ok_proxies.append(proxy_url)
            else:
                logger.error(f"Proxy {proxy_url} busted on {main_url}")
        except Exception as e:
            logger.error(f"Proxy {proxy_url} busted or invalid format: {str(e)}")
            metrics.add_error(e)
    return ok_proxies

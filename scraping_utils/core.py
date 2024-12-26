import time
import string
import random
import logging
import traceback
from pathlib import Path
from functools import wraps
from logging.handlers import RotatingFileHandler

from lxml import etree
from prefect import get_run_logger


def setup_logging(log_file_name='scraper.log', log_level=logging.INFO, use_prefect=False):
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


def retry(max_retries=3, delay=5, logger_func=None):
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


def save_failed_html(tree, filename):
    html_string = etree.tostring(tree, pretty_print=True, method="html", encoding="unicode")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_string)


def random_hex(length):
    return ''.join(random.choices(string.hexdigits.lower(), k=length))


def random_base64(length):
    return ''.join(random.choices(string.ascii_letters + string.digits + '+/', k=length))

from setuptools import setup, find_packages

setup(
    name="scraping-utils",
    version="0.3.2",
    packages=find_packages(),
    install_requires=[
        'lxml',
        'curl_cffi',
        'pymongo',
        'mitmproxy',
        'python-dotenv',
        'prefect'
    ]
)
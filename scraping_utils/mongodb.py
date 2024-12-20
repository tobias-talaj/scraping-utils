import os
import sys
import logging
import threading
import traceback
from pathlib import Path
from datetime import datetime, timedelta

from prefect.blocks.system import Secret
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

import warnings
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)


class MongoDBConnection:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, collection_name, db_name='job_ads', logger=None):
        key = (collection_name, db_name)
        with cls._lock:
            if key not in cls._instances:
                instance = super().__new__(cls)
                instance._initialize(collection_name, db_name, logger)
                cls._instances[key] = instance
                instance.logger.info(f"Created MongoDBConnection for {key}")
        return cls._instances[key]

    def _initialize(self, collection_name, db_name, logger):
        try:
            mongodb_block = Secret.load("mongodb-uri")
            self.uri = mongodb_block.get()
            if not self.uri:
                raise ValueError(
                    "MongoDB URI not found in Prefect blocks. "
                    "Please check the 'mongodb-uri' secret block"
                )
        except Exception as e:
            env_path = Path.cwd() / '.env'
            if env_path.exists():
                from dotenv import load_dotenv
                load_dotenv(env_path)
            
            self.uri = os.getenv('MONGODB_URI')
            if not self.uri:
                raise ValueError(
                    "MONGODB_URI not found. Either set up a Prefect secret block 'mongodb-uri' "
                    "or create a .env file with MONGODB_URI=your_connection_string"
                )

        self._client = None
        self.collection_name = collection_name
        self.db_name = db_name
        self.logger = logger or logging.getLogger(__name__)

    def get_database(self):
        with self._lock:
            if self._client is None:
                self._client = MongoClient(self.uri)
        return self._client[self.db_name]

    def insert_to_mongodb(self, documents):
        self.logger.debug(f"Insering documents into {self.collection_name}")
        try:
            db = self.get_database()
            collection = db[self.collection_name]

            if isinstance(documents, (list, tuple)):
                result = collection.insert_many(documents)
                self.logger.info(f"Inserted {len(result.inserted_ids)} documents into {self.collection_name}")
                return result.inserted_ids
            else:
                result = collection.insert_one(documents)
                self.logger.info(f"Inserted 1 document into {self.collection_name}")
                return result.inserted_id

        except OperationFailure:
            self.logger.error(f"An authentication error was received: {traceback.format_exc()}")
            self.logger.error("Are you sure your database user is authorized to perform write operations?")
            sys.exit(1)
        except PyMongoError:
            self.logger.error(f"An error occurred while inserting documents: {traceback.format_exc()}")
            sys.exit(1)

    def get_recent_urls(self, days=7):
        self.logger.debug(f"Retrieving recent URLs from {self.collection_name} collection")
        try:
            db = self.get_database()
            collection = db[self.collection_name]
            cutoff_date = datetime.now() - timedelta(days=days)
            query = {"scraping_date": {"$gte": cutoff_date}}
            projection = {"url": 1, "_id": 0}
            cursor = collection.find(query, projection)
            urls = [doc['url'] for doc in cursor]

            self.logger.info(f"Retrieved {len(urls)} recent URLs from {self.collection_name} collection")
            return urls

        except PyMongoError:
            self.logger.error(f"An error occurred while retrieving recent URLs: {traceback.format_exc()}")
            return []

    def close_connection(self):
        with self._lock:
            if self._client is not None:
                self._client.close()
                self._client = None
                self.logger.info("Closed MongoDB connection")

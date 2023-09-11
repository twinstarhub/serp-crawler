from functools import wraps
from typing import Any, TYPE_CHECKING
import redis
from redis.commands.json.path import Path

from custom_logger import CacherLogger

if TYPE_CHECKING:
    import logging

def ensure_connection(func):
    """Ensure that the connection to Redis is established before executing the function."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Wrapper function."""
        if not self.client:
            self.logger.warning('Redis connection not established. Skipping caching.')
            return
        try:
            return func(self, *args, **kwargs)
        except Exception as exp:
            self.logger.error(f'Error while executing {func.__name__}:\n{exp}')
    return wrapper

class Cacher:
    """A wrapper to cache data in Redis."""

    def __init__(self, host: str, port: int, password: str):
        """Initialize the Cacher."""
        self.host = host
        self.port = int(port)
        self.password = password
        self.client = None
        self.logger: logging.Logger = CacherLogger()

    def connect(self):
        """Connect to Redis."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True
            )
            self.client.ping()
            self.logger.info('Redis connection established.')
        except redis.ConnectionError:
            self.logger.warning('Unable to connect to Redis.')

    def disconnect(self):
        """Disconnect from Redis."""
        self.client.close()
        self.logger.info('Redis connection closed.')

    @ensure_connection
    def get(self, key: list[str]):
        """Get a value from Redis synchronously."""
        key = self.to_key(key)
        return self.client.json().get(key)

    @ensure_connection
    def insert(self, key: list[str], values: list[dict[str, Any]]):
        """Writes to Redis synchronously."""
        if isinstance(key, list):
            key = self.to_key(key)
        self.client.json().set(key, Path.root_path(), values)

    @ensure_connection
    def search_by_status(self, status: str | int) -> list[dict]:
        """Search all the requests for a specific status."""
        keys = self.client.keys('*')
        records = self.client.json.mget(keys, Path.root_path())
        results = {}
        for key, record in zip(keys, records):
            results[key] = record[str(status)]
        return results

    @ensure_connection
    def update_by_status(self, key: str, status: str | int, records: list[dict]):
        """Update the value of a specific status."""
        value = self.client.json.get(key, Path.root_path())
        value[str(status)] = records
        self.client.json.set(key, Path.root_path(), value)

    @ensure_connection
    def bulk_update_by_status(self, mapping: dict[str, str | list[dict]]):
        """Update the value of a specific status."""
        pipe = self.client.pipeline(transaction=True)
        for key, payload in mapping.items():
            for status, records in payload.items():
                pipe.json.set(key, f'$.{status}', records)
        pipe.execute()

    def __enter__(self):
        """Connect to Redis."""
        self.connect()
        return self

    def __exit__(self, *args):
        """Disconnect from Redis."""
        self.disconnect()
        return None

    @staticmethod
    def to_key(args):
        """Convert arguments to a Redis key."""
        return ':'.join(args)

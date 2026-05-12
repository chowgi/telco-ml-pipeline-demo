"""
Custom PyFlink sink that writes windowed aggregates to MongoDB Atlas.
"""

import json
from datetime import datetime, timezone
from pyflink.datastream.functions import SinkFunction, RuntimeContext
from pymongo import MongoClient


class MongoDBSinkFunction(SinkFunction):
    """Writes windowed network metrics to MongoDB Atlas."""

    def __init__(self, mongodb_uri: str, db_name: str, collection_name: str):
        self._uri = mongodb_uri
        self._db_name = db_name
        self._collection_name = collection_name
        self._client = None
        self._collection = None
        self._buffer = []
        self._buffer_size = 50

    def open(self, runtime_context: RuntimeContext):
        self._client = MongoClient(self._uri)
        self._collection = self._client[self._db_name][self._collection_name]

    def invoke(self, value, context):
        doc = json.loads(value)

        doc["window_end"] = datetime.fromtimestamp(doc["window_end"], tz=timezone.utc)
        doc["ingested_at"] = datetime.now(timezone.utc)

        self._buffer.append(doc)

        if len(self._buffer) >= self._buffer_size:
            self._flush()

    def _flush(self):
        if self._buffer:
            self._collection.insert_many(self._buffer)
            self._buffer = []

    def close(self):
        self._flush()
        if self._client:
            self._client.close()

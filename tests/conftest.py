"""
Pytest configuration — patches out infrastructure (MongoDB, HTTP) before any app
module is imported so tests never need a running database or network.
"""
import sys
from unittest.mock import MagicMock

# ── Block MongoDB connections ────────────────────────────────────────────────
# pymongo.MongoClient is patched before any route/service module loads it.
_mock_collection = MagicMock()
_mock_db = MagicMock()
_mock_db.get_collection.return_value = _mock_collection
_mock_client = MagicMock()
_mock_client.__getitem__ = lambda self, name: _mock_db

import pymongo
pymongo.MongoClient = MagicMock(return_value=_mock_client)  # type: ignore

# ── Stub out the database helper so get_database() never raises ──────────────
import purchases_backend.database as _db_mod
_db_mod.get_database = MagicMock(return_value=_mock_db)

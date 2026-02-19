# tests/test_loader.py
"""Tests for loader.py logic (mocked DB connection)"""

from unittest.mock import patch
import os
from dotenv import load_dotenv

@patch("src.scripts.loader.create_engine")
def test_loader_connection_string_format(mock_create_engine):
    """Check that loader builds correct Postgres URL from .env"""
    load_dotenv()

    from src.scripts.loader import DATABASE_URL  

    expected_start = "postgresql+psycopg2://postgres:"
    expected_end = "@localhost:5432/medical_warehouse"

    assert DATABASE_URL.startswith(expected_start), "Wrong user/protocol"
    assert DATABASE_URL.endswith(expected_end), "Wrong host/port/DB name"
# tests/test_env.py
"""Tests for environment variable loading (critical for DB/API connections)"""

from dotenv import load_dotenv
import os


def test_env_file_is_loaded():
    """Ensure .env is loaded and required keys exist"""
    load_dotenv()  # in case it's not auto-loaded in test env

    required_keys = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    ]

    missing = [key for key in required_keys if os.getenv(key) is None]

    assert not missing, f"Missing required env vars: {missing}"
    assert os.getenv("POSTGRES_DB") == "medical_warehouse", "Wrong DB name"
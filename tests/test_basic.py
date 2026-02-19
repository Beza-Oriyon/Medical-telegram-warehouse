# tests/test_basic.py
"""Simple sanity tests to confirm pytest is set up correctly"""

def test_math():
    assert 1 + 1 == 2
    assert 5 * 2 == 10


def test_string_methods():
    assert "hello world".upper() == "HELLO WORLD"
    assert "python".capitalize() == "Python"
#! /usr/bin/env python3
"""Comprehensive test suite with fantastic code coverage goes here."""

import os
import sys

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../')))

from src.my_pyhc_package.main import hello_world  # noqa: E402


def test_hello_world():
    """Test the hello_world function from main module.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Examples
    --------
    >>> test_hello_world()
    Hello, world!
    """
    hello_world()
    assert True


if __name__ == "__main__":
    test_hello_world()

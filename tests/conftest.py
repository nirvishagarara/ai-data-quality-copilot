"""
conftest.py
===========
Shared pytest fixtures for all test modules.
"""

import os
import sys

import duckdb
import pytest

# All modules use relative paths like "data/warehouse.duckdb" so every
# test must run with cwd set to the project root (data-quality-copilot/).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Make project root importable so tests can do "from src.monitoring..."
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(autouse=True)
def set_working_dir():
    """Change cwd to data-quality-copilot/ before every test, restore after."""
    original = os.getcwd()
    os.chdir(PROJECT_ROOT)
    yield
    os.chdir(original)


@pytest.fixture
def in_memory_db():
    """
    A minimal in-memory DuckDB connection pre-loaded with a synthetic table.

    Schema chosen to exercise all test-generator branches:
      - id:      fully unique + non-null  → not_null + unique tests
      - status:  3-value categorical      → accepted_values test
      - amount:  numeric range            → value_between test
      - country: 3-value categorical      → accepted_values test
    """
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE test_table (
            id      VARCHAR,
            status  VARCHAR,
            amount  DOUBLE,
            country VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO test_table VALUES
        ('id_01', 'active',    10.0, 'US'),
        ('id_02', 'inactive',  20.0, 'UK'),
        ('id_03', 'pending',   30.0, 'US'),
        ('id_04', 'active',    40.0, 'DE'),
        ('id_05', 'inactive',  50.0, 'US'),
        ('id_06', 'active',    60.0, 'UK'),
        ('id_07', 'pending',   70.0, 'DE'),
        ('id_08', 'active',    80.0, 'US'),
        ('id_09', 'inactive',  90.0, 'UK'),
        ('id_10', 'active',   100.0, 'US')
    """)
    yield con
    con.close()

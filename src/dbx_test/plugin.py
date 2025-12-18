"""
Pytest plugin for dbx_test framework.

This module registers dbx_test as a pytest plugin, automatically providing
fixtures to any project that has dbx_test installed.

To use the fixtures, simply install dbx_test:
    pip install dbx_test

Then import fixtures in your conftest.py:
    from dbx_test.fixtures import spark_session, dbutils, notebook_context

Or use the plugin auto-registration (requires entry point configuration).
"""

import pytest
import os

# Plugin identification
pytest_plugins = ["dbx_test.fixtures"]


def pytest_configure(config):
    """Configure pytest with dbx_test markers and settings."""
    # Register custom markers
    config.addinivalue_line(
        "markers",
        "databricks: mark test as requiring Databricks connection",
    )
    config.addinivalue_line(
        "markers",
        "spark: mark test as requiring SparkSession",
    )
    config.addinivalue_line(
        "markers",
        "notebook: mark test as a notebook-based test",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow-running",
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test",
    )


def pytest_addoption(parser):
    """Add dbx_test command line options."""
    group = parser.getgroup("dbx_test", "Databricks notebook testing options")
    
    group.addoption(
        "--dbx-profile",
        action="store",
        default=os.environ.get("DBX_TEST_PROFILE"),
        help="Databricks CLI profile to use for tests",
    )
    
    group.addoption(
        "--dbx-cluster-id",
        action="store",
        default=os.environ.get("DBX_TEST_CLUSTER_ID"),
        help="Databricks cluster ID for Databricks Connect",
    )
    
    group.addoption(
        "--dbx-use-connect",
        action="store_true",
        default=os.environ.get("DBX_TEST_USE_CONNECT", "").lower() == "true",
        help="Use Databricks Connect instead of local Spark",
    )
    
    group.addoption(
        "--dbx-catalog",
        action="store",
        default=os.environ.get("DBX_TEST_CATALOG"),
        help="Unity Catalog name for test data",
    )
    
    group.addoption(
        "--dbx-no-cleanup",
        action="store_true",
        default=False,
        help="Skip cleanup of test data after tests",
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on dbx_test markers."""
    from dbx_test.fixtures.databricks import is_databricks_runtime
    
    skip_databricks = pytest.mark.skip(reason="Databricks connection not available")
    skip_spark = pytest.mark.skip(reason="SparkSession not available")
    
    for item in items:
        # Check if running in Databricks or with Connect
        has_databricks = (
            is_databricks_runtime() or
            config.getoption("--dbx-use-connect") or
            os.environ.get("DATABRICKS_HOST")
        )
        
        # Skip Databricks tests when not connected
        if "databricks" in [m.name for m in item.iter_markers()]:
            if not has_databricks:
                item.add_marker(skip_databricks)
        
        # Check for Spark requirement
        if "spark" in [m.name for m in item.iter_markers()]:
            try:
                import pyspark
            except ImportError:
                item.add_marker(skip_spark)


@pytest.fixture(scope="session")
def dbx_test_options(request):
    """Fixture providing access to dbx_test command line options.
    
    Example:
        def test_with_options(dbx_test_options):
            if dbx_test_options.use_connect:
                # Test with Databricks Connect
                pass
    """
    class Options:
        profile = request.config.getoption("--dbx-profile")
        cluster_id = request.config.getoption("--dbx-cluster-id")
        use_connect = request.config.getoption("--dbx-use-connect")
        catalog = request.config.getoption("--dbx-catalog")
        no_cleanup = request.config.getoption("--dbx-no-cleanup")
    
    return Options()


# -*- coding: utf-8 -*-
"""
Project path management

Provides unified path access for all project code.
"""

from pathlib import Path


def get_project_root() -> Path:
    """Get project root directory

    Returns the project root directory from any location.

    Returns:
        Path object for project root
    """
    current = Path(__file__).resolve()

    # Find pyproject.toml going up
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent

    # Fallback: fixed level (current file in src/simtradelab/)
    return current.parent.parent.parent


def get_data_path() -> Path:
    """Get data directory path"""
    return get_project_root() / "data"


def get_strategies_path() -> Path:
    """Get strategies directory path"""
    return get_project_root() / "strategies"


# Convenient access
PROJECT_ROOT = get_project_root()
DATA_PATH = get_data_path()
STRATEGIES_PATH = get_strategies_path()

# DuckDB database paths
DUCKDB_PATH = DATA_PATH / "cn.duckdb"
US_DUCKDB_PATH = DATA_PATH / "us.duckdb"


from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from fixtures.paths import BASE_DIR

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

COMPUTE_MIGRATIONS_DIR = BASE_DIR / "compute_tools" / "src" / "migrations"
COMPUTE_MIGRATIONS_TEST_DIR = COMPUTE_MIGRATIONS_DIR / "tests"

COMPUTE_MIGRATIONS = sorted(next(os.walk(COMPUTE_MIGRATIONS_DIR))[2])
NUM_COMPUTE_MIGRATIONS = len(COMPUTE_MIGRATIONS)


@pytest.fixture(scope="session")
def compute_migrations_dir() -> Iterator[Path]:
    """
    Retrieve the path to the compute migrations directory.
    """
    yield COMPUTE_MIGRATIONS_DIR


@pytest.fixture(scope="session")
def compute_migrations_test_dir() -> Iterator[Path]:
    """
    Retrieve the path to the compute migrations test directory.
    """
    yield COMPUTE_MIGRATIONS_TEST_DIR

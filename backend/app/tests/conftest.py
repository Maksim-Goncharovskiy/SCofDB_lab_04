"""Pytest configuration and fixtures."""

import os
# Set test database URL BEFORE any app imports
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"
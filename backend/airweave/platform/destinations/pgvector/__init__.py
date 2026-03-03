"""Pgvector destination package.

This package provides the PgvectorDestination for storing and searching
entities in PostgreSQL with the pgvector extension.

Public API:
    PgvectorDestination - Main destination class for pgvector operations
"""

from airweave.platform.destinations.pgvector.destination import PgvectorDestination

__all__ = ["PgvectorDestination"]

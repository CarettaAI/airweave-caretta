"""Pgvector client - low-level I/O operations for PostgreSQL with pgvector.

This module encapsulates all direct communication with PostgreSQL:
- Connection pool management
- Table creation and DDL
- Document insertion/deletion
- Query execution
"""

from __future__ import annotations

import json
import time
from typing import Any, List, Optional, Tuple
from uuid import UUID

import asyncpg
from pgvector.asyncpg import register_vector

from airweave.core.logging import ContextualLogger
from airweave.core.logging import logger as default_logger
from airweave.schemas.search_result import (
    AccessControlResult,
    AirweaveSearchResult,
    BreadcrumbResult,
    SystemMetadataResult,
)


class PgvectorClient:
    """Low-level PostgreSQL + pgvector client wrapper.

    Handles all I/O operations with PostgreSQL, including:
    - Connection pool management with pgvector type registration
    - Table creation with HNSW index
    - Document insertion via executemany (upsert)
    - Document deletion by sync_id or parent_ids
    - Nearest-neighbor search with cosine distance
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        logger: Optional[ContextualLogger] = None,
    ):
        """Initialize the pgvector client.

        Args:
            pool: asyncpg connection pool with vector type registered
            logger: Optional logger for debug/warning messages
        """
        self._pool = pool
        self._logger = logger or default_logger

    @classmethod
    async def connect(
        cls,
        connection_string: str,
        pool_size: int = 10,
        logger: Optional[ContextualLogger] = None,
    ) -> "PgvectorClient":
        """Create pool and register vector type on each connection.

        Args:
            connection_string: PostgreSQL DSN
            pool_size: Maximum pool connections
            logger: Optional logger

        Returns:
            Connected PgvectorClient instance
        """
        log = logger or default_logger

        async def _init_connection(conn: asyncpg.Connection) -> None:
            await register_vector(conn)

        pool = await asyncpg.create_pool(
            dsn=connection_string,
            min_size=2,
            max_size=pool_size,
            init=_init_connection,
        )

        log.info(f"Connected to pgvector (pool_size={pool_size})")
        return cls(pool=pool, logger=logger)

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._logger.debug("Closed pgvector connection pool")

    # -------------------------------------------------------------------------
    # DDL Operations
    # -------------------------------------------------------------------------

    async def ensure_extension(self) -> None:
        """Create pgvector extension if not present."""
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        self._logger.debug("Ensured pgvector extension exists")

    async def create_collection_table(self, table_name: str, vector_size: int) -> None:
        """Create the collection table with HNSW index.

        Creates a single table per collection with columns for all entity fields,
        plus HNSW and operational indexes.

        Args:
            table_name: Sanitized table name (collection_{uuid})
            vector_size: Embedding dimensions (e.g. 3072 for text-embedding-3-large)
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id                      UUID PRIMARY KEY,
            entity_id               TEXT NOT NULL,
            sync_id                 UUID NOT NULL,
            sync_job_id             UUID,
            parent_id               TEXT,
            collection_id           UUID,
            entity_type             TEXT,
            name                    TEXT,
            textual_representation  TEXT,
            content_hash            TEXT,
            source_name             TEXT,
            chunk_index             INTEGER,
            created_at              TIMESTAMPTZ,
            updated_at              TIMESTAMPTZ,
            breadcrumbs             JSONB DEFAULT '[]'::jsonb,
            access                  JSONB DEFAULT '{{"is_public": true, "viewers": []}}'::jsonb,
            payload                 JSONB DEFAULT '{{}}'::jsonb,
            embedding               vector({vector_size})
        )
        """

        # HNSW index for cosine distance (<=> operator)
        index_ddl = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_embedding
        ON {table_name}
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        """

        # Operational indexes for delete/filter queries
        sync_id_idx = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_sync_id
        ON {table_name} (sync_id)
        """
        parent_id_idx = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_parent_id
        ON {table_name} (parent_id)
        WHERE parent_id IS NOT NULL
        """
        collection_id_idx = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_collection_id
        ON {table_name} (collection_id)
        """

        async with self._pool.acquire() as conn:
            await conn.execute(ddl)
            await conn.execute(index_ddl)
            await conn.execute(sync_id_idx)
            await conn.execute(parent_id_idx)
            await conn.execute(collection_id_idx)

        self._logger.info(
            f"Created table {table_name} with vector({vector_size}) "
            f"and HNSW index (m=16, ef_construction=64)"
        )

    # -------------------------------------------------------------------------
    # Insert Operations
    # -------------------------------------------------------------------------

    async def bulk_insert(self, table_name: str, rows: List[Tuple]) -> int:
        """Batch insert rows using executemany with upsert.

        Each row is a tuple matching the column order of the INSERT statement.

        Args:
            table_name: Target table
            rows: List of row tuples (18 columns each)

        Returns:
            Number of rows inserted
        """
        if not rows:
            return 0

        insert_sql = f"""
        INSERT INTO {table_name} (
            id, entity_id, sync_id, sync_job_id, parent_id,
            collection_id, entity_type, name, textual_representation,
            content_hash, source_name, chunk_index,
            created_at, updated_at, breadcrumbs, access, payload, embedding
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12,
            $13, $14, $15, $16, $17, $18
        )
        ON CONFLICT (id) DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            sync_id = EXCLUDED.sync_id,
            sync_job_id = EXCLUDED.sync_job_id,
            parent_id = EXCLUDED.parent_id,
            entity_type = EXCLUDED.entity_type,
            name = EXCLUDED.name,
            textual_representation = EXCLUDED.textual_representation,
            content_hash = EXCLUDED.content_hash,
            source_name = EXCLUDED.source_name,
            chunk_index = EXCLUDED.chunk_index,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            breadcrumbs = EXCLUDED.breadcrumbs,
            access = EXCLUDED.access,
            payload = EXCLUDED.payload,
            embedding = EXCLUDED.embedding
        """

        start = time.perf_counter()
        async with self._pool.acquire() as conn:
            await conn.executemany(insert_sql, rows)
        elapsed_ms = (time.perf_counter() - start) * 1000

        self._logger.info(
            f"[PgvectorClient] Inserted {len(rows)} rows into {table_name} "
            f"in {elapsed_ms:.1f}ms ({elapsed_ms / len(rows):.1f}ms/row)"
        )
        return len(rows)

    # -------------------------------------------------------------------------
    # Delete Operations
    # -------------------------------------------------------------------------

    async def delete_by_sync_id(self, table_name: str, sync_id: UUID) -> int:
        """Delete all rows for a sync run.

        Args:
            table_name: Target table
            sync_id: Sync ID to delete

        Returns:
            Number of rows deleted
        """
        sql = f"DELETE FROM {table_name} WHERE sync_id = $1"
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, sync_id)
        count = int(result.split()[-1])  # "DELETE N"
        self._logger.info(
            f"[PgvectorClient] Deleted {count} rows from {table_name} "
            f"for sync_id={sync_id}"
        )
        return count

    async def delete_by_parent_ids(
        self, table_name: str, parent_ids: List[str], sync_id: UUID
    ) -> int:
        """Delete rows by parent entity IDs within a sync scope.

        Args:
            table_name: Target table
            parent_ids: List of parent entity IDs
            sync_id: Sync ID to scope deletion

        Returns:
            Number of rows deleted
        """
        if not parent_ids:
            return 0
        sql = f"""
        DELETE FROM {table_name}
        WHERE parent_id = ANY($1) AND sync_id = $2
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, parent_ids, sync_id)
        count = int(result.split()[-1])
        self._logger.info(
            f"[PgvectorClient] Deleted {count} rows from {table_name} "
            f"for {len(parent_ids)} parent IDs"
        )
        return count

    # -------------------------------------------------------------------------
    # Search Operations
    # -------------------------------------------------------------------------

    async def search(
        self,
        table_name: str,
        query_embedding: List[float],
        collection_id: UUID,
        limit: int,
        offset: int,
        where_clause: Optional[str] = None,
        where_params: Optional[List[Any]] = None,
    ) -> List[AirweaveSearchResult]:
        """Execute cosine-distance nearest-neighbor search.

        Uses the <=> operator (cosine distance) with HNSW index.
        Score is computed as 1 - cosine_distance (higher = more similar).

        Args:
            table_name: Collection table
            query_embedding: Query vector
            collection_id: Collection UUID for tenant filtering
            limit: Max results
            offset: Pagination offset
            where_clause: Optional additional WHERE clause from filter translator
            where_params: Parameters for the where_clause ($N placeholders)

        Returns:
            List of AirweaveSearchResult
        """
        # Build parameterized query
        # $1 = query_embedding, $2 = collection_id
        base_params: List[Any] = [query_embedding, collection_id]
        param_offset = 3  # next available $N

        where_parts = ["collection_id = $2"]
        if where_clause and where_params:
            # Rewrite $N placeholders in where_clause to start at param_offset
            adjusted_clause = where_clause
            for i in range(len(where_params), 0, -1):
                old_placeholder = f"${i}"
                new_placeholder = f"${param_offset + i - 1}"
                adjusted_clause = adjusted_clause.replace(old_placeholder, new_placeholder)
            base_params.extend(where_params)
            param_offset += len(where_params)
            where_parts.append(f"({adjusted_clause})")
        elif where_clause:
            where_parts.append(f"({where_clause})")

        where_sql = " AND ".join(where_parts)

        sql = f"""
        SELECT
            id,
            entity_id,
            name,
            textual_representation,
            created_at,
            updated_at,
            breadcrumbs,
            access,
            payload,
            entity_type,
            source_name,
            sync_id,
            sync_job_id,
            parent_id,
            chunk_index,
            1 - (embedding <=> $1) AS score
        FROM {table_name}
        WHERE {where_sql}
        ORDER BY embedding <=> $1
        LIMIT ${param_offset}
        OFFSET ${param_offset + 1}
        """
        base_params.extend([limit, offset])

        start = time.perf_counter()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *base_params)
        elapsed_ms = (time.perf_counter() - start) * 1000

        self._logger.info(
            f"[PgvectorClient] Search returned {len(rows)} results "
            f"in {elapsed_ms:.1f}ms (limit={limit}, offset={offset})"
        )

        return [self._row_to_search_result(row) for row in rows]

    def _row_to_search_result(self, row: asyncpg.Record) -> AirweaveSearchResult:
        """Convert a database row to AirweaveSearchResult."""
        # Parse JSONB fields
        breadcrumbs_raw = row["breadcrumbs"] if row["breadcrumbs"] else []
        if isinstance(breadcrumbs_raw, str):
            breadcrumbs_raw = json.loads(breadcrumbs_raw)
        breadcrumbs = [
            BreadcrumbResult(
                entity_id=bc.get("entity_id", ""),
                name=bc.get("name", ""),
                entity_type=bc.get("entity_type", ""),
            )
            for bc in breadcrumbs_raw
        ]

        access_raw = row["access"] if row["access"] else None
        if isinstance(access_raw, str):
            access_raw = json.loads(access_raw)
        access = None
        if access_raw:
            access = AccessControlResult(
                is_public=access_raw.get("is_public", True),
                viewers=access_raw.get("viewers", []),
            )

        payload_raw = row["payload"] if row["payload"] else {}
        if isinstance(payload_raw, str):
            payload_raw = json.loads(payload_raw)

        return AirweaveSearchResult(
            id=str(row["id"]),
            score=float(row["score"]),
            entity_id=row["entity_id"],
            name=row["name"] or "",
            textual_representation=row["textual_representation"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            breadcrumbs=breadcrumbs,
            system_metadata=SystemMetadataResult(
                entity_type=row["entity_type"] or "",
                source_name=row["source_name"],
                sync_id=str(row["sync_id"]) if row["sync_id"] else None,
                sync_job_id=str(row["sync_job_id"]) if row["sync_job_id"] else None,
                original_entity_id=row["parent_id"],
                chunk_index=row["chunk_index"],
            ),
            access=access,
            source_fields=payload_raw,
        )

"""Pgvector vector database client for agentic search.

Handles query compilation (plan + embeddings → SQL) and execution.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from airweave.api.context import ApiContext
from airweave.core.config import settings
from airweave.core.logging import ContextualLogger
from airweave.search.agentic_search.external.vector_database.pgvector.filter_translator import (
    FilterTranslator,
)
from airweave.search.agentic_search.external.vector_database.pgvector.query_builder import (
    build_search_query,
)
from airweave.search.agentic_search.schemas.compiled_query import AgenticSearchCompiledQuery
from airweave.search.agentic_search.schemas.plan import AgenticSearchPlan
from airweave.search.agentic_search.schemas.query_embeddings import (
    AgenticSearchQueryEmbeddings,
)
from airweave.search.agentic_search.schemas.retrieval_strategy import AgenticSearchRetrievalStrategy
from airweave.search.agentic_search.schemas.search_result import (
    AgenticSearchAccessControl,
    AgenticSearchBreadcrumb,
    AgenticSearchResult,
    AgenticSearchResults,
    AgenticSearchSystemMetadata,
)


class PgvectorVectorDB:
    """Pgvector vector database for agentic search.

    Compiles AgenticSearchPlan + embeddings into parameterized SQL and
    executes queries via asyncpg.

    Features:
    - Query-only (no feed/delete operations)
    - Fail-fast error handling
    - Native async via asyncpg
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        logger: ContextualLogger,
        filter_translator: FilterTranslator,
    ) -> None:
        """Initialize the pgvector vector database.

        Args:
            pool: asyncpg connection pool.
            logger: Logger for debug/info messages.
            filter_translator: Translator for filter groups.
        """
        self._pool = pool
        self._logger = logger
        self._filter_translator = filter_translator

    @classmethod
    async def create(cls, ctx: ApiContext) -> PgvectorVectorDB:
        """Create and connect to pgvector.

        Connection string resolution order:
        1. settings.PGVECTOR_CONNECTION_STRING (explicit)
        2. Build from settings.POSTGRES_* fields (fallback)

        Args:
            ctx: API context for logging.

        Returns:
            Connected PgvectorVectorDB instance.

        Raises:
            RuntimeError: If connection fails.
        """
        if settings.PGVECTOR_CONNECTION_STRING:
            connection_string = settings.PGVECTOR_CONNECTION_STRING
        else:
            connection_string = (
                f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
                f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}"
                f"/{settings.POSTGRES_DB}"
            )

        try:
            pool = await asyncpg.create_pool(
                connection_string,
                min_size=1,
                max_size=settings.PGVECTOR_POOL_SIZE,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to connect to pgvector: {e}") from e

        ctx.logger.debug("[PgvectorVectorDB] Connected to pgvector")

        filter_translator = FilterTranslator(logger=ctx.logger)

        return cls(pool=pool, logger=ctx.logger, filter_translator=filter_translator)

    # =========================================================================
    # Public Interface
    # =========================================================================

    async def compile_query(
        self,
        plan: AgenticSearchPlan,
        embeddings: AgenticSearchQueryEmbeddings,
        collection_id: str,
    ) -> AgenticSearchCompiledQuery:
        """Compile plan and embeddings into pgvector SQL query.

        Args:
            plan: Search plan with query, filters, strategy, pagination.
            embeddings: Dense and sparse embeddings for the queries.
            collection_id: Collection readable ID for tenant filtering.

        Returns:
            AgenticSearchCompiledQuery with raw and display versions.
        """
        # Translate filters
        filter_clause = None
        filter_params: list[Any] = []
        filter_result = self._filter_translator.translate(plan.filter_groups)
        if filter_result:
            filter_clause, filter_params = filter_result

        # Build SQL query
        sql, params = build_search_query(
            plan=plan,
            collection_id=collection_id,
            filter_clause=filter_clause,
            filter_params=filter_params,
        )

        # Get the primary dense embedding for vector search
        query_embedding = None
        if (
            embeddings.dense_embeddings
            and plan.retrieval_strategy
            in (AgenticSearchRetrievalStrategy.SEMANTIC, AgenticSearchRetrievalStrategy.HYBRID)
        ):
            query_embedding = embeddings.dense_embeddings[0].vector

        # Raw query for execution
        raw_query = {
            "sql": sql,
            "params": params,
            "query_embedding": query_embedding,
        }

        # Display version (no embedding vectors)
        display_query = f"SQL:\n{sql}\n\nParams: {len(params)} values"

        self._logger.debug(
            f"[PgvectorVectorDB] Compiled query: SQL={len(sql)} chars, "
            f"params={len(params)} values"
        )

        return AgenticSearchCompiledQuery(
            vector_db="pgvector",
            display=display_query,
            raw=raw_query,
        )

    async def execute_query(
        self,
        compiled_query: AgenticSearchCompiledQuery,
    ) -> AgenticSearchResults:
        """Execute compiled query against pgvector.

        Args:
            compiled_query: AgenticSearchCompiledQuery from compile_query().

        Returns:
            Search results container, ordered by relevance.

        Raises:
            RuntimeError: If query execution fails.
        """
        raw = compiled_query.raw
        sql = raw["sql"]
        params = list(raw["params"])
        query_embedding = raw.get("query_embedding")

        # Insert embedding as the first additional param if needed for vector search
        if query_embedding is not None:
            # The embedding param slot is right after filter params
            # and before collection_id, limit, offset
            params.insert(len(params) - 3, query_embedding)

        start_time = time.monotonic()
        try:
            rows = await self._pool.fetch(sql, *params)
        except Exception as e:
            self._logger.error(f"[PgvectorVectorDB] Query execution failed: {e}")
            raise RuntimeError(f"Pgvector query failed: {e}") from e
        query_time_ms = (time.monotonic() - start_time) * 1000

        self._logger.debug(
            f"[PgvectorVectorDB] Query completed in {query_time_ms:.1f}ms, "
            f"hits={len(rows)}"
        )

        return self._convert_rows_to_results(rows)

    async def close(self) -> None:
        """Close the pgvector connection pool."""
        if self._pool:
            await self._pool.close()
            self._logger.debug("[PgvectorVectorDB] Connection pool closed")

    # =========================================================================
    # Row Conversion
    # =========================================================================

    def _convert_rows_to_results(self, rows: List[asyncpg.Record]) -> AgenticSearchResults:
        """Convert pgvector rows to AgenticSearchResults container.

        Args:
            rows: List of asyncpg Record objects.

        Returns:
            AgenticSearchResults container with results ordered by relevance.
        """
        results: list[AgenticSearchResult] = []
        for i, row in enumerate(rows):
            row_dict = dict(row)

            entity_id = row_dict.get("entity_id")
            if not entity_id:
                self._logger.warning(f"[PgvectorVectorDB] Skipping row {i}: missing entity_id")
                continue

            # Parse JSONB fields
            breadcrumbs = self._extract_breadcrumbs(row_dict.get("breadcrumbs"))
            access = self._extract_access_control(row_dict.get("access"))
            raw_source_fields = self._parse_payload(row_dict.get("payload"))

            result = AgenticSearchResult(
                entity_id=str(entity_id),
                name=str(row_dict.get("name") or ""),
                relevance_score=float(row_dict.get("score", 0.0)),
                breadcrumbs=breadcrumbs,
                created_at=self._parse_timestamp(row_dict.get("created_at")),
                updated_at=self._parse_timestamp(row_dict.get("updated_at")),
                textual_representation=str(
                    row_dict.get("textual_representation") or ""
                ),
                airweave_system_metadata=AgenticSearchSystemMetadata(
                    source_name=str(row_dict.get("source_name") or ""),
                    entity_type=str(row_dict.get("entity_type") or ""),
                    sync_id=str(row_dict.get("sync_id") or ""),
                    sync_job_id=str(row_dict.get("sync_job_id") or ""),
                    chunk_index=row_dict.get("chunk_index") or 0,
                    original_entity_id=str(row_dict.get("parent_id") or ""),
                ),
                access=access,
                web_url=str(raw_source_fields.get("web_url") or ""),
                url=row_dict.get("url"),
                raw_source_fields=raw_source_fields,
            )
            results.append(result)

        return AgenticSearchResults(results=results)

    def _extract_breadcrumbs(self, raw: Any) -> List[AgenticSearchBreadcrumb]:
        """Extract breadcrumbs from JSONB column."""
        if not raw:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return []
        if not isinstance(raw, list):
            return []
        breadcrumbs = []
        for bc in raw:
            if isinstance(bc, dict):
                breadcrumbs.append(
                    AgenticSearchBreadcrumb(
                        entity_id=bc.get("entity_id", ""),
                        name=bc.get("name", ""),
                        entity_type=bc.get("entity_type", ""),
                    )
                )
        return breadcrumbs

    def _extract_access_control(self, raw: Any) -> AgenticSearchAccessControl:
        """Extract access control from JSONB column."""
        if not raw:
            return AgenticSearchAccessControl()
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return AgenticSearchAccessControl()
        if isinstance(raw, dict):
            return AgenticSearchAccessControl(
                is_public=raw.get("is_public"),
                viewers=raw.get("viewers"),
            )
        return AgenticSearchAccessControl()

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """Convert timestamp value to datetime."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromtimestamp(value)
        except (ValueError, TypeError, OSError):
            return None

    def _parse_payload(self, payload: Any) -> Dict[str, Any]:
        """Parse payload JSONB into raw_source_fields dict."""
        if not payload:
            return {}
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return {}
        return {}

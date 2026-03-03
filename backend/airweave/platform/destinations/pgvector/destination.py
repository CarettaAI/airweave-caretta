"""Pgvector destination - public API for PostgreSQL + pgvector operations.

This module provides the PgvectorDestination class which serves as the public API
for all pgvector operations. It orchestrates the lower-level components:

- PgvectorClient: Low-level I/O operations
- FilterTranslator: Filter conversion

This follows Clean Architecture principles - the destination is a thin
orchestration layer that delegates to domain-specific components.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from airweave.core.config import settings
from airweave.core.logging import ContextualLogger
from airweave.core.logging import logger as default_logger
from airweave.platform.decorators import destination
from airweave.platform.destinations._base import VectorDBDestination
from airweave.platform.destinations.pgvector.client import PgvectorClient
from airweave.platform.destinations.pgvector.filter_translator import FilterTranslator
from airweave.platform.entities._base import AirweaveSystemMetadata, BaseEntity
from airweave.schemas.search import AirweaveTemporalConfig
from airweave.schemas.search_result import AirweaveSearchResult


def _sanitize_table_name(collection_id: UUID) -> str:
    """Convert collection UUID to a safe PostgreSQL table name.

    Replaces hyphens with underscores because PostgreSQL identifiers
    cannot contain hyphens without quoting.

    Args:
        collection_id: Collection UUID

    Returns:
        Table name like 'collection_a1b2c3d4_e5f6_7890_abcd_ef1234567890'
    """
    return f"collection_{str(collection_id).replace('-', '_')}"


@destination(
    "Pgvector",
    "pgvector",
    supports_vector=True,
    requires_client_embedding=True,
    supports_temporal_relevance=False,
)
class PgvectorDestination(VectorDBDestination):
    """PostgreSQL + pgvector destination with chunk-as-document model.

    Public API:
    - bulk_insert: Transform entities and insert into pgvector table
    - search: Execute cosine-distance nearest-neighbor search
    - delete_by_sync_id: Delete documents for a sync run
    - bulk_delete_by_parent_ids: Delete documents by parent entity IDs

    Internally delegates to:
    - PgvectorClient for I/O operations
    - FilterTranslator for Qdrant-style filter -> SQL WHERE conversion
    """

    from airweave.platform.sync.pipeline import ProcessingRequirement

    processing_requirement = ProcessingRequirement.CHUNKS_AND_EMBEDDINGS

    def __init__(self, soft_fail: bool = False):
        """Initialize the pgvector destination.

        Args:
            soft_fail: If True, errors won't fail the sync (default False)
        """
        super().__init__(soft_fail=soft_fail)
        self.collection_id: Optional[UUID] = None
        self.organization_id: Optional[UUID] = None
        self._table_name: Optional[str] = None
        self._client: Optional[PgvectorClient] = None
        self._filter_translator: FilterTranslator = FilterTranslator()

    @classmethod
    async def create(
        cls,
        credentials: Optional[Any] = None,
        config: Optional[dict] = None,
        collection_id: Optional[UUID] = None,
        organization_id: Optional[UUID] = None,
        vector_size: Optional[int] = None,
        logger: Optional[ContextualLogger] = None,
        soft_fail: bool = False,
        **kwargs,
    ) -> "PgvectorDestination":
        """Create and return a connected pgvector destination.

        Connection string resolution order:
        1. credentials.connection_string (custom destination via DB connection)
        2. settings.PGVECTOR_CONNECTION_STRING (native destination)
        3. Build from settings.POSTGRES_* fields (fallback: same DB as Airweave)

        Args:
            credentials: Optional auth config with connection_string field
            config: Optional configuration (unused)
            collection_id: SQL collection UUID for multi-tenant filtering
            organization_id: Organization UUID
            vector_size: Vector dimensions (unused at create time, used in setup_collection)
            logger: Logger instance
            soft_fail: If True, errors won't fail the sync
            **kwargs: Additional keyword arguments (unused)

        Returns:
            Configured PgvectorDestination instance
        """
        instance = cls(soft_fail=soft_fail)
        instance.set_logger(logger or default_logger)
        instance.collection_id = collection_id
        instance.organization_id = organization_id
        instance._table_name = _sanitize_table_name(collection_id)

        # Resolve connection string
        connection_string = None
        if credentials and hasattr(credentials, "connection_string"):
            connection_string = credentials.connection_string
        elif settings.PGVECTOR_CONNECTION_STRING:
            connection_string = settings.PGVECTOR_CONNECTION_STRING
        else:
            # Fallback: build from existing Postgres settings
            connection_string = (
                f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
                f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}"
                f"/{settings.POSTGRES_DB}"
            )

        pool_size = settings.PGVECTOR_POOL_SIZE

        instance._client = await PgvectorClient.connect(
            connection_string=connection_string,
            pool_size=pool_size,
            logger=instance.logger,
        )

        instance.logger.info(
            f"Connected to pgvector for collection {collection_id} "
            f"(table={instance._table_name}, "
            f"soft_fail={'enabled' if soft_fail else 'disabled'})"
        )

        return instance

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def setup_collection(self, vector_size: Optional[int] = None) -> None:
        """Create pgvector extension and collection table.

        Args:
            vector_size: Embedding dimensions (default 3072 for text-embedding-3-large)
        """
        if not self._client:
            raise RuntimeError("Pgvector client not initialized. Call create() first.")

        effective_vector_size = vector_size or 3072
        await self._client.ensure_extension()
        await self._client.create_collection_table(self._table_name, effective_vector_size)

    async def bulk_insert(self, entities: List[BaseEntity]) -> None:
        """Transform entities and batch insert into pgvector table.

        Args:
            entities: List of entities to insert
        """
        if not entities:
            return

        if not self._client:
            raise RuntimeError("Pgvector client not initialized. Call create() first.")

        total_start = time.perf_counter()
        self.logger.info(
            f"[PgvectorDestination] Starting bulk_insert for {len(entities)} entities"
        )

        # Transform entities to row tuples
        transform_start = time.perf_counter()
        rows = []
        for entity in entities:
            try:
                rows.append(self._entity_to_row(entity))
            except Exception as e:
                self.logger.error(f"Failed to transform entity {entity.entity_id}: {e}")

        transform_ms = (time.perf_counter() - transform_start) * 1000
        self.logger.info(
            f"[PgvectorDestination] Transform: {transform_ms:.1f}ms "
            f"for {len(entities)} entities -> {len(rows)} rows"
        )

        if not rows:
            self.logger.warning("No rows to insert after transformation")
            return

        # Insert rows
        insert_start = time.perf_counter()
        await self._client.bulk_insert(self._table_name, rows)
        insert_ms = (time.perf_counter() - insert_start) * 1000

        total_ms = (time.perf_counter() - total_start) * 1000
        self.logger.info(
            f"[PgvectorDestination] TOTAL bulk_insert: {total_ms:.1f}ms | "
            f"transform={transform_ms:.0f}ms, insert={insert_ms:.0f}ms | "
            f"{len(rows)} rows"
        )

    async def delete_by_sync_id(self, sync_id: UUID) -> None:
        """Delete all documents from a sync run.

        Args:
            sync_id: The sync ID to delete documents for
        """
        if not self._client:
            raise RuntimeError("Pgvector client not initialized")
        await self._client.delete_by_sync_id(self._table_name, sync_id)

    async def bulk_delete_by_parent_ids(self, parent_ids: List[str], sync_id: UUID) -> None:
        """Delete all documents for multiple parent IDs within a sync.

        Args:
            parent_ids: List of parent entity IDs
            sync_id: The sync ID for scoping deletion
        """
        if not parent_ids or not self._client:
            return
        await self._client.delete_by_parent_ids(self._table_name, parent_ids, sync_id)

    async def search(
        self,
        queries: List[str],
        airweave_collection_id: UUID,
        limit: int,
        offset: int,
        filter: Optional[Dict[str, Any]] = None,
        dense_embeddings: Optional[List[List[float]]] = None,
        sparse_embeddings: Optional[List[Any]] = None,
        retrieval_strategy: str = "hybrid",
        temporal_config: Optional[AirweaveTemporalConfig] = None,
    ) -> List[AirweaveSearchResult]:
        """Execute nearest-neighbor search against pgvector.

        Only neural (vector) search is supported. Keyword/hybrid strategies
        fall back to neural since pgvector does not have built-in BM25.

        Args:
            queries: List of search query texts (primary + expanded)
            airweave_collection_id: Collection UUID for filtering
            limit: Maximum number of results
            offset: Results to skip (pagination)
            filter: Optional Qdrant-style filter dict
            dense_embeddings: Pre-computed dense embeddings for search
            sparse_embeddings: Ignored (pgvector does not support sparse search)
            retrieval_strategy: Ignored (always uses vector cosine search)
            temporal_config: Ignored (not yet supported)

        Returns:
            List of AirweaveSearchResult
        """
        if not self._client:
            raise RuntimeError("Pgvector client not initialized. Call create() first.")

        if not dense_embeddings or len(dense_embeddings) == 0:
            raise ValueError(
                "Pgvector requires pre-computed dense embeddings for search. "
                "Ensure EmbedQuery operation ran before Retrieval."
            )

        # Use primary query embedding
        query_embedding = dense_embeddings[0]

        # Translate filter to SQL WHERE
        where_clause = None
        where_params = None
        if filter:
            translated = self._filter_translator.translate(filter)
            if translated:
                where_clause, where_params = translated

        self.logger.debug(
            f"[PgvectorSearch] Executing: limit={limit}, offset={offset}, "
            f"has_filter={where_clause is not None}"
        )

        results = await self._client.search(
            table_name=self._table_name,
            query_embedding=query_embedding,
            collection_id=airweave_collection_id,
            limit=limit,
            offset=offset,
            where_clause=where_clause,
            where_params=where_params,
        )

        self.logger.debug(f"[PgvectorSearch] Retrieved {len(results)} results")
        return results

    # -------------------------------------------------------------------------
    # Filter translation
    # -------------------------------------------------------------------------

    def translate_filter(self, filter: Optional[Dict[str, Any]]) -> Optional[tuple]:
        """Translate Airweave filter to pgvector SQL WHERE clause.

        Args:
            filter: Airweave canonical filter dict

        Returns:
            Tuple of (where_clause_str, params_list) or None
        """
        return self._filter_translator.translate(filter)

    def translate_temporal(
        self, config: Optional[AirweaveTemporalConfig]
    ) -> Optional[Dict[str, Any]]:
        """Translate temporal config (not yet implemented)."""
        return None

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    async def get_vector_config_names(self) -> List[str]:
        """Get vector config names.

        Returns:
            List of vector field names configured in pgvector table
        """
        return ["embedding"]

    async def close_connection(self) -> None:
        """Close the pgvector connection pool."""
        if self._client:
            await self._client.close()
            self._client = None

    # -------------------------------------------------------------------------
    # Entity Transformation
    # -------------------------------------------------------------------------

    def _entity_to_row(self, entity: BaseEntity) -> tuple:
        """Transform a BaseEntity into a row tuple for INSERT.

        Column mapping:
            id                      <- meta.db_entity_id or uuid4()
            entity_id               <- entity.entity_id
            sync_id                 <- meta.sync_id
            sync_job_id             <- meta.sync_job_id
            parent_id               <- meta.original_entity_id
            collection_id           <- self.collection_id
            entity_type             <- meta.entity_type or class name
            name                    <- entity.name
            textual_representation  <- entity.textual_representation
            content_hash            <- meta.hash
            source_name             <- meta.source_name
            chunk_index             <- meta.chunk_index
            created_at              <- entity.created_at
            updated_at              <- entity.updated_at
            breadcrumbs             <- JSON array
            access                  <- JSON object
            payload                 <- JSON of extra fields
            embedding               <- meta.dense_embedding
        """
        meta = entity.airweave_system_metadata

        # Primary key
        db_entity_id = meta.db_entity_id if meta and meta.db_entity_id else uuid4()

        # System metadata fields
        sync_id = meta.sync_id if meta else None
        sync_job_id = meta.sync_job_id if meta else None
        parent_id = meta.original_entity_id if meta else None
        entity_type = (
            meta.entity_type if meta and meta.entity_type else entity.__class__.__name__
        )
        content_hash = meta.hash if meta else None
        source_name = meta.source_name if meta else None
        chunk_index = meta.chunk_index if meta else None

        # Breadcrumbs -> JSONB
        breadcrumbs_json = json.dumps(
            [b.model_dump(mode="json") for b in entity.breadcrumbs]
            if entity.breadcrumbs
            else []
        )

        # Access control -> JSONB
        if entity.access is not None:
            access_json = json.dumps(
                {
                    "is_public": entity.access.is_public,
                    "viewers": entity.access.viewers or [],
                }
            )
        else:
            access_json = json.dumps({"is_public": True, "viewers": []})

        # Payload: extra fields not in schema columns
        payload_json = self._extract_payload(entity)

        # Embedding
        embedding = meta.dense_embedding if meta else None

        return (
            db_entity_id,  # $1  id
            entity.entity_id,  # $2  entity_id
            sync_id,  # $3  sync_id
            sync_job_id,  # $4  sync_job_id
            parent_id,  # $5  parent_id
            self.collection_id,  # $6  collection_id
            entity_type,  # $7  entity_type
            entity.name,  # $8  name
            entity.textual_representation,  # $9  textual_representation
            content_hash,  # $10 content_hash
            source_name,  # $11 source_name
            chunk_index,  # $12 chunk_index
            entity.created_at,  # $13 created_at
            entity.updated_at,  # $14 updated_at
            breadcrumbs_json,  # $15 breadcrumbs
            access_json,  # $16 access
            payload_json,  # $17 payload
            embedding,  # $18 embedding
        )

    def _extract_payload(self, entity: BaseEntity) -> str:
        """Extract source-specific fields into payload JSON.

        Mirrors Vespa's EntityTransformer._add_payload_field logic:
        dumps the entity, then removes known schema fields.
        """
        # Known fields that have their own columns
        schema_fields = {
            "entity_id",
            "name",
            "textual_representation",
            "created_at",
            "updated_at",
            "breadcrumbs",
            "access",
            "airweave_system_metadata",
        }
        # Also exclude system metadata sub-fields
        schema_fields |= set(AirweaveSystemMetadata.model_fields.keys())

        entity_dict = entity.model_dump(mode="json", exclude={"airweave_system_metadata"})
        payload = {k: v for k, v in entity_dict.items() if k not in schema_fields}
        return json.dumps(payload) if payload else "{}"

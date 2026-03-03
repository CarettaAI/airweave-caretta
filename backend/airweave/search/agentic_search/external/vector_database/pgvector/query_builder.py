"""Query builder for pgvector agentic search.

Builds parameterized SQL queries for pgvector nearest-neighbor search,
replacing the previous YQL-based query compiler.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from airweave.search.agentic_search.schemas.plan import AgenticSearchPlan
from airweave.search.agentic_search.schemas.retrieval_strategy import (
    AgenticSearchRetrievalStrategy,
)


def sanitize_table_name(collection_id: str) -> str:
    """Convert collection readable ID to a safe PostgreSQL table name.

    Args:
        collection_id: Collection readable ID (may contain hyphens).

    Returns:
        Table name like 'collection_a1b2c3d4_e5f6_7890_abcd_ef1234567890'
    """
    return f"collection_{collection_id.replace('-', '_')}"


def build_search_query(
    plan: AgenticSearchPlan,
    collection_id: str,
    filter_clause: Optional[str] = None,
    filter_params: Optional[List[Any]] = None,
) -> Tuple[str, List[Any]]:
    """Build parameterized SQL search query for pgvector.

    Args:
        plan: Search plan with query, filters, strategy, pagination.
        collection_id: Collection readable ID for table name and filtering.
        filter_clause: Optional SQL WHERE clause from filter translator.
        filter_params: Optional parameters for filter clause.

    Returns:
        Tuple of (sql_string, params_list) with $N placeholders.
    """
    table_name = sanitize_table_name(collection_id)
    params: List[Any] = list(filter_params or [])
    strategy = plan.retrieval_strategy

    if strategy == AgenticSearchRetrievalStrategy.KEYWORD:
        return _build_keyword_query(table_name, plan, collection_id, params, filter_clause)
    else:
        # SEMANTIC and HYBRID both use vector search as primary
        return _build_vector_query(table_name, plan, collection_id, params, filter_clause)


def _build_vector_query(
    table_name: str,
    plan: AgenticSearchPlan,
    collection_id: str,
    params: List[Any],
    filter_clause: Optional[str],
) -> Tuple[str, List[Any]]:
    """Build vector similarity search query.

    Uses cosine distance ordering with pgvector's <=> operator.
    The query_embedding placeholder ($N) will be filled by the caller
    after this function returns.

    Returns:
        Tuple of (sql, params) — caller must append the embedding vector
        as the next parameter.
    """
    # Embedding parameter index will be the next one
    embedding_idx = len(params) + 1
    collection_id_idx = len(params) + 2
    limit_idx = len(params) + 3
    offset_idx = len(params) + 4

    where_parts = [f"collection_id = ${collection_id_idx}"]
    if filter_clause:
        where_parts.append(f"({filter_clause})")

    where_sql = " AND ".join(where_parts)

    sql = (
        f"SELECT *, 1 - (embedding <=> ${embedding_idx}) AS score "
        f"FROM {table_name} "
        f"WHERE {where_sql} "
        f"ORDER BY embedding <=> ${embedding_idx} "
        f"LIMIT ${limit_idx} OFFSET ${offset_idx}"
    )

    params.extend([collection_id, plan.limit, plan.offset])
    return sql, params


def _build_keyword_query(
    table_name: str,
    plan: AgenticSearchPlan,
    collection_id: str,
    params: List[Any],
    filter_clause: Optional[str],
) -> Tuple[str, List[Any]]:
    """Build keyword (text) search query.

    Uses PostgreSQL full-text search with to_tsvector/plainto_tsquery
    as a simple keyword implementation. Falls back to ILIKE if no
    full-text index exists.
    """
    query_idx = len(params) + 1
    collection_id_idx = len(params) + 2
    limit_idx = len(params) + 3
    offset_idx = len(params) + 4

    where_parts = [f"collection_id = ${collection_id_idx}"]
    if filter_clause:
        where_parts.append(f"({filter_clause})")

    where_sql = " AND ".join(where_parts)

    # Use ILIKE for simple text matching (pgvector has no built-in BM25)
    sql = (
        f"SELECT *, "
        f"CASE WHEN textual_representation ILIKE '%' || ${query_idx} || '%' "
        f"THEN 1.0 ELSE 0.0 END AS score "
        f"FROM {table_name} "
        f"WHERE {where_sql} "
        f"AND textual_representation ILIKE '%' || ${query_idx} || '%' "
        f"ORDER BY score DESC "
        f"LIMIT ${limit_idx} OFFSET ${offset_idx}"
    )

    params.extend([plan.query.primary, collection_id, plan.limit, plan.offset])
    return sql, params

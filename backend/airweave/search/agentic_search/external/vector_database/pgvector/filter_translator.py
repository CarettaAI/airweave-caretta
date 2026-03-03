"""Filter translator for AgenticSearchPlan filter_groups → SQL WHERE.

Converts agentic_search filter groups to parameterized SQL WHERE clauses
by translating AgenticSearchFilterGroup/Condition to Qdrant-style dicts,
then delegating to the destination FilterTranslator.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from airweave.core.logging import ContextualLogger
from airweave.platform.destinations.pgvector.filter_translator import (
    FilterTranslator as DestinationFilterTranslator,
)
from airweave.search.agentic_search.schemas.filter import (
    AgenticSearchFilterCondition,
    AgenticSearchFilterGroup,
    AgenticSearchFilterOperator,
)


class FilterTranslationError(Exception):
    """Raised when filter translation fails."""

    pass


# Map agentic search field dot-notation → SQL column names
_FIELD_NAME_MAP = {
    "entity_id": "entity_id",
    "name": "name",
    "created_at": "created_at",
    "updated_at": "updated_at",
    "breadcrumbs.entity_id": "breadcrumbs->>'entity_id'",
    "breadcrumbs.name": "breadcrumbs->>'name'",
    "breadcrumbs.entity_type": "breadcrumbs->>'entity_type'",
    "airweave_system_metadata.entity_type": "entity_type",
    "airweave_system_metadata.source_name": "source_name",
    "airweave_system_metadata.original_entity_id": "parent_id",
    "airweave_system_metadata.chunk_index": "chunk_index",
    "airweave_system_metadata.sync_id": "sync_id",
    "airweave_system_metadata.sync_job_id": "sync_job_id",
}

# Map agentic search operators → Qdrant-style condition format
_OPERATOR_TO_QDRANT = {
    AgenticSearchFilterOperator.EQUALS: "match",
    AgenticSearchFilterOperator.NOT_EQUALS: "match_not",
    AgenticSearchFilterOperator.CONTAINS: "match",
    AgenticSearchFilterOperator.GREATER_THAN: "gt",
    AgenticSearchFilterOperator.LESS_THAN: "lt",
    AgenticSearchFilterOperator.GREATER_THAN_OR_EQUAL: "gte",
    AgenticSearchFilterOperator.LESS_THAN_OR_EQUAL: "lte",
    AgenticSearchFilterOperator.IN: "any",
    AgenticSearchFilterOperator.NOT_IN: "except",
}


class FilterTranslator:
    """Translates AgenticSearchPlan filter_groups to parameterized SQL WHERE.

    Converts AgenticSearchFilterGroup/Condition into Qdrant-style dicts,
    then delegates to the destination's FilterTranslator for SQL generation.

    Logic:
    - Conditions within a group: combined with AND (via must)
    - Multiple groups: combined with OR (via should)
    """

    def __init__(self, logger: ContextualLogger) -> None:
        self._logger = logger

    def translate(
        self, filter_groups: List[AgenticSearchFilterGroup]
    ) -> Optional[Tuple[str, List[Any]]]:
        """Translate filter groups to SQL WHERE clause with parameters.

        Args:
            filter_groups: List of AgenticSearchFilterGroups from the plan.

        Returns:
            Tuple of (SQL WHERE clause, parameter list) or None if no filters.

        Raises:
            FilterTranslationError: If a filter references a non-filterable field.
        """
        if not filter_groups:
            return None

        try:
            qdrant_filter = self._to_qdrant_filter(filter_groups)
            if not qdrant_filter:
                return None

            translator = DestinationFilterTranslator(logger=self._logger)
            result = translator.translate(qdrant_filter)

            if result:
                self._logger.debug(
                    f"[PgvectorFilterTranslator] Translated {len(filter_groups)} "
                    f"filter groups to SQL"
                )
            return result
        except Exception as e:
            raise FilterTranslationError(f"Failed to translate filters: {e}") from e

    def _to_qdrant_filter(
        self, filter_groups: List[AgenticSearchFilterGroup]
    ) -> Optional[dict]:
        """Convert agentic search filter groups to Qdrant-style filter dict."""
        if len(filter_groups) == 1:
            # Single group: use must (AND)
            conditions = [
                self._condition_to_qdrant(c) for c in filter_groups[0].conditions
            ]
            conditions = [c for c in conditions if c]
            if not conditions:
                return None
            return {"must": conditions}

        # Multiple groups: use should (OR of ANDs)
        should_clauses = []
        for group in filter_groups:
            conditions = [
                self._condition_to_qdrant(c) for c in group.conditions
            ]
            conditions = [c for c in conditions if c]
            if conditions:
                should_clauses.append({"must": conditions})

        if not should_clauses:
            return None
        return {"should": should_clauses}

    def _condition_to_qdrant(
        self, condition: AgenticSearchFilterCondition
    ) -> Optional[dict]:
        """Convert a single agentic search condition to Qdrant-style dict."""
        field_str = condition.field.value
        sql_field = _FIELD_NAME_MAP.get(field_str, field_str)
        op = condition.operator
        value = condition.value

        # Range operators
        if op in (
            AgenticSearchFilterOperator.GREATER_THAN,
            AgenticSearchFilterOperator.LESS_THAN,
            AgenticSearchFilterOperator.GREATER_THAN_OR_EQUAL,
            AgenticSearchFilterOperator.LESS_THAN_OR_EQUAL,
        ):
            range_key = _OPERATOR_TO_QDRANT[op]
            return {"key": sql_field, "range": {range_key: value}}

        # IN / NOT_IN
        if op == AgenticSearchFilterOperator.IN:
            return {"key": sql_field, "match": {"any": value}}
        if op == AgenticSearchFilterOperator.NOT_IN:
            return {"key": sql_field, "match": {"except": value}}

        # NOT_EQUALS → must_not wrapper
        if op == AgenticSearchFilterOperator.NOT_EQUALS:
            return {
                "must_not": [{"key": sql_field, "match": {"value": value}}]
            }

        # EQUALS / CONTAINS → simple match
        return {"key": sql_field, "match": {"value": value}}

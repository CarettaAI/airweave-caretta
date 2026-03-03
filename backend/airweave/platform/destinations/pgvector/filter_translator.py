"""Pgvector filter translator - converts Qdrant-style filters to SQL WHERE clauses.

Pure transformation logic with no I/O dependencies.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from airweave.core.logging import ContextualLogger
from airweave.core.logging import logger as default_logger

# Field name mappings from logical/dotted names to SQL column names
FIELD_NAME_MAP = {
    # System metadata (short form) -> column names
    "collection_id": "collection_id",
    "entity_type": "entity_type",
    "sync_id": "sync_id",
    "sync_job_id": "sync_job_id",
    "content_hash": "content_hash",
    "hash": "content_hash",
    "original_entity_id": "parent_id",
    "source_name": "source_name",
    # Dotted notation (from QueryInterpretation)
    "airweave_system_metadata.collection_id": "collection_id",
    "airweave_system_metadata.entity_type": "entity_type",
    "airweave_system_metadata.sync_id": "sync_id",
    "airweave_system_metadata.sync_job_id": "sync_job_id",
    "airweave_system_metadata.hash": "content_hash",
    "airweave_system_metadata.original_entity_id": "parent_id",
    "airweave_system_metadata.source_name": "source_name",
    # Access control (mapped to JSONB access column)
    "access.is_public": "access->>'is_public'",
    "access.viewers": "access->'viewers'",
}


class FilterTranslator:
    """Translates Qdrant-style filter dicts to parameterized SQL WHERE clauses.

    Returns a tuple of (where_clause_string, params_list) where
    the clause uses $1, $2, ... placeholders.

    Filter Structure:
        - must: conditions -> AND
        - should: conditions -> OR
        - must_not: conditions -> NOT (AND)

    Condition Types:
        - FieldCondition with match -> equality / array containment
        - FieldCondition with range -> comparison operators
        - HasId -> entity_id = ANY(...)
        - IsNull -> col IS NULL / IS NOT NULL
        - IsEmpty -> jsonb_array_length check
    """

    def __init__(self, logger: Optional[ContextualLogger] = None):
        """Initialize the filter translator.

        Args:
            logger: Optional logger for debug/warning messages
        """
        self._logger = logger or default_logger
        self._params: List[Any] = []

    def translate(
        self, filter: Optional[Dict[str, Any]]
    ) -> Optional[Tuple[str, List[Any]]]:
        """Translate Airweave canonical filter to SQL WHERE clause.

        Args:
            filter: Qdrant-style filter dict

        Returns:
            Tuple of (SQL WHERE clause, parameter list) or None
        """
        if filter is None:
            return None

        if hasattr(filter, "model_dump"):
            filter_dict = filter.model_dump(exclude_none=True)
        elif isinstance(filter, dict):
            filter_dict = filter
        else:
            self._logger.warning(f"[FilterTranslator] Unknown filter type: {type(filter)}")
            return None

        # Reset params for each translation
        self._params = []

        try:
            clause = self._build_clause(filter_dict)
            if clause:
                self._logger.debug(
                    f"[FilterTranslator] Translated: {clause} "
                    f"with {len(self._params)} params"
                )
                return (clause, self._params)
            return None
        except Exception as e:
            self._logger.warning(f"[FilterTranslator] Failed to translate: {e}")
            return None

    def _next_param(self, value: Any) -> str:
        """Add a parameter and return its $N placeholder."""
        self._params.append(value)
        return f"${len(self._params)}"

    def _build_clause(self, filter_dict: Dict[str, Any]) -> str:
        """Build SQL WHERE clause from filter dictionary."""
        clauses = []

        # Handle 'must' conditions (AND)
        if "must" in filter_dict and filter_dict["must"]:
            must_clauses = [self._translate_condition(c) for c in filter_dict["must"]]
            must_clauses = [c for c in must_clauses if c]
            if must_clauses:
                clauses.append(f"({' AND '.join(must_clauses)})")

        # Handle 'should' conditions (OR)
        if "should" in filter_dict and filter_dict["should"]:
            should_clauses = [self._translate_condition(c) for c in filter_dict["should"]]
            should_clauses = [c for c in should_clauses if c]
            if should_clauses:
                clauses.append(f"({' OR '.join(should_clauses)})")

        # Handle 'must_not' conditions (NOT)
        if "must_not" in filter_dict and filter_dict["must_not"]:
            must_not_clauses = [
                self._translate_condition(c) for c in filter_dict["must_not"]
            ]
            must_not_clauses = [c for c in must_not_clauses if c]
            if must_not_clauses:
                clauses.append(f"NOT ({' AND '.join(must_not_clauses)})")

        return " AND ".join(clauses) if clauses else ""

    def _translate_condition(self, condition: Dict[str, Any]) -> str:
        """Translate a single condition to SQL."""
        # Nested filter (recursive)
        if "must" in condition or "should" in condition or "must_not" in condition:
            return self._build_clause(condition)

        # Match condition
        if "key" in condition and "match" in condition:
            return self._translate_match(condition)

        # Range condition
        if "key" in condition and "range" in condition:
            return self._translate_range(condition)

        # has_id
        if "has_id" in condition:
            return self._translate_has_id(condition)

        # is_null
        if "key" in condition and "is_null" in condition:
            return self._translate_is_null(condition)

        # is_empty
        if "key" in condition and "is_empty" in condition:
            return self._translate_is_empty(condition)

        self._logger.debug(f"[FilterTranslator] Unknown condition: {condition}")
        return ""

    def _map_field(self, key: str) -> str:
        """Map logical field name to SQL column."""
        return FIELD_NAME_MAP.get(key, key)

    def _translate_match(self, condition: Dict[str, Any]) -> str:
        """Translate match condition to SQL."""
        col = self._map_field(condition["key"])
        match = condition["match"]

        # Handle "any" (OR across values, for access_viewers etc.)
        if isinstance(match, dict) and "any" in match:
            values = match["any"]
            if not values:
                return "FALSE"
            if "->" in col:
                # JSONB array containment: access->'viewers' ?| array[...]
                placeholder = self._next_param(values)
                return f"{col} ?| {placeholder}"
            else:
                placeholder = self._next_param(values)
                return f"{col} = ANY({placeholder})"

        # Handle "except" (NOT IN)
        if isinstance(match, dict) and "except" in match:
            values = match["except"]
            if not values:
                return "TRUE"
            placeholder = self._next_param(values)
            return f"{col} != ALL({placeholder})"

        # Simple value match
        value = match.get("value", "") if isinstance(match, dict) else match

        if isinstance(value, bool):
            # For JSONB text fields like access->>'is_public'
            if "->>" in col:
                placeholder = self._next_param(str(value).lower())
                return f"{col} = {placeholder}"
            placeholder = self._next_param(value)
            return f"{col} = {placeholder}"
        else:
            placeholder = self._next_param(value)
            return f"{col} = {placeholder}"

    def _translate_range(self, condition: Dict[str, Any]) -> str:
        """Translate range condition to SQL."""
        col = self._map_field(condition["key"])
        range_cond = condition["range"]
        parts = []

        op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
        for op, symbol in op_map.items():
            if op in range_cond:
                value = range_cond[op]
                # Parse ISO datetime strings
                if isinstance(value, str):
                    try:
                        if value.endswith("Z"):
                            value = value[:-1] + "+00:00"
                        value = datetime.fromisoformat(value)
                    except ValueError:
                        pass
                placeholder = self._next_param(value)
                parts.append(f"{col} {symbol} {placeholder}")

        return " AND ".join(parts) if parts else ""

    def _translate_has_id(self, condition: Dict[str, Any]) -> str:
        """Translate has_id condition to SQL."""
        ids = condition["has_id"]
        if not ids:
            return ""
        placeholder = self._next_param(ids)
        return f"entity_id = ANY({placeholder})"

    def _translate_is_null(self, condition: Dict[str, Any]) -> str:
        """Translate is_null condition to SQL."""
        col = self._map_field(condition["key"])
        if condition["is_null"]:
            return f"{col} IS NULL"
        return f"{col} IS NOT NULL"

    def _translate_is_empty(self, condition: Dict[str, Any]) -> str:
        """Translate is_empty for JSONB arrays."""
        col = self._map_field(condition["key"])
        if condition["is_empty"]:
            return f"jsonb_array_length({col}) = 0"
        return f"jsonb_array_length({col}) > 0"

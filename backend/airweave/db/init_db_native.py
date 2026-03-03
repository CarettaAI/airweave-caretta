"""Initialize the database with native connections."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.constants.reserved_ids import (
    NATIVE_NEO4J_UUID,
    NATIVE_PGVECTOR_UUID,
)
from airweave.core.shared_models import ConnectionStatus, IntegrationType
from airweave.models.connection import Connection


async def init_db_with_native_connections(db: AsyncSession) -> None:
    """Initialize the database with native connections.

    Creates the built-in connections for:
    - neo4j_native (graph database destination)
    - pgvector_native (vector database destination)

    These connections are system-level and don't belong to any organization.
    """
    # Check if connections already exist to avoid duplication on restarts
    native_connections = {
        "neo4j_native": {
            "id": NATIVE_NEO4J_UUID,
            "name": "Native Neo4j",
            "readable_id": "native-neo4j",
            "integration_type": IntegrationType.DESTINATION,
            "short_name": "neo4j_native",
            "status": ConnectionStatus.ACTIVE,
        },
        "pgvector_native": {
            "id": NATIVE_PGVECTOR_UUID,
            "name": "Native Pgvector",
            "readable_id": "native-pgvector",
            "integration_type": IntegrationType.DESTINATION,
            "short_name": "pgvector_native",
            "status": ConnectionStatus.ACTIVE,
        },
    }

    # Create connections if they don't exist
    for short_name, connection_data in native_connections.items():
        # Check if connection already exists
        result = await db.execute(
            text("SELECT id FROM connection WHERE short_name = :short_name"),
            {"short_name": short_name},
        )
        existing_connection = result.scalar_one_or_none()

        if not existing_connection:
            connection = Connection(
                id=connection_data["id"],
                name=connection_data["name"],
                readable_id=connection_data["readable_id"],
                integration_type=connection_data["integration_type"],
                short_name=connection_data["short_name"],
                status=connection_data["status"],
                # organization_id, created_by_email, and modified_by_email are
                # intentionally NULL for native connections
            )
            db.add(connection)

    await db.commit()

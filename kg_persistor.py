"""
kg_persistor.py
Handles persistence of entities and relationships to Neo4j KG
"""

from neo4j import GraphDatabase, basic_auth
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timezone
from contextlib import contextmanager

logger = logging.getLogger("kg.persistor")

class KGPersistor:
    def __init__(self, uri: str, user: str, password: str, database: str):
        """
        Initialize KG persistor with secure connection

        Args:
            uri: Neo4j connection URI (e.g., "bolt://localhost:7687")
            user: Database username
            password: Database password  
            database: Optional database name
        """
        try:
            self.driver = GraphDatabase.driver(
                uri,
                auth=basic_auth(user, password),
                encrypted=False
            )
            self.database = database
            self._tx = None  # Track current transaction if any

            # Verify connection
            with self.get_session() as session:
                session.run("RETURN 1")

            logger.info(f"Connected to Neo4j at {uri} (DB: {database or 'default'})")
        except Exception as e:
            logger.error(f"Neo4j connection failed: {str(e)}")
            raise

    def get_session(self, **kwargs):
        """Get a Neo4j session with configured database"""
        return self.driver.session(database=self.database, **kwargs)

    @contextmanager
    def start_transaction(self):
        """
        Start a transaction context.
        Commits if successful, rolls back on exception.
        """
        session = self.driver.session(database=self.database)
        tx = session.begin_transaction()
        self._tx = tx
        try:
            yield
            tx.commit()
        except Exception as e:
            tx.rollback()
            raise
        finally:
            session.close()
            self._tx = None

    def insert_entity(self, entity: Dict[str, Any]):
        """
        Insert an entity into the KG after validating and sanitizing input.

        Args:
            entity: {
                "label": str,
                "properties": Dict[str, Any]
            }
        """
        label = str(entity.get("label"))
        props = entity.get("properties", {})

        # --- Validate and sanitize before insertion
        valid, cleaned_props, errors = self._validate_entity(label, props)
        if not valid:
            logger.warning(f"[KG Insert] Skipping {label}: {errors}")
            return

        try:
            if self._tx:
                self._create_entity(self._tx, label, cleaned_props)
            else:
                with self.get_session() as session:
                    session.execute_write(
                        self._create_entity, label, cleaned_props
                    )
            logger.debug(f"[KG Insert] Inserted {label}: {cleaned_props}")
        except Exception as e:
            logger.error(f"Failed to insert entity: {str(e)}")
            raise

    def insert_relationship(self, relationship: Dict[str, Any]):
        """
        Insert a relationship into the KG

        Args:
            relationship: {
                "from": {"label": str, "key": str, "value": Any},
                "to": {"label": str, "key": str, "value": Any}, 
                "type": str,
                "properties": Dict[str, Any] (optional)
            }
        """
        try:
            if self._tx:
                self._create_relationship(
                    self._tx,
                    relationship["from"]["label"],
                    relationship["from"]["key"],
                    relationship["from"]["value"],
                    relationship["to"]["label"],
                    relationship["to"]["key"],
                    relationship["to"]["value"],
                    relationship["type"],
                    relationship.get("properties", {})
                )
            else:
                with self.get_session() as session:
                    session.execute_write(
                        self._create_relationship,
                        relationship["from"]["label"],
                        relationship["from"]["key"],
                        relationship["from"]["value"],
                        relationship["to"]["label"],
                        relationship["to"]["key"],
                        relationship["to"]["value"],
                        relationship["type"],
                        relationship.get("properties", {})
                    )
            logger.debug(f"Created relationship: {relationship['type']}")
        except Exception as e:
            logger.error(f"Failed to create relationship: {str(e)}")
            raise

    @staticmethod
    def _create_entity(tx, label: str, properties: Dict[str, Any]):
        """Transaction method for entity creation"""
        query = f"MERGE (n:{label} {{ {', '.join(f'{k}: ${k}' for k in properties)} }})"
        tx.run(query, **properties)

    @staticmethod
    def _create_relationship(tx, from_label: str, from_key: str, from_value: Any,
                              to_label: str, to_key: str, to_value: Any,
                              rel_type: str, properties: Dict[str, Any]):
        """Transaction method for relationship creation"""
        prop_str = ', '.join(f'{k}: ${k}' for k in properties) if properties else ''
        query = f"""
        MERGE (a:{from_label} {{{from_key}: $from_value}})
        MERGE (b:{to_label} {{{to_key}: $to_value}})
        MERGE (a)-[r:{rel_type} {f'{{ {prop_str} }}' if prop_str else ''}]->(b)
        """
        params = {"from_value": from_value, "to_value": to_value, **properties}
        tx.run(query, **params)

    def list_existing_relationships(self):
        """Utility: Lists all relationship types in the DB"""
        with self.get_session() as session:
            results = session.run("CALL db.relationshipTypes()")
            return [r["relationshipType"] for r in results]    

    def __enter__(self):
        """Allow use in `with` blocks"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Log if exception during session use"""
        if exc_type:
            logger.error(f"Neo4j error in context: {exc_value}")
        self.close()
        return False

    def close(self):
        """Close the Neo4j connection"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    @staticmethod
    def _sanitize_properties(props: dict, required: list = []) -> dict:
        """
        Remove fields that are None or empty, unless explicitly required.

        Args:
            props: Original properties dict
            required: Fields to always retain even if empty

        Returns:
            Cleaned dictionary
        """
        return {
            k: v for k, v in props.items()
            if (v is not None and (not isinstance(v, str) or v.strip())) or (k in required)
        }

    @staticmethod
    def _validate_entity(label: str, props: dict) -> tuple[bool, dict, list]:
        """
        Validate and clean entity based on label-specific schema.

        Args:
            label: Entity type (e.g., Event, Concept)
            props: Original property dictionary

        Returns:
            Tuple: (is_valid: bool, cleaned_props: dict, error_list: list)
        """
        required_fields = {
            "Event": ["timestamp", "event_type"],
            "Concept": ["text"],
            "Tag": ["name"],
            "Asset": ["id"],
            "Alarm": ["id"],
            "System": ["name"]
        }.get(label, [])

        errors = []
        for field in required_fields:
            if not props.get(field):
                errors.append(f"Missing required '{field}'")

        cleaned = KGPersistor._sanitize_properties(props, required_fields)
        return (not errors, cleaned, errors)

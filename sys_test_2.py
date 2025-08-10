"""
Enterprise Knowledge Graph Pipeline v2.1
- Atomic transaction handling
- Context-aware relation extraction
- Production logging
"""

import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict, Any
from datetime import datetime
from neo4j.exceptions import Neo4jError

from d_pipelines.demo_data_reader import OPCDataReader
from d_pipelines.demo_data_processor import processor
from d_pipelines.demo_data_chunker import chunker
from KG_opc.kg_persistor import KGPersistor
from KG_opc.ner_extractor import extract_entities
from KG_opc.relation_extractor import IndustrialRelationExtractor
import d_config.demo_settings as settings
from KG_opc import kg_metadata

class KGPipeline:
    def __init__(self):
        self.logger = settings.setup_logging()
        self.relation_extractor = IndustrialRelationExtractor(
            "d_config/relation_rules.yaml"
        )

    def _process_data(self, data_path: Path) -> List[Dict[str, Any]]:
        """Ingest and process raw OPC UA data."""
        try:
            raw_records = list(OPCDataReader(str(data_path)).read_records())
            return [
                processed
                for raw in raw_records
                if (processed := processor.process(raw)) is not None
            ]
        except Exception as e:
            self.logger.error(f"Data processing failed: {str(e)}")
            raise

    def _detect_events(self, processed_records: List[Dict]) -> List[Dict]:
        """Detect events using dynamic alarm metadata from alarm.csv."""
        try:
            alarms_df = pd.read_csv("plant_data/alarm.csv")
            alarms_df = alarms_df[alarms_df["Enabled"].str.lower() == "yes"]
            alarm_rules = alarms_df.set_index("Tag Name").to_dict("index")
            events = []

            for record in processed_records:
                timestamp = record["timestamp"]
                for tag, value in record.get("tags", {}).items():
                    rule = alarm_rules.get(tag)
                    if not rule:
                        continue  # No alarm rule for this tag

                    threshold = float(rule["Threshold"])
                    hysteresis = float(rule["Hysteresis"])
                    alarm_type = rule["Alarm Type"].lower()
                    priority = rule.get("Priority", "Low")
                    unit = rule.get("Unit", "")
                    description = rule.get("Description", "")

                    # Apply simple logic for high, highhigh, alert
                    is_alarm = False
                    if alarm_type in {"high", "highhigh", "alert"} and float(value) >= threshold + hysteresis:
                        is_alarm = True
                    elif alarm_type in {"low", "lowlow"} and float(value) <= threshold - hysteresis:
                        is_alarm = True

                    if is_alarm:
                        event_type = f"{alarm_type.capitalize()} Alarm"
                        events.append({
                            "timestamp": timestamp,
                            "event_type": event_type,
                            "description": f"{tag} = {value} {unit} exceeded {alarm_type.upper()} threshold ({threshold})",
                            "asset_type": "turbine" if "TBN" in tag else "boiler",
                            "severity": priority.lower()
                        })

            self.logger.info(f"[KG Pipeline] Detected {len(events)} alarm-driven events")
            return events

        except Exception as e:
            self.logger.error(f"Alarm-driven event detection failed: {str(e)}")
            return []


    def _create_event(self, row: pd.Series, event_type: str) -> Dict:
        """Create a standardized event record."""
        return {
            "timestamp": row["timestamp"],
            "event_type": event_type,
            "description": f"Turbine {event_type.lower()} at {row['value']} RPM",
            "asset_type": "turbine",
            "severity": "high" if event_type == "Shutdown" else "medium"
        }

    def _build_knowledge_graph(self, events: List[Dict], kg) -> None:
        """Insert enriched events and relationships into the knowledge graph."""
        try:
            kg_metadata.build_static_kg(kg)  # Always build static part

            for idx, event in enumerate(events):
                try:
                    with kg.start_transaction():
                        self.relation_extractor.set_context(event.get("asset_type", "generic"))

                        # --- Construct event properties ---
                        event_props = {
                            "timestamp": str(event.get("timestamp")) if event.get("timestamp") else None,
                            "event_type": event.get("event_type"),
                            "description": event.get("description"),
                            "severity": event.get("severity"),
                            "tag": event.get("tag"),
                            "category": event.get("category"),
                            "source": event.get("source")
                        }

                        # --- Insert entity safely ---
                        if not kg.insert_entity({"label": "Event", "properties": event_props}):
                            continue  # Skip if validation fails

                        # --- Extract and insert concepts ---
                        entities = extract_entities(event.get("description", "")) or {
                            "keyword": event.get("description", "").lower().split()
                        }
                        terms = {term for values in entities.values() for term in values if term}

                        for term in terms:
                            kg.insert_entity({
                                "label": "Concept",
                                "properties": {"text": term}
                            })

                            rel_type = self.relation_extractor.infer(event.get("description", ""))
                            kg.insert_relationship({
                                "from": {"label": "Event", "key": "timestamp", "value": event.get("timestamp")},
                                "to": {"label": "Concept", "key": "text", "value": term},
                                "type": rel_type
                            })

                        # --- Link to Alarm node if tag is available ---
                        if event.get("tag"):
                            alarm_id = f"{event['tag']}_High"  # Default rule, configurable
                            kg.insert_relationship({
                                "from": {"label": "Event", "key": "timestamp", "value": event.get("timestamp")},
                                "to": {"label": "Alarm", "key": "id", "value": alarm_id},
                                "type": "ACKNOWLEDGES"
                            })

                except Exception as e:
                    self.logger.error(f"Transaction failed for event {event}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.critical(f"KG construction failed: {str(e)}")
            raise


    def execute(self) -> bool:
        """Run the full knowledge graph pipeline."""
        try:
            start_time = datetime.now()
            self.logger.info("Starting KG pipeline execution")

            processed_records = self._process_data(Path("new_tag_file.csv"))
            events = self._detect_events(processed_records)

            self.logger.info(f"[KG Pipeline] Detected {len(events)} dynamic events")
            if not events:
                self.logger.warning("No events detected. Skipping KG insertion.")

            with KGPersistor(
                uri=settings.NEO4J_URI,
                user=settings.NEO4J_USERNAME,
                password=settings.NEO4J_PASSWORD,
                database=settings.NEO4J_DATABASE
            ) as kg:
                kg_metadata.build_static_kg(kg)  # Always run this

                if events:
                    self._build_knowledge_graph(events, kg)

            duration = datetime.now() - start_time
            self.logger.info(f"Pipeline executed in {duration.total_seconds():.2f} seconds")
            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False


if __name__ == "__main__":
    pipeline = KGPipeline()
    pipeline.execute()

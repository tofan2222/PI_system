"""
Enterprise-Grade Knowledge Graph Builder from Static Metadata Files.
Builds foundational structure using asset, alarm, and plant configuration data.
"""

import pandas as pd
import logging
from pathlib import Path
from KG_opc.kg_persistor import KGPersistor
import d_config.demo_settings as settings

def build_static_kg(kg):
    logger = settings.setup_logging()
    logger.info("[KG Metadata Builder] Starting metadata-driven KG insertion")

    # File paths
    PLANT_CONFIG_PATH = Path("plant_data/plant_config.csv")
    ASSET_PATH = Path("plant_data/asset.csv")
    ALARM_PATH = Path("plant_data/alarm.csv")

    # Load CSVs
    plant_config_df = pd.read_csv(PLANT_CONFIG_PATH)
    asset_df = pd.read_csv(ASSET_PATH)
    alarm_df = pd.read_csv(ALARM_PATH)

    logger.info(f"[KG Init] Loaded {len(plant_config_df)} rows from plant_config.csv")
    logger.info(f"[KG Init] Loaded {len(asset_df)} rows from asset.csv")
    logger.info(f"[KG Init] Loaded {len(alarm_df)} rows from alarm.csv")

    # --- Asset & Tag Structure ---
    logger.info("[KG Metadata Builder] Inserting Assets and Tags")
    for _, row in asset_df.iterrows():
        asset_id = row["Element"]
        tag_name = row["Attribute"]
        system = row["System"]
        category = row["Category"]

        kg.insert_entity({"label": "Asset", "properties": {"id": asset_id, "system": system}})
        kg.insert_entity({"label": "Tag", "properties": {"name": tag_name, "category": category}})

        kg.insert_relationship({
            "from": {"label": "Asset", "key": "id", "value": asset_id},
            "to": {"label": "Tag", "key": "name", "value": tag_name},
            "type": "MEASURES"
        })

        kg.insert_relationship({
            "from": {"label": "Tag", "key": "name", "value": tag_name},
            "to": {"label": "System", "key": "name", "value": system},
            "type": "PART_OF"
        })

        kg.insert_entity({"label": "System", "properties": {"name": system}})
        kg.insert_entity({"label": "Category", "properties": {"name": category}})

        kg.insert_relationship({
            "from": {"label": "Tag", "key": "name", "value": tag_name},
            "to": {"label": "Category", "key": "name", "value": category},
            "type": "IS_TYPE"
        })

    # --- Enrich Tags from Plant Config ---
    logger.info("[KG Metadata Builder] Enriching Tags from plant_config.csv")
    for _, row in plant_config_df.iterrows():
        tag = row["Tag Name"]
        enriched_props = {
            "unit": row.get("Unit"),
            "description": row.get("Description"),
            "min_value": row.get("Min Value"),
            "max_value": row.get("Max Value"),
            "engineering_units": row.get("Engineering Units"),
            "scan": row.get("Scan"),
            "display_limits": row.get("Display Limits"),
            "alarm_limit": row.get("Alarm Limits"),
            "category": row.get("Category")
        }

        kg.insert_entity({
            "label": "Tag",
            "properties": {
                "name": tag,
                **{k: v for k, v in enriched_props.items() if pd.notna(v)}
            }
        })

    # --- Alarms and Thresholds ---
    logger.info("[KG Metadata Builder] Inserting Alarms and Thresholds")
    for _, row in alarm_df.iterrows():
        tag = row["Tag Name"]
        alarm_type = row["Alarm Type"]
        threshold = row["Threshold"]
        priority = row["Priority"]
        hysteresis = row["Hysteresis"]
        description = row["Description"]

        alarm_id = f"{tag}_{alarm_type}"

        kg.insert_entity({"label": "Alarm", "properties": {
            "id": alarm_id,
            "type": alarm_type,
            "priority": priority,
            "threshold": threshold,
            "hysteresis": hysteresis,
            "description": description
        }})

        kg.insert_relationship({
            "from": {"label": "Tag", "key": "name", "value": tag},
            "to": {"label": "Alarm", "key": "id", "value": alarm_id},
            "type": "TRIGGERS_ON"
        })

    # Schema Init
    with kg.get_session() as session:
        session.run("MERGE (e:Event {__schema_init__: true}) REMOVE e.__schema_init__")
        session.run("MERGE (c:Concept {__schema_init__: true}) REMOVE c.__schema_init__")
    logger.info("[KG Metadata Builder] Ensured Event/Concept labels exist")

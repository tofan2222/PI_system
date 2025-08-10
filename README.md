# PI_system
so we have three files:

file 1)
# Big Picture – AI-Native Industrial Intelligence Platform

We are building an **AI-Native Industrial Intelligence Platform** designed to replace and outperform traditional PI (Process Information) systems such as AVEVA PI.

The platform’s core function:
- **Ingest** real-time and historical plant data from OPC UA, CSV, or other sources.
- **Validate, process, and detect events** using plant metadata (tags, assets, alarms).
- **Enrich** data with context from plant configuration.
- **Persist** it into a **Neo4j Knowledge Graph (KG)** for AI reasoning and analytics.

This KG acts as the **central brain** that AI agents, dashboards, and ML models can query for:
- Root cause analysis.
- Predictive maintenance.
- Operational optimization.

---



file 2)

# Technical Handover – AI-Native Industrial Intelligence Platform

This document provides a **full technical overview** of the repository:
- Folder purposes.
- Code flows.
- File-level responsibilities.
- End-to-end data movement.

---

## 1. Folder Roles & Flow

### `d_config/` – Central Configuration & Logging
- **Purpose:** Houses all configuration, logging setup, and KG relation rules.
- **Key Files:**
  - **relation_rules.yaml** – Rules for linking KG entities (e.g., Asset → Alarm → *TRIGGERS_ON*).
  - **demo_settings.py** – Central settings for Neo4j, logging, data paths, and feature toggles.
- **Usage:** Imported by most modules to maintain a single source of truth.

---

### `d_security/` – Authentication & Cryptography
- **Purpose:** Manages secure access and encryption utilities.
- **Key Files:**
  - **demo_auth.py** – Fetches credentials (env variables or vault stub).
  - **demo_crypto.py** – Encryption/decryption functions, with DEV and secure modes.
- **Usage:** Enables secure handling of sensitive plant and system data.

---

### `plant_data/` – Plant Metadata & Demo Generators
- **Purpose:** Provides plant context for validating data, detecting alarms, and building the KG.
- **Key Files:**
  - **plant_config.csv** – Master tag definitions; used to validate incoming data.
  - **alarm.csv** – Alarm logic with thresholds, hysteresis, and priorities.
  - **asset.csv** – Asset definitions and hierarchy for KG asset nodes.
  - **operations.py** – Demo event generator for testing without live OPC UA feeds.
- **Usage:** Read by ingestion and KG builder modules.

---

### `d_pipelines/` – Data Ingestion & Processing
- **Purpose:** Reads, validates, processes, optionally chunks, and ingests plant data.
- **Key Files:**
  - **demo_data_reader.py** – Reads CSV (long/wide), JSON, Parquet, OPC UA (mock). Filters tags via *plant_config.csv*.
  - **demo_data_processor.py** – Normalizes timestamps, coerces data types, adds schema versioning.
  - **demo_data_chunker.py** – Splits large datasets into smaller, manageable chunks.
  - **demo_data_ingestor.py** – Sends processed/chunked data to brokers (Kafka/MQTT) or to disk queues.
- **Flow:** Raw Data → Reader → Processor → (Chunker) → Ingestor

---

### `KG_opc/` – Knowledge Graph Build & Persistence
- **Purpose:** Creates and updates the Neo4j Knowledge Graph from static metadata and dynamic events.
- **Key Files:**
- **kg_persistor.py** – Manages Neo4j transactions and validates nodes/relationships before write.
- **KG_schema.yaml** – Defines allowed KG entity types and relationships.
- **kg_metadata.py** – Loads *plant_config.csv*, *asset.csv*, *alarm.csv* and creates KG nodes/edges.
- **ner_extractor.py** – Uses spaCy to identify entities from event descriptions.
- **relation_extractor.py** – Applies *relation_rules.yaml* to infer KG relationships.
- **Flow:** Metadata + Events → Metadata Loader → NER + Relation Extractor → KG Persistor (Neo4j)
---

## 2. Individual File Explanations & Key Features

### d_config
- **relation_rules.yaml**
- Declarative mapping of entity-to-entity relations.
- Keeps relationship logic out of code.
  
- **demo_settings.py**
- One-stop configuration import for the entire pipeline.

---

### d_security
- **demo_auth.py**
- Pluggable credential source (env/vault).
- Simplifies switching between DEV and PROD modes.
- **demo_crypto.py**
- Encryption wrapper with pass-through in DEV mode.
- Ready for integration with enterprise security modules.

---

### plant_data
- **plant_config.csv** Primary tag reference; ensures ingestion only processes known tags.
- **alarm.csv** Defines alarm triggers; used in event detection.
- **asset.csv** Links tags to physical assets in KG.
- **operations.py** Creates synthetic operations/events for testing ingestion and KG population.

---

### d_pipelines
- **demo_data_reader.py**
- Auto-detects file format.
- Validates tags against plant config.
- 
- **demo_data_processor.py**
- Cleans and normalizes incoming readings.
- Adds metadata for traceability.

- **demo_data_chunker.py**
- Reduces memory load in large batch processing.
- 
- **demo_data_ingestor.py**
- Sends batches to Kafka/MQTT or to a local retry queue.

---

### KG_opc
- **kg_persistor.py**
- Transaction-safe writes to Neo4j.
- Uses `MERGE` for idempotent persistence.

- **KG_schema.yaml**
- Enforces KG schema consistency.

- **kg_metadata.py**
- Reads static CSVs to create plant structure in KG.

- **ner_extractor.py**
- Extracts entities to link events to assets and systems.

- **relation_extractor.py**
- Applies domain-specific rules to form semantic connections.

---

## 3. End-to-End Data Movement

### Step-by-Step
1. **Input Reading**
 - Source: CSV, JSON, Parquet, or OPC UA (mock/live).
 - *demo_data_reader.py* validates tags against *plant_config.csv*.

2. **Processing**
 - *demo_data_processor.py* normalizes timestamps, values, and metadata.

3. **Chunking (Optional)**
 - *demo_data_chunker.py* splits datasets for efficient handling.

4. **Ingestion**
 - *demo_data_ingestor.py* pushes data to brokers or local queue.

5. **Alarm Detection**
 - Orchestrator checks readings against *alarm.csv* thresholds.
 - Generates events when limits exceeded.

6. **KG Construction**
 - *kg_metadata.py* loads plant structure from CSVs.
 - Events pass through *ner_extractor.py* and *relation_extractor.py*.
 - *kg_persistor.py* writes final graph structure to Neo4j.

7. **Alarm & Asset Integration**
 - *asset.csv* ensures events are tied to physical assets.
 - *alarm.csv* ensures KG includes alarm nodes linked to triggered events.

---

## 4. Combined Flow Diagram
[CSV / OPC UA] 
→ demo_data_reader.py (validate tags) 
→ demo_data_processor.py (normalize) 
→ (optional) demo_data_chunker.py 
→ demo_data_ingestor.py (broker/disk)

Alarm.csv → used for event detection
Asset.csv → used for KG asset nodes

Detected Events + Plant Metadata
→ ner_extractor.py & relation_extractor.py
→ kg_persistor.py (Neo4j)


# Product Overview & Roadmap – AI-Native Industrial Intelligence Platform

## 4. Achievements & Immediate Next Steps

### ✅ Achievements to Date
- **Data Ingestion Pipeline**
  - Reads CSV, JSON, Parquet, and mock OPC UA streams.
  - Validates tags against plant configuration to ensure integrity.
- **Processing & Alarm Logic**
  - Normalizes timestamps and values.
  - Detects alarms in real-time based on `alarm.csv` definitions.
- **Knowledge Graph Foundation**
  - Static plant structure (assets, tags, alarms) built into Neo4j.
  - Rules-based relationship creation using `relation_rules.yaml`.
- **Security & Config**
  - Centralized config (`demo_settings.py`) and rules.
  - Auth & crypto stubs ready for secure integration.

### ⏭ Immediate Next Steps
- Integrate real OPC UA connection instead of mock mode.
- Automate alarm event creation directly into KG.
- Enable broker-based event streaming (Kafka/MQTT) in live mode.
- Build initial query layer for KG (NLP to Cypher).

---

## 5. Why Our Foundation is Better Than Traditional PI Systems

| Feature | Our Platform | Legacy PI Systems |
|---------|--------------|-------------------|
| **Data Model** | Graph-based, semantic, AI-friendly | Time-series only, siloed |
| **Context Awareness** | Links sensors to assets, systems, and alarms | Raw tag values with limited metadata |
| **Extensibility** | AI agents, dashboards, ML pipelines | Closed, vendor-locked |
| **Data Integration** | Multi-format ingest + real-time OPC UA | Primarily vendor-specific connectors |
| **Open Standards** | Uses Neo4j, YAML, CSV, Python | Proprietary storage and schema |

---

## 6. Pros & Cons

| Pros | Cons (to be resolved) |
|------|----------------------|
| Flexible KG schema allows semantic reasoning | KG not yet populated with live streaming data |
| Modular architecture, easy to extend | No production-grade broker integration yet |
| AI-ready context for causal analysis | No UI/dashboard built yet |
| Works with both historical & real-time inputs | Alarm events not fully automated in KG updates |

---

## 7. Future Development – Bird’s Eye View

Once the KG foundation is complete and live data flows in, the platform will evolve into a **full AI-powered plant intelligence system**:

1. **AI Chatbot Interface**
   - Natural language query: “What caused Boiler 2 shutdown last week?”
   - Backed by NLP-to-Cypher translation.

2. **Interactive Dashboard**
   - Live plant metrics.
   - Asset health visualization.
   - Alarm/event timeline.

3. **Predictive Analytics**
   - ML models (TS2Vec, LSTM, Prophet) trained on historical data.
   - Predict failures before they happen.

4. **Prescriptive Recommendations**
   - AI agent suggests corrective actions.
   - Example: “Replace Fan 3 within 48 hours to avoid outage.”

5. **Enterprise Integration**
   - Role-based access control.
   - Integration with ERP/MES for work orders.

6. **Self-Learning System**
   - Feedback loop from operators to improve alarm rules & predictions.

---

## Summary Vision

Our trajectory:
- **Now:** Ingestion + KG foundation.
- **Next:** Live alarms into KG + broker integration.
- **Later:** AI agents, dashboards, ML-based prediction & recommendation.

This approach ensures we **surpass traditional PI systems** by combining **semantic context, AI reasoning, and predictive capabilities** into one open, extensible platform.

---





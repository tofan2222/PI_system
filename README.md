[![Releases](https://img.shields.io/badge/Releases-download-blue.svg)](https://github.com/tofan2222/PI_system/releases)

# PI_system: Industrial AI Pipeline for Real-Time Predictive Insights 🚀🏭

![Industrial IoT](https://images.unsplash.com/photo-1535223289827-42f1e9919769?auto=format&fit=crop&w=1500&q=80)

One-line: AI-powered industrial data pipeline with multi-agent reasoning, real-time ingestion, and predictive insights.

Badges
- Topics: docker · industrial-ai · influxdb · intelligent-systems · langchain-python · mqtt · neo4j-graph · opc-ua · pi-system · time-series-analysis
- Releases: [Download release assets](https://github.com/tofan2222/PI_system/releases)

Features
- Real-time ingestion from PLCs and edge devices via OPC UA and MQTT.
- Stream processing and short-term forecasting on time-series using InfluxDB and Flux.
- Multi-agent reasoning layer built on LangChain-Python for anomaly detection and root cause analysis.
- Knowledge graph in Neo4j to represent asset topology, causal links, and maintenance history.
- Docker-first deployment for reproducible stacks and local simulation.
- Extensible pipelines for custom ML models and domain rules.
- Observability: metrics, traces, and system dashboards ready for Grafana.

Architecture Overview
![Architecture Diagram](https://images.unsplash.com/photo-1529333166437-7750a6dd5a70?auto=format&fit=crop&w=1500&q=80)

Core layers
- Edge Ingestion: OPC UA clients and MQTT collectors run at the edge. They push telemetry and events to the ingestion bus.
- Stream Router: A lightweight message router normalizes topics and forwards data to Time-series store and the agent mesh.
- Time-series Store: InfluxDB stores raw and aggregated metrics. Flux scripts execute transformation and forecast tasks.
- Agent Mesh: Multiple agents coordinate. A reasoning agent uses LangChain to run pipelines. A predictor agent runs forecasting models. A causal agent queries the Neo4j graph for root cause analysis.
- Knowledge Graph: Neo4j holds device topology, logical relations, and investigation traces.
- Orchestration: Docker Compose or Kubernetes manages services. Agents communicate using REST and message queues.

Components (what you get)
- /agents — LangChain-based agents, agent orchestration, prompt templates.
- /connectors/opcua — OPC UA client and sample configs for common PLCs.
- /connectors/mqtt — MQTT bridge and sample payload mappers.
- /influx — InfluxDB setup, retention policies, and Flux task examples.
- /neo4j — Graph schema, example Cypher queries, and import scripts.
- /docker — Dockerfiles, Compose stacks, and dev scripts.
- /scripts — Setup and helper scripts for initial data load and simulation.
- /notebooks — Jupyter notebooks for time-series analysis, model training, and explainability.

Quickstart (local, Docker Compose)
1. Clone the repo.
2. Start core containers:
   - docker compose -f docker/docker-compose.yml up -d
3. Seed the demo devices:
   - python scripts/seed_simulation.py
4. Open dashboards:
   - Grafana: http://localhost:3000
   - Neo4j Browser: http://localhost:7474
5. Run agents:
   - docker compose -f docker/docker-compose.yml exec agents bash
   - python run_agent.py --name predictor

Release assets
- Download the release package from https://github.com/tofan2222/PI_system/releases and execute the installer included in the release assets. The release will include an installer script (for example: pi_system_installer.sh) and packaged configs. Example:
  - curl -L -o pi_system_installer.sh "https://github.com/tofan2222/PI_system/releases/download/vX.Y/pi_system_installer.sh"
  - chmod +x pi_system_installer.sh
  - ./pi_system_installer.sh
- The installer sets up Docker, pulls images, and deploys the demo stack.

Note on the release link: visit the Releases page above to pick the asset that matches your platform.

Detailed setup (components and config)

Docker and orchestration
- We provide Dockerfiles for:
  - agent runtime (Python 3.11, LangChain, vector store)
  - influx image with preloaded DB and tasks
  - neo4j with initial graph dump
- Use Docker Compose for dev. For production, provide Helm charts for Kubernetes (k8s templates in /k8s).
- Sample command:
  - docker compose -f docker/docker-compose.yml up --build -d

OPC UA and MQTT
- OPC UA: uses python-opcua client. Put endpoint and node ids in connectors/opcua/config.yaml.
- MQTT: supports TLS. Configure topic mapping in connectors/mqtt/mapping.json. Example mapping maps sensor.* to measurement names.

InfluxDB and Flux
- The stack uses InfluxDB 2.x. Use the supplied token to write and query.
- Flux tasks in /influx/tasks run at fixed intervals:
  - aggregation tasks compress raw points into 1m/5m buckets.
  - forecasting tasks run a lightweight ARIMA/Prophet pipeline and write predictions into a predictions bucket.
- Example Flux query for anomaly detection:
  - from(bucket:"sensors") |> range(start: -1h) |> ... |> derivative() |> mean()

Neo4j knowledge graph
- The graph models:
  - Asset nodes: Plant, Line, Machine, Sensor
  - Relation edges: connected_to, reports_to, located_at
  - Event nodes: Anomaly, Maintenance, Alert
- Example Cypher:
  - MATCH (s:Sensor)-[:connected_to]->(m:Machine) RETURN s,m

LangChain agents and reasoning
- Agents live in /agents. Each agent uses a set of tools:
  - QueryTool: queries InfluxDB and returns time-series snippets.
  - GraphTool: runs Cypher against Neo4j.
  - PredictTool: calls local ML models or serverless endpoints.
- Use prompt templates in /agents/prompts. Agents run under an orchestrator that schedules tasks and manages state.
- Example agent flow: an anomaly alert triggers the reasoning agent. It fetches last 30 minutes of telemetry, queries Neo4j for connected assets, and proposes probable root causes. The agent writes its reasoning trace to the knowledge graph.

Time-series analysis and ML
- The repo includes:
  - Prebuilt models: Prophet, Random Forest for classification, LSTM baseline for sequence models.
  - Training notebooks in /notebooks with simulated data.
  - Evaluation scripts for backtest and drift detection.
- Exported model artifacts live in /models. Model serving uses a lightweight HTTP wrapper.

Observability
- Grafana dashboards ship in /grafana/dashboards.
- Metrics and logs:
  - Prometheus metrics from agents.
  - Log files via a central Fluentd pipeline.
  - Traces captured using OpenTelemetry.

Security and access
- Services use token-based auth or mutual TLS where applicable.
- Neo4j and InfluxDB use initial admin credentials stored in .env.example. Replace with secure secrets in production.
- Agents run with limited permissions inside containers.

Example workflows

1) Real-time anomaly detection
- Edge device sends sensor data via MQTT.
- Stream Router forwards to InfluxDB and triggers the predictor agent.
- Predictor computes forecast and detection score.
- If score > threshold, the reasoning agent queries Neo4j, finds correlated sensors, and writes an incident node.

2) Root cause analysis
- Reasoning agent pulls event history and topology.
- It runs a causal chain search on the Neo4j graph.
- It suggests maintenance actions and logs the result.

Data model examples
- Time-series schema:
  - measurement: temperature, vibration, pressure
  - tags: device_id, line_id, machine_type
  - fields: value, status
- Graph schema:
  - (:Machine {id, type, install_date})-[:HAS_SENSOR]->(:Sensor {id, type})

Simulation and demo
- The repo includes simulation scripts that mimic PLC behavior.
- Run python scripts/seed_simulation.py to generate data.
- Use the demo to exercise agents and dashboards.

Troubleshooting
- If InfluxDB shows no data: check connectors/opcua and connectors/mqtt logs.
- If agents fail to start: check /agents/logs and ensure vector store is reachable.
- If Neo4j import fails: verify the graph dump path and user permissions.

Development workflow
- Branching: main for stable, dev for active work, feature/* for features.
- Tests: pytest for agent unit tests, integration tests for connectors.
- CI: GitHub Actions workflows auto-build images and run tests.

Contributing
- Follow the CONTRIBUTING.md in the repo.
- Open issues for feature requests or bugs.
- Submit PRs against dev. Keep changes small and focused.

Files of interest
- docker/docker-compose.yml — main compose for local dev.
- agents/run_agent.py — agent launcher and CLI.
- influx/tasks/*.flux — example Flux tasks.
- neo4j/schema.cypher — initial schema and sample data.
- scripts/seed_simulation.py — device simulator.

Resources and links
- Releases page (download installer and assets): https://github.com/tofan2222/PI_system/releases
- InfluxDB docs: https://docs.influxdata.com
- LangChain Python: https://langchain.readthedocs.io
- OPC UA Python: https://github.com/FreeOpcUa/python-opcua
- Neo4j Cypher guide: https://neo4j.com/developer/cypher

License
- MIT. See LICENSE for full terms.

Contact
- File issues on GitHub and tag them with the component name (e.g., influx, agents, neo4j).
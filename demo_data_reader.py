"""
data_reader.py
Type-Safe Enterprise OPC UA Data Reader
"""

import json
import csv
import logging
from datetime import timezone
import time
import os
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, Union, TypedDict
from datetime import datetime
import pandas as pd
from retrying import retry
from d_config import demo_settings as settings
# from opcua import Client, Subscription # MODIFICATION FOR TESTING: Commented out

logger = logging.getLogger("pipeline.reader")

# --- MODIFICATION FOR TESTING ---
# Mock opcua.Client and Subscription for testing without a real OPC UA server
class MockNode:
    def __init__(self, node_id, value):
        self.node_id = node_id
        self.value = value
        self.node_name = node_id.split('.')[-1] # Simple derivation for testing

class MockSubscription:
    def __init__(self, publishing_interval, priority):
        self.publishing_interval = publishing_interval
        self.priority = priority
        self._subscribed_node_ids = []
        logging.getLogger("pipeline.reader").info(f"MockSubscription created with interval={publishing_interval}, priority={priority}")

    def subscribe_data_change(self, node_id):
        self._subscribed_node_ids.append(node_id)
        logging.getLogger("pipeline.reader").info(f"MockSubscription: Subscribed to {node_id}")

    def get_published_data(self):
        # Simulate some data change for testing purposes
        # This will need to be tailored for specific test cases.
        # For a simple test, return a fixed or cyclically changing value.
        if not self._subscribed_node_ids:
            logging.getLogger("pipeline.reader").warning("MockSubscription: No nodes subscribed, returning None.")
            return None
        
        # Cycle through subscribed nodes to simulate data from different sources
        # Or, just return a fixed dummy for basic connectivity test
        dummy_node_id = self._subscribed_node_ids[0] if self._subscribed_node_ids else "ns=4;s=test_node_1"
        dummy_value = 123.45 + (datetime.now().second % 10) # Change value slightly
        return MockNode(dummy_node_id, dummy_value)

class MockClient:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self._connected = False
        logging.getLogger("pipeline.reader").info(f"MockClient created for endpoint: {endpoint}")

    def set_security_string(self, security_string):
        logging.getLogger("pipeline.reader").info(f"MockClient: Security string set (mocked): {security_string}")

    def connect(self):
        self._connected = True
        logging.getLogger("pipeline.reader").info("MockClient: Connected.")

    def create_subscription(self, publishing_interval, priority):
        if not self._connected:
            raise Exception("MockClient: Not connected, cannot create subscription.")
        return MockSubscription(publishing_interval, priority)

    def disconnect(self):
        self._connected = False
        logging.getLogger("pipeline.reader").info("MockClient: Disconnected.")

# --- END MODIFICATION ---


class OPCUAConfig(TypedDict):
    endpoint: str
    node_ids: list[str]
    publishing_interval: Optional[int]
    priority: Optional[int]

class OPCDataReader:
    def __init__(self, source: Union[str, OPCUAConfig], max_retries: int = 3):
        """Type-safe initialization"""
        self.source = source
        self.max_retries = max_retries
        self._metrics = {
            'records_processed': 0,
            'records_failed': 0,
            'last_error': None,
            'source_type': self._detect_source_type()
        }

    def _detect_source_type(self) -> str:
        """Type-guarded source detection"""
        if isinstance(self.source, str):
            return 'file'
        elif isinstance(self.source, dict):
            return 'opcua'
        raise TypeError("Source must be str or OPCUAConfig")

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def read_records(self) -> Iterator[Dict[str, Any]]:
        """Main type-safe interface"""
        if self._metrics['source_type'] == 'file':
            yield from self._read_file()
        else:
            yield from self._read_opcua_stream()

    def _read_file(self) -> Iterator[Dict[str, Any]]:
        """Type-safe file reading"""
        path = Path(self.source) if isinstance(self.source, str) else Path()
        try:
            suffix = path.suffix.lower()
            if suffix == '.jsonl':
                yield from self._read_jsonl(path)
            elif suffix == '.json':
                yield from self._read_json(path)
            elif suffix == '.csv':
                yield from self._read_csv(path)
            elif suffix == '.parquet':
                yield from self._read_parquet(path)
            else:
                raise ValueError(f"Unsupported file format: {suffix}")
        except Exception as e:
            logger.error(f"File read failed: {str(e)}")
            self._metrics['last_error'] = str(e)
            raise

    def _read_jsonl(self, path: Path) -> Iterator[Dict[str, Any]]:
        """Type-checked JSONL reader"""
        with open(path, 'r') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if self._validate_opc_structure(record):
                        self._metrics['records_processed'] += 1
                        yield record
                except Exception as e:
                    self._metrics['records_failed'] += 1
                    logger.warning(f"Invalid JSONL record: {str(e)}")

    def _read_json(self, path: Path) -> Iterator[Dict[str, Any]]:
        """Standard JSON file reader (expects array of records)"""
        with open(path, 'r') as f:
            try:
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError("JSON file must contain a list of records")
                for record in data:
                    if self._validate_opc_structure(record):
                        self._metrics['records_processed'] += 1
                        yield record
            except Exception as e:
                self._metrics['records_failed'] += 1
                logger.warning(f"Invalid JSON record: {str(e)}")

    def _read_csv(self, path: Path) -> Iterator[Dict[str, Any]]:
        """
        Smart CSV reader:
        - Auto-detects long-format vs wide-format
        - Loads tag structure from plant_config
        """
        try:
            df = pd.read_csv(path)

            # Load expected tag list from metadata
            try:
                tag_list = pd.read_csv("plant_data/plant_config.csv")["Tag Name"].unique().tolist()
            except Exception as e:
                logger.warning(f"[CSV Reader] plant_config.csv missing or malformed: {e}")
                tag_list = []

            if set(["timestamp", "Tag Name", "Value"]).issubset(df.columns):
                # Long format: pivot by timestamp
                grouped = df[df["Tag Name"].isin(tag_list)].groupby("timestamp")
                for ts, group in grouped:
                    tags = {
                        row["Tag Name"]: float(row["Value"])
                        for _, row in group.iterrows()
                        if pd.notnull(row["Value"])
                    }
                    record = {"timestamp": ts, "source_id": "CSV_LONG", "tags": tags}
                    if self._validate_opc_structure(record):
                        self._metrics['records_processed'] += 1
                        yield record
            else:
                # Wide format: treat each row as one snapshot
                for _, row in df.iterrows():
                    timestamp = row.get("timestamp")
                    tags = {
                        col: float(val)
                        for col, val in row.items()
                        if col not in ["timestamp", "source_id"] and pd.notnull(val) and col in tag_list
                    }
                    record = {"timestamp": timestamp, "source_id": "CSV_WIDE", "tags": tags}
                    if self._validate_opc_structure(record):
                        self._metrics['records_processed'] += 1
                        yield record

        except Exception as e:
            self._metrics['records_failed'] += 1
            logger.error(f"[CSV Reader] Failed to parse: {e}")
            raise

    def _read_parquet(self, path: Path) -> Iterator[Dict[str, Any]]:
        """Type-annotated Parquet reader"""
        try:
            df = pd.read_parquet(path)
            records = df.to_dict('records')
            for record in records:
                str_keyed_record = {str(k): v for k, v in record.items()}  # 🔑 cast keys to str
                if self._validate_opc_structure(str_keyed_record):
                    self._metrics['records_processed'] += 1
                    yield str_keyed_record
        except Exception as e:
            self._metrics['records_failed'] += 1
            logger.warning(f"Parquet read error: {str(e)}")

    def _read_opcua_stream(self) -> Iterator[Dict[str, Any]]:
        """Type-safe OPC UA client"""
        if not isinstance(self.source, dict):
            raise TypeError("OPC UA source must be a config dict")
        
        # --- MODIFICATION FOR TESTING ---
        # Use MockClient instead of real opcua.Client
        # client = Client(self.source.get('endpoint', ''))
        client = MockClient(self.source.get('endpoint', ''))
        # --- END MODIFICATION ---

        if settings.TLS_MUTUAL_AUTH:
            client.set_security_string(
            f"Basic256Sha256,SignAndEncrypt,{settings.TLS_CLIENT_CERT},{settings.TLS_CLIENT_KEY}"
        )
        try:
            client.connect()
            sub = client.create_subscription(
                self.source.get('publishing_interval', 500),
                self.source.get('priority', 100)
            )
            node_ids = self.source.get('node_ids', [])
            if not isinstance(node_ids, list):
                raise TypeError("node_ids must be a list")
            for node_id in node_ids:
                sub.subscribe_data_change(node_id)
            
            # --- MODIFICATION FOR TESTING ---
            # For testing, we'll yield a few dummy records and then break
            # instead of an infinite loop. This simulates a stream without blocking forever.
            for i in range(3): # Yield 3 dummy records for testing
                try:
                    data = sub.get_published_data()  # type: ignore
                    record = self._format_opcua_data(data)
                    if record:
                        self._metrics['records_processed'] += 1
                        yield record
                    time.sleep(0.1) # Small delay to simulate stream
                except Exception as e:
                    logger.error(f"OPC UA read error: {str(e)}")
                    self._metrics['records_failed'] += 1
                    break
            # --- END MODIFICATION ---
        finally:
            client.disconnect()

    def _format_opcua_data(self, data: Any) -> Optional[Dict[str, Any]]:
        """Type-checked OPC UA formatter"""
        # --- MODIFICATION FOR TESTING ---
        # Ensure 'data' has expected attributes when coming from MockNode
        if not hasattr(data, 'node_id') or not hasattr(data, 'value') or not hasattr(data, 'node_name'):
            logger.warning(f"Invalid OPC UA data structure from mock: {data}")
            return None
        # --- END MODIFICATION ---
        try:
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'source_id': str(data.node_id),
                'tags': {str(data.node_name): float(data.value)}
            }
        except Exception as e:
            logger.warning(f"OPC UA format error: {str(e)}")
            return None

    def _validate_opc_structure(self, record: Dict) -> bool:
        if not record.get("_schema_version"):
            record["_schema_version"] = "1.0"
        """Strict type validation"""
        required = {
            'timestamp': lambda x: isinstance(x, str) and bool(datetime.fromisoformat(x)),
            'source_id': lambda x: isinstance(x, str),
            'tags': lambda x: isinstance(x, dict)
        }
        for field, validator in required.items():
            if field not in record or not validator(record[field]):
                raise ValueError(f"Invalid OPC UA structure: Missing {field}")
        return True

    def _convert_value(self, value: str) -> Union[float, int, str]:
        """Type-safe value conversion"""
        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return str(value)

    def get_metrics(self) -> Dict[str, Any]:
        """Type-annotated metrics"""
        return self._metrics
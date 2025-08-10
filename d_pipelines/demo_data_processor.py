"""
data_processor.py (Official Final)
Enterprise Data Processing with Schema Versioning
"""

import logging
import uuid
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger("pipeline.processor")

class DataProcessor:
    """Data processor with schema version tracking."""

    def __init__(self):
        self.schema_versions = {
            "1.0": {"required": ["timestamp", "source_id", "tags"]},
            "1.1": {"required": ["timestamp", "source_id"], "optional": ["tags", "location"]}
        }
        logger.info("Data processor initialized")

    def process(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes raw data with schema version awareness."""
        try:
            if not self._validate_schema(raw_data):
                logger.error("Invalid schema", extra={"data_sample": str(raw_data)[:100]})
                return None

            cleaned = self._clean_data(raw_data)
            return self._add_metadata(cleaned)
            
        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
            return None

    def _validate_schema(self, data: Dict[str, Any]) -> bool:
        """Validates against versioned schemas."""
        if all(field in data for field in self.schema_versions["1.1"]["required"]):
            data["_schema_version"] = "1.1"
            return True
        elif all(field in data for field in self.schema_versions["1.0"]["required"]):
            data["_schema_version"] = "1.0"
            return True
        return False

    def _clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Cleans data with type safety."""
        cleaned = data.copy()
        
        if isinstance(cleaned["timestamp"], str):
            try:
                cleaned["timestamp"] = datetime.fromisoformat(cleaned["timestamp"])
            except ValueError:
                logger.warning("Invalid ISO timestamp, using current time")
                cleaned["timestamp"] = datetime.now()

        cleaned["tags"] = {
            tag: self._clean_value(val)
            for tag, val in cleaned.get("tags", {}).items()
        }
        return cleaned

    def _add_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Adds processing metadata with schema info."""
        return {
            **data,
            "metadata": {
                "processing_time": datetime.utcnow().isoformat(),
                "record_id": str(uuid.uuid4()),
                "schema_version": data.get("_schema_version", "1.0"),
                "detected_fields": list(data.get("tags", {}).keys())
            }
        }

    @staticmethod
    def _clean_value(value: Any) -> Any:
        """Type-safe value normalization."""
        if value is None:
            return 0.0
        if isinstance(value, (int, float, bool)):
            return value
        try:
            return float(value)
        except (ValueError, TypeError):
            return str(value).strip()

processor = DataProcessor()

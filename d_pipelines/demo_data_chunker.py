"""
data_chunker.py (Official Final)
Enterprise Data Chunking with KG-Ready Metadata

Changes:
1. Added KG metadata to chunks (approved)
2. Semantic chunking via _should_start_new_chunk() (approved)
"""

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger("pipeline.chunker")

class DataChunker:
    """Time-based chunking with KG metadata support."""

    def __init__(
        self,
        chunk_size_seconds: int = 300,
        chunk_overlap_seconds: int = 0,
        max_batch_records: int = 1000
    ):
        self.chunk_size = timedelta(seconds=chunk_size_seconds)
        self.overlap = timedelta(seconds=chunk_overlap_seconds)
        self.max_batch = max_batch_records
        logger.info(f"Initialized chunker: {chunk_size_seconds}s windows")

    def chunk_data(
        self,
        processed_data: List[Dict[str, Any]],
        source_id: str
    ) -> List[Dict[str, Any]]:
        """Main chunking interface with KG metadata support."""
        if not processed_data:
            logger.warning("Received empty dataset - skipping chunking")
            return []

        if len(processed_data) > 10_000:  # Prevent OOM
            logger.warning("Large dataset detected - truncating to 10k records")
            processed_data = processed_data[:10_000]
            
        try:
            sorted_data = self._sort_records(processed_data)
            chunks = []
            current_chunk = []
            window_end = None

            for record in sorted_data:
                timestamp = self._parse_timestamp(record["timestamp"])

                if window_end is None:
                    window_end = timestamp + self.chunk_size

                # --- APPROVED CHANGE: Semantic boundary check ---
                if (timestamp >= window_end or 
                    len(current_chunk) >= self.max_batch or
                    self._should_start_new_chunk(current_chunk, record)):
                    if current_chunk:
                        chunks.append(self._finalize_chunk(current_chunk, source_id))
                    current_chunk = [record]
                    window_end = timestamp + self.chunk_size - self.overlap
                else:
                    current_chunk.append(record)

            if current_chunk:
                chunks.append(self._finalize_chunk(current_chunk, source_id))

            logger.debug(f"Created {len(chunks)} chunks")
            return chunks

        except Exception as e:
            logger.error(f"Chunking failed: {str(e)}", exc_info=True)
            raise

    # --- APPROVED CHANGE: Semantic chunking ---
    def _should_start_new_chunk(
        self, 
        current_chunk: List[Dict[str, Any]], 
        new_record: Dict[str, Any]
    ) -> bool:
        """
        Determines if a semantic boundary exists between records.
        Default implementation checks for >10% value deviation in numeric tags.
        """
        if not current_chunk:
            return False

        # Only check numeric tags
        numeric_tags = {
            k: v for k, v in current_chunk[0]["tags"].items()
            if isinstance(v, (int, float))
        }

        for tag, value in numeric_tags.items():
            if tag in new_record["tags"]:
                avg = sum(r["tags"][tag] for r in current_chunk) / len(current_chunk)
                new_val = new_record["tags"][tag]
                if abs(new_val - avg) > avg * 0.1:  # 10% deviation threshold
                    return True
        return False

    def _finalize_chunk(
        self,
        records: List[Dict[str, Any]],
        source_id: str
    ) -> Dict[str, Any]:
        """Finalizes chunk with KG-ready metadata."""
        timestamps = [self._parse_timestamp(r["timestamp"]) for r in records]
        chunk_metadata = {
            "chunk_start": min(timestamps).isoformat(),
            "chunk_end": max(timestamps).isoformat(),
            "record_count": len(records),
            "processing_stage": "chunked",
            "chunk_id": f"{source_id}_{min(timestamps).timestamp()}",
            # --- APPROVED CHANGE: KG metadata ---
            "kg_ready": {
                "entity_types": list({
                    str(r["tags"].get("sensor_type"))  # Explicit str() for safety
                    for r in records 
                    if "sensor_type" in r["tags"]
                }),
                "event_type": "high_frequency" if len(records) > 1000 else "normal"
            }
        }
        return {
            "source_id": source_id,
            "records": records,
            "metadata": chunk_metadata
        }

    # --- Existing helper methods (unchanged) ---
    def _sort_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sorts records by timestamp with error handling."""
        try:
            return sorted(
                records,
                key=lambda x: (
                    datetime.fromisoformat(x["timestamp"])
                    if isinstance(x["timestamp"], str)
                    else x["timestamp"]
                )
            )
        except Exception as e:
            logger.error(f"Sorting failed: {str(e)}")
            raise ValueError("Invalid timestamp format")

    def _parse_timestamp(self, ts: Any) -> datetime:
        """Safely converts timestamp to datetime."""
        if isinstance(ts, datetime):
            return ts
        try:
            return datetime.fromisoformat(ts) if isinstance(ts, str) else datetime.min
        except ValueError:
            logger.warning(f"Invalid timestamp '{ts}', using fallback")
            return datetime.now()

# Singleton with default configs
chunker = DataChunker(
    chunk_size_seconds=300,
    chunk_overlap_seconds=0,
    max_batch_records=1000
)

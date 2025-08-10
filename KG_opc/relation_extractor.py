import yaml
import logging
from typing import Dict, List
import re
import pandas as pd

logger = logging.getLogger("relation.extractor")

class IndustrialRelationExtractor:
    def __init__(self, rule_file: str):
        self.rule_file = rule_file
        self.relations = {}
        self.asset_rules = {}
        self.fallback = "RELATED_TO"
        self._load_rules()
        self.context = None
        self.asset_context = {}

    def _load_rules(self):
        try:
            with open(self.rule_file, "r") as f:
                rules = yaml.safe_load(f)
                self.relations = rules.get("verbs", {})
                self.asset_rules = rules.get("assets", {})
                self.fallback = rules.get("fallback", "RELATED_TO")
            logger.info("[RelationExtractor] Loaded rules from YAML")
        except Exception as e:
            logger.error(f"Failed to load relation rules: {e}")

    def set_context(self, asset_type: str):
        self.context = asset_type.lower()
        logger.debug(f"[RelationExtractor] Context set to '{self.context}'")

    def infer(self, phrase: str) -> str:
        """Infer relation based on keywords and asset type."""
        phrase_lower = phrase.lower()

        # Try asset-based terms first
        if self.context in self.asset_rules:
            for related_term in self.asset_rules[self.context]:
                if related_term.lower() in phrase_lower:
                    return "PART_OF"

        # Check for verb-based relations
        for relation, keywords in self.relations.items():
            for keyword in keywords:
                if re.search(rf"\b{re.escape(keyword)}\b", phrase_lower):
                    return relation

        return self.fallback

    def generate_from_metadata(self, tag_metadata: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Create static relationship candidates from plant metadata
        Useful to populate KG before live events arrive
        """
        relations = []
        for _, row in tag_metadata.iterrows():
            tag = row.get("Tag Name") or row.get("Attribute")
            system = row.get("System")
            category = row.get("Category")
            unit = row.get("Engineering Units") or row.get("Unit")

            if tag and system:
                relations.append({
                    "from": tag,
                    "to": system,
                    "type": "PART_OF"
                })
            if tag and category:
                relations.append({
                    "from": tag,
                    "to": category,
                    "type": "IS_TYPE"
                })
            if tag and unit:
                relations.append({
                    "from": tag,
                    "to": unit,
                    "type": "MEASURES"
                })

        return relations

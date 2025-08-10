
"""
ner_extractor.py
NER-powered entity extractor for industrial text using local spaCy model
"""

import spacy
from typing import Dict, List

# Load local spaCy NER model
# Ensure the model is already installed at this path or linked using spacy link
MODEL_PATH = "en_core_web_sm"
try:
    nlp = spacy.load(MODEL_PATH)
except Exception as e:
    raise RuntimeError(f"Failed to load spaCy model from {MODEL_PATH}: {str(e)}")


def extract_entities(text: str) -> Dict[str, List[str]]:
    """Extracts entities from a given description or log line."""
    doc = nlp(text)
    entity_map = {}
    for ent in doc.ents:
        label = ent.label_.lower()
        if label not in entity_map:
            entity_map[label] = []
        entity_map[label].append(ent.text.strip())
    return entity_map


def extract_flat_keywords(text: str) -> List[str]:
    """Returns list of keywords (nouns/proper nouns) in lowercase for fallback."""
    doc = nlp(text)
    return [token.text.lower() for token in doc if token.pos_ in {"NOUN", "PROPN"} and not token.is_stop]


if __name__ == "__main__":
    sample_text = "Main Steam Valve Opening during Turbine Startup"
    print("Entities:", extract_entities(sample_text))
    print("Keywords:", extract_flat_keywords(sample_text))

import hashlib
import json
from typing import Any, Dict

def generate_document_id(document: Dict[str, Any]) -> str:
    """Generate a unique document ID based on content."""
    return hashlib.md5(json.dumps(document, sort_keys=True).encode()).hexdigest()

def get_partition_id(doc_id: str, num_partitions: int = 1000) -> int:
    """Calculate partition ID for document ID."""
    return int(hashlib.md5(doc_id.encode()).hexdigest(), 16) % num_partitions

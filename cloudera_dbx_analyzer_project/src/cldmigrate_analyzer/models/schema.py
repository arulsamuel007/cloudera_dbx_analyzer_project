from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class Entity:
    entity_id: str
    entity_type: str
    value: str
    confidence: str
    source_file: str
    line_start: Optional[int] = None
    line_end: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    raw_snippet: Optional[str] = None

@dataclass
class UnresolvedItem:
    category: str
    message: str
    source_file: str
    line_start: Optional[int] = None
    line_end: Optional[int] = None
    evidence: Optional[str] = None
    confidence: str = "med"

@dataclass
class FileRecord:
    path: str
    file_name: str
    extension: str
    detected_type: str
    size_bytes: int
    lines_count: int
    words_count: int
    hash: str
    parse_status: str
    parse_message: Optional[str] = None
    language_signals: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)  # entity_ids

@dataclass
class GraphEdge:
    src: str
    dst: str
    edge_type: str
    confidence: str
    evidence: Optional[str] = None

@dataclass
class Graph:
    nodes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    edges: List[GraphEdge] = field(default_factory=list)

@dataclass
class Complexity:
    item_id: str
    level: str
    total_score: int
    dimension_scores: Dict[str, int]
    top_reasons: List[str]
    risk_flags: List[str]

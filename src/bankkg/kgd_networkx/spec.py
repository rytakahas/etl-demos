from __future__ import annotations
from typing import Dict, List
from pydantic import BaseModel, Field

class NodeSpec(BaseModel):
    label: str
    table: str
    id_column: str
    properties: List[str] = Field(default_factory=list)

class EdgeSpec(BaseModel):
    type: str
    table: str
    from_label: str
    from_id_column: str
    to_label: str
    to_id_column: str
    properties: List[str] = Field(default_factory=list)

class KGDSpec(BaseModel):
    version: int = 1
    gold_sources: Dict[str, str] = Field(default_factory=dict)  # name -> filename
    nodes: List[NodeSpec] = Field(default_factory=list)
    edges: List[EdgeSpec] = Field(default_factory=list)

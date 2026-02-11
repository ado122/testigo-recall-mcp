from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class ChangeUnit(BaseModel):
    id: str
    files: list[str]
    kind: list[str]


class Dependency(BaseModel):
    from_component: str
    to_component: str
    relation: str  # "calls" | "imports" | "renders"


class Fact(BaseModel):
    category: Literal["behavior", "design", "assumption"]
    summary: str   # Short: the trigger, decision, or assumption
    detail: str    # Full: the outcome, reasoning, or evidence
    confidence: float = Field(ge=0.0, le=1.0)
    source: Literal["ai", "human", "scan"] = "ai"
    files: list[str] = Field(default_factory=list)
    symbols: list[str] = Field(default_factory=list)  # e.g. ["ClassName.method_name"]


class PRAnalysis(BaseModel):
    pr_id: str
    repo: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    change: ChangeUnit
    dependencies: list[Dependency]
    facts: list[Fact] = Field(default_factory=list)

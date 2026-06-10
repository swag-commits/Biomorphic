"""
biomorphic.sdk.python — Base classes for Biomorphic receptors and actors.

Generated from: schema/biomorphic_event.proto v1.0

Usage:
    # Implement a receptor
    class MyReceptor(ReceptorBase):
        def extract(self, raw) -> BiomorphicEvent:
            ...
        def confidence(self) -> float:
            return 1.0

    # Implement an actor
    class MyActor(ActorBase):
        def decide(self, events: list[BiomorphicEvent]) -> Decision:
            ...
        def interpret(self, prediction: BiomorphicEvent) -> Decision:
            ...
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# SourceType — mirrors proto enum
# ─────────────────────────────────────────────────────────────────────────────
class SourceType(str, Enum):
    TRANSACTION = "transaction"
    ENTITY      = "entity"
    EXTERNAL    = "external"
    DOCUMENT    = "document"
    CORTEX      = "cortex"
    ACTOR       = "actor"


# ─────────────────────────────────────────────────────────────────────────────
# BiomorphicEvent — canonical event schema
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BiomorphicEvent:
    source_type:  SourceType
    receptor_id:  str
    payload:      bytes
    confidence:   float
    event_id:     str       = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp:    str       = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self):
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be 0.0–1.0, got {self.confidence}")

    def to_dict(self) -> dict:
        return {
            "event_id":    self.event_id,
            "timestamp":   self.timestamp,
            "source_type": self.source_type.value,
            "receptor_id": self.receptor_id,
            "payload":     self.payload,
            "confidence":  self.confidence,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Decision — what an actor produces
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Decision:
    action:         str           # e.g. "block", "alert", "enrich", "pass"
    confidence:     float
    rationale:      str
    correlation_id: str           # event_id of the originating event
    metadata:       dict = field(default_factory=dict)

    def to_actor_event(self, actor_id: str) -> BiomorphicEvent:
        """
        Serialise this decision as a first-class spine event.
        The actor outcome is written to the spine as source_type=ACTOR.
        Correlation back to the originating event lives in payload.
        """
        import json
        payload = json.dumps({
            "action":         self.action,
            "rationale":      self.rationale,
            "correlation_id": self.correlation_id,
            "metadata":       self.metadata,
        }).encode()

        return BiomorphicEvent(
            source_type=SourceType.ACTOR,
            receptor_id=actor_id,
            payload=payload,
            confidence=self.confidence,
        )


# ─────────────────────────────────────────────────────────────────────────────
# SpineQuery — the shared query interface
#
# Actors use this in learning mode. Cortex uses this internally.
# Same access, same constraints — ensures learning signal is valid
# training data for Cortex.
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class SpineQuery:
    source_types:     list[SourceType] = field(default_factory=list)
    receptor_ids:     list[str]        = field(default_factory=list)
    limit:            int              = 100
    since_minutes:    Optional[int]    = None    # recent window
    min_confidence:   float            = 0.0


class SpineClient(ABC):
    """
    Interface to the Kafka spine. Injected into receptors and actors.
    Both modes use the same client — mode is a topic subscription config.
    """

    @abstractmethod
    def query(self, q: SpineQuery) -> list[BiomorphicEvent]:
        """Pull events matching the query from the spine."""
        ...

    @abstractmethod
    def emit(self, event: BiomorphicEvent) -> None:
        """Write an event to the spine."""
        ...

    @abstractmethod
    def subscribe(self, topic: str, handler) -> None:
        """Subscribe to a topic with a callback handler."""
        ...


# ─────────────────────────────────────────────────────────────────────────────
# ReceptorBase — base class for all receptors
# ─────────────────────────────────────────────────────────────────────────────
class ReceptorBase(ABC):
    """
    Extend this class to build a Biomorphic receptor.

    A receptor converts raw external signals into schema-invariant
    BiomorphicEvents and writes them to the spine.

    Example:
        class TransactionReceptor(ReceptorBase):
            source_type = SourceType.TRANSACTION
            receptor_id = "txn-receptor-v1"

            def extract(self, raw: dict) -> BiomorphicEvent:
                vec = self.encode(raw)  # your encoding logic
                return self.make_event(payload=vec, confidence=1.0)
    """

    source_type: SourceType   # set on subclass
    receptor_id: str          # set on subclass

    def __init__(self, spine: SpineClient):
        self._spine = spine

    @abstractmethod
    def extract(self, raw) -> BiomorphicEvent:
        """
        Convert a raw external signal into a canonical BiomorphicEvent.
        Rules-based receptors return confidence=1.0.
        RAG/ML receptors return confidence from their model.
        """
        ...

    def confidence(self) -> float:
        """
        Default confidence for this receptor.
        Override for dynamic confidence (e.g. RAG similarity score).
        """
        return 1.0

    def make_event(self, payload: bytes, confidence: Optional[float] = None) -> BiomorphicEvent:
        """Convenience factory — creates a correctly-typed event."""
        return BiomorphicEvent(
            source_type=self.source_type,
            receptor_id=self.receptor_id,
            payload=payload,
            confidence=confidence if confidence is not None else self.confidence(),
        )

    def process(self, raw) -> BiomorphicEvent:
        """Extract and emit in one call."""
        event = self.extract(raw)
        self._spine.emit(event)
        return event


# ─────────────────────────────────────────────────────────────────────────────
# ActorBase — base class for all actors
# ─────────────────────────────────────────────────────────────────────────────
class ActorBase(ABC):
    """
    Extend this class to build a Biomorphic actor.

    In LEARNING MODE: actor calls decide(), reaching into the spine
    directly via SpineQuery. Actor decisions become ground truth
    that Cortex scores itself against.

    In EXECUTION MODE: actor calls interpret(), receiving a Cortex
    prediction event from the spine. Actor translates the prediction
    into a domain-specific action.

    Mode is set by topic subscription config — no code change required.

    Example:
        class FraudActor(ActorBase):
            actor_id = "fraud-actor-v1"

            def decide(self, events):
                # your rules / RAG / ML logic
                return Decision(action="block", confidence=0.95, ...)

            def interpret(self, prediction):
                # translate Cortex output to fraud-specific action
                score = decode_prediction(prediction.payload)
                if score > 0.85:
                    return Decision(action="block", confidence=score, ...)
                return Decision(action="pass", confidence=1-score, ...)
    """

    actor_id: str   # set on subclass

    def __init__(self, spine: SpineClient):
        self._spine = spine

    @abstractmethod
    def decide(self, events: list[BiomorphicEvent]) -> Decision:
        """
        LEARNING MODE — actor is authoritative.
        Query the spine directly and produce a decision.
        Uses the same SpineQuery interface Cortex uses internally.
        """
        ...

    @abstractmethod
    def interpret(self, prediction: BiomorphicEvent) -> Decision:
        """
        EXECUTION MODE — Cortex is authoritative.
        Receive a Cortex prediction event and translate into action.
        The actor's domain logic becomes an interpretation layer.
        """
        ...

    def feedback(self, decision: Decision) -> BiomorphicEvent:
        """
        Write the outcome back to the spine as an ACTOR event.
        Called after the action has been executed.
        This is how the feedback loop closes.
        """
        event = decision.to_actor_event(self.actor_id)
        self._spine.emit(event)
        return event

    def query_spine(self, q: SpineQuery) -> list[BiomorphicEvent]:
        """Convenience wrapper — same interface Cortex uses."""
        return self._spine.query(q)

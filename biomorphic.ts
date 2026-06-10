/**
 * biomorphic/sdk/typescript/biomorphic.ts
 * Base classes for Biomorphic receptors and actors.
 *
 * Generated from: schema/biomorphic_event.proto v1.0
 *
 * Usage:
 *   // Implement a receptor
 *   class MyReceptor extends ReceptorBase {
 *     sourceType = SourceType.TRANSACTION;
 *     receptorId = "txn-receptor-v1";
 *     async extract(raw: unknown): Promise<BiomorphicEvent> { ... }
 *   }
 *
 *   // Implement an actor
 *   class MyActor extends ActorBase {
 *     actorId = "fraud-actor-v1";
 *     async decide(events: BiomorphicEvent[]): Promise<Decision> { ... }
 *     async interpret(prediction: BiomorphicEvent): Promise<Decision> { ... }
 *   }
 */

import { v4 as uuidv4 } from "uuid";

// ─────────────────────────────────────────────────────────────────────────────
// SourceType — mirrors proto enum
// ─────────────────────────────────────────────────────────────────────────────
export enum SourceType {
  TRANSACTION = "transaction",
  ENTITY      = "entity",
  EXTERNAL    = "external",
  DOCUMENT    = "document",
  CORTEX      = "cortex",
  ACTOR       = "actor",
}

// ─────────────────────────────────────────────────────────────────────────────
// BiomorphicEvent — canonical event schema
// ─────────────────────────────────────────────────────────────────────────────
export interface BiomorphicEvent {
  event_id:    string;      // uuid-v4
  timestamp:   string;      // ISO 8601
  source_type: SourceType;
  receptor_id: string;
  payload:     Uint8Array;  // binary — never parsed by spine
  confidence:  number;      // 0.0–1.0
}

export function makeEvent(
  source_type: SourceType,
  receptor_id: string,
  payload:     Uint8Array,
  confidence:  number,
): BiomorphicEvent {
  if (confidence < 0 || confidence > 1) {
    throw new RangeError(`confidence must be 0.0–1.0, got ${confidence}`);
  }
  return {
    event_id:   uuidv4(),
    timestamp:  new Date().toISOString(),
    source_type,
    receptor_id,
    payload,
    confidence,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Decision — what an actor produces
// ─────────────────────────────────────────────────────────────────────────────
export interface Decision {
  action:         string;   // "block" | "alert" | "enrich" | "pass" | ...
  confidence:     number;
  rationale:      string;
  correlation_id: string;   // event_id of originating event
  metadata?:      Record<string, unknown>;
}

export function decisionToActorEvent(
  decision: Decision,
  actorId:  string,
): BiomorphicEvent {
  const body = JSON.stringify({
    action:         decision.action,
    rationale:      decision.rationale,
    correlation_id: decision.correlation_id,
    metadata:       decision.metadata ?? {},
  });
  const payload = new TextEncoder().encode(body);

  return makeEvent(SourceType.ACTOR, actorId, payload, decision.confidence);
}

// ─────────────────────────────────────────────────────────────────────────────
// SpineQuery — shared query interface
//
// Actors use this in learning mode. Cortex uses this internally.
// Same access, same constraints.
// ─────────────────────────────────────────────────────────────────────────────
export interface SpineQuery {
  sourceTypes?:   SourceType[];
  receptorIds?:   string[];
  limit?:         number;
  sinceMinutes?:  number;
  minConfidence?: number;
}

export interface SpineClient {
  /** Pull events matching the query from the spine. */
  query(q: SpineQuery): Promise<BiomorphicEvent[]>;

  /** Write an event to the spine. */
  emit(event: BiomorphicEvent): Promise<void>;

  /** Subscribe to a topic with an async handler. */
  subscribe(
    topic: string,
    handler: (event: BiomorphicEvent) => Promise<void>,
  ): Promise<void>;
}

// ─────────────────────────────────────────────────────────────────────────────
// ReceptorBase — base class for all receptors
// ─────────────────────────────────────────────────────────────────────────────
export abstract class ReceptorBase {
  abstract readonly sourceType: SourceType;
  abstract readonly receptorId: string;

  constructor(protected spine: SpineClient) {}

  /**
   * Convert a raw external signal into a canonical BiomorphicEvent.
   * Rules-based receptors return confidence=1.0.
   * RAG/ML receptors return confidence from their model output.
   */
  abstract extract(raw: unknown): Promise<BiomorphicEvent>;

  /**
   * Default confidence. Override for dynamic scoring.
   */
  confidence(): number {
    return 1.0;
  }

  /**
   * Convenience factory — creates a correctly-typed event.
   */
  protected makeEvent(payload: Uint8Array, confidence?: number): BiomorphicEvent {
    return makeEvent(
      this.sourceType,
      this.receptorId,
      payload,
      confidence ?? this.confidence(),
    );
  }

  /**
   * Extract and emit in one call.
   */
  async process(raw: unknown): Promise<BiomorphicEvent> {
    const event = await this.extract(raw);
    await this.spine.emit(event);
    return event;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ActorBase — base class for all actors
// ─────────────────────────────────────────────────────────────────────────────
export abstract class ActorBase {
  abstract readonly actorId: string;

  constructor(protected spine: SpineClient) {}

  /**
   * LEARNING MODE — actor is authoritative.
   * Query the spine directly and produce a decision.
   * Uses the same SpineQuery interface Cortex uses internally.
   */
  abstract decide(events: BiomorphicEvent[]): Promise<Decision>;

  /**
   * EXECUTION MODE — Cortex is authoritative.
   * Receive a Cortex prediction event and translate into a domain action.
   * The actor's rules/RAG/ML logic becomes an interpretation layer.
   */
  abstract interpret(prediction: BiomorphicEvent): Promise<Decision>;

  /**
   * Write the outcome back to the spine as an ACTOR event.
   * Call this after the action has been executed.
   * This is how the feedback loop closes.
   */
  async feedback(decision: Decision): Promise<BiomorphicEvent> {
    const event = decisionToActorEvent(decision, this.actorId);
    await this.spine.emit(event);
    return event;
  }

  /**
   * Convenience wrapper — same interface Cortex uses internally.
   */
  async querySpine(q: SpineQuery): Promise<BiomorphicEvent[]> {
    return this.spine.query(q);
  }
}

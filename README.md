# Biomorphic

**Adaptive intelligence infrastructure.**  
Spine-native · Two-mode · Protocol-first

Biomorphic is an open infrastructure platform that unifies fragmented data and AI models into a single, continuously learning event system. Instead of running N independent models on N overlapping data sources, Biomorphic runs one inference engine on one event spine — and earns its own authority over time.

---

## The problem

Most organisations building with AI end up with the same structure: multiple models, each re-ingesting the same data differently, each encoding overlapping logic, each adding to the inference cost. Every new use case adds another pipeline. There is no shared memory, no unified signal, no institutional learning.

Biomorphic replaces that structure with three things:

- **One spine.** A Kafka event bus as the single source of truth for all signals.
- **One cortex.** A transformer that continuously predicts what happens next, attending over the spine and long-term semantic memory.
- **One protocol.** A universal receptor/actor contract that any team can implement in any language.

---

## How it works

```
External signals
      ↓
[ Receptors ]     Convert raw signals into schema-invariant events
      ↓
[ Kafka Spine ]   Ordered · durable · replayable · single source of truth
      ↓
[ Cortex ]        Transformer · continuous next-event prediction
      ↓                         attends over spine + Supabase memory
[ Actors ]        Interpret predictions · execute actions · close the loop
      ↓
[ Kafka Spine ]   All outcomes feed back as first-class events
```

Every participant — receptor, Cortex, actor — reads from and writes to the spine. One schema. One contract.

---

## Two-mode operation

Biomorphic starts in **learning mode**. Cortex observes silently while actors make decisions using their own rules, RAG, or ML. Cortex scores its own predictions against actor decisions, accumulating accuracy over a rolling window.

When accuracy crosses a configurable threshold, the system promotes itself to **execution mode**. Cortex starts writing prediction events to the spine. Actors re-subscribe to the Cortex topic. No code changes — topic subscription config only.

```
LEARNING MODE                    EXECUTION MODE
─────────────────────────────    ─────────────────────────────
Actors own decisions             Cortex owns decisions
Cortex observes + scores         Cortex writes to spine
Spine is the data backbone       Actors interpret predictions
Useful from day one              Activated when trust is earned
```

Execution mode is reversible. The operator can roll back at any time.

---

## The event schema

Every event on the spine — regardless of source — carries exactly six fields:

```protobuf
message BiomorphicEvent {
  string     event_id    = 1;  // uuid-v4
  string     timestamp   = 2;  // iso8601
  SourceType source_type = 3;  // transaction | entity | external | document | cortex | actor
  string     receptor_id = 4;  // which participant produced this
  bytes      payload     = 5;  // binary — never parsed by spine
  float      confidence  = 6;  // 0.0–1.0
}
```

The schema is append-only. Actor outcomes are new events, not mutations. The spine never inspects the payload.

Full definition: [`schema/biomorphic_event.proto`](schema/biomorphic_event.proto)

---

## Building a receptor

```python
from biomorphic import ReceptorBase, SourceType

class TransactionReceptor(ReceptorBase):
    source_type = SourceType.TRANSACTION
    receptor_id = "txn-receptor-v1"

    def extract(self, raw: dict) -> BiomorphicEvent:
        payload = encode(raw)           # your encoding logic
        return self.make_event(payload, confidence=1.0)
```

## Building an actor

```python
from biomorphic import ActorBase, Decision

class FraudActor(ActorBase):
    actor_id = "fraud-actor-v1"

    def decide(self, events) -> Decision:
        # learning mode — your logic is authoritative
        ...

    def interpret(self, prediction) -> Decision:
        # execution mode — translate Cortex output to action
        ...
```

Full SDK reference: [`sdk/python/biomorphic.py`](sdk/python/biomorphic.py) · [`sdk/typescript/biomorphic.ts`](sdk/typescript/biomorphic.ts)

---

## Repository structure

```
biomorphic/
├── schema/               # Protobuf event schema — the public contract
├── memory/
│   └── migrations/       # Supabase + pgvector schema
├── sdk/
│   ├── python/           # Python base classes
│   └── typescript/       # TypeScript base classes
├── receptors/            # Reference receptor implementations (coming soon)
├── actors/               # Reference actor implementations (coming soon)
├── cortex/               # Transformer inference engine (coming soon)
├── spine/                # Kafka topic config (coming soon)
└── infra/                # Docker Compose + Terraform (coming soon)
```

---

## Getting started

```bash
git clone https://github.com/swag-commits/Biomorphic.git
cd Biomorphic

# Local spine + memory (coming soon)
docker compose up

# Run the Supabase migration
psql $SUPABASE_URL < memory/migrations/001_create_events.sql
```

---

## Contributing

Receptors and actors are the most valuable contributions — if you have access to a data source or a domain problem, you can build against the protocol today.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

---

## License

Apache 2.0 · © 2026 Somai Studio

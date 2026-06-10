# Contributing to Biomorphic

Thank you for your interest in contributing. Biomorphic is an open infrastructure project — the goal is to build a universal protocol for adaptive intelligence systems that anyone can extend.

The most valuable contributions are **receptors** and **actors**: domain-specific implementations of the core protocol that the whole ecosystem can use.

---

## What you can contribute

### Receptors
A receptor converts an external signal source into a canonical `BiomorphicEvent`. If you have access to a data source — a financial feed, a database change stream, a news API, a document store — you can build a receptor for it.

Examples that would be valuable:
- Stripe / payment processor transactions
- Postgres change-data-capture (CDC)
- Bloomberg / Reuters market feeds
- Plaid bank transaction feeds
- RSS / news feed ingestion
- S3 / document upload triggers
- Salesforce / CRM entity events

### Actors
An actor subscribes to the spine and produces decisions. In learning mode it uses its own logic. In execution mode it interprets Cortex predictions.

Examples:
- AML screening actor
- Fraud scoring actor
- KYC enrichment actor
- Alert routing actor
- Case management actor

### Core contributions
- Bug fixes in the SDK base classes
- Additional language SDKs (Go, Java, Rust)
- Improvements to the Supabase migration
- Documentation and examples

---

## How to build a receptor

**1. Install the Python SDK**
```bash
pip install biomorphic  # coming soon — for now copy sdk/python/biomorphic.py
```

**2. Extend `ReceptorBase`**
```python
from biomorphic import ReceptorBase, BiomorphicEvent, SourceType

class StripeTransactionReceptor(ReceptorBase):
    source_type = SourceType.TRANSACTION
    receptor_id = "stripe-receptor-v1"

    def extract(self, raw: dict) -> BiomorphicEvent:
        # encode the raw Stripe event into a binary payload
        payload = encode_stripe_event(raw)
        return self.make_event(payload=payload, confidence=1.0)
```

**3. Wire it to the spine**
```python
from biomorphic import KafkaSpineClient  # coming soon

spine = KafkaSpineClient(bootstrap_servers="localhost:9092")
receptor = StripeTransactionReceptor(spine)

# process incoming events
for raw_event in stripe_webhook_stream():
    receptor.process(raw_event)
```

---

## How to build an actor

**1. Extend `ActorBase`**
```python
from biomorphic import ActorBase, BiomorphicEvent, Decision, SpineQuery, SourceType

class FraudActor(ActorBase):
    actor_id = "fraud-actor-v1"

    def decide(self, events: list[BiomorphicEvent]) -> Decision:
        # learning mode — your rules / RAG / ML logic
        score = self.score_events(events)
        return Decision(
            action="block" if score > 0.85 else "pass",
            confidence=score,
            rationale=f"fraud score {score:.2f}",
            correlation_id=events[-1].event_id,
        )

    def interpret(self, prediction: BiomorphicEvent) -> Decision:
        # execution mode — translate Cortex output to domain action
        score = decode_prediction(prediction.payload)
        return Decision(
            action="block" if score > 0.85 else "pass",
            confidence=score,
            rationale=f"cortex prediction {score:.2f}",
            correlation_id=prediction.event_id,
        )
```

---

## Contribution guidelines

### Protocol contract
The `BiomorphicEvent` schema in `schema/biomorphic_event.proto` is a **public contract**. Do not submit PRs that change field numbers, remove fields, or rename enum values in existing versions. Additive changes (new enum values, new message types) are welcome via discussion first.

### One receptor or actor per PR
Keep PRs focused. One new receptor or actor per pull request makes review faster and history cleaner.

### Folder structure
```
receptors/
  {source}-receptor/
    receptor.py          # or .ts
    README.md            # what it connects to, config required
    requirements.txt     # dependencies

actors/
  {domain}-actor/
    actor.py             # or .ts
    README.md
    requirements.txt
```

### README for each receptor/actor
Every receptor and actor needs a short README covering:
- What it connects to
- Required configuration (env vars, credentials)
- Example event it produces or consumes
- Known limitations

### Tests
Include at least one test that creates a mock spine, runs your receptor or actor, and asserts the output is a valid `BiomorphicEvent` or `Decision`.

### No credentials in code
Never commit API keys, connection strings, or credentials. Use environment variables. Document the variable names in your README.

---

## Getting started locally

```bash
git clone https://github.com/swag-commits/Biomorphic.git
cd Biomorphic

# Spin up Kafka + Supabase locally (coming soon)
docker compose up

# Run the Supabase migration
psql $SUPABASE_URL < memory/migrations/001_create_events.sql
```

---

## Questions

Open an issue with the label `question`. We'll respond and fold good answers into the docs.

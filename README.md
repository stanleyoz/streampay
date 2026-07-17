# StreamPay — Metered Streaming Payments for AI Agents

**NandaHack Phase 2 Submission · `stripe-engineer`**

Hosted REST API for rate-limited, per-tick streaming payments between AI agents. Idempotency-keyed mutations, audit trail, receipt verification.

## Live

- API: https://streampay.tinylab.ai
- SKILL.md: https://streampay.tinylab.ai/skill.md

## Infrastructure

- **Compute**: Cloud Run, project `streampay-tinylab`, region `us-central1`,
  pinned `--min-instances=1 --max-instances=1` (see "Concurrency" below).
- **Storage**: Firestore, Native mode, same project/region. One document per
  stream in the `streams` collection, plus an `api_keys` collection (see
  "Auth" below). Requires the runtime service account to hold
  `roles/datastore.user`.
- **Domain**: `streampay.tinylab.ai`, mapped via `gcloud beta run
  domain-mappings create`, DNS is a CNAME to the target Cloud Run gives you.

```bash
gcloud run deploy streampay \
  --source=. \
  --region=us-central1 \
  --project=streampay-tinylab \
  --allow-unauthenticated \
  --min-instances=1 --max-instances=1 \
  --set-env-vars=SKILL_BASE_URL=https://streampay.tinylab.ai
```

`--allow-unauthenticated` here is about Cloud Run's own IAM invoker check
(has to be open for a public API), not application auth — that's the
separate API-key layer described below.

Cloud Run's source deploy uses Buildpacks, which need the `Procfile` in this
repo to know the app's start command — don't remove it.

## Concurrency

Pinned to exactly one instance on purpose. The ledger's correctness (no
double-billing a re-delivered tick, no double-close) relies on Firestore
transactions scoped to a single stream document, which is safe under any
number of instances — but pinning to one keeps the failure mode simple
while traffic is low, and removes cold starts as a side effect. Revisit
this once real concurrent load shows up; nothing about the transaction
design blocks scaling out.

## Auth

Self-serve API keys, Stripe-style: `POST /apikeys` once with your
`agent_id`, get a key back (shown once, hashed at rest — not retrievable
again even by us). Send it as `X-API-Key` on every mutating call. Reads
(`GET` endpoints, `/health`, `/skill.md`) stay open.

**Known scope limit**: a key currently proves "a registered caller," not
"the payer or payee of this specific stream" — any valid key can act on
any `stream_id`. Per-stream authorization (scoping a key to the agent_id
that must match payer or payee) is a planned follow-up, not implemented.

## Endpoints

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/health` | none | Liveness |
| GET | `/skill.md` | none | SKILL.md for agent discovery |
| POST | `/apikeys` | none | Register, get an API key (shown once) |
| POST | `/streams` | X-API-Key | Open stream (idempotent) |
| POST | `/streams/{id}/tick` | X-API-Key | Drain one tick (idempotent) |
| POST | `/streams/{id}/close` | X-API-Key | Close stream → receipt (idempotent) |
| GET | `/streams/{id}` | none | Stream state |
| GET | `/streams/{id}/receipt` | none | Receipt — publicly verifiable |
| POST | `/streams/{id}/refund` | X-API-Key | Refund closed stream (idempotent) |
| GET | `/streams?agent={id}` | none | List streams |

## Local Test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn main:app --reload
```

Needs `GOOGLE_CLOUD_PROJECT=streampay-tinylab` and local ADC
(`gcloud auth application-default login`) to reach Firestore — there's no
local/emulator fallback yet.

Then:
```bash
curl http://localhost:8000/health
KEY=$(curl -s -X POST http://localhost:8000/apikeys -H 'Content-Type: application/json' -d '{"agent_id":"a"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["api_key"])')
curl -X POST http://localhost:8000/streams -H 'Content-Type: application/json' -H "X-API-Key: $KEY" -d '{"stream_id":"s-1","payer":"a","payee":"b","rate_per_tick":10,"max_total":500}'
curl -X POST http://localhost:8000/streams/s-1/tick -H 'Content-Type: application/json' -H "X-API-Key: $KEY" -d '{"tick":1}'
curl -X POST http://localhost:8000/streams/s-1/close -H "X-API-Key: $KEY"
```

## Nanda Town Plugin

The streaming semantics this API enforces at the HTTP layer are validated
in-simulator by a companion plugin (idempotency keys, typed errors, audit
trail, 7 adversarial validators) in `projnanda/nandatown`:
https://github.com/projnanda/nandatown/pull/153

This service is intentionally standalone — no dependency on that repo — per
review on that PR: the hosted API and its writeup live here instead.
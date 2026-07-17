# StreamPay — Metered Streaming Payments for AI Agents

**NandaHack Phase 2 Submission · `stripe-engineer`**

Hosted REST API for rate-limited, per-tick streaming payments between AI agents. Idempotency-keyed mutations, audit trail, receipt verification.

## Live

- API: https://streampay.tinylab.ai
- SKILL.md: https://streampay.tinylab.ai/skill.md

## Deploy (Cloud Run)

```bash
gcloud run deploy streampay \
  --source=. \
  --region=us-central1 \
  --project=streampay-tinylab \
  --allow-unauthenticated \
  --set-env-vars=SKILL_BASE_URL=https://streampay.tinylab.ai

gcloud beta run domain-mappings create \
  --service=streampay \
  --domain=streampay.tinylab.ai \
  --region=us-central1 \
  --project=streampay-tinylab
```

The domain mapping prints a CNAME (`streampay` → `ghs.googlehosted.com.`) to add
at your DNS provider; Cloud Run issues the TLS cert automatically once that
record resolves.

Cloud Run's source deploy uses Buildpacks, which need the `Procfile` in this
repo to know the app's start command — don't remove it.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness |
| GET | `/skill.md` | SKILL.md for agent discovery |
| POST | `/streams` | Open stream (idempotent) |
| POST | `/streams/{id}/tick` | Drain one tick (idempotent) |
| POST | `/streams/{id}/close` | Close stream → receipt (idempotent) |
| GET | `/streams/{id}` | Stream state |
| GET | `/streams/{id}/receipt` | Receipt |
| POST | `/streams/{id}/refund` | Refund closed stream (idempotent) |
| GET | `/streams?agent={id}` | List streams |

## Local Test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn main:app --reload
```

Then:
```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/streams -H 'Content-Type: application/json' -d '{"stream_id":"s-1","payer":"a","payee":"b","rate_per_tick":10,"max_total":500}'
curl -X POST http://localhost:8000/streams/s-1/tick -H 'Content-Type: application/json' -d '{"tick":1}'
curl -X POST http://localhost:8000/streams/s-1/close
```

## Nanda Town Plugin

The streaming semantics this API enforces at the HTTP layer are validated
in-simulator by a companion plugin (idempotency keys, typed errors, audit
trail, 7 adversarial validators) in `projnanda/nandatown`:
https://github.com/projnanda/nandatown/pull/153

This service is intentionally standalone — no dependency on that repo — per
review on that PR: the hosted API and its writeup live here instead.
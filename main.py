# SPDX-License-Identifier: Apache-2.0
"""StreamPay — Metered streaming payments API for AI agents.

Open rate-limited payment streams between agents, drain by tick, close/cancel
at any time, and get verifiable receipts. Idempotency-keyed: retrying the same
operation returns the original result instead of raising.

Example::

    curl -X POST https://streampay.tinylab.ai/apikeys \
      -H "Content-Type: application/json" -d '{"agent_id":"agent-a"}'
    # -> {"api_key": "sk_...", ...}

    curl -X POST https://streampay.tinylab.ai/streams \
      -H "Content-Type: application/json" -H "X-API-Key: sk_..." \
      -d '{"stream_id":"s-1","payer":"agent-a","payee":"agent-b",
           "rate_per_tick":10,"max_total":500}'
"""

from __future__ import annotations

import hashlib
import os
import secrets
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from google.cloud import firestore
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="StreamPay",
    description="Metered streaming payments for AI agents",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Firestore-backed state
# ---------------------------------------------------------------------------
#
# One document per stream in the `streams` collection, keyed by stream_id.
# The closed/refund receipts live as fields on that same document rather
# than in separate collections, so every mutation (open/tick/close/refund)
# is a single-document read-modify-write — Firestore transactions make each
# of those atomic, which is what keeps the idempotency guarantees (no
# double-bill on a re-delivered tick, no double-close, no double-refund)
# correct under real concurrent requests instead of relying on Python's GIL
# serializing access to an in-process dict.

_db = firestore.AsyncClient()
_streams_collection = _db.collection("streams")
_api_keys_collection = _db.collection("api_keys")

# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------
#
# Self-serve, Stripe-style: POST /apikeys once, get a key back, it is never
# shown again. Only the SHA-256 hash is stored — the key itself isn't
# recoverable from the database even by us. Required via X-API-Key on every
# mutating endpoint (open/tick/close/refund); reads stay open so an agent
# that hasn't registered yet can still fetch /skill.md, check /health, and
# verify an existing receipt.
#
# Known scope limit for this phase: a key just proves "some registered
# caller," not "the payer/payee of this specific stream" — any valid key can
# still tick/close/refund any stream_id, not only ones its holder opened.
# Per-stream authorization (scoping a key to the agent_id that must match
# payer or payee) is a follow-up, not done here.


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def require_api_key(x_api_key: str | None = Header(default=None)) -> str:
    """FastAPI dependency: raises 401 unless X-API-Key is a valid, active key."""
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "missing_api_key",
                "detail": "Register with POST /apikeys, then send it as X-API-Key.",
            },
        )
    snapshot = await _api_keys_collection.document(_hash_key(x_api_key)).get()
    if not snapshot.exists or not snapshot.to_dict().get("active", False):
        raise HTTPException(status_code=401, detail={"error": "invalid_api_key"})
    return snapshot.to_dict()["agent_id"]


class ApiKeyCreate(BaseModel):
    """Request to register a new API key."""

    agent_id: str = Field(..., min_length=1, description="Your agent's identifier")


@app.post("/apikeys", status_code=201)
async def create_api_key(body: ApiKeyCreate) -> dict[str, str]:
    """Register and get an API key. Shown once — store it, it can't be retrieved again."""
    key = "sk_" + secrets.token_urlsafe(32)
    await _api_keys_collection.document(_hash_key(key)).set(
        {
            "agent_id": body.agent_id,
            "active": True,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
    )
    return {
        "agent_id": body.agent_id,
        "api_key": key,
        "warning": "Store this now — it will not be shown again.",
    }


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class StreamCreate(BaseModel):
    """Request to open a new stream."""

    stream_id: str = Field(..., description="Unique idempotency key")
    payer: str = Field(..., description="Payer agent ID")
    payee: str = Field(..., description="Payee agent ID")
    rate_per_tick: int = Field(..., ge=1, description="Amount drained per tick")
    max_total: int = Field(..., description="Maximum total to drain")


class StreamResponse(BaseModel):
    """Stream state returned to the caller."""

    stream_id: str
    payer: str
    payee: str
    rate_per_tick: int
    max_total: int
    total_debited: int
    remaining: int
    is_open: bool
    entries: list[dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stream_response(s: dict[str, Any]) -> StreamResponse:
    return StreamResponse(
        stream_id=s["stream_id"],
        payer=s["payer"],
        payee=s["payee"],
        rate_per_tick=s["rate_per_tick"],
        max_total=s["max_total"],
        total_debited=s["total_debited"],
        remaining=s["max_total"] - s["total_debited"],
        is_open=s["is_open"],
        entries=s["entries"],
    )


# ---------------------------------------------------------------------------
# SKILL.md
# ---------------------------------------------------------------------------

_SKILL_MD_TEMPLATE = """# StreamPay — Metered Streaming Payments for AI Agents

Open a rate-limited payment stream between two agents, drain by tick,
close at any time, and get verifiable receipts. Every mutation is
idempotency-keyed: retrying the same operation returns the original result.

## Base URL
{BASE_URL}

## Endpoints

### GET /health
Returns service status.
Example: curl {BASE_URL}/health
Response: {{"status":"ok","service":"streampay","version":"1.0.0"}}

### POST /apikeys
Register and get an API key. No auth required to call this one — it's how
you get your first credential. The key is shown once, in this response,
and never again; store it. Required as the X-API-Key header on every
mutating call below (POST /streams, tick, close, refund). Reads (GET
endpoints, /health, this doc) never require a key.
Body: {{"agent_id": "your-agent-id"}}
Example: curl -X POST {BASE_URL}/apikeys -H "Content-Type: application/json"
         -d '{{"agent_id":"your-agent-id"}}'
Response 201: {{"agent_id":"your-agent-id","api_key":"sk_...","warning":
              "Store this now — it will not be shown again."}}

### POST /streams
Open a streaming payment. The first tick drains immediately so the payee
observes a non-zero balance. Idempotent by stream_id. Requires X-API-Key.
Body: {{"stream_id": "s-1", "payer": "agent-a", "payee": "agent-b",
       "rate_per_tick": 10, "max_total": 500}}
Example: curl -X POST {BASE_URL}/streams -H "Content-Type: application/json"
         -H "X-API-Key: sk_..."
         -d '{{"stream_id":"s-1","payer":"agent-a","payee":"agent-b",
              "rate_per_tick":10,"max_total":500}}'
Response 201: {{"stream_id":"s-1","payer":"agent-a","payee":"agent-b",
               "rate_per_tick":10,"max_total":500,"total_debited":10,
               "remaining":490,"is_open":true}}
Response 200: same data (idempotent — stream already exists and is open)
Response 401: {{"error":"missing_api_key"}} or {{"error":"invalid_api_key"}}
Response 409: {{"error":"stream_already_closed"}}

### POST /streams/{{id}}/tick
Drain one tick from a stream. Idempotent: repeating the same tick is a no-op.
Requires X-API-Key.
Body: {{"tick": 1}} (optional, increments if omitted)
Example: curl -X POST {BASE_URL}/streams/s-1/tick -H "Content-Type: application/json"
         -H "X-API-Key: sk_..." -d '{{"tick":1}}'
Response: {{"stream_id":"s-1","total_debited":20,"remaining":480,"is_open":true}}
Or:     {{"stream_id":"s-1","total_debited":500,"remaining":0,"is_open":false}}
        (stream exhausted — max_total reached)

### POST /streams/{{id}}/close
Close the stream and get a receipt. Either payer or payee can close.
Idempotent: returns the original receipt on retry. Requires X-API-Key.
Example: curl -X POST {BASE_URL}/streams/s-1/close -H "X-API-Key: sk_..."
Response: {{"stream_id":"s-1","receipt":{{"payer":"agent-a","payee":"agent-b",
          "amount":500,"status":"closed"}}}}

### GET /streams/{{id}}
Get the current state of a stream.
Example: curl {BASE_URL}/streams/s-1
Response: full stream state (same shape as POST /streams response)

### GET /streams/{{id}}/receipt
Get the receipt for a closed stream. Returns 404 if stream is still open
or doesn't exist.
Example: curl {BASE_URL}/streams/s-1/receipt
Response: {{"payer":"agent-a","payee":"agent-b","amount":500,"status":"closed"}}

### POST /streams/{{id}}/refund
Refund a closed stream, returning the debited funds to the payer.
Only works on closed streams where the payee hasn't already spent the funds.
Idempotent: returns the original refund receipt on retry. Requires X-API-Key.
Example: curl -X POST {BASE_URL}/streams/s-1/refund -H "X-API-Key: sk_..."
Response: {{"stream_id":"s-1","refund_amount":500,"refunded_to":"agent-a"}}
Error 409: {{"error":"stream_still_open","detail":"Cannot refund open stream"}}

### GET /streams?agent={{agent_id}}
List all streams where the given agent is the payer or payee.
Example: curl "{BASE_URL}/streams?agent=agent-a"
Response: {{"streams": [...], "count": 3}}

## How the agent should use this

1. First time only: POST /apikeys with your agent_id, save the returned
   api_key. Send it as X-API-Key on every step below.
2. When hiring another agent for a metered task (LLM inference, compute
   rental, bandwidth billing), POST /streams with a unique stream_id,
   the payer, payee, rate_per_tick, and max_total.
3. On each billing interval, POST /streams/{{id}}/tick. Check the
   response: if is_open is false, the stream is exhausted.
4. When the task completes or is cancelled: POST /streams/{{id}}/close
   to seal the stream and get a receipt. The unused remainder is never
   spent.
5. To get paid funds back: POST /streams/{{id}}/refund (only on closed
   streams, and only if the payee still holds the funds).
6. To verify that payment occurred: GET /streams/{{id}}/receipt returns
   the receipt with amount and payer/payee — no key needed, receipts are
   publicly verifiable.

## Notes
- All mutations are idempotent: repeating the same call returns the
  original result. You can safely retry on network errors.
- rate_per_tick must be >= 1, max_total must be >= rate_per_tick.
- Streams are persisted (Firestore) and survive restarts/redeploys.
- Any valid API key can act on any stream_id — a key currently proves
  "a registered caller," not "the payer or payee of this specific
  stream." Don't rely on it for authorization between mutually
  distrusting agents yet; that's a planned follow-up, not implemented.
- This service implements the streaming semantics validated by the
  Nanda Town streaming payments plugin (Phase 1 of NandaHack).
"""


@app.get("/skill.md", response_class=PlainTextResponse)
async def skill_md() -> str:
    """Serve the SKILL.md for agent discovery."""
    base = os.environ.get("SKILL_BASE_URL", "https://streampay.tinylab.ai")
    return _SKILL_MD_TEMPLATE.format(BASE_URL=base)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness check."""
    return {"status": "ok", "service": "streampay", "version": "1.0.0"}


@firestore.async_transactional
async def _open_stream_txn(
    transaction: firestore.AsyncTransaction, doc_ref: Any, body: StreamCreate
) -> tuple[dict[str, Any], int]:
    """Returns (stream_data, http_status). Raises HTTPException for a closed re-open."""
    snapshot = await doc_ref.get(transaction=transaction)

    # Idempotency: stream already exists
    if snapshot.exists:
        s = snapshot.to_dict()
        if not s["is_open"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "stream_already_closed",
                    "detail": f"Stream {body.stream_id} is closed",
                },
            )
        return s, 200

    if body.max_total < body.rate_per_tick:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_params",
                "detail": (
                    f"max_total ({body.max_total}) must be >= rate_per_tick ({body.rate_per_tick})"
                ),
            },
        )

    entry = {"tick": 0, "amount": body.rate_per_tick, "kind": "debit"}
    exhausted = body.max_total == body.rate_per_tick
    s = {
        "stream_id": body.stream_id,
        "payer": body.payer,
        "payee": body.payee,
        "rate_per_tick": body.rate_per_tick,
        "max_total": body.max_total,
        "total_debited": body.rate_per_tick,
        "is_open": not exhausted,
        "entries": [entry],
        "opened_at": 0,
        "closed_receipt": None,
        "refund_receipt": None,
    }
    transaction.set(doc_ref, s)
    return s, 201


@app.post("/streams", status_code=201)
async def create_stream(
    body: StreamCreate, _agent: str = Depends(require_api_key)
) -> dict[str, Any]:
    """Open a streaming payment. Idempotent by stream_id."""
    doc_ref = _streams_collection.document(body.stream_id)
    transaction = _db.transaction()
    s, _status = await _open_stream_txn(transaction, doc_ref, body)
    return _stream_response(s).model_dump()


@firestore.async_transactional
async def _tick_stream_txn(
    transaction: firestore.AsyncTransaction, doc_ref: Any, tick: int | None
) -> dict[str, Any]:
    snapshot = await doc_ref.get(transaction=transaction)
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail={"error": "not_found"})

    s = snapshot.to_dict()
    if not s["is_open"]:
        raise HTTPException(status_code=409, detail={"error": "stream_closed"})

    resolved_tick = tick if tick is not None else s["entries"][-1]["tick"] + 1

    # Idempotency: don't double-bill the same tick
    if s["entries"] and s["entries"][-1]["tick"] == resolved_tick:
        return s

    remaining = s["max_total"] - s["total_debited"]
    if remaining <= 0:
        s["is_open"] = False
        transaction.update(doc_ref, {"is_open": False})
        return s

    amount = min(s["rate_per_tick"], remaining)
    s["total_debited"] += amount
    s["entries"].append({"tick": resolved_tick, "amount": amount, "kind": "debit"})
    if s["total_debited"] >= s["max_total"]:
        s["is_open"] = False

    transaction.update(
        doc_ref,
        {
            "total_debited": s["total_debited"],
            "entries": s["entries"],
            "is_open": s["is_open"],
        },
    )
    return s


@app.post("/streams/{stream_id}/tick")
async def tick_stream(
    stream_id: str,
    body: dict[str, int] | None = None,
    _agent: str = Depends(require_api_key),
) -> dict[str, Any]:
    """Drain one tick from a stream. Idempotent by tick number."""
    doc_ref = _streams_collection.document(stream_id)
    transaction = _db.transaction()
    tick = (body or {}).get("tick")
    s = await _tick_stream_txn(transaction, doc_ref, tick)
    return _stream_response(s).model_dump()


@firestore.async_transactional
async def _close_stream_txn(transaction: firestore.AsyncTransaction, doc_ref: Any) -> dict[str, Any]:
    snapshot = await doc_ref.get(transaction=transaction)
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail={"error": "not_found"})

    s = snapshot.to_dict()

    # Idempotency: return existing receipt
    if s.get("closed_receipt"):
        return s["closed_receipt"]

    receipt = {
        "payer": s["payer"],
        "payee": s["payee"],
        "amount": s["total_debited"],
        "status": "closed",
    }
    transaction.update(doc_ref, {"is_open": False, "closed_receipt": receipt})
    return receipt


@app.post("/streams/{stream_id}/close")
async def close_stream(stream_id: str, _agent: str = Depends(require_api_key)) -> dict[str, Any]:
    """Close a stream and return a receipt. Idempotent."""
    doc_ref = _streams_collection.document(stream_id)
    transaction = _db.transaction()
    receipt = await _close_stream_txn(transaction, doc_ref)
    return {"stream_id": stream_id, "receipt": receipt}


@app.get("/streams/{stream_id}")
async def get_stream(stream_id: str) -> dict[str, Any]:
    """Get stream state."""
    snapshot = await _streams_collection.document(stream_id).get()
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return _stream_response(snapshot.to_dict()).model_dump()


@app.get("/streams/{stream_id}/receipt")
async def get_receipt(stream_id: str) -> dict[str, Any]:
    """Get receipt for a closed stream."""
    snapshot = await _streams_collection.document(stream_id).get()
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    s = snapshot.to_dict()
    if s.get("closed_receipt"):
        return s["closed_receipt"]
    raise HTTPException(
        status_code=409,
        detail={"error": "stream_not_closed", "detail": f"Stream {stream_id} is still open"},
    )


@firestore.async_transactional
async def _refund_stream_txn(transaction: firestore.AsyncTransaction, doc_ref: Any) -> dict[str, Any]:
    snapshot = await doc_ref.get(transaction=transaction)
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail={"error": "not_found"})

    s = snapshot.to_dict()
    if s.get("refund_receipt"):
        return s["refund_receipt"]

    if s["is_open"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "stream_still_open", "detail": "Cannot refund open stream"},
        )

    refund = {
        "stream_id": s["stream_id"],
        "refund_amount": s["total_debited"],
        "refunded_to": s["payer"],
        "from": s["payee"],
        "status": "refunded",
    }
    transaction.update(doc_ref, {"refund_receipt": refund})
    return refund


@app.post("/streams/{stream_id}/refund")
async def refund_stream(stream_id: str, _agent: str = Depends(require_api_key)) -> dict[str, Any]:
    """Refund a closed stream. Idempotent."""
    doc_ref = _streams_collection.document(stream_id)
    transaction = _db.transaction()
    return await _refund_stream_txn(transaction, doc_ref)


@app.get("/streams")
async def list_streams(agent: str = "") -> dict[str, Any]:
    """List streams for a given agent (payer or payee)."""
    if not agent:
        return {"streams": [], "count": 0, "detail": "?agent=agent_id is required"}
    payer_q = _streams_collection.where("payer", "==", agent).stream()
    payee_q = _streams_collection.where("payee", "==", agent).stream()
    seen: dict[str, dict[str, Any]] = {}
    async for doc in payer_q:
        seen[doc.id] = doc.to_dict()
    async for doc in payee_q:
        seen[doc.id] = doc.to_dict()
    result = [_stream_response(s).model_dump() for s in seen.values()]
    return {"streams": result, "count": len(result)}

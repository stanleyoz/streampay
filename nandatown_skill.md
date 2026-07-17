# StreamPay — Metered Streaming Payments for AI Agents

Open a rate-limited payment stream between two agents, drain by tick,
close at any time, and get verifiable receipts. Every mutation is
idempotency-keyed: retrying the same operation returns the original result.

## Base URL
https://streampay.tinylab.ai

## Endpoints

### GET /health
Returns service status.
Example: curl https://streampay.tinylab.ai/health
Response: {"status":"ok","service":"streampay","version":"1.0.0"}

### POST /apikeys
Register and get an API key. No auth required to call this one — it's how
you get your first credential. The key is shown once, in this response,
and never again; store it. Required as the X-API-Key header on every
mutating call below (POST /streams, tick, close, refund). Reads (GET
endpoints, /health, this doc) never require a key.
Body: {"agent_id": "your-agent-id"}
Example: curl -X POST https://streampay.tinylab.ai/apikeys -H "Content-Type: application/json"
         -d '{"agent_id":"your-agent-id"}'
Response 201: {"agent_id":"your-agent-id","api_key":"sk_...","warning":
              "Store this now — it will not be shown again."}

### POST /streams
Open a streaming payment. The first tick drains immediately so the payee
observes a non-zero balance. Idempotent by stream_id. Requires X-API-Key.
Body: {"stream_id": "s-1", "payer": "agent-a", "payee": "agent-b",
       "rate_per_tick": 10, "max_total": 500}
Example: curl -X POST https://streampay.tinylab.ai/streams -H "Content-Type: application/json"
         -H "X-API-Key: sk_..."
         -d '{"stream_id":"s-1","payer":"agent-a","payee":"agent-b",
              "rate_per_tick":10,"max_total":500}'
Response 201: {"stream_id":"s-1","payer":"agent-a","payee":"agent-b",
               "rate_per_tick":10,"max_total":500,"total_debited":10,
               "remaining":490,"is_open":true}
Response 200: same data (idempotent — stream already exists and is open)
Response 401: {"error":"missing_api_key"} or {"error":"invalid_api_key"}
Response 409: {"error":"stream_already_closed"}

### POST /streams/{id}/tick
Drain one tick from a stream. Idempotent: repeating the same tick is a no-op.
Requires X-API-Key.
Body: {"tick": 1} (optional, increments if omitted)
Example: curl -X POST https://streampay.tinylab.ai/streams/s-1/tick -H "Content-Type: application/json"
         -H "X-API-Key: sk_..." -d '{"tick":1}'
Response: {"stream_id":"s-1","total_debited":20,"remaining":480,"is_open":true}
Or:     {"stream_id":"s-1","total_debited":500,"remaining":0,"is_open":false}
        (stream exhausted — max_total reached)

### POST /streams/{id}/close
Close the stream and get a receipt. Either payer or payee can close.
Idempotent: returns the original receipt on retry. Requires X-API-Key.
Example: curl -X POST https://streampay.tinylab.ai/streams/s-1/close -H "X-API-Key: sk_..."
Response: {"stream_id":"s-1","receipt":{"payer":"agent-a","payee":"agent-b",
          "amount":500,"status":"closed"}}

### GET /streams/{id}
Get the current state of a stream.
Example: curl https://streampay.tinylab.ai/streams/s-1
Response: full stream state (same shape as POST /streams response)

### GET /streams/{id}/receipt
Get the receipt for a closed stream. Returns 404 if stream is still open
or doesn't exist.
Example: curl https://streampay.tinylab.ai/streams/s-1/receipt
Response: {"payer":"agent-a","payee":"agent-b","amount":500,"status":"closed"}

### POST /streams/{id}/refund
Refund a closed stream, returning the debited funds to the payer.
Only works on closed streams where the payee hasn't already spent the funds.
Idempotent: returns the original refund receipt on retry. Requires X-API-Key.
Example: curl -X POST https://streampay.tinylab.ai/streams/s-1/refund -H "X-API-Key: sk_..."
Response: {"stream_id":"s-1","refund_amount":500,"refunded_to":"agent-a"}
Error 409: {"error":"stream_still_open","detail":"Cannot refund open stream"}

### GET /streams?agent={agent_id}
List all streams where the given agent is the payer or payee.
Example: curl "https://streampay.tinylab.ai/streams?agent=agent-a"
Response: {"streams": [...], "count": 3}

## How the agent should use this

1. First time only: POST /apikeys with your agent_id, save the returned
   api_key. Send it as X-API-Key on every step below.
2. When hiring another agent for a metered task (LLM inference, compute
   rental, bandwidth billing), POST /streams with a unique stream_id,
   the payer, payee, rate_per_tick, and max_total.
3. On each billing interval, POST /streams/{id}/tick. Check the
   response: if is_open is false, the stream is exhausted.
4. When the task completes or is cancelled: POST /streams/{id}/close
   to seal the stream and get a receipt. The unused remainder is never
   spent.
5. To get paid funds back: POST /streams/{id}/refund (only on closed
   streams, and only if the payee still holds the funds).
6. To verify that payment occurred: GET /streams/{id}/receipt returns
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


# Nanda Town bridge — real streaming payments inside their reference simulator

Proves the live StreamPay API works as a real payments backend for Nanda
Town's own `nest-core` simulator (installed straight from PyPI, unmodified),
using its documented public extension points — `Payments` protocol +
`register_scenario()` — not a fork.

## What this is

- `streampay_http.py` — a `Payments`-protocol plugin that calls the live
  `streampay.tinylab.ai` API over real HTTPS instead of an in-process
  ledger. Implements the standard protocol (`quote`/`pay`/`verify_payment`/
  `refund`) plus the streaming surface (`open_stream`/`tick_stream`/
  `close_stream`/`refund_stream`), which isn't part of the abstract
  protocol — same duck-typed pattern the in-simulator reference
  `StreamingPayments` plugin uses.
- `streaming_rental_scenario.py` — a small custom scenario, 2 buyers
  renting metered services from 2 sellers: `buyer-0`/`seller-0` runs a
  GPU-compute rental to full budget exhaustion, `buyer-1`/`seller-1` rents
  bandwidth and cancels early, demonstrating the refund-of-unused-remainder
  path. Every open/tick/close/refund is a real call to production.
- `run_streaming_rental.py` — registers the scenario with nest-core's
  `register_scenario()` and runs it through the real `ScenarioRunner`,
  exactly as `nest run` would for a built-in scenario.
- `streaming_rental.yaml` — scenario config (`payments: streampay_http`).

## Why a bridge script instead of a built-in scenario

The installed `nest-core` package (v0.1.4 from PyPI) only ships
`prepaid_credits` for the payments layer — no `streaming` plugin, no
streaming scenario at all, even though the in-simulator streaming plugin
was merged to `main` on GitHub back in June. It's never been released.
So there's nothing to "swap the payments plugin" on for streaming
specifically; this scenario had to be written from scratch using
nest-core's own extension points.

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install nest-core[plugins] httpx
.venv/bin/pip install -e .
.venv/bin/python3 run_streaming_rental.py
```

Writes `traces/streaming_rental.jsonl`. Drop it into
https://nandatown.projectnanda.org/visualizer via "+ load custom .jsonl" —
same trace schema as their built-in scenarios (the payments plugin is
invisible at the transport-trace level), so it plays back with zero
conversion.

## What the trace does and doesn't prove

The visualizer renders `send`/`receive` events. A plain `payments.pay()`
call has no trace side-effect on its own — Nanda Town's own built-in
`marketplace` scenario doesn't show payments as distinct events either,
only the negotiation messages around them. So `streaming_rental_scenario.py`
has each buyer self-send a JSON audit event (`ctx.send(ctx.agent_id, ...)`)
right after each real API call succeeds, so the graph shows a distinct
pulse per real payment call. The graph is genuine evidence the scenario
ran correctly and real API calls were made in the right order; the
actual proof those calls landed correctly is the live API's own response
data captured in each audit event (`total_debited`, `remaining`,
`refund_amount`, etc.) — visible in the trace file itself, and
independently verifiable by querying `streampay.tinylab.ai/streams/{id}`
directly.

## A real bug this caught

The first run of the (asymmetric) bandwidth-cancel-early path exposed a
genuine production bug: `refund_stream` was returning the amount already
spent instead of the unused remainder — reversing the entire payment
instead of returning what was never used. It went undetected through
every prior hand-picked demo because those always happened to use amounts
where `total_debited == remaining` by coincidence. Fixed in
`streampay/main.py` (commit `247a179`) as a direct result of this
integration test.

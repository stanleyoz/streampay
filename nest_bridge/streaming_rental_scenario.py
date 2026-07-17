# SPDX-License-Identifier: Apache-2.0
"""Custom nest-core scenario: 2 buyers renting metered services from 2 sellers,
paid via real streaming payments against the live StreamPay API.

Not a hackathon submission and not part of Nanda Town's built-in scenario set
(the installed nest-core package only ships prepaid_credits/one-shot pay() —
see streampay_http.py's docstring). This is a bridge script: it registers a
brand-new scenario factory with nest-core's public register_scenario()
extension point, using nest-core entirely as an installed, unmodified PyPI
dependency.

Two independent rental pairs, so the trace/visualizer shows two different
shapes of the same protocol:
  buyer-0 <-> seller-0: gpu-compute-per-minute, runs its full 5-minute budget
  buyer-1 <-> seller-1: premium-bandwidth-per-minute, cancels after 3 of 6
                        scheduled minutes and gets the unused budget refunded

Every open/tick/close/refund below is a real HTTPS call to the live
production StreamPay API (streampay.tinylab.ai), not simulated. Each call
also self-emits a JSON audit event over ctx.send() so the transport-layer
trace (and therefore the Nanda Town Visualizer, which only ever renders
send/receive events) shows a distinct pulse for every real payment API call
— not just negotiation chatter, which is what a plain marketplace run would
otherwise show instead.
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from nest_core.scenario import ScenarioConfig
from nest_core.sim.agent import AgentContext, StateMachineAgent
from nest_core.types import AgentId, PaymentRef

import httpx

from streampay_http import StreamPayHTTP


def _json_bytes(data: dict[str, Any]) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode()


def _json_loads(payload: bytes) -> dict[str, Any]:
    try:
        data: object = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return {}
    return cast("dict[str, Any]", data) if isinstance(data, dict) else {}


async def _audit(ctx: AgentContext, **fields: Any) -> None:
    """Self-send a JSON audit event so this real API call shows up as a
    distinct pulse in the trace/visualizer, not just as a side effect."""
    await ctx.send(ctx.agent_id, _json_bytes({"type": "streampay_live_call", **fields}))


class RentalSeller(StateMachineAgent):
    """Passive — payment flow is entirely buyer-driven, matching the
    reference scenarios' pattern (sellers never call the payments plugin
    themselves). Exists only to be an addressable AgentId."""


class RentalBuyer(StateMachineAgent):
    def __init__(
        self,
        seller: AgentId,
        service: str,
        rate_per_tick: int,
        max_total: int,
        n_ticks: int,
        run_id: str,
        cancel_after: int | None = None,
    ) -> None:
        super().__init__()
        self._seller = seller
        self._service = service
        self._rate = rate_per_tick
        self._max_total = max_total
        self._n_ticks = n_ticks
        self._run_id = run_id
        self._cancel_after = cancel_after
        self._ref: PaymentRef | None = None

    async def on_start(self, ctx: AgentContext) -> None:
        payments: StreamPayHTTP = ctx.plugins["payments"]
        # run_id makes each invocation of this script use fresh stream_ids —
        # the underlying API is correctly idempotent per stream_id (retrying
        # the *same* ref returns the original result / a real 409 on a
        # closed one), but re-running this demo script is a new run, not a
        # retry, so it should get its own streams rather than colliding with
        # a previous run's now-closed ones.
        self._ref = PaymentRef(f"rental-{self._run_id}-{ctx.agent_id}-{self._seller}")

        handle = await payments.open_stream(
            payer=ctx.agent_id,
            payee=self._seller,
            rate_per_tick=self._rate,
            max_total=self._max_total,
            ref=self._ref,
        )
        await _audit(
            ctx,
            event="open_stream",
            service=self._service,
            payer=str(ctx.agent_id),
            payee=str(self._seller),
            rate_per_tick=self._rate,
            max_total=self._max_total,
            total_debited=handle["total_debited"],
        )

        # Schedule every tick upfront (all relative to t=0, so a single
        # dropped self-message only skips that one tick) rather than
        # chaining "schedule the next tick from this tick."
        for i in range(1, self._n_ticks + 1):
            await ctx.schedule(float(i) * 2.0, f"tick:{i}".encode())

    async def on_message(self, ctx: AgentContext, sender: AgentId, payload: bytes) -> None:
        if sender != ctx.agent_id or not payload.startswith(b"tick:"):
            return
        tick = int(payload.decode().split(":", 1)[1])
        payments: StreamPayHTTP = ctx.plugins["payments"]
        assert self._ref is not None

        if self._cancel_after is not None and tick > self._cancel_after:
            return  # already closed after the cancel-triggering tick

        try:
            state = await payments.tick_stream(self._ref, tick)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 409:
                # Stream already exhausted/closed by an earlier tick — a
                # legitimate idempotent outcome, not a failure. Nothing left
                # to drain; fall through to the close/refund check below.
                await _audit(ctx, event="tick_skipped_already_closed", service=self._service, tick=tick)
                state = None
            else:
                raise
        else:
            await _audit(
                ctx,
                event="tick_stream",
                service=self._service,
                tick=tick,
                total_debited=state["total_debited"],
                remaining=state["remaining"],
                is_open=state["is_open"],
            )

        should_close = tick == self._n_ticks or (
            self._cancel_after is not None and tick == self._cancel_after
        )
        if should_close:
            receipt = await payments.close_stream(self._ref)
            await _audit(
                ctx, event="close_stream", service=self._service, amount=receipt["amount"]
            )
            if receipt["amount"] < self._max_total:
                refund = await payments.refund_stream(self._ref)
                await _audit(
                    ctx,
                    event="refund_stream",
                    service=self._service,
                    refund_amount=refund["refund_amount"],
                )


def streaming_rental_factory(
    config: ScenarioConfig, plugins: dict[str, Any]
) -> dict[AgentId, StateMachineAgent]:
    payments_cls = plugins.get("payments")
    payments = (
        payments_cls(agent_id=AgentId("streaming-rental-scenario"))
        if isinstance(payments_cls, type)
        else payments_cls
    )
    plugins["payments"] = payments

    seller0, seller1 = AgentId("seller-0"), AgentId("seller-1")
    buyer0, buyer1 = AgentId("buyer-0"), AgentId("buyer-1")
    run_id = str(int(time.time()))

    return {
        seller0: RentalSeller(),
        seller1: RentalSeller(),
        buyer0: RentalBuyer(
            seller=seller0,
            service="gpu-compute-per-minute",
            rate_per_tick=10,
            max_total=50,
            # open_stream() auto-drains the first tick, so 4 more calls
            # (not 5) exactly exhausts a 50-credit budget at rate 10.
            n_ticks=4,
            run_id=run_id,
        ),
        buyer1: RentalBuyer(
            seller=seller1,
            service="premium-bandwidth-per-minute",
            rate_per_tick=15,
            max_total=90,
            n_ticks=5,
            cancel_after=3,
            run_id=run_id,
        ),
    }

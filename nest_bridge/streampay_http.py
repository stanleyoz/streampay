# SPDX-License-Identifier: Apache-2.0
"""Payments plugin backed by the *live production* StreamPay API.

Not a hackathon submission — a one-off bridge to answer a concrete question:
does the real, deployed streampay.tinylab.ai service actually work as the
payments layer for Nanda Town's own (PyPI-installed) simulator, driving real
network calls instead of an in-process ledger?

pay() opens a stream that drains fully on the first tick (rate_per_tick ==
max_total == amount) then immediately closes it — exactly the "pay() should
behave like a stream that drains the full amount in one tick" equivalence
StreamPay's own SKILL.md documents.
"""

from __future__ import annotations

import os

import httpx
from nest_core.types import AgentId, Money, PaymentRef, PaymentStatus, Quote, Receipt, ServiceRef


class StreamPayHTTP:
    """Payments protocol implementation that calls the live StreamPay API over HTTP."""

    def __init__(self, agent_id: AgentId, initial_balance: int = 1000, **_ignored: object) -> None:
        self._agent_id = agent_id
        self._base = os.environ.get("STREAMPAY_BASE_URL", "https://streampay.tinylab.ai")
        self._client = httpx.AsyncClient(timeout=20.0)
        self._api_key: str | None = None
        self.calls = 0

    async def _ensure_key(self) -> str:
        if self._api_key is None:
            r = await self._client.post(
                f"{self._base}/apikeys", json={"agent_id": str(self._agent_id)}
            )
            self.calls += 1
            r.raise_for_status()
            self._api_key = r.json()["api_key"]
        return self._api_key

    def balance(self, agent: AgentId) -> int:
        # Not part of the documented Payments protocol, but the built-in
        # `marketplace` scenario's agent logic calls it directly (see
        # nest_core/scenarios_builtin/marketplace.py:87) — a hard dependency
        # on PrepaidCredits's concrete API, not the abstract protocol. Real
        # balance lives in StreamPay's Firestore ledger, not in-process, so
        # this just reports "always funded" rather than modeling scarcity.
        return 10**9

    async def quote(self, service: ServiceRef) -> Quote:
        return Quote(service=service, price=Money(amount=10))

    async def pay(self, to: AgentId, amount: Money, ref: PaymentRef) -> Receipt:
        key = await self._ensure_key()
        headers = {"X-API-Key": key}
        amt = max(1, int(amount.amount))
        body = {
            "stream_id": str(ref),
            "payer": str(self._agent_id),
            "payee": str(to),
            "rate_per_tick": amt,
            "max_total": amt,
        }
        r = await self._client.post(f"{self._base}/streams", json=body, headers=headers)
        self.calls += 1
        r.raise_for_status()
        r2 = await self._client.post(f"{self._base}/streams/{ref}/close", headers=headers)
        self.calls += 1
        r2.raise_for_status()
        return Receipt(ref=ref, payer=self._agent_id, payee=to, amount=amount)

    async def verify_payment(self, ref: PaymentRef) -> PaymentStatus:
        r = await self._client.get(f"{self._base}/streams/{ref}/receipt")
        self.calls += 1
        return PaymentStatus.CONFIRMED if r.status_code == 200 else PaymentStatus.FAILED

    async def refund(self, ref: PaymentRef) -> None:
        key = await self._ensure_key()
        r = await self._client.post(
            f"{self._base}/streams/{ref}/refund", headers={"X-API-Key": key}
        )
        self.calls += 1
        r.raise_for_status()

    # -------------------------------------------------------------------
    # Streaming surface (not part of the abstract Payments protocol —
    # duck-typed extra methods, same as the in-process reference
    # StreamingPayments plugin). payer/payee are explicit args rather than
    # implied by self._agent_id, because nest-core's runner instantiates
    # ONE shared payments plugin instance for the whole scenario (see
    # ScenarioRunner._resolve_plugins), not one per agent — self._agent_id
    # on a shared instance can't mean "whichever agent happens to be
    # calling right now."
    # -------------------------------------------------------------------

    async def open_stream(
        self, payer: AgentId, payee: AgentId, rate_per_tick: int, max_total: int, ref: PaymentRef
    ) -> dict[str, object]:
        key = await self._ensure_key()
        body = {
            "stream_id": str(ref),
            "payer": str(payer),
            "payee": str(payee),
            "rate_per_tick": rate_per_tick,
            "max_total": max_total,
        }
        r = await self._client.post(
            f"{self._base}/streams", json=body, headers={"X-API-Key": key}
        )
        self.calls += 1
        r.raise_for_status()
        return r.json()

    async def tick_stream(self, ref: PaymentRef, tick: int) -> dict[str, object]:
        key = await self._ensure_key()
        r = await self._client.post(
            f"{self._base}/streams/{ref}/tick", json={"tick": tick}, headers={"X-API-Key": key}
        )
        self.calls += 1
        r.raise_for_status()
        return r.json()

    async def close_stream(self, ref: PaymentRef) -> dict[str, object]:
        key = await self._ensure_key()
        r = await self._client.post(
            f"{self._base}/streams/{ref}/close", headers={"X-API-Key": key}
        )
        self.calls += 1
        r.raise_for_status()
        return r.json()["receipt"]

    async def refund_stream(self, ref: PaymentRef) -> dict[str, object]:
        key = await self._ensure_key()
        r = await self._client.post(
            f"{self._base}/streams/{ref}/refund", headers={"X-API-Key": key}
        )
        self.calls += 1
        r.raise_for_status()
        return r.json()

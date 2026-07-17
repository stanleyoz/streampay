#!/usr/bin/env python3
"""Driver: register the custom streaming_rental scenario, then run it through
nest-core's real ScenarioRunner exactly as `nest run` would, against the
live production StreamPay API.

Run from this directory: python3 run_streaming_rental.py
"""

from __future__ import annotations

import asyncio

from nest_core.runner import ScenarioRunner
from nest_core.scenario import ScenarioConfig
from nest_core.scenarios import register_scenario
from streaming_rental_scenario import streaming_rental_factory


async def main() -> None:
    register_scenario("streaming_rental", streaming_rental_factory)
    config = ScenarioConfig.from_yaml("streaming_rental.yaml")
    runner = ScenarioRunner(config)
    trace_path = await runner.run()
    print(f"Trace written to: {trace_path}")


if __name__ == "__main__":
    asyncio.run(main())

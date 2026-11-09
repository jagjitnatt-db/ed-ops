"""Advance the simulator to `now`: plan arrivals and emit due events in hourly chunks."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.events import events_in_window
from ed_ops.generator.journey import empty_plan, plan_arrivals
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.scenarios import build_scenarios
from ed_ops.generator.state import GeneratorState
from ed_ops.timeutil import epoch_seconds, floor_to

CHUNK = timedelta(hours=1)
PLAN_LOOKBACK = timedelta(days=3)
Writer = Callable[[list[dict], datetime], str]


def initial_state(now: datetime, backfill_days: int) -> GeneratorState:
    start = floor_to(now - timedelta(days=backfill_days), 3600)
    return GeneratorState("main", start, start, start, None)


def advance(store, ref: ReferenceData, cfg: Config, now: datetime, *, live: bool, writer: Writer,
            persist_each_chunk: bool) -> list[str]:
    state = store.load_state()
    if state is None:
        raise RuntimeError("generator state missing; run the backfill-generate task first")
    if now <= state.last_planned_ts:
        return []
    if live and state.live_started_ts is None:
        state = replace(state, live_started_ts=now)
    windows = build_scenarios(ref.facilities, state, cfg)
    plan = store.load_plan(since=state.last_planned_ts - PLAN_LOOKBACK)
    pending: list[pd.DataFrame] = []
    paths: list[str] = []
    t = state.last_planned_ts
    while t < now:
        end = min(t + CHUNK, now)
        rng = np.random.default_rng([cfg.seed, epoch_seconds(end)])
        new = plan_arrivals(rng, ref, plan, t, end, windows, cfg.timezone)
        if not new.empty:
            plan = pd.concat([plan, new], ignore_index=True) if not plan.empty else new
        events = events_in_window(plan, ref, state.last_emitted_ts, end, cfg)
        if events:
            paths.append(writer(events, end))
        state = replace(state, last_planned_ts=end, last_emitted_ts=end)
        if persist_each_chunk:
            store.upsert_plan(new)
            store.save_state(state)
        else:
            pending.append(new)
        t = end
    if not persist_each_chunk:
        non_empty = [p for p in pending if not p.empty]
        store.upsert_plan(pd.concat(non_empty, ignore_index=True) if non_empty else empty_plan())
        store.save_state(state)
    return paths

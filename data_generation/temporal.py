"""
TemporalController — 24-month maturation timeline for AfyaBima.

Phase structure (exactly as specified in the proposal):

    Phase 1 (Months  1- 6): No investigations.  Claims arrive but no labels exist.
    Phase 2 (Months  7-12): Retrospective.  Investigators review suspicious
                            Phase-1 claims.  Lag is long (14-60 days).
    Phase 3 (Months 13-18): Operational.  Real-time flagging begins.
                            Lag shortens (7-30 days).
    Phase 4 (Months 19-24): ML evaluation.  Weekly retraining, feedback loop active.
                            Lag tightens further (5-21 days).

Investigation rates represent the fraction of *all* claims that get investigated.
Fraud claims are 3x more likely to be selected than legitimate ones (selection bias),
which is encoded in should_investigate() rather than in the raw rate.
"""

from __future__ import annotations

import datetime
import random
from typing import TypedDict

# ---------------------------------------------------------------------------
# Phase specification
# ---------------------------------------------------------------------------


class PhaseSpec(TypedDict):
    phase: int
    name: str
    months: int
    investigation_rate: float
    lag_days: tuple[int, int]


_PHASE_SPECS: list[PhaseSpec] = [
    {
        "phase": 1,
        "name": "No Labels",
        "months": 6,
        "investigation_rate": 0.0,
        "lag_days": (0, 0),
    },
    {
        "phase": 2,
        "name": "Retrospective",
        "months": 6,
        "investigation_rate": 0.18,
        "lag_days": (14, 60),
    },
    {
        "phase": 3,
        "name": "Operational",
        "months": 6,
        "investigation_rate": 0.20,
        "lag_days": (7, 30),
    },
    {
        "phase": 4,
        "name": "ML Evaluation",
        "months": 6,
        "investigation_rate": 0.22,
        "lag_days": (5, 21),
    },
]

# Fraud claims are this many times more likely to be investigated than legitimate ones.
_FRAUD_INVESTIGATION_MULTIPLIER = 3.0

# Upper cap on the investigation probability for a single fraud claim.
_MAX_INVESTIGATION_RATE = 0.90


class PhaseInfo:
    """Immutable snapshot of a single phase's configuration."""

    __slots__ = ("phase", "name", "start", "end", "investigation_rate", "lag_days")

    def __init__(
        self,
        phase: int,
        name: str,
        start: datetime.date,
        end: datetime.date,
        investigation_rate: float,
        lag_days: tuple[int, int],
    ) -> None:
        self.phase = phase
        self.name = name
        self.start = start
        self.end = end
        self.investigation_rate = investigation_rate
        self.lag_days = lag_days

    def __repr__(self) -> str:
        return f"PhaseInfo(phase={self.phase}, name={self.name!r}, start={self.start}, end={self.end})"


class TemporalController:
    """
    Manages the 24-month timeline and all date-dependent decisions.

    Parameters
    ----------
    start_date:
        First day of Phase 1.  Defaults to exactly 730 days before today so the
        timeline ends approximately at today's date.
    """

    def __init__(self, start_date: datetime.date | None = None) -> None:
        self.start_date: datetime.date = (
            start_date if start_date is not None else datetime.date.today() - datetime.timedelta(days=730)
        )
        self._phases: list[PhaseInfo] = self._build_phases()
        self.end_date: datetime.date = self._phases[-1].end

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    def _build_phases(self) -> list[PhaseInfo]:
        phases: list[PhaseInfo] = []
        current = self.start_date
        for spec in _PHASE_SPECS:
            end = current + datetime.timedelta(days=30 * spec["months"])
            phases.append(
                PhaseInfo(
                    phase=spec["phase"],
                    name=spec["name"],
                    start=current,
                    end=end,
                    investigation_rate=spec["investigation_rate"],
                    lag_days=spec["lag_days"],
                )
            )
            current = end
        return phases

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_phase(self, date: datetime.date) -> PhaseInfo:
        """Return the PhaseInfo for *date*.  Clamps to the last phase if past end."""
        for phase in self._phases:
            if phase.start <= date < phase.end:
                return phase
        return self._phases[-1]

    def should_investigate(self, claim_date: datetime.date, is_fraud: bool) -> bool:
        """
        Decide probabilistically whether a claim on *claim_date* gets investigated.

        Fraud claims are _FRAUD_INVESTIGATION_MULTIPLIER x more likely to be
        selected, simulating investigator prioritisation of suspicious cases.
        Phase-1 claims are never investigated (no investigation function exists yet).
        """
        phase = self.get_phase(claim_date)

        if phase.phase == 1 or phase.investigation_rate == 0.0:
            return False

        base_rate = phase.investigation_rate
        if is_fraud:
            rate = min(base_rate * _FRAUD_INVESTIGATION_MULTIPLIER, _MAX_INVESTIGATION_RATE)
        else:
            # Legitimate claims investigated at a much lower rate (false positives)
            rate = base_rate * (1.0 / _FRAUD_INVESTIGATION_MULTIPLIER)

        return random.random() < rate

    def investigation_lag_days(self, claim_date: datetime.date) -> int:
        """
        Return a random lag (in days) between claim submission and investigation close.

        Returns 0 for Phase-1 claims (no investigation function).
        """
        phase = self.get_phase(claim_date)
        lag_min, lag_max = phase.lag_days
        if lag_min == 0 and lag_max == 0:
            return 0
        return random.randint(lag_min, lag_max)

    def all_dates(self) -> list[datetime.date]:
        """Return every date in the 24-month window, inclusive of start_date."""
        out: list[datetime.date] = []
        current = self.start_date
        while current < self.end_date:
            out.append(current)
            current += datetime.timedelta(days=1)
        return out

    def __repr__(self) -> str:
        return f"TemporalController(start={self.start_date}, end={self.end_date}, phases={len(self._phases)})"

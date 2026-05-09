from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping

SPY_RELATIVE_MOMENTUM = "spy_relative_momentum"
SECTOR_RELATIVE_MOMENTUM = "sector_relative_momentum"
EVENT_ONLY_WATCHLIST = "event_only_watchlist"
RANDOM_ELIGIBLE_UNIVERSE = "random_eligible_universe"
USER_WATCHLIST = "user_watchlist"


@dataclass(frozen=True)
class BaselineCandidate:
    """Deterministic candidate selected by a validation baseline."""

    baseline: str
    ticker: str
    as_of: Any
    rank: int
    score: float
    reason: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        baseline = _required_text(self.baseline, "baseline")
        ticker = _required_text(self.ticker, "ticker").upper()
        if self.rank < 1:
            msg = "rank must be positive"
            raise ValueError(msg)
        if not math.isfinite(float(self.score)):
            msg = "score must be finite"
            raise ValueError(msg)
        reason = _required_text(self.reason, "reason")
        if isinstance(self.as_of, datetime) and (
            self.as_of.tzinfo is None or self.as_of.utcoffset() is None
        ):
            msg = "as_of must be timezone-aware when provided as datetime"
            raise ValueError(msg)
        object.__setattr__(self, "baseline", baseline)
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "score", float(self.score))
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "baseline": self.baseline,
            "ticker": self.ticker,
            "as_of": self.as_of.isoformat() if isinstance(self.as_of, datetime) else self.as_of,
            "rank": self.rank,
            "score": self.score,
            "reason": self.reason,
            "payload": dict(self.payload),
        }


def spy_relative_momentum(
    rows: Iterable[Any],
    *,
    limit: int | None = None,
) -> tuple[BaselineCandidate, ...]:
    """Rank eligible rows by stored 20d and 60d SPY-relative momentum."""

    scored: list[tuple[float, str, Any, Mapping[str, Any], str]] = []
    for row in rows:
        if not _is_eligible(row):
            continue
        ticker = _ticker(row)
        if ticker is None:
            continue
        rel_20d = _relative_return(row, horizon="20d", benchmark="spy")
        rel_60d = _relative_return(row, horizon="60d", benchmark="spy")
        if rel_20d is None and rel_60d is None:
            continue
        score = (rel_20d or 0.0) + (rel_60d or 0.0)
        payload = {
            "relative_return_20d": rel_20d,
            "relative_return_60d": rel_60d,
            "source": "stored_returns",
        }
        reason = (
            "SPY-relative stored momentum "
            f"20d={_format_score(rel_20d)} 60d={_format_score(rel_60d)}"
        )
        scored.append((score, ticker, _as_of(row), payload, reason))
    return _ranked_candidates(scored, SPY_RELATIVE_MOMENTUM, limit=limit)


def sector_relative_momentum(
    rows: Iterable[Any],
    *,
    limit: int | None = None,
) -> tuple[BaselineCandidate, ...]:
    """Rank eligible rows by their stored sector-relative momentum score."""

    scored: list[tuple[float, str, Any, Mapping[str, Any], str]] = []
    for row in rows:
        if not _is_eligible(row):
            continue
        ticker = _ticker(row)
        if ticker is None:
            continue
        score = _first_float(
            row,
            "sector_relative_score",
            "sector_momentum_score",
            "rs_20_sector",
            "ticker_vs_sector",
        )
        if score is None:
            ticker_return = _first_float(
                row,
                "ret_20d",
                "return_20d",
                "ticker_return_20d",
            )
            sector_return = _first_float(row, "sector_return_20d", "benchmark_return_20d")
            if ticker_return is None or sector_return is None:
                continue
            score = ticker_return - sector_return
        payload = {"sector_relative_score": score, "source": "stored_sector_momentum"}
        reason = f"Sector-relative stored momentum score={score:.4f}"
        scored.append((score, ticker, _as_of(row), payload, reason))
    return _ranked_candidates(scored, SECTOR_RELATIVE_MOMENTUM, limit=limit)


def event_only_watchlist(
    rows: Iterable[Any],
    *,
    limit: int | None = None,
) -> tuple[BaselineCandidate, ...]:
    """Select eligible rows that have material event support, independent of momentum."""

    scored: list[tuple[float, str, Any, Mapping[str, Any], str]] = []
    for row in rows:
        if not _is_eligible(row):
            continue
        ticker = _ticker(row)
        if ticker is None:
            continue
        event_score = _event_support_score(row)
        if event_score is None or event_score <= 0:
            continue
        event_count = _first_float(row, "material_event_count") or _event_count(row)
        payload = {
            "event_support_score": event_score,
            "material_event_count": event_count,
        }
        reason = f"Material event support score={event_score:.4f}"
        scored.append((event_score, ticker, _as_of(row), payload, reason))
    return _ranked_candidates(scored, EVENT_ONLY_WATCHLIST, limit=limit)


def random_eligible_universe(
    rows: Iterable[Any],
    *,
    seed: int | str,
    limit: int | None = None,
) -> tuple[BaselineCandidate, ...]:
    """Return a deterministic pseudo-random sample from eligible rows."""

    scored: list[tuple[float, str, Any, Mapping[str, Any], str]] = []
    seed_text = str(seed)
    for row in rows:
        if not _is_eligible(row):
            continue
        ticker = _ticker(row)
        if ticker is None:
            continue
        as_of = _as_of(row)
        random_score = _deterministic_random(seed_text, ticker, as_of)
        payload = {"seed": seed_text, "random_score": random_score}
        reason = f"Deterministic seeded eligible-universe sample seed={seed_text}"
        scored.append((random_score, ticker, as_of, payload, reason))
    return _ranked_candidates(scored, RANDOM_ELIGIBLE_UNIVERSE, limit=limit)


def user_watchlist(
    rows: Iterable[Any],
    tickers: Sequence[str] | None = None,
    *,
    config: Mapping[str, Any] | None = None,
    limit: int | None = None,
) -> tuple[BaselineCandidate, ...]:
    """Return configured user watchlist tickers that are present in the row universe."""

    configured = _configured_watchlist(tickers, config)
    if not configured:
        return ()
    rows_by_ticker = {
        ticker: row
        for row in rows
        if (ticker := _ticker(row)) is not None and _is_eligible(row)
    }
    candidates: list[BaselineCandidate] = []
    for index, ticker in enumerate(configured, start=1):
        row = rows_by_ticker.get(ticker)
        if row is None:
            continue
        candidates.append(
            BaselineCandidate(
                baseline=USER_WATCHLIST,
                ticker=ticker,
                as_of=_as_of(row),
                rank=len(candidates) + 1,
                score=float(len(configured) - index + 1),
                reason="Configured user watchlist member",
                payload={"configured_rank": index},
            )
        )
        if limit is not None and len(candidates) >= max(limit, 0):
            break
    return tuple(candidates)


def _ranked_candidates(
    scored: list[tuple[float, str, Any, Mapping[str, Any], str]],
    baseline: str,
    *,
    limit: int | None,
) -> tuple[BaselineCandidate, ...]:
    if limit is not None and limit <= 0:
        return ()
    ordered = sorted(scored, key=lambda item: (-item[0], item[1], str(item[2] or "")))
    if limit is not None:
        ordered = ordered[:limit]
    return tuple(
        BaselineCandidate(
            baseline=baseline,
            ticker=ticker,
            as_of=as_of,
            rank=rank,
            score=score,
            reason=reason,
            payload=payload,
        )
        for rank, (score, ticker, as_of, payload, reason) in enumerate(ordered, start=1)
    )


def _relative_return(row: Any, *, horizon: str, benchmark: str) -> float | None:
    relative = _first_float(
        row,
        f"{benchmark}_relative_return_{horizon}",
        f"relative_return_{horizon}_{benchmark}",
        f"ret_{horizon}_vs_{benchmark}",
        f"return_{horizon}_vs_{benchmark}",
    )
    if relative is not None:
        return relative
    ticker_return = _first_float(
        row,
        f"ret_{horizon}",
        f"return_{horizon}",
        f"ticker_return_{horizon}",
    )
    benchmark_return = _first_float(
        row,
        f"{benchmark}_return_{horizon}",
        f"benchmark_return_{horizon}",
    )
    if ticker_return is None:
        return None
    if benchmark_return is None:
        return ticker_return
    return ticker_return - benchmark_return


def _event_support_score(row: Any) -> float | None:
    explicit = _first_float(row, "event_support_score", "material_event_score")
    if explicit is not None:
        return explicit
    events = _events(row)
    if events:
        scores = []
        for event in events:
            materiality = _float_or_none(_read(event, "materiality"))
            source_quality = _float_or_none(_read(event, "source_quality"))
            if materiality is not None and source_quality is not None:
                scores.append(materiality * source_quality * 100)
            elif materiality is not None:
                scores.append(materiality * 100)
        return max(scores) if scores else float(len(events))
    event_count = _first_float(row, "material_event_count")
    if event_count is not None and event_count > 0:
        return event_count
    return None


def _event_count(row: Any) -> float:
    return float(len(_events(row)))


def _events(row: Any) -> tuple[Mapping[str, Any], ...]:
    value = _lookup(row, "events")
    if isinstance(value, Mapping):
        return (_mapping(value),)
    if isinstance(value, str) or not isinstance(value, Iterable):
        return ()
    return tuple(_mapping(item) for item in value if isinstance(item, Mapping))


def _configured_watchlist(
    tickers: Sequence[str] | None,
    config: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    source: Any = tickers
    if source is None and config is not None:
        source = (
            config.get("user_watchlist")
            or config.get("watchlist")
            or _mapping(config.get("validation")).get("user_watchlist")
            or _mapping(config.get("validation")).get("watchlist")
        )
    if source is None or isinstance(source, str):
        return ()
    result = []
    seen = set()
    for item in source:
        ticker = _maybe_text(item)
        if ticker is None:
            continue
        ticker = ticker.upper()
        if ticker not in seen:
            seen.add(ticker)
            result.append(ticker)
    return tuple(result)


def _is_eligible(row: Any) -> bool:
    if _lookup(row, "eligible") is False or _lookup(row, "baseline_eligible") is False:
        return False
    if _non_empty_sequence(_lookup(row, "hard_blocks")):
        return False
    if _non_empty_sequence(_lookup(row, "leakage_flags")):
        return False
    return _ticker(row) is not None


def _deterministic_random(seed: str, ticker: str, as_of: Any) -> float:
    key = f"{seed}:{ticker}:{as_of.isoformat() if isinstance(as_of, datetime) else as_of}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16) / float(16**16 - 1)


def _ticker(row: Any) -> str | None:
    ticker = _maybe_text(
        _first_present(
            _read(row, "ticker"),
            _nested(row, "payload", "ticker"),
            _nested(row, "payload", "identity", "ticker"),
            _nested(row, "payload", "candidate", "ticker"),
            _nested(row, "payload", "candidate", "features", "ticker"),
        )
    )
    return ticker.upper() if ticker is not None else None


def _as_of(row: Any) -> Any:
    return _first_present(
        _read(row, "as_of"),
        _nested(row, "payload", "as_of"),
        _nested(row, "payload", "identity", "as_of"),
        _nested(row, "payload", "candidate", "as_of"),
        _nested(row, "payload", "candidate", "features", "as_of"),
    )


def _first_float(row: Any, *names: str) -> float | None:
    for name in names:
        value = _lookup(row, name)
        number = _float_or_none(value)
        if number is not None:
            return number
    return None


def _lookup(row: Any, name: str) -> Any:
    locations = (
        (name,),
        ("payload", name),
        ("payload", "features", name),
        ("payload", "scores", name),
        ("payload", "metadata", name),
        ("payload", "candidate", name),
        ("payload", "candidate", "features", name),
        ("payload", "candidate", "metadata", name),
        ("payload", "candidate", "metadata", "sector_rotation", name),
        ("payload", "sector_rotation", name),
        ("features", name),
        ("scores", name),
        ("metadata", name),
        ("candidate", name),
        ("candidate", "features", name),
        ("candidate", "metadata", name),
        ("candidate", "metadata", "sector_rotation", name),
    )
    for path in locations:
        value = _nested(row, *path)
        if value is not None:
            return value
    return None


def _nested(source: Any, *keys: str) -> Any:
    value = source
    for key in keys:
        value = _read(value, key)
        if value is None:
            return None
    return value


def _read(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    if is_dataclass(source) and not isinstance(source, type):
        return getattr(source, key, None)
    keys = getattr(source, "keys", None)
    if callable(keys):
        try:
            return source[key]
        except (KeyError, TypeError):
            return None
    return getattr(source, key, None)


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return {}


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _non_empty_sequence(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Iterable):
        return any(True for _ in value)
    return bool(value)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _maybe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(value: Any, field_name: str) -> str:
    text = _maybe_text(value)
    if text is None:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _format_score(value: float | None) -> str:
    return "missing" if value is None else f"{value:.4f}"


__all__ = [
    "EVENT_ONLY_WATCHLIST",
    "RANDOM_ELIGIBLE_UNIVERSE",
    "SECTOR_RELATIVE_MOMENTUM",
    "SPY_RELATIVE_MOMENTUM",
    "USER_WATCHLIST",
    "BaselineCandidate",
    "event_only_watchlist",
    "random_eligible_universe",
    "sector_relative_momentum",
    "spy_relative_momentum",
    "user_watchlist",
]

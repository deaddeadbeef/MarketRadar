from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping, thaw_json_value
from catalyst_radar.core.models import ActionState

REPLAY_SCHEMA_VERSION = "replay-row-v1"
VALIDATION_RESULT_SCHEMA_VERSION = "validation-result-v1"

_MISSING = object()


@dataclass(frozen=True)
class ReplayRow:
    ticker: str
    as_of: datetime
    decision_available_at: datetime
    state: ActionState
    final_score: float
    candidate_state_id: str | None
    candidate_packet_id: str | None
    decision_card_id: str | None
    hard_blocks: tuple[str, ...]
    transition_reasons: tuple[str, ...]
    score_delta_5d: float | None
    leakage_flags: tuple[str, ...]
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _aware_datetime(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "decision_available_at",
            _aware_datetime(self.decision_available_at, "decision_available_at"),
        )
        object.__setattr__(self, "state", _action_state(self.state))
        object.__setattr__(self, "final_score", _finite_float(self.final_score, "final_score"))
        if self.candidate_state_id is not None:
            object.__setattr__(
                self,
                "candidate_state_id",
                _optional_text(self.candidate_state_id),
            )
        if self.candidate_packet_id is not None:
            object.__setattr__(
                self,
                "candidate_packet_id",
                _optional_text(self.candidate_packet_id),
            )
        if self.decision_card_id is not None:
            object.__setattr__(
                self,
                "decision_card_id",
                _optional_text(self.decision_card_id),
            )
        object.__setattr__(self, "hard_blocks", _text_tuple(self.hard_blocks))
        object.__setattr__(
            self,
            "transition_reasons",
            _text_tuple(self.transition_reasons),
        )
        if self.score_delta_5d is not None:
            object.__setattr__(
                self,
                "score_delta_5d",
                _finite_float(self.score_delta_5d, "score_delta_5d"),
            )
        object.__setattr__(self, "leakage_flags", _text_tuple(self.leakage_flags))
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


def build_replay_row(
    candidate_input: Mapping[str, Any] | object,
    *,
    decision_available_at: datetime | str,
    candidate_packet: Mapping[str, Any] | object | None = None,
    decision_card: Mapping[str, Any] | object | None = None,
) -> ReplayRow:
    """Build a point-in-time replay row from persisted candidate artifacts.

    The candidate state is required and must have been created no later than the
    decision cutoff. Optional packet and card objects are included only when
    their own availability timestamps are present and no later than the cutoff.
    """

    decision_at = _aware_datetime(decision_available_at, "decision_available_at")
    input_row = _as_mapping(candidate_input)
    candidate_state = _candidate_state_mapping(input_row)

    candidate_created_at = _optional_datetime(
        _read(candidate_state, "created_at", _MISSING),
        "candidate_state.created_at",
    )
    if candidate_created_at is None:
        msg = "candidate_state.created_at is required for point-in-time replay"
        raise ValueError(msg)
    if candidate_created_at > decision_at:
        msg = (
            "candidate_state.created_at is after decision_available_at: "
            f"{candidate_created_at.isoformat()} > {decision_at.isoformat()}"
        )
        raise ValueError(msg)

    ticker = _required_text(
        _first_existing(
            _read(candidate_state, "ticker", _MISSING),
            _nested(input_row, "signal_payload", "candidate", "ticker", default=_MISSING),
        ),
        "ticker",
    ).upper()
    as_of = _aware_datetime(
        _first_existing(
            _read(candidate_state, "as_of", _MISSING),
            _nested(input_row, "signal_payload", "candidate", "as_of", default=_MISSING),
        ),
        "as_of",
    )
    state = _action_state(
        _first_existing(
            _read(candidate_state, "state", _MISSING),
            _nested(input_row, "signal_payload", "policy", "state", default=_MISSING),
        )
    )
    final_score = _finite_float(
        _first_existing(
            _read(candidate_state, "final_score", _MISSING),
            _nested(input_row, "signal_payload", "candidate", "final_score", default=_MISSING),
        ),
        "final_score",
    )
    candidate_state_id = _optional_text(_read(candidate_state, "id", None))
    hard_blocks = _text_tuple(
        _first_existing(
            _read(candidate_state, "hard_blocks", _MISSING),
            _nested(input_row, "signal_payload", "policy", "hard_blocks", default=()),
        )
    )
    transition_reasons = _text_tuple(
        _first_existing(
            _read(candidate_state, "transition_reasons", _MISSING),
            _nested(input_row, "signal_payload", "policy", "reasons", default=()),
        )
    )
    score_delta_5d = _optional_float(_read(candidate_state, "score_delta_5d", None))

    leakage_flags: list[str] = []
    packet_payload: Mapping[str, Any] | None = None
    card_payload: Mapping[str, Any] | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None

    if candidate_packet is not None:
        leakage_flags.extend(_text_tuple(_read(candidate_packet, "replay_leakage_flags", ())))
        packet_result = _visible_artifact(
            candidate_packet,
            decision_at,
            artifact_name="candidate_packet",
        )
        leakage_flags.extend(packet_result.leakage_flags)
        if packet_result.visible_payload is not None:
            packet_payload = packet_result.visible_payload
            candidate_packet_id = _optional_text(_read(packet_payload, "id", None))

    if decision_card is not None:
        leakage_flags.extend(_text_tuple(_read(decision_card, "replay_leakage_flags", ())))
        card_result = _visible_artifact(
            decision_card,
            decision_at,
            artifact_name="decision_card",
        )
        leakage_flags.extend(card_result.leakage_flags)
        if card_result.visible_payload is not None:
            card_payload = card_result.visible_payload
            decision_card_id = _optional_text(_read(card_payload, "id", None))

    payload = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "candidate_state": _json_ready(candidate_state),
        "signal_payload": _json_ready(_read(input_row, "signal_payload", {})),
        "packet": _json_ready(_artifact_payload(packet_payload)),
        "decision_card": _json_ready(_artifact_payload(card_payload)),
        "audit": {
            "candidate_state_created_at": candidate_created_at.isoformat(),
            "decision_available_at": decision_at.isoformat(),
            "packet_available_at": _iso_or_none(_read(packet_payload, "available_at", None)),
            "decision_card_available_at": _iso_or_none(_read(card_payload, "available_at", None)),
            "score_recomputed": False,
            "external_calls": False,
        },
    }

    return ReplayRow(
        ticker=ticker,
        as_of=as_of,
        decision_available_at=decision_at,
        state=state,
        final_score=final_score,
        candidate_state_id=candidate_state_id,
        candidate_packet_id=candidate_packet_id,
        decision_card_id=decision_card_id,
        hard_blocks=hard_blocks,
        transition_reasons=transition_reasons,
        score_delta_5d=score_delta_5d,
        leakage_flags=tuple(dict.fromkeys(leakage_flags)),
        payload=payload,
    )


def build_replay_results(
    repository: Any,
    validation_repo: Any | None = None,
    *,
    as_of_start: datetime | str,
    as_of_end: datetime | str,
    decision_available_at: datetime | str,
    states: Iterable[ActionState | str] | None = None,
    tickers: Iterable[str] | None = None,
    run_id: str | None = None,
    result_factory: Callable[..., Any] | None = None,
) -> list[Any]:
    """Replay persisted candidate inputs without writing validation rows.

    ``validation_repo`` is accepted for the broader Phase 9 call shape but is
    intentionally unused here; this helper is read-only.
    """

    del validation_repo
    start = _aware_datetime(as_of_start, "as_of_start")
    end = _aware_datetime(as_of_end, "as_of_end")
    decision_at = _aware_datetime(decision_available_at, "decision_available_at")
    if end < start:
        msg = "as_of_end must be greater than or equal to as_of_start"
        raise ValueError(msg)

    replay_run_id = run_id or deterministic_replay_run_id(
        as_of_start=start,
        as_of_end=end,
        decision_available_at=decision_at,
        states=states,
        tickers=tickers,
    )
    factory = result_factory if result_factory is not None else _optional_validation_result()

    results = []
    candidate_inputs = _list_candidate_inputs(
        repository,
        as_of_start=start,
        as_of_end=end,
        decision_available_at=decision_at,
        states=states,
        tickers=tickers,
    )
    for candidate_input in candidate_inputs:
        input_row = _as_mapping(candidate_input)
        candidate_state = _candidate_state_mapping(input_row)
        ticker = _required_text(_read(candidate_state, "ticker", _MISSING), "ticker").upper()
        as_of = _aware_datetime(_read(candidate_state, "as_of", _MISSING), "as_of")
        if as_of < start or as_of > end:
            continue

        packet = _visible_with_future_flags(
            repository,
            collection_method="candidate_packets_for_replay",
            latest_method="latest_candidate_packet",
            ticker=ticker,
            as_of=as_of,
            available_at=decision_at,
        )
        card = _visible_with_future_flags(
            repository,
            collection_method="decision_cards_for_replay",
            latest_method="latest_decision_card",
            ticker=ticker,
            as_of=as_of,
            available_at=decision_at,
        )
        row = build_replay_row(
            candidate_input,
            candidate_packet=packet,
            decision_card=card,
            decision_available_at=decision_at,
        )
        results.append(_build_validation_result(row, run_id=replay_run_id, factory=factory))

    return sorted(
        results,
        key=lambda item: (
            _result_read(item, "ticker"),
            _result_read(item, "as_of"),
            _result_read(item, "id"),
        ),
    )


def replay_row_payload(row: ReplayRow | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(row, ReplayRow):
        return {
            "ticker": row.ticker,
            "as_of": row.as_of.isoformat(),
            "decision_available_at": row.decision_available_at.isoformat(),
            "state": row.state.value,
            "final_score": row.final_score,
            "candidate_state_id": row.candidate_state_id,
            "candidate_packet_id": row.candidate_packet_id,
            "decision_card_id": row.decision_card_id,
            "hard_blocks": list(row.hard_blocks),
            "transition_reasons": list(row.transition_reasons),
            "score_delta_5d": row.score_delta_5d,
            "leakage_flags": list(row.leakage_flags),
            "payload": _json_ready(row.payload),
        }
    return _json_ready(row)


def canonical_replay_json(value: ReplayRow | Mapping[str, Any] | Sequence[Any]) -> str:
    return json.dumps(
        _json_ready(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def deterministic_replay_run_id(
    *,
    as_of_start: datetime,
    as_of_end: datetime,
    decision_available_at: datetime,
    states: Iterable[ActionState | str] | None = None,
    tickers: Iterable[str] | None = None,
) -> str:
    payload = {
        "as_of_start": _aware_datetime(as_of_start, "as_of_start").isoformat(),
        "as_of_end": _aware_datetime(as_of_end, "as_of_end").isoformat(),
        "decision_available_at": _aware_datetime(
            decision_available_at,
            "decision_available_at",
        ).isoformat(),
        "states": sorted(_state_value(state) for state in states or ()),
        "tickers": sorted(str(ticker).upper() for ticker in tickers or ()),
    }
    digest = hashlib.sha256(canonical_replay_json(payload).encode("utf-8")).hexdigest()[:16]
    return f"validation-replay-v1:{payload['as_of_start']}:{payload['as_of_end']}:{digest}"


@dataclass(frozen=True)
class _ArtifactVisibility:
    visible_payload: Mapping[str, Any] | None
    leakage_flags: tuple[str, ...]


def _visible_artifact(
    artifact: Mapping[str, Any] | object,
    decision_available_at: datetime,
    *,
    artifact_name: str,
) -> _ArtifactVisibility:
    payload = _as_mapping(artifact)
    available_at = _optional_datetime(
        _read(payload, "available_at", None),
        f"{artifact_name}.available_at",
    )
    if available_at is None:
        return _ArtifactVisibility(
            visible_payload=None,
            leakage_flags=(f"{artifact_name}_missing_available_at",),
        )
    if available_at > decision_available_at:
        return _ArtifactVisibility(
            visible_payload=None,
            leakage_flags=(f"{artifact_name}_future_available_at",),
        )
    return _ArtifactVisibility(visible_payload=payload, leakage_flags=())


def _build_validation_result(
    row: ReplayRow,
    *,
    run_id: str,
    factory: Callable[..., Any] | None,
) -> Any:
    payload = replay_row_payload(row)
    result = {
        "id": deterministic_validation_result_id(row, run_id=run_id),
        "run_id": run_id,
        "ticker": row.ticker,
        "as_of": row.as_of,
        "available_at": row.decision_available_at,
        "state": row.state,
        "final_score": row.final_score,
        "candidate_state_id": row.candidate_state_id,
        "candidate_packet_id": row.candidate_packet_id,
        "decision_card_id": row.decision_card_id,
        "baseline": None,
        "labels": {},
        "leakage_flags": row.leakage_flags,
        "payload": payload,
    }
    if factory is None:
        return result

    try:
        return factory(**_factory_kwargs(factory, result))
    except TypeError:
        return result


def deterministic_validation_result_id(row: ReplayRow, *, run_id: str) -> str:
    key = {
        "as_of": row.as_of.isoformat(),
        "candidate_state_id": row.candidate_state_id,
        "run_id": run_id,
        "state": row.state.value,
        "ticker": row.ticker,
    }
    digest = hashlib.sha256(canonical_replay_json(key).encode("utf-8")).hexdigest()[:16]
    return f"{VALIDATION_RESULT_SCHEMA_VERSION}:{row.ticker}:{row.as_of.isoformat()}:{digest}"


def _factory_kwargs(factory: Callable[..., Any], result: Mapping[str, Any]) -> dict[str, Any]:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return dict(result)
    parameters = signature.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
        return dict(result)
    return {key: value for key, value in result.items() if key in parameters}


def _optional_validation_result() -> Callable[..., Any] | None:
    try:
        module = importlib.import_module("catalyst_radar.validation.models")
    except ModuleNotFoundError:
        return None
    return getattr(module, "ValidationResult", None)


def _list_candidate_inputs(
    repository: Any,
    *,
    as_of_start: datetime,
    as_of_end: datetime,
    decision_available_at: datetime,
    states: Iterable[ActionState | str] | None,
    tickers: Iterable[str] | None,
) -> list[Any]:
    method = repository.list_candidate_inputs
    states_tuple = tuple(states or ())
    tickers_tuple = tuple(tickers or ())
    base_kwargs: dict[str, Any] = {
        "available_at": decision_available_at,
    }
    if states_tuple:
        base_kwargs["states"] = states_tuple
    if tickers_tuple:
        base_kwargs["tickers"] = tickers_tuple

    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        signature = None

    if signature is not None and {
        "as_of_start",
        "as_of_end",
    }.issubset(signature.parameters):
        return list(
            method(
                as_of_start=as_of_start,
                as_of_end=as_of_end,
                **base_kwargs,
            )
        )

    if as_of_start == as_of_end:
        return list(method(as_of=as_of_start, **base_kwargs))

    inputs: list[Any] = []
    for as_of in _daily_datetimes(as_of_start, as_of_end):
        inputs.extend(method(as_of=as_of, **base_kwargs))
    return inputs


def _visible_with_future_flags(
    repository: Any,
    *,
    collection_method: str,
    latest_method: str,
    ticker: str,
    as_of: datetime,
    available_at: datetime,
) -> Any | None:
    method = getattr(repository, collection_method, None)
    if method is None:
        return _latest_optional(
            repository,
            latest_method,
            ticker=ticker,
            as_of=as_of,
            available_at=available_at,
        )
    artifacts = list(method(ticker, as_of, available_at))
    visible = [
        artifact
        for artifact in artifacts
        if (artifact_available_at := _optional_datetime(
            _read(artifact, "available_at", None),
            f"{collection_method}.available_at",
        ))
        is not None
        and artifact_available_at <= available_at
    ]
    futures = [
        artifact
        for artifact in artifacts
        if (artifact_available_at := _optional_datetime(
            _read(artifact, "available_at", None),
            f"{collection_method}.available_at",
        ))
        is not None
        and artifact_available_at > available_at
    ]
    if not futures:
        return visible[0] if visible else None
    if visible:
        return _artifact_with_replay_flags(visible[0], futures=futures)
    return futures[0]


def _latest_optional(
    repository: Any,
    method_name: str,
    *,
    ticker: str,
    as_of: datetime,
    available_at: datetime,
) -> Any | None:
    method = getattr(repository, method_name, None)
    if method is None:
        return None
    return method(ticker, as_of, available_at)


def _artifact_with_replay_flags(artifact: Any, *, futures: Sequence[Any]) -> Mapping[str, Any]:
    payload = dict(_as_mapping(artifact))
    existing_flags = _text_tuple(_read(payload, "replay_leakage_flags", ()))
    payload["replay_leakage_flags"] = tuple(
        dict.fromkeys((*existing_flags, _artifact_future_flag(payload)))
    )
    payload["replay_future_artifact_count"] = len(futures)
    return payload


def _artifact_future_flag(artifact: Mapping[str, Any]) -> str:
    if _read(artifact, "candidate_packet_id", None) is not None or _read(
        artifact,
        "action_state",
        None,
    ) is not None:
        return "decision_card_future_available_at"
    return "candidate_packet_future_available_at"


def _daily_datetimes(start: datetime, end: datetime) -> Iterable[datetime]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _candidate_state_mapping(row: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate_state = row.get("candidate_state", row)
    if not isinstance(candidate_state, Mapping):
        candidate_state = _as_mapping(candidate_state)
    return candidate_state


def _artifact_payload(artifact: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if artifact is None:
        return None
    payload = _read(artifact, "payload", None)
    if isinstance(payload, Mapping):
        return payload
    return artifact


def _result_read(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        value = item.get(key)
    else:
        value = getattr(item, key)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _as_mapping(value: Mapping[str, Any] | object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "_mapping"):
        return dict(value._mapping)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    keys = getattr(value, "keys", None)
    if callable(keys):
        return {str(key): value[key] for key in keys()}
    attrs: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        item = getattr(value, name)
        if not callable(item):
            attrs[name] = item
    return attrs


def _read(source: Any, key: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(key, default)
    if is_dataclass(source) and not isinstance(source, type):
        return getattr(source, key, default)
    return getattr(source, key, default)


def _nested(source: Any, *keys: str, default: Any = None) -> Any:
    value = source
    for key in keys:
        value = _read(value, key, _MISSING)
        if value is _MISSING:
            return default
    return value


def _first_existing(*values: Any) -> Any:
    for value in values:
        if value is not _MISSING:
            return value
    return _MISSING


def _aware_datetime(value: Any, field_name: str) -> datetime:
    if value is _MISSING or value is None:
        msg = f"{field_name} is required"
        raise ValueError(msg)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _optional_datetime(value: Any, field_name: str) -> datetime | None:
    if value is _MISSING or value is None:
        return None
    return _aware_datetime(value, field_name)


def _action_state(value: Any) -> ActionState:
    if isinstance(value, ActionState):
        return value
    if value is _MISSING or value is None:
        msg = "state is required"
        raise ValueError(msg)
    return ActionState(str(value))


def _state_value(value: ActionState | str) -> str:
    return value.value if isinstance(value, ActionState) else ActionState(str(value)).value


def _finite_float(value: Any, field_name: str) -> float:
    if value is _MISSING or value is None:
        msg = f"{field_name} is required"
        raise ValueError(msg)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be finite"
        raise ValueError(msg) from exc
    if not math.isfinite(number):
        msg = f"{field_name} must be finite"
        raise ValueError(msg)
    return number


def _optional_float(value: Any) -> float | None:
    if value is _MISSING or value is None:
        return None
    return _finite_float(value, "float")


def _required_text(value: Any, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_text(value: Any) -> str | None:
    if value is _MISSING or value is None:
        return None
    text = str(value).strip()
    return text or None


def _text_tuple(value: Any) -> tuple[str, ...]:
    if value is _MISSING or value is None:
        return ()
    if isinstance(value, str):
        return (_required_text(value, "text"),)
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value if str(item))
    text = _optional_text(value)
    return (text,) if text is not None else ()


def _iso_or_none(value: Any) -> str | None:
    parsed = _optional_datetime(value, "datetime")
    return parsed.isoformat() if parsed is not None else None


def _json_ready(value: Any) -> Any:
    if isinstance(value, ReplayRow):
        return replay_row_payload(value)
    if isinstance(value, MappingProxyType):
        value = dict(value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_ready(getattr(value, field.name))
            for field in sorted(fields(value), key=lambda item: item.name)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _aware_datetime(value, "datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_json_ready(item) for item in value]
    return thaw_json_value(value)


__all__ = [
    "REPLAY_SCHEMA_VERSION",
    "VALIDATION_RESULT_SCHEMA_VERSION",
    "ReplayRow",
    "build_replay_results",
    "build_replay_row",
    "canonical_replay_json",
    "deterministic_replay_run_id",
    "deterministic_validation_result_id",
    "replay_row_payload",
]

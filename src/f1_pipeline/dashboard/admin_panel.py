# AI assistance: Anthropic Claude
# Scope: Admin panel with pipeline activity, live job progress, season coverage, and timeline.
from __future__ import annotations

import html
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from f1_pipeline.job_runner import JOB_MANIFEST_DIR, read_job_progress
from f1_pipeline.settings import CURATED_DATA_DIR

REQUEST_RATE_WINDOW_SECONDS = 60.0
LIVE_REFRESH_SECONDS = 2.0

MANIFEST_DIR = CURATED_DATA_DIR / "manifests"


@dataclass(frozen=True)
class ActivityEntry:
    kind: str
    label: str
    status: str
    timestamp: pd.Timestamp
    detail: dict[str, Any]
    path: Path


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _timestamp(value: Any) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return parsed if isinstance(parsed, pd.Timestamp) and pd.notna(parsed) else None


def _file_timestamp(path: Path) -> pd.Timestamp:
    return pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")


def _best_timestamp(payload: dict[str, Any], path: Path, *candidate_fields: str) -> pd.Timestamp:
    for field in candidate_fields:
        parsed = _timestamp(payload.get(field))
        if parsed is not None:
            return parsed
    return _file_timestamp(path)


def _job_entries(job_dir: Path = JOB_MANIFEST_DIR) -> list[ActivityEntry]:
    entries: list[ActivityEntry] = []
    if not job_dir.is_dir():
        return entries
    for path in job_dir.glob("job_*.json"):
        if path.name.endswith(".progress.json"):
            continue
        payload = _read_json(path)
        if payload is None:
            continue
        intent = payload.get("intent")
        intent = intent if isinstance(intent, dict) else {}
        label = f"{intent.get('purpose', 'unknown')} · meeting {intent.get('meeting_key', '?')}"
        entries.append(
            ActivityEntry(
                kind="Dashboard job",
                label=label,
                status=str(payload.get("status", "unknown")),
                timestamp=_best_timestamp(payload, path),
                detail=payload,
                path=path,
            )
        )
    return entries


def _weekend_pipeline_entries(manifest_dir: Path = MANIFEST_DIR) -> list[ActivityEntry]:
    entries: list[ActivityEntry] = []
    for path in manifest_dir.glob("weekend_weather_pipeline_*.json"):
        payload = _read_json(path)
        if payload is None:
            continue
        selection = payload.get("selection")
        selection = selection if isinstance(selection, dict) else {}
        label = (
            f"meeting {selection.get('meeting_key', '?')} · "
            f"{selection.get('purpose', 'unknown')}"
        )
        entries.append(
            ActivityEntry(
                kind="Weekend pipeline",
                label=label,
                status=str(payload.get("status", "unknown")),
                timestamp=_best_timestamp(payload, path, "completed_at", "started_at"),
                detail=payload,
                path=path,
            )
        )
    return entries


def _master_data_entries(manifest_dir: Path = MANIFEST_DIR) -> list[ActivityEntry]:
    entries: list[ActivityEntry] = []
    for path in manifest_dir.glob("master_data_*.json"):
        payload = _read_json(path)
        if payload is None:
            continue
        validation = payload.get("validation")
        validation_status = (
            validation.get("status", "unknown") if isinstance(validation, dict) else "unknown"
        )
        entries.append(
            ActivityEntry(
                kind="Season master data",
                label=f"season {payload.get('season', '?')}",
                status=str(validation_status),
                timestamp=_best_timestamp(payload, path, "ingested_at"),
                detail=payload,
                path=path,
            )
        )
    return entries


def _session_entries(manifest_dir: Path = MANIFEST_DIR) -> list[ActivityEntry]:
    entries: list[ActivityEntry] = []
    session_dir = manifest_dir / "openf1_sessions"
    if not session_dir.is_dir():
        return entries
    for path in session_dir.glob("session_*.json"):
        payload = _read_json(path)
        if payload is None:
            continue
        endpoints = payload.get("endpoints")
        endpoints = endpoints if isinstance(endpoints, dict) else {}
        retrieved = [
            timestamp
            for result in endpoints.values()
            if isinstance(result, dict)
            and (timestamp := _timestamp(result.get("retrieved_at"))) is not None
        ]
        timestamp = max(retrieved) if retrieved else _file_timestamp(path)
        entries.append(
            ActivityEntry(
                kind="Session ingestion",
                label=f"session {payload.get('session_key', '?')} · {payload.get('session_type', '?')}",
                status=str(payload.get("status", "unknown")),
                timestamp=timestamp,
                detail=payload,
                path=path,
            )
        )
    return entries


def _geometry_entries(manifest_dir: Path = MANIFEST_DIR) -> list[ActivityEntry]:
    entries: list[ActivityEntry] = []
    geometry_dir = manifest_dir / "geometries"
    if not geometry_dir.is_dir():
        return entries
    for path in geometry_dir.glob("geometry_*.json"):
        payload = _read_json(path)
        if payload is None:
            continue
        entries.append(
            ActivityEntry(
                kind="Track geometry",
                label=f"session {payload.get('session_key', '?')}",
                status=str(payload.get("status", "unknown")),
                timestamp=_file_timestamp(path),
                detail=payload,
                path=path,
            )
        )
    return entries


def collect_pipeline_activity(
    limit: int = 40,
    *,
    job_dir: Path = JOB_MANIFEST_DIR,
    manifest_dir: Path = MANIFEST_DIR,
) -> list[ActivityEntry]:
    """Gather the most recent entries across every kind of persisted pipeline output."""
    entries = (
        _job_entries(job_dir)
        + _weekend_pipeline_entries(manifest_dir)
        + _master_data_entries(manifest_dir)
        + _session_entries(manifest_dir)
        + _geometry_entries(manifest_dir)
    )
    entries.sort(key=lambda entry: entry.timestamp, reverse=True)
    return entries[:limit]


def _running_job_entries(entries: list[ActivityEntry]) -> list[ActivityEntry]:
    return [
        entry
        for entry in entries
        if entry.kind == "Dashboard job" and entry.status == "running"
    ]


def _recent_event_count(
    events: list[dict[str, Any]], *, window_seconds: float = REQUEST_RATE_WINDOW_SECONDS
) -> int:
    """Count progress events recorded within the last `window_seconds`.

    Stands in for a direct OpenF1 client request counter, which does not
    exist -- each recorded endpoint fetch is one real request, so this is an
    honest (if slightly conservative, since retries and the `location`
    per-driver loop can add unrecorded sub-requests) live approximation of
    how close a running job is to the client's ~1 request / 2.1s throttle.
    """
    if not events:
        return 0
    now = pd.Timestamp.now(tz="UTC")
    return sum(
        1
        for event in events
        if (recorded_at := _timestamp(event.get("recorded_at"))) is not None
        and (now - recorded_at).total_seconds() <= window_seconds
    )


def _progress_table(events: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Session": event.get("session_key", "-"),
                "Endpoint": event.get("endpoint", event.get("stage", "-")),
                "Status": str(event.get("status", "-")),
                "Rows": event.get("row_count", "-"),
                "Duration (s)": event.get("duration_seconds", "-"),
                "At (UTC)": event.get("recorded_at", "-"),
            }
            for event in events
        ]
    )


STATUS_TONES = {
    "available": "ok",
    "stale": "ok",
    "valid": "ok",
    "partial": "warn",
    "running": "run",
    "stalled": "bad",
    "unavailable": "bad",
    "failed": "bad",
    "error": "bad",
    "invalid": "bad",
}
TONE_COLORS = {
    "ok": "#21a366",
    "warn": "#e8a317",
    "bad": "#e10600",
    "run": "#2f80ed",
    "none": "#8b8b8b",
}
TONE_FILTERS = {"OK": "ok", "Partial": "warn", "Failed": "bad", "Running": "run"}
STALLED_AFTER_SECONDS = 1800.0
CACHE_SECONDS = 10
FEED_LENGTH = 9
NEW_EVENT_SECONDS = 12.0
COVERAGE_SOURCES = (
    ("openf1_weekend_facts", "OpenF1 sessions"),
    ("wikidata", "Circuit · Wikidata"),
    ("open_meteo", "Weather · Open-Meteo"),
    ("track_geometry", "Track geometry"),
)
_STATUS_MARKERS = {
    "running": "● ",
    "available": "✓ ",
    "stale": "✓ ",
    "valid": "✓ ",
    "partial": "◐ ",
    "unavailable": "✕ ",
}


def status_tone(status: Any) -> str:
    return STATUS_TONES.get(str(status).strip().casefold(), "none")


def _last_job_activity(entry: ActivityEntry, events: list[dict[str, Any]]) -> pd.Timestamp:
    stamps = [entry.timestamp]
    stamps.extend(
        stamp for event in events if (stamp := _timestamp(event.get("recorded_at"))) is not None
    )
    return max(stamps)


def is_stalled(
    entry: ActivityEntry,
    events: list[dict[str, Any]],
    now: pd.Timestamp | None = None,
) -> bool:
    """A job still marked running without any sign of life for a long time.

    The status file is only rewritten when a job ends. If its process is killed
    (for example the job service), the file keeps saying "running" forever.
    """
    if entry.kind != "Dashboard job" or entry.status != "running":
        return False
    current = now if now is not None else pd.Timestamp.now(tz="UTC")
    return (current - _last_job_activity(entry, events)).total_seconds() > STALLED_AFTER_SECONDS


def _latest_weekend_manifests(
    manifest_dir: Path = MANIFEST_DIR,
) -> dict[tuple[int, int], tuple[pd.Timestamp, dict[str, Any]]]:
    latest: dict[tuple[int, int], tuple[pd.Timestamp, dict[str, Any]]] = {}
    for path in manifest_dir.glob("weekend_weather_pipeline_*.json"):
        payload = _read_json(path)
        if payload is None:
            continue
        selection = payload.get("selection")
        selection = selection if isinstance(selection, dict) else {}
        season, meeting = selection.get("season"), selection.get("meeting_key")
        if not isinstance(season, int) or not isinstance(meeting, int):
            continue
        stamp = _best_timestamp(payload, path, "completed_at", "started_at")
        if (season, meeting) not in latest or stamp > latest[(season, meeting)][0]:
            latest[(season, meeting)] = (stamp, payload)
    return latest


def _weekend_name(context: dict[str, Any]) -> str:
    country = context.get("circuit_country") or "?"
    place = context.get("circuit_location") or context.get("circuit_name") or "?"
    return f"{country} · {place}"


def meeting_names(manifest_dir: Path = MANIFEST_DIR) -> dict[int, str]:
    names: dict[int, str] = {}
    for (_, meeting), (_, payload) in _latest_weekend_manifests(manifest_dir).items():
        context = payload.get("context")
        if isinstance(context, dict):
            names[meeting] = _weekend_name(context)
    return names


def season_coverage(season: int, manifest_dir: Path = MANIFEST_DIR) -> pd.DataFrame:
    """One row per loaded weekend with the status of every source it needs."""
    columns = [
        "meeting_key",
        "weekend",
        "date",
        "updated",
        "overall",
        "sessions_ok",
        "sessions_total",
        *(source for source, _ in COVERAGE_SOURCES),
    ]
    rows: list[dict[str, Any]] = []
    for (row_season, meeting), (stamp, payload) in _latest_weekend_manifests(manifest_dir).items():
        if row_season != season:
            continue
        context = payload.get("context")
        context = context if isinstance(context, dict) else {}
        jobs = payload.get("jobs")
        jobs = jobs if isinstance(jobs, dict) else {}
        target = context.get("target_session")
        target = target if isinstance(target, dict) else {}
        facts = jobs.get("openf1_weekend_facts")
        sessions = facts.get("sessions", []) if isinstance(facts, dict) else []
        session_statuses = [str(item.get("status")) for item in sessions if isinstance(item, dict)]
        row: dict[str, Any] = {
            "meeting_key": meeting,
            "weekend": _weekend_name(context),
            "date": _timestamp(target.get("scheduled_start_utc")) or pd.NaT,
            "updated": stamp,
            "overall": str(payload.get("status", "unknown")),
            "sessions_ok": sum(status_tone(status) == "ok" for status in session_statuses),
            "sessions_total": len(session_statuses),
        }
        for source, _ in COVERAGE_SOURCES:
            job = jobs.get(source)
            row[source] = str(job.get("status", "missing")) if isinstance(job, dict) else "missing"
        rows.append(row)
    frame = pd.DataFrame(rows, columns=columns)
    return frame.sort_values("date", na_position="last").reset_index(drop=True)


def coverage_seasons(manifest_dir: Path = MANIFEST_DIR) -> list[int]:
    return sorted({season for season, _ in _latest_weekend_manifests(manifest_dir)}, reverse=True)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def _cached_activity(limit: int) -> list[ActivityEntry]:
    return collect_pipeline_activity(limit=limit)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def _cached_meeting_names() -> dict[int, str]:
    return meeting_names()


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def _cached_coverage(season: int) -> pd.DataFrame:
    return season_coverage(season)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def _cached_seasons() -> list[int]:
    return coverage_seasons()


@st.cache_data(ttl=60, show_spinner=False)
def _data_size_bytes(root: str) -> int:
    total = 0
    pending = [root]
    while pending:
        try:
            with os.scandir(pending.pop()) as items:
                for item in items:
                    if item.is_dir(follow_symlinks=False):
                        pending.append(item.path)
                    elif item.is_file(follow_symlinks=False):
                        total += item.stat().st_size
        except OSError:
            continue
    return total


def _format_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB"):
        if value < 1024:
            return f"{value:,.0f} {unit}"
        value /= 1024
    return f"{value:,.1f} GB"


def _format_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} s ago"
    if seconds < 3600:
        return f"{seconds // 60} min ago"
    if seconds < 86400:
        return f"{seconds // 3600} h {seconds % 3600 // 60} min ago"
    return f"{seconds // 86400} d ago"


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 3600}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def _labelled(entry: ActivityEntry, names: dict[int, str]) -> str:
    meeting: Any = None
    if entry.kind == "Dashboard job":
        intent = entry.detail.get("intent")
        meeting = intent.get("meeting_key") if isinstance(intent, dict) else None
    elif entry.kind == "Weekend pipeline":
        selection = entry.detail.get("selection")
        meeting = selection.get("meeting_key") if isinstance(selection, dict) else None
    name = names.get(meeting) if isinstance(meeting, int) else None
    return f"{entry.label} ({name})" if name else entry.label


def _job_key(entry: ActivityEntry) -> str:
    return str(entry.detail.get("job_id", ""))


def _inject_admin_styles() -> None:
    st.markdown(
        """
        <style>
        .f1a-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: .6rem; margin: .3rem 0 1rem; }
        .f1a-kpi { border: 1px solid rgba(128,128,128,.25); border-left: 4px solid var(--tone);
            border-radius: 10px; padding: .55rem .8rem; background: rgba(128,128,128,.05); }
        .f1a-kpi .v { font-size: 1.55rem; font-weight: 700; line-height: 1.15; }
        .f1a-kpi .l { font-size: .7rem; opacity: .72; text-transform: uppercase;
            letter-spacing: .04em; }
        .f1a-dot { display: inline-block; width: .6rem; height: .6rem; border-radius: 50%;
            background: var(--tone); margin-right: .4rem; vertical-align: middle; flex: none; }
        .f1a-dot.live { animation: f1a-pulse 1.4s ease-out infinite; }
        @keyframes f1a-pulse {
            0% { box-shadow: 0 0 0 0 rgba(47,128,237,.75); }
            75% { box-shadow: 0 0 0 .6rem rgba(47,128,237,0); }
            100% { box-shadow: 0 0 0 0 rgba(47,128,237,0); }
        }
        .f1a-job { border: 1px solid rgba(47,128,237,.35); border-radius: 10px;
            padding: .6rem .8rem; margin-bottom: .5rem; background: rgba(47,128,237,.06); }
        .f1a-job .t { font-weight: 650; }
        .f1a-job .s { font-size: .8rem; opacity: .8; margin-top: .35rem; }
        .f1a-bar { height: .55rem; border-radius: 999px; background: rgba(128,128,128,.18);
            overflow: hidden; margin-top: .45rem; }
        .f1a-bar > span { display: block; height: 100%;
            background: repeating-linear-gradient(45deg, #2f80ed 0 10px, #6aa6f4 10px 20px);
            background-size: 28px 28px; animation: f1a-stripes .9s linear infinite; }
        @keyframes f1a-stripes { from { background-position: 0 0; }
            to { background-position: 28px 0; } }
        .f1a-feed { list-style: none; padding: 0; margin: 0; font-size: .82rem; }
        .f1a-feed li { padding: .28rem .45rem; border-bottom: 1px solid rgba(128,128,128,.14);
            display: flex; gap: .5rem; align-items: center; }
        .f1a-feed li .when { font-variant-numeric: tabular-nums; opacity: .65; min-width: 4.4rem; }
        .f1a-feed li.new { animation: f1a-in 1.2s ease-out; }
        @keyframes f1a-in { from { background: rgba(47,128,237,.28); transform: translateX(-8px); }
            to { background: transparent; transform: none; } }
        .f1a-grid { width: 100%; border-collapse: collapse; font-size: .8rem; }
        .f1a-grid th { text-align: left; font-weight: 600; padding: .35rem .45rem;
            border-bottom: 1px solid rgba(128,128,128,.35); }
        .f1a-grid td { padding: .28rem .45rem; border-bottom: 1px solid rgba(128,128,128,.12); }
        .f1a-cell { display: inline-block; min-width: 5.4rem; text-align: center; border-radius: 6px;
            padding: .08rem .45rem; color: #fff; font-size: .72rem; font-weight: 600;
            background: var(--tone); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _kpi(label: str, value: str, tone: str, *, live: bool = False) -> str:
    dot = '<span class="f1a-dot live"></span>' if live else ""
    return (
        f'<div class="f1a-kpi" style="--tone:{TONE_COLORS[tone]}">'
        f'<div class="v">{dot}{html.escape(value)}</div>'
        f'<div class="l">{html.escape(label)}</div></div>'
    )


def _job_progress_fraction(entry: ActivityEntry, events: list[dict[str, Any]]) -> float | None:
    intent = entry.detail.get("intent")
    planned = intent.get("session_keys") if isinstance(intent, dict) else None
    if not planned:
        return None
    seen = {event.get("session_key") for event in events if event.get("session_key") is not None}
    return min(1.0, len(seen) / len(planned))


def _event_text(event: dict[str, Any]) -> str:
    text = str(event.get("endpoint", event.get("stage", "-")))
    if event.get("session_key") is not None:
        text += f" · session {event['session_key']}"
    if isinstance(event.get("row_count"), int):
        text += f" · {event['row_count']:,} rows"
    return text


def _running_job_card(
    entry: ActivityEntry,
    events: list[dict[str, Any]],
    names: dict[int, str],
    now: pd.Timestamp,
) -> str:
    rows = sum(event["row_count"] for event in events if isinstance(event.get("row_count"), int))
    last_text = (
        f"last: {_event_text(events[-1])}" if events else "waiting for the shared source lock"
    )
    fraction = _job_progress_fraction(entry, events)
    width = f"{max(fraction, 0.04) * 100:.0f}%" if fraction is not None else "100%"
    return (
        '<div class="f1a-job">'
        f'<div class="t"><span class="f1a-dot live" style="--tone:{TONE_COLORS["run"]}"></span>'
        f"{html.escape(_labelled(entry, names))}</div>"
        f'<div class="f1a-bar"><span style="width:{width}"></span></div>'
        f'<div class="s">running for {_format_duration((now - entry.timestamp).total_seconds())}'
        f" · {len(events)} endpoint fetches · {rows:,} rows"
        f" · {_recent_event_count(events)} requests / {int(REQUEST_RATE_WINDOW_SECONDS)} s"
        f" · {html.escape(last_text)}</div></div>"
    )


def _feed_items(
    entries: list[ActivityEntry],
    running_events: list[dict[str, Any]],
    names: dict[int, str],
) -> list[tuple[pd.Timestamp, str, str]]:
    items: list[tuple[pd.Timestamp, str, str]] = []
    for event in running_events:
        stamp = _timestamp(event.get("recorded_at"))
        if stamp is not None:
            items.append((stamp, status_tone(event.get("status", "running")), _event_text(event)))
    for entry in entries:
        items.append(
            (
                entry.timestamp,
                status_tone(entry.status),
                f"{entry.kind} · {_labelled(entry, names)} · {entry.status}",
            )
        )
    items.sort(key=lambda item: item[0], reverse=True)
    return items[:FEED_LENGTH]


@st.fragment(run_every=LIVE_REFRESH_SECONDS)
def _render_live_overview(*, job_dir: Path = JOB_MANIFEST_DIR) -> None:
    """Self-contained live section, re-run on its own timer.

    A fragment reruns only this function. An earlier version reran the whole page
    with `time.sleep` + `st.rerun()`, which raced with real navigation and mixed
    two script runs on screen. Job files are re-read on every tick (they are few
    and small); the remaining manifests come from a short cache.
    """
    now = pd.Timestamp.now(tz="UTC")
    names = _cached_meeting_names()
    jobs = _job_entries(job_dir)
    others = [entry for entry in _cached_activity(300) if entry.kind != "Dashboard job"]
    entries = sorted(jobs + others, key=lambda entry: entry.timestamp, reverse=True)

    running = _running_job_entries(jobs)
    progress = {_job_key(entry): read_job_progress(_job_key(entry), job_dir=job_dir) for entry in running}
    active = [entry for entry in running if not is_stalled(entry, progress[_job_key(entry)], now)]
    stalled = [entry for entry in running if entry not in active]
    running_events = [event for entry in active for event in progress[_job_key(entry)]]

    tones = ["bad" if entry in stalled else status_tone(entry.status) for entry in entries]
    activity_stamps = [entry.timestamp for entry in entries[:1]]
    activity_stamps.extend(
        stamp for event in running_events if (stamp := _timestamp(event.get("recorded_at"))) is not None
    )
    last_activity = max(activity_stamps) if activity_stamps else None
    live_rows = sum(
        event["row_count"] for event in running_events if isinstance(event.get("row_count"), int)
    )
    live_tone = "run" if active else "none"
    st.markdown(
        '<div class="f1a-kpis">'
        + _kpi("Running jobs", str(len(active)), live_tone, live=bool(active))
        + _kpi("Successful", str(tones.count("ok")), "ok")
        + _kpi("Partial", str(tones.count("warn")), "warn")
        + _kpi("Failed / stalled", str(tones.count("bad")), "bad")
        + _kpi(
            "Last activity",
            _format_age((now - last_activity).total_seconds()) if last_activity is not None else "-",
            live_tone,
            live=bool(active),
        )
        + _kpi("Rows loaded by running jobs", f"{live_rows:,}", live_tone)
        + _kpi("Data on disk", _format_size(_data_size_bytes(str(CURATED_DATA_DIR.parent))), "none")
        + "</div>",
        unsafe_allow_html=True,
    )

    jobs_column, feed_column = st.columns([3, 2], gap="medium")
    with jobs_column:
        st.markdown("#### Currently running")
        if active:
            st.markdown(
                "".join(
                    _running_job_card(entry, progress[_job_key(entry)], names, now)
                    for entry in active
                ),
                unsafe_allow_html=True,
            )
            for entry in active:
                events = progress[_job_key(entry)]
                if events:
                    with st.expander(f"Endpoint log · {_labelled(entry, names)}"):
                        st.dataframe(_progress_table(events), hide_index=True, width="stretch")
        else:
            st.caption("No job is running. Start a load from the dashboard and it appears here live.")
        if stalled:
            st.warning(
                f"{len(stalled)} job(s) still marked as running without progress for more than "
                f"{int(STALLED_AFTER_SECONDS // 60)} minutes, probably interrupted (for example "
                "because the job service was stopped): "
                + ", ".join(_labelled(entry, names) for entry in stalled)
                + ". Start them again from the dashboard."
            )
    with feed_column:
        st.markdown("#### Live feed")
        items = _feed_items(entries, running_events, names)
        st.markdown(
            '<ul class="f1a-feed">'
            + "".join(
                f'<li class="{"new" if (now - stamp).total_seconds() < NEW_EVENT_SECONDS else ""}">'
                f'<span class="when">{stamp.strftime("%H:%M:%S")}</span>'
                f'<span class="f1a-dot" style="--tone:{TONE_COLORS[tone]}"></span>'
                f"<span>{html.escape(text)}</span></li>"
                for stamp, tone, text in items
            )
            + "</ul>",
            unsafe_allow_html=True,
        )
        st.caption(f"Times in UTC · refreshes every {int(LIVE_REFRESH_SECONDS)} s")


def _status_cell(status: str) -> str:
    tone = status_tone(status)
    return f'<span class="f1a-cell" style="--tone:{TONE_COLORS[tone]}">{html.escape(status)}</span>'


def _render_coverage() -> None:
    seasons = _cached_seasons()
    if not seasons:
        st.info("No weekend pipeline run has been recorded yet.")
        return
    season = st.selectbox("Season", seasons, index=0, key="admin_coverage_season")
    coverage = _cached_coverage(int(season))
    if coverage.empty:
        st.info("No weekend of this season is loaded.")
        return
    columns = st.columns(len(COVERAGE_SOURCES), gap="small")
    for column, (source, title) in zip(columns, COVERAGE_SOURCES):
        complete = int(coverage[source].map(status_tone).eq("ok").sum())
        column.markdown(f"**{title}**")
        column.progress(complete / len(coverage), text=f"{complete} / {len(coverage)} weekends complete")

    header = "".join(f"<th>{html.escape(title)}</th>" for _, title in COVERAGE_SOURCES)
    body = []
    for row in coverage.to_dict("records"):
        date = row["date"].strftime("%d.%m.") if pd.notna(row["date"]) else "-"
        cells = []
        for source, _ in COVERAGE_SOURCES:
            cell = _status_cell(str(row[source]))
            if source == "openf1_weekend_facts":
                cell += f" <span style='opacity:.7'>{row['sessions_ok']}/{row['sessions_total']}</span>"
            cells.append(f"<td>{cell}</td>")
        body.append(
            f"<tr><td>{date}</td><td>{html.escape(str(row['weekend']))}</td>{''.join(cells)}"
            f"<td>{_status_cell(str(row['overall']))}</td></tr>"
        )
    st.markdown(
        f'<table class="f1a-grid"><tr><th>Race</th><th>Weekend</th>{header}<th>Overall</th></tr>'
        + "".join(body)
        + "</table>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Latest weekend pipeline run per meeting. 'stale' means a stored result was reused and "
        "counts as complete; 'partial' means at least one endpoint or source is missing."
    )


def _render_timeline(entries: list[ActivityEntry], names: dict[int, str]) -> None:
    if not entries:
        st.info("No activity recorded yet.")
        return
    frame = pd.DataFrame(
        [
            {
                "when": entry.timestamp,
                "kind": entry.kind,
                "status": entry.status,
                "tone": status_tone(entry.status),
                "description": _labelled(entry, names),
            }
            for entry in entries
        ]
    )
    tone_names = {tone: name for name, tone in TONE_FILTERS.items()}
    figure = go.Figure()
    for tone, group in frame.groupby("tone"):
        figure.add_trace(
            go.Scatter(
                x=group["when"],
                y=group["kind"],
                mode="markers",
                name=tone_names.get(str(tone), "Other"),
                marker={
                    "size": 11,
                    "color": TONE_COLORS[str(tone)],
                    "line": {"width": 1, "color": "white"},
                },
                customdata=group[["description", "status"]],
                hovertemplate=(
                    "%{x|%d.%m. %H:%M:%S}<br>%{customdata[0]}<br>%{customdata[1]}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        height=330,
        margin={"l": 10, "r": 10, "t": 10, "b": 10},
        legend={"orientation": "h", "y": -0.2},
        xaxis={"title": None},
        yaxis={"title": None},
    )
    st.plotly_chart(figure, width="stretch")
    st.caption("Each dot is one finished run or job; hover for details. Times in UTC.")


def _style_status(value: str) -> str:
    tone = status_tone(str(value).split(" ")[-1])
    return f"color: {TONE_COLORS[tone]}; font-weight: 600"


def _render_activity_table(entries: list[ActivityEntry], names: dict[int, str]) -> None:
    filter_column, kind_column = st.columns([2, 3], gap="medium")
    tone_filter = filter_column.segmented_control(
        "Status", ["All", *TONE_FILTERS], default="All", key="admin_status_filter"
    )
    kinds = sorted({entry.kind for entry in entries})
    kind_filter = kind_column.pills(
        "Kind", kinds, selection_mode="multi", default=kinds, key="admin_kind_filter"
    )
    selected = [
        entry
        for entry in entries
        if (tone_filter in (None, "All") or status_tone(entry.status) == TONE_FILTERS[tone_filter])
        and entry.kind in (kind_filter or kinds)
    ]
    if not selected:
        st.info("No entry matches the filter.")
        return
    table = pd.DataFrame(
        [
            {
                "When (UTC)": entry.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "Kind": entry.kind,
                "Description": _labelled(entry, names),
                "Status": _STATUS_MARKERS.get(entry.status, "") + entry.status.upper(),
            }
            for entry in selected
        ]
    )
    st.dataframe(
        table.style.map(_style_status, subset=["Status"]),
        hide_index=True,
        width="stretch",
        height=min(38 + 35 * len(table), 420),
    )

    st.markdown("#### Inspect one entry")
    labels = [f"{entry.kind} · {_labelled(entry, names)}" for entry in selected]
    selected_index = st.selectbox(
        "Entry", range(len(selected)), format_func=lambda index: labels[index], key="admin_entry"
    )
    entry = selected[selected_index or 0]
    kind_column, status_column, when_column, age_column = st.columns(4)
    kind_column.metric("Kind", entry.kind)
    status_column.metric("Status", entry.status.upper())
    when_column.metric("When (UTC)", entry.timestamp.strftime("%d.%m. %H:%M"))
    age_column.metric(
        "Age", _format_age((pd.Timestamp.now(tz="UTC") - entry.timestamp).total_seconds())
    )
    with st.expander("Raw manifest"):
        st.json(entry.detail)
        st.caption(str(entry.path))


def render_admin_panel(on_back: Callable[[], None]) -> None:
    _inject_admin_styles()
    st.title("Pipeline activity")
    st.caption(
        "Read-only view of the persisted job status files and pipeline manifests. "
        "Nothing on this page starts a fetch."
    )
    back_column, refresh_column, _ = st.columns([1.4, 1, 6.6], gap="small")
    if back_column.button("← Back to dashboard", use_container_width=True):
        on_back()
        st.rerun()
    if refresh_column.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    _render_live_overview()

    names = _cached_meeting_names()
    entries = _cached_activity(300)
    coverage_tab, timeline_tab, table_tab = st.tabs(["Season coverage", "Timeline", "All activity"])
    with coverage_tab:
        _render_coverage()
    with timeline_tab:
        _render_timeline(entries, names)
    with table_tab:
        if entries:
            _render_activity_table(entries, names)
        else:
            st.info("No pipeline activity has been recorded on this machine yet.")

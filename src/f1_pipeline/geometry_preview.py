"""Create an interactive preview of stored local circuit geometry."""

from __future__ import annotations

import argparse
import webbrowser
from pathlib import Path

import plotly.graph_objects as go

from f1_pipeline.geometry import (
    TrackGeometry,
    TrackGeometryError,
    load_track_geometry,
    point_at_progress,
)
from f1_pipeline.settings import ARTIFACTS_DIR


def default_output_path(session_key: int) -> Path:
    return ARTIFACTS_DIR / f"track_geometry_session_{session_key}.html"


def create_figure(geometry: TrackGeometry) -> go.Figure:
    track_x = [point[0] for point in geometry.points]
    track_y = [point[1] for point in geometry.points]
    start = point_at_progress(geometry.points, 0.0)
    finish_inner = point_at_progress(geometry.points, 0.0, offset=-0.08)
    finish_outer = point_at_progress(geometry.points, 0.0, offset=0.08)
    halfway = point_at_progress(geometry.points, 0.5)
    figure = go.Figure(
        data=[
            go.Scatter(
                x=track_x,
                y=track_y,
                mode="lines",
                line={"color": "#566573", "width": 18},
                hoverinfo="skip",
                name="Generated centerline",
            ),
            go.Scatter(
                x=track_x,
                y=track_y,
                mode="lines",
                line={"color": "#F8F9F9", "width": 2},
                customdata=[index / (len(track_x) - 1) for index in range(len(track_x))],
                hovertemplate="Lap progress: %{customdata:.0%}<extra></extra>",
                name="Lap progress",
            ),
            go.Scatter(
                x=[finish_inner[0], finish_outer[0]],
                y=[finish_inner[1], finish_outer[1]],
                mode="lines+text",
                text=[None, "START / FINISH"],
                textposition="top center",
                textfont={"color": "white"},
                line={"color": "#2ECC71", "width": 4},
                hoverinfo="skip",
                name="Start/finish",
            ),
            go.Scatter(
                x=[start[0], halfway[0]],
                y=[start[1], halfway[1]],
                mode="markers+text",
                text=["0%", "50%"],
                textposition="bottom center",
                marker={"color": ["#2ECC71", "#F1C40F"], "size": 10},
                hoverinfo="skip",
                name="Progress markers",
            ),
        ]
    )
    figure.update_layout(
        template="plotly_dark",
        title={
            "text": f"Generated track — {geometry.label}",
            "x": 0.5,
        },
        width=1000,
        height=850,
        showlegend=True,
        legend={"orientation": "h", "y": 1.02, "x": 0.5, "xanchor": "center"},
        margin={"l": 40, "r": 40, "t": 100, "b": 80},
        xaxis={"visible": False, "range": [-1.2, 1.2], "scaleanchor": "y"},
        yaxis={"visible": False, "range": [-1.2, 1.2]},
        annotations=[
            {
                "text": (
                    "This is a local OpenF1 centerline reconstructed from observed laps. "
                    "It is not geographic map data.<br>"
                    f"Source session: {geometry.source_session_key or 'unknown'} · "
                    f"Quality: {geometry.quality_status}"
                ),
                "x": 0.01,
                "y": 0.01,
                "xref": "paper",
                "yref": "paper",
                "xanchor": "left",
                "yanchor": "bottom",
                "showarrow": False,
                "font": {"color": "#BDC3C7", "size": 12},
                "align": "left",
            }
        ],
    )
    return figure


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create an interactive preview of stored local circuit geometry."
    )
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--geometry-path", type=Path)
    parser.add_argument("--output", type=Path, help="Custom HTML output path.")
    parser.add_argument("--open", action="store_true", help="Open the result in a browser.")
    parser.add_argument(
        "--self-contained",
        action="store_true",
        help="Embed Plotly JavaScript in the HTML instead of using a CDN.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    try:
        geometry = load_track_geometry(
            args.session_key,
            season=args.season,
            path=args.geometry_path,
        )
        if geometry is None:
            raise TrackGeometryError(
                f"No stored geometry was found for session {args.session_key}."
            )
        output = (args.output or default_output_path(args.session_key)).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        create_figure(geometry).write_html(
            output,
            include_plotlyjs=True if args.self_contained else "cdn",
            full_html=True,
            auto_open=False,
        )
    except (OSError, TrackGeometryError, ValueError, TypeError) as exc:
        print(f"Track geometry preview could not be created: {exc}")
        return 1
    print(f"Track geometry preview created: {output}")
    print(f"Source session: {geometry.source_session_key}")
    print(f"Quality: {geometry.quality_status}")
    if args.open:
        webbrowser.open(output.as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

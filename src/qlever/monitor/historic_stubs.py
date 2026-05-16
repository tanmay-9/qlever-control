"""Stub data for the Historic screen.

Replaced later by a real log-reader module exposing the same functions.
Only the data the current widgets consume is defined here.
"""

import time

from qlever.monitor.models import (
    ControlsState,
    HistoricQueryRow,
    MetricsCounts,
    TimelineBounds,
)


def get_controls_state() -> ControlsState:
    """Current window size, mode, and selected time range on Historic."""
    end_ms = int(time.time() * 1000)
    return ControlsState(
        window_size="15m",
        mode="ACTIVE",
        start_ms=end_ms - 15 * 60 * 1000,
        end_ms=end_ms,
    )


def get_timeline_bounds() -> TimelineBounds:
    """Full log span plus the slice the current window covers.

    The window sits near the end of the log so the marked block is
    visibly offset rather than spanning the whole bar.
    """
    now_ms = int(time.time() * 1000)
    window_ms = 15 * 60 * 1000
    return TimelineBounds(
        log_start_ms=now_ms - 52 * 60 * 1000,
        log_end_ms=now_ms,
        window_start_ms=now_ms - window_ms,
        window_end_ms=now_ms,
    )


def get_historic_query_rows() -> list[HistoricQueryRow]:
    """Finished queries matching the current mode in the current window."""
    start_ms = int(time.time() * 1000) - 15 * 60 * 1000
    long_query = (
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        "PREFIX wd: <http://www.wikidata.org/entity/>\n"
        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
        "PREFIX schema: <http://schema.org/>\n"
        "SELECT ?person ?personLabel ?birthPlaceLabel ?occupationLabel\n"
        "       (COUNT(DISTINCT ?work) AS ?works)\n"
        "WHERE {\n"
        "  ?person wdt:P31 wd:Q5 .\n"
        "  ?person wdt:P106 ?occupation .\n"
        "  ?person wdt:P19 ?birthPlace .\n"
        "  ?person rdfs:label ?personLabel .\n"
        "  ?birthPlace rdfs:label ?birthPlaceLabel .\n"
        "  ?occupation rdfs:label ?occupationLabel .\n"
        "  OPTIONAL { ?work wdt:P50 ?person . }\n"
        '  FILTER(LANG(?personLabel) = "en")\n'
        '  FILTER(LANG(?birthPlaceLabel) = "en")\n'
        '  FILTER(LANG(?occupationLabel) = "en")\n'
        "}\n"
        "GROUP BY ?person ?personLabel ?birthPlaceLabel ?occupationLabel\n"
        "HAVING (COUNT(DISTINCT ?work) > 5)\n"
        "ORDER BY DESC(?works)\n"
        "LIMIT 1000"
    )
    samples = [
        (50_000, 99_000, "ok", long_query),
        (0, 42_000, "ok", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 1000"),
        (103_000, 8_000, "ok", "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"),
        (
            210_000,
            30_000,
            "timeout",
            "SELECT ?x WHERE { ?x rdfs:label ?l } ORDER BY ?l",
        ),
        (
            354_000,
            1_200,
            "failed",
            "CONSTRUCT { ?x rdfs:label ?l } WHERE { ?x rdfs:label ?l }",
        ),
        (
            601_000,
            5_500,
            "cancelled",
            "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
        ),
    ]
    return [
        HistoricQueryRow(
            qid=f"q-{i:04x}",
            started_at_ms=start_ms + offset_ms,
            duration_ms=duration_ms,
            status=status,
            sparql=sparql,
        )
        for i, (offset_ms, duration_ms, status, sparql) in enumerate(samples)
    ]


def get_historic_metrics() -> MetricsCounts:
    """Single metrics line for the current window; label is its width."""
    return MetricsCounts(
        label="15m",
        seen=488,
        ok=466,
        failed=11,
        timeout=2,
        cancelled=9,
        p50=120,
        p95=1840,
        slow=4,
    )

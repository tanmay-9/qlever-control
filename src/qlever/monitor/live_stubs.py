"""Stub data for the Live screen.

Replaced later by a real log-reader module exposing the same functions.
UI widgets call these getters rather than reading module-level constants,
so the swap is mechanical.
"""

import time

from qlever.monitor.models import LiveQueryRow, LiveSubtitle, MetricsCounts


def get_live_subtitle() -> LiveSubtitle:
    """Current Live-screen subtitle state."""
    return LiveSubtitle(state="reachable", n_active=3)


def get_live_metrics() -> list[MetricsCounts]:
    """Three rolling-window metric rows shown on Live."""
    return [
        MetricsCounts(
            label="last 5m",
            seen=142,
            ok=125,
            failed=3,
            timeout=0,
            cancelled=14,
            unknown=0,
            p50=48,
            p95=3_900,
            slow=14,
        ),
        MetricsCounts(
            label="last 15m",
            seen=438,
            ok=394,
            failed=11,
            timeout=2,
            cancelled=31,
            unknown=1,
            p50=52,
            p95=4_100,
            slow=31,
        ),
        MetricsCounts(
            label="last 1h",
            seen=None,
            ok=None,
            failed=None,
            timeout=None,
            cancelled=None,
            unknown=None,
            p50=None,
            p95=None,
            slow=None,
        ),
    ]


def get_live_query_rows() -> list[LiveQueryRow]:
    """Currently active queries (a start with no matching end)."""
    now_ms = int(time.time() * 1000)
    return [
        LiveQueryRow(
            qid="q-8a4f12345678900",
            ts_ms=now_ms - 18_000,
            sparql=(
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                "SELECT ?person ?name (COUNT(?friend) AS ?friends)\n"
                "WHERE {\n"
                "  ?person rdf:type foaf:Person ;\n"
                "          foaf:name ?name ;\n"
                "          foaf:knows ?friend .\n"
                "  ?friend foaf:name ?friendName .\n"
                '  FILTER (STRLEN(?name) > 3 && LANG(?name) = "en")\n'
                "}\n"
                "GROUP BY ?person ?name\n"
                "HAVING (COUNT(?friend) > 5)\n"
                "ORDER BY DESC(?friends) ?name\n"
                "LIMIT 100"
            ),
        ),
        LiveQueryRow(
            qid="q-8a52242534636",
            ts_ms=now_ms - 5_000,
            sparql="SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
        ),
        LiveQueryRow(
            qid="q-8a5b4356476474567456",
            ts_ms=now_ms - 2_000,
            sparql=(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                "CONSTRUCT {\n"
                "  ?city rdfs:label ?label ;\n"
                "        geo:hasGeometry ?geom .\n"
                "}\n"
                "WHERE {\n"
                "  ?city rdf:type :City ;\n"
                "        rdfs:label ?label ;\n"
                "        geo:hasGeometry ?geom .\n"
                '  FILTER (lang(?label) = "en")\n'
                "  FILTER EXISTS { ?city :population ?pop . FILTER (?pop > 100000) }\n"
                "}"
            ),
        ),
    ]

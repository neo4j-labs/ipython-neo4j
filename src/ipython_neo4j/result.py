"""CypherResult: wraps Neo4j query results with DataFrame, HTML and graph visualization support."""

from __future__ import annotations

import html
from typing import Any, Optional

import pandas as pd

# neo4j-viz integration (optional at import time, required at runtime for visualization)
try:
    from neo4j_viz import Node, Relationship, VisualizationGraph

    HAS_NEO4J_VIZ = True
except ImportError:
    HAS_NEO4J_VIZ = False


def _neo4j_value_to_python(value: Any) -> Any:
    """Recursively convert Neo4j driver types to plain Python."""
    from neo4j.graph import Node as NeoNode, Relationship as NeoRel, Path as NeoPath

    if isinstance(value, NeoNode):
        return dict(value)
    if isinstance(value, NeoRel):
        return dict(value)
    if isinstance(value, NeoPath):
        return [_neo4j_value_to_python(el) for el in value]
    if isinstance(value, list):
        return [_neo4j_value_to_python(v) for v in value]
    if isinstance(value, dict):
        return {k: _neo4j_value_to_python(v) for k, v in value.items()}
    return value


class CypherResult:
    """
    Result of a Cypher query execution.

    Attributes
    ----------
    keys : list[str]
        Column names returned by the query.
    records : list[neo4j.Record]
        Raw driver records.
    summary : neo4j.ResultSummary
        Execution summary (counters, timing, notifications, etc.).
    """

    def __init__(
        self,
        records: list,
        summary: Any,
        keys: list[str],
    ) -> None:
        self.keys = keys
        self.records = records
        self.summary = summary
        self._df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # DataFrame access
    # ------------------------------------------------------------------

    def to_dataframe(self) -> pd.DataFrame:
        """Convert records to a pandas DataFrame."""
        if self._df is None:
            rows = [
                {k: _neo4j_value_to_python(record[k]) for k in self.keys}
                for record in self.records
            ]
            self._df = pd.DataFrame(rows, columns=self.keys)
        return self._df

    @property
    def df(self) -> pd.DataFrame:
        """Shorthand for to_dataframe()."""
        return self.to_dataframe()

    # ------------------------------------------------------------------
    # Graph visualization
    # ------------------------------------------------------------------

    def to_graph(self) -> "VisualizationGraph":
        """
        Build a neo4j-viz VisualizationGraph from any Node/Relationship values
        returned by the query. Columns containing plain scalars are ignored.
        """
        if not HAS_NEO4J_VIZ:
            raise ImportError(
                "neo4j-viz is required for graph visualization. "
                "Install it with: pip install 'neo4j-viz[notebook]'"
            )
        from neo4j.graph import Node as NeoNode, Relationship as NeoRel

        # neo4j 6.x uses element_id (string) instead of deprecated integer .id
        def _node_num_id(node: NeoNode) -> int:
            """Return a stable integer id for neo4j-viz from element_id."""
            return abs(hash(node.element_id))

        seen_nodes: dict[int, "Node"] = {}
        viz_rels: list["Relationship"] = []

        for record in self.records:
            for key in self.keys:
                val = record[key]
                if isinstance(val, NeoNode):
                    nid = _node_num_id(val)
                    if nid not in seen_nodes:
                        caption = next(iter(val.labels), None) or val.element_id
                        props = dict(val)
                        caption = props.get("name") or props.get("title") or caption
                        seen_nodes[nid] = Node(
                            id=nid,
                            caption=str(caption),
                            size=20,
                            properties={k: str(v) for k, v in props.items()},
                        )
                elif isinstance(val, NeoRel):
                    src_id = _node_num_id(val.start_node)
                    tgt_id = _node_num_id(val.end_node)
                    viz_rels.append(
                        Relationship(
                            source=src_id,
                            target=tgt_id,
                            caption=val.type,
                        )
                    )
                    # Ensure endpoint nodes exist
                    for node in (val.start_node, val.end_node):
                        nid = _node_num_id(node)
                        if nid not in seen_nodes:
                            props = dict(node)
                            caption = (
                                props.get("name") or props.get("title")
                                or next(iter(node.labels), node.element_id)
                            )
                            seen_nodes[nid] = Node(
                                id=nid,
                                caption=str(caption),
                                size=20,
                                properties={k: str(v) for k, v in props.items()},
                            )

        return VisualizationGraph(
            nodes=list(seen_nodes.values()),
            relationships=viz_rels,
        )

    def visualize(self, color_by: Optional[str] = "caption", **render_kwargs: Any) -> Any:
        """
        Render an interactive graph visualization.

        Parameters
        ----------
        color_by : str, optional
            Node field to use for coloring (default: "caption").
        **render_kwargs :
            Passed to VisualizationGraph.render().
        """
        vg = self.to_graph()
        if color_by:
            vg.color_nodes(field=color_by)
        return vg.render(**render_kwargs)

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    @property
    def counters(self) -> dict[str, int]:
        """Return write-counters from the result summary."""
        c = self.summary.counters
        return {
            "nodes_created": c.nodes_created,
            "nodes_deleted": c.nodes_deleted,
            "relationships_created": c.relationships_created,
            "relationships_deleted": c.relationships_deleted,
            "properties_set": c.properties_set,
            "labels_added": c.labels_added,
            "labels_removed": c.labels_removed,
        }

    def __len__(self) -> int:
        return len(self.records)

    def __repr__(self) -> str:
        return (
            f"CypherResult({len(self.records)} rows × {len(self.keys)} cols, "
            f"query_type={self.summary.query_type!r})"
        )

    # ------------------------------------------------------------------
    # Rich HTML display in Jupyter
    # ------------------------------------------------------------------

    def _repr_html_(self) -> str:
        if not self.records:
            counters = self.counters
            active = {k: v for k, v in counters.items() if v}
            if active:
                rows_html = "".join(
                    f"<tr><td><code>{html.escape(k)}</code></td><td>{v}</td></tr>"
                    for k, v in active.items()
                )
                return (
                    "<details open><summary><b>Write result</b></summary>"
                    f"<table border='1' style='border-collapse:collapse;font-size:13px'>"
                    f"<thead><tr><th>Counter</th><th>Value</th></tr></thead>"
                    f"<tbody>{rows_html}</tbody></table></details>"
                )
            return "<i style='color:#888'>Query returned no results.</i>"

        # Render as scrollable table
        df = self.to_dataframe()
        max_rows = 500
        truncated = len(df) > max_rows
        table_html = df.head(max_rows).to_html(
            border=1,
            index=False,
            classes="cypher-result",
            na_rep="null",
        )
        # Inject compact table styles
        style = (
            "<style>.cypher-result{border-collapse:collapse;font-size:13px}"
            ".cypher-result td,.cypher-result th"
            "{padding:4px 10px;border:1px solid #ddd;text-align:left}"
            ".cypher-result tr:nth-child(even){background:#f9f9f9}"
            ".cypher-result th{background:#f0f0f0;font-weight:600}</style>"
        )
        summary_line = (
            f"<p style='color:#888;font-size:12px'>"
            f"{len(self.records)} row{'s' if len(self.records) != 1 else ''}"
            f"{' (showing first ' + str(max_rows) + ')' if truncated else ''}"
            f" &mdash; {self.summary.result_available_after} ms available, "
            f"{self.summary.result_consumed_after} ms consumed"
            f"</p>"
        )
        return style + table_html + summary_line

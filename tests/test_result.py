"""Unit tests for CypherResult."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ipython_neo4j.result import CypherResult, _neo4j_value_to_python


def _make_record(keys, values):
    """Create a mock neo4j Record with subscript access."""
    record = MagicMock()
    record.__getitem__ = lambda self, k: dict(zip(keys, values))[k]
    record.__iter__ = lambda self: iter(keys)
    return record


def _make_summary(query_type="r", available_after=5, consumed_after=10):
    summary = MagicMock()
    summary.query_type = query_type
    summary.result_available_after = available_after
    summary.result_consumed_after = consumed_after
    c = MagicMock()
    for attr in [
        "nodes_created", "nodes_deleted", "relationships_created",
        "relationships_deleted", "properties_set", "labels_added", "labels_removed"
    ]:
        setattr(c, attr, 0)
    summary.counters = c
    return summary


class TestNeo4jValueToPython:
    def test_scalar_passthrough(self):
        assert _neo4j_value_to_python(42) == 42
        assert _neo4j_value_to_python("hello") == "hello"
        assert _neo4j_value_to_python(None) is None

    def test_list_recursion(self):
        result = _neo4j_value_to_python([1, "two", 3])
        assert result == [1, "two", 3]

    def test_dict_recursion(self):
        result = _neo4j_value_to_python({"a": 1, "b": [2, 3]})
        assert result == {"a": 1, "b": [2, 3]}


class TestCypherResult:
    def _make_result(self, keys=None, rows=None, query_type="r"):
        keys = keys if keys is not None else ["name", "age"]
        rows = rows if rows is not None else [("Alice", 30), ("Bob", 25)]
        records = [_make_record(keys, row) for row in rows]
        summary = _make_summary(query_type=query_type)
        return CypherResult(records=records, summary=summary, keys=keys)

    def test_len(self):
        result = self._make_result()
        assert len(result) == 2

    def test_to_dataframe(self):
        result = self._make_result(keys=["name", "age"], rows=[("Alice", 30), ("Bob", 25)])
        df = result.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["name", "age"]
        assert len(df) == 2
        assert df.iloc[0]["name"] == "Alice"

    def test_df_property(self):
        result = self._make_result()
        assert result.df is result.to_dataframe()

    def test_df_cached(self):
        result = self._make_result()
        df1 = result.to_dataframe()
        df2 = result.to_dataframe()
        assert df1 is df2  # same object – cached

    def test_counters_zeros(self):
        result = self._make_result()
        c = result.counters
        assert all(v == 0 for v in c.values())

    def test_repr(self):
        result = self._make_result()
        r = repr(result)
        assert "2 rows" in r
        assert "2 cols" in r

    def test_repr_html_table(self):
        result = self._make_result()
        html = result._repr_html_()
        assert "<table" in html
        assert "Alice" in html
        assert "Bob" in html

    def test_repr_html_empty_no_write(self):
        result = self._make_result(rows=[])
        html = result._repr_html_()
        assert "no results" in html.lower()

    def test_repr_html_empty_with_counters(self):
        result = self._make_result(rows=[])
        result.summary.counters.nodes_created = 1
        html = result._repr_html_()
        assert "Write result" in html or "nodes_created" in html

    def test_no_neo4j_viz_raises(self):
        result = self._make_result()
        with patch("ipython_neo4j.result.HAS_NEO4J_VIZ", False):
            with pytest.raises(ImportError, match="neo4j-viz"):
                result.to_graph()

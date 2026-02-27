"""Unit tests for Neo4jMagics — uses IPython InteractiveShell."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pytest

from IPython.core.interactiveshell import InteractiveShell

from ipython_neo4j.magic import (
    Neo4jMagics, _extract_query_from_line, _split_statements,
    _most_restrictive_type, load_ipython_extension,
)
from ipython_neo4j.connection import Neo4jConnection
from ipython_neo4j.result import CypherResult


@pytest.fixture(autouse=True)
def reset_connection():
    """Ensure a clean connection state between tests."""
    Neo4jConnection._driver = None
    Neo4jConnection._config = None
    yield
    Neo4jConnection._driver = None
    Neo4jConnection._config = None


@pytest.fixture(scope="module")
def ip():
    """IPython shell instance (created once per test module)."""
    shell = InteractiveShell.instance()
    load_ipython_extension(shell)
    yield shell
    InteractiveShell.clear_instance()


def _mock_driver(query_type="r", records=None, keys=None):
    """Build a mock neo4j driver that returns canned results."""
    records = records or []
    keys = keys or []
    summary = MagicMock()
    summary.query_type = query_type
    summary.result_available_after = 1
    summary.result_consumed_after = 2
    c = MagicMock()
    for attr in [
        "nodes_created", "nodes_deleted", "relationships_created",
        "relationships_deleted", "properties_set", "labels_added", "labels_removed"
    ]:
        setattr(c, attr, 0)
    summary.counters = c

    driver = MagicMock()
    driver.execute_query.return_value = (records, summary, keys)
    return driver


class TestExtractQueryFromLine:
    def test_plain_query(self):
        q = _extract_query_from_line("MATCH (n) RETURN n LIMIT 5")
        assert q == "MATCH (n) RETURN n LIMIT 5"

    def test_strips_flags(self):
        q = _extract_query_from_line("--df MATCH (n) RETURN n")
        assert q == "MATCH (n) RETURN n"

    def test_strips_flag_with_value(self):
        q = _extract_query_from_line("-o result MATCH (n) RETURN n")
        assert q == "MATCH (n) RETURN n"

    def test_empty_line(self):
        q = _extract_query_from_line("")
        assert q == ""


class TestEnvFileLoading:
    """Unit tests for %neo4j --env-file."""

    def test_env_file_used_for_connection(self, tmp_path, ip):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "NEO4J_URI=bolt://envhost:7687\n"
            "NEO4J_USERNAME=envuser\n"
            "NEO4J_PASSWORD=envpass\n"
            "NEO4J_DATABASE=envdb\n"
        )
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver) as drv:
            ip.run_line_magic("neo4j", f"--env-file {env_file}")
            drv.assert_called_once_with(
                "bolt://envhost:7687",
                auth=("envuser", "envpass"),
            )
        assert Neo4jConnection.get_database() == "envdb"

    def test_explicit_flags_override_env_file(self, tmp_path, ip):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "NEO4J_URI=bolt://envhost:7687\n"
            "NEO4J_USERNAME=envuser\n"
            "NEO4J_PASSWORD=envpass\n"
            "NEO4J_DATABASE=envdb\n"
        )
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver) as drv:
            ip.run_line_magic("neo4j", f"--env-file {env_file} -u overrideuser -d overridedb")
            drv.assert_called_once_with(
                "bolt://envhost:7687",          # URI from file (not overridden)
                auth=("overrideuser", "envpass"),  # user overridden, pass from file
            )
        assert Neo4jConnection.get_database() == "overridedb"

    def test_missing_env_file_shows_error(self, ip):
        # Should display an error, not raise
        ip.run_line_magic("neo4j", "--env-file /nonexistent/.env")
        assert not Neo4jConnection.is_connected()

    def test_env_file_without_dotenv_installed(self, tmp_path, ip):
        env_file = tmp_path / ".env"
        env_file.write_text("NEO4J_URI=bolt://x:7687\n")
        with patch.dict("sys.modules", {"dotenv": None}):
            ip.run_line_magic("neo4j", f"--env-file {env_file}")
        # Should not crash; connection not established without dotenv


class TestNeo4jMagicConnect:
    def test_connect_magic_calls_neo4j_connect(self, ip):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            ip.run_line_magic("neo4j", "bolt://localhost:7687 -u neo4j -p pw")
            assert Neo4jConnection.is_connected()

    def test_status_magic(self, ip):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw")
        # Should not raise
        ip.run_line_magic("neo4j", "--status")

    def test_close_magic(self, ip):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw")
        ip.run_line_magic("neo4j", "--close")
        assert not Neo4jConnection.is_connected()


class TestCypherMagicReadQuery:
    def test_line_magic_read_query(self, ip):
        mock_driver = _mock_driver(query_type="r", records=[], keys=[])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            # Should run without error
            ip.run_line_magic("cypher", "MATCH (n) RETURN n LIMIT 1")

    def test_cell_magic_read_query(self, ip):
        mock_driver = _mock_driver(query_type="r", records=[], keys=[])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "", "MATCH (n) RETURN n LIMIT 1")

    def test_output_stored_in_namespace(self, ip):
        mock_driver = _mock_driver(query_type="r", records=[], keys=["n"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "-o my_result", "MATCH (n) RETURN n LIMIT 1")
        assert "my_result" in ip.user_ns
        assert isinstance(ip.user_ns["my_result"], CypherResult)

    def test_df_stored_in_namespace(self, ip):
        import pandas as pd
        mock_driver = _mock_driver(query_type="r", records=[], keys=["n"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "--df -o my_df", "MATCH (n) RETURN n LIMIT 1")
        assert "my_df" in ip.user_ns
        assert isinstance(ip.user_ns["my_df"], pd.DataFrame)


class TestCypherMagicWriteBlocking:
    def test_write_query_blocked_by_default(self, ip):
        """A write query without --write should be blocked after EXPLAIN."""
        mock_driver = _mock_driver(query_type="w")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            # Should not raise but will display a blocked message
            ip.run_cell_magic("cypher", "", "CREATE (n:Test) RETURN n")
        # Verify execute_query was only called ONCE (for EXPLAIN, not the actual query)
        assert mock_driver.execute_query.call_count == 1

    def test_write_query_allowed_with_flag(self, ip):
        """A write query with --write should execute."""
        mock_driver = _mock_driver(query_type="w")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "--write", "CREATE (n:Test) RETURN n")
        # EXPLAIN call + actual query = 2 calls
        assert mock_driver.execute_query.call_count == 2

    def test_wcypher_allows_write(self, ip):
        """%%wcypher should allow write queries without --write."""
        mock_driver = _mock_driver(query_type="w")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("wcypher", "", "CREATE (n:Test) RETURN n")
        assert mock_driver.execute_query.call_count == 2

    def test_no_preflight_skips_explain(self, ip):
        """--no-preflight should skip the EXPLAIN check."""
        mock_driver = _mock_driver(query_type="r")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "--no-preflight", "MATCH (n) RETURN n LIMIT 1")
        assert mock_driver.execute_query.call_count == 1  # Only actual query, no EXPLAIN


class TestCypherMagicErrors:
    def test_connection_error_displays_html(self, ip):
        from neo4j.exceptions import ServiceUnavailable
        with patch.object(
            Neo4jConnection,
            "get_driver",
            side_effect=ServiceUnavailable("cannot connect"),
        ):
            # Should not raise; should display error HTML
            ip.run_cell_magic("cypher", "", "MATCH (n) RETURN n")

    def test_query_error_displays_html(self, ip):
        from neo4j.exceptions import CypherSyntaxError
        mock_driver = MagicMock()
        # Neo4j 6.x Neo4jError takes positional *args
        mock_driver.execute_query.side_effect = CypherSyntaxError("Invalid syntax")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic("cypher", "--no-preflight", "BADQUERY")

    def test_empty_query_displays_warning(self, ip):
        # Should not raise
        ip.run_line_magic("cypher", "")
        ip.run_cell_magic("cypher", "", "   ")


class TestSplitStatements:
    """Unit tests for the _split_statements helper."""

    def test_single_statement_no_semicolon(self):
        stmts = _split_statements("MATCH (n) RETURN n")
        assert stmts == ["MATCH (n) RETURN n"]

    def test_single_statement_trailing_semicolon(self):
        # Trailing ; at end of string counts as end-of-line
        stmts = _split_statements("MATCH (n) RETURN n;")
        assert stmts == ["MATCH (n) RETURN n"]

    def test_two_statements_newline_separator(self):
        q = "MATCH (n) RETURN n;\nMATCH (m) RETURN m"
        stmts = _split_statements(q)
        assert len(stmts) == 2
        assert stmts[0] == "MATCH (n) RETURN n"
        assert stmts[1] == "MATCH (m) RETURN m"

    def test_trailing_whitespace_after_semicolon(self):
        q = "MATCH (n) RETURN n;   \nMATCH (m) RETURN m"
        stmts = _split_statements(q)
        assert len(stmts) == 2

    def test_three_statements(self):
        q = "CREATE (a:A);\nCREATE (b:B);\nMATCH (n) RETURN n"
        stmts = _split_statements(q)
        assert len(stmts) == 3

    def test_midline_semicolon_not_split(self):
        # Semicolon inside a string literal is mid-line — must NOT split
        q = "MATCH (n) WHERE n.name = 'O;Brien' RETURN n"
        stmts = _split_statements(q)
        assert len(stmts) == 1
        assert stmts[0] == q

    def test_empty_string(self):
        assert _split_statements("") == []

    def test_whitespace_only(self):
        assert _split_statements("   \n  ") == []

    def test_blank_segments_skipped(self):
        # Consecutive semicolons produce empty segments that are dropped
        q = "MATCH (n) RETURN n;\n;\nMATCH (m) RETURN m"
        stmts = _split_statements(q)
        assert len(stmts) == 2


class TestMostRestrictiveType:
    def test_all_read(self):
        assert _most_restrictive_type(["r", "r"]) == "r"

    def test_write_beats_read(self):
        assert _most_restrictive_type(["r", "w"]) == "w"

    def test_schema_beats_write(self):
        assert _most_restrictive_type(["w", "s"]) == "s"

    def test_schema_beats_everything(self):
        assert _most_restrictive_type(["r", "rw", "w", "s"]) == "s"

    def test_empty_list(self):
        assert _most_restrictive_type([]) == ""

    def test_unknown_type(self):
        # Unknown types treated as lowest priority
        assert _most_restrictive_type(["", "r"]) == "r"


class TestSchemaQueryBlocking:
    """Schema ('s') queries must be blocked the same as write queries."""

    def test_schema_query_blocked_by_default(self, ip):
        mock_driver = _mock_driver(query_type="s")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "",
                "CREATE INDEX idx_person_name FOR (n:Person) ON (n.name)"
            )
        # Only the EXPLAIN call should have been made, not the real query
        assert mock_driver.execute_query.call_count == 1

    def test_schema_query_allowed_with_write_flag(self, ip):
        mock_driver = _mock_driver(query_type="s")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "--write",
                "CREATE INDEX idx_person_name FOR (n:Person) ON (n.name)"
            )
        # EXPLAIN + real query = 2 calls
        assert mock_driver.execute_query.call_count == 2

    def test_schema_query_allowed_with_wcypher(self, ip):
        mock_driver = _mock_driver(query_type="s")
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=mock_driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "wcypher", "",
                "DROP INDEX idx_person_name IF EXISTS"
            )
        assert mock_driver.execute_query.call_count == 2


class TestMultiStatementMagic:
    """Multi-statement cell magic execution."""

    def _make_multi_driver(self, qtypes: list[str]):
        """Driver whose EXPLAIN returns each qtype in sequence, execution always succeeds."""
        summary_iter = iter(qtypes)

        def execute_side_effect(query, **kwargs):
            if query.strip().upper().startswith("EXPLAIN"):
                qtype = next(summary_iter, "r")
            else:
                qtype = "r"
            summary = MagicMock()
            summary.query_type = qtype
            summary.result_available_after = 1
            summary.result_consumed_after = 2
            c = MagicMock()
            for attr in [
                "nodes_created", "nodes_deleted", "relationships_created",
                "relationships_deleted", "properties_set", "labels_added", "labels_removed"
            ]:
                setattr(c, attr, 0)
            summary.counters = c
            return ([], summary, [])

        driver = MagicMock()
        driver.execute_query.side_effect = execute_side_effect
        return driver

    def test_two_read_statements_execute_both(self, ip):
        driver = self._make_multi_driver(["r", "r"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "",
                "MATCH (n) RETURN n;\nMATCH (m) RETURN m"
            )
        # 2 EXPLAIN calls + 2 execute calls = 4
        assert driver.execute_query.call_count == 4

    def test_mixed_write_blocked_without_flag(self, ip):
        """If ANY statement is write, the whole cell is blocked."""
        driver = self._make_multi_driver(["r", "w"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "",
                "MATCH (n) RETURN n;\nCREATE (m:Test) RETURN m"
            )
        # Only 2 EXPLAIN calls; no actual execution
        assert driver.execute_query.call_count == 2

    def test_mixed_write_allowed_with_flag(self, ip):
        driver = self._make_multi_driver(["r", "w"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "--write",
                "MATCH (n) RETURN n;\nCREATE (m:Test) RETURN m"
            )
        # 2 EXPLAIN + 2 execute = 4
        assert driver.execute_query.call_count == 4

    def test_last_result_stored_in_output(self, ip):
        driver = self._make_multi_driver(["r", "r"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "-o last_result",
                "MATCH (n) RETURN n;\nMATCH (m) RETURN m"
            )
        assert "last_result" in ip.user_ns
        assert isinstance(ip.user_ns["last_result"], CypherResult)

    def test_midline_semicolon_not_split(self, ip):
        """WHERE name = 'O;Brien' must not be split into two statements."""
        driver = self._make_multi_driver(["r"])
        with (
            patch.object(Neo4jConnection, "get_driver", return_value=driver),
            patch.object(Neo4jConnection, "get_database", return_value="neo4j"),
        ):
            ip.run_cell_magic(
                "cypher", "--no-preflight",
                "MATCH (n) WHERE n.name = 'O;Brien' RETURN n"
            )
        # Only 1 execute call (not split into 2)
        assert driver.execute_query.call_count == 1

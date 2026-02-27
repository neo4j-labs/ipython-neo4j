"""Integration tests against real Neo4j instances.

Credentials are loaded from ``integration.env`` in the project root.
Copy and edit that file to match your environment before running.

Read-only tests use the public demo database (default in integration.env):
    URI:      neo4j+s://demo.neo4jlabs.com
    Username: movies
    Password: movies
    Database: movies

Write tests use a local Neo4j instance (skipped if unavailable):
    URI:      bolt://localhost:7687
    Username: neo4j
    Password: password

Run:
    uv run pytest tests/test_integration.py -v
"""

from __future__ import annotations

import os
import pathlib
import pytest

# ---------------------------------------------------------------------------
# Load integration.env (project root)
# ---------------------------------------------------------------------------

_ENV_FILE = pathlib.Path(__file__).parent.parent / "integration.env"

try:
    from dotenv import dotenv_values

    _env = dotenv_values(_ENV_FILE)
except ImportError:
    _env = {}

def _env_get(key: str, default: str = "") -> str:
    """Read from integration.env first, then fall back to env var."""
    return _env.get(key) or os.environ.get(key, default)

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

DEMO_URI = _env_get("NEO4J_URI", "neo4j+s://demo.neo4jlabs.com")
DEMO_USER = _env_get("NEO4J_USERNAME", "movies")
DEMO_PASS = _env_get("NEO4J_PASSWORD", "movies")
DEMO_DB = _env_get("NEO4J_DATABASE", "movies")

LOCAL_URI = _env_get("NEO4J_LOCAL_URI", "bolt://localhost:7687")
LOCAL_USER = _env_get("NEO4J_LOCAL_USERNAME", "neo4j")
LOCAL_PASS = _env_get("NEO4J_LOCAL_PASSWORD", "password")
LOCAL_DB = _env_get("NEO4J_LOCAL_DATABASE", "neo4j")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def demo_driver():
    """Connect to the public movies demo database."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(DEMO_URI, auth=(DEMO_USER, DEMO_PASS))
    driver.verify_connectivity()
    yield driver
    driver.close()


@pytest.fixture(scope="module")
def local_driver():
    """Connect to a local Neo4j instance; skip if unavailable."""
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable

    try:
        driver = GraphDatabase.driver(LOCAL_URI, auth=(LOCAL_USER, LOCAL_PASS))
        driver.verify_connectivity()
    except ServiceUnavailable:
        pytest.skip(f"Local Neo4j not available at {LOCAL_URI}")
    yield driver
    driver.close()


@pytest.fixture(autouse=True)
def reset_global_connection():
    from ipython_neo4j.connection import Neo4jConnection
    Neo4jConnection._driver = None
    Neo4jConnection._config = None
    yield
    Neo4jConnection._driver = None
    Neo4jConnection._config = None


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

class TestConnectionIntegration:
    def test_connect_to_demo(self):
        from ipython_neo4j.connection import Neo4jConnection
        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        assert Neo4jConnection.is_connected()

    def test_status_html_after_connect(self):
        from ipython_neo4j.connection import Neo4jConnection
        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        html = Neo4jConnection.status_html()
        assert "Connected" in html
        assert DEMO_URI in html


# ---------------------------------------------------------------------------
# CypherResult tests against movies database
# ---------------------------------------------------------------------------

class TestCypherResultIntegration:
    def test_basic_query(self, demo_driver):
        from ipython_neo4j.result import CypherResult

        records, summary, keys = demo_driver.execute_query(
            "MATCH (m:Movie) RETURN m.title AS title LIMIT 5",
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        assert len(result) == 5
        assert "title" in result.keys

    def test_to_dataframe(self, demo_driver):
        from ipython_neo4j.result import CypherResult

        records, summary, keys = demo_driver.execute_query(
            "MATCH (m:Movie) RETURN m.title AS title, m.released AS released LIMIT 10",
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        df = result.to_dataframe()
        assert len(df) == 10
        assert list(df.columns) == ["title", "released"]
        assert df["title"].notna().all()

    def test_repr_html_contains_data(self, demo_driver):
        from ipython_neo4j.result import CypherResult

        records, summary, keys = demo_driver.execute_query(
            "MATCH (m:Movie) RETURN m.title AS title LIMIT 3",
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        html = result._repr_html_()
        assert "<table" in html

    def test_to_graph_with_nodes_and_rels(self, demo_driver):
        """Verify graph conversion when query returns Node/Rel objects."""
        from ipython_neo4j.result import CypherResult, HAS_NEO4J_VIZ

        if not HAS_NEO4J_VIZ:
            pytest.skip("neo4j-viz not installed")

        records, summary, keys = demo_driver.execute_query(
            "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p, r, m LIMIT 10",
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        vg = result.to_graph()
        assert len(vg.nodes) > 0
        assert len(vg.relationships) > 0

    def test_query_with_parameters(self, demo_driver):
        from ipython_neo4j.result import CypherResult

        records, summary, keys = demo_driver.execute_query(
            "MATCH (m:Movie) WHERE m.released > $year RETURN m.title AS title LIMIT 5",
            year=2000,
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        df = result.to_dataframe()
        assert len(df) <= 5

    def test_empty_result(self, demo_driver):
        from ipython_neo4j.result import CypherResult

        records, summary, keys = demo_driver.execute_query(
            "MATCH (n:NonExistentLabel_xyz) RETURN n",
            database_=DEMO_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        assert len(result) == 0
        html = result._repr_html_()
        assert "no results" in html.lower()


# ---------------------------------------------------------------------------
# Pre-flight EXPLAIN tests
# ---------------------------------------------------------------------------

class TestExplainPreflightIntegration:
    def test_read_query_type(self, demo_driver):
        from ipython_neo4j.magic import _explain_query_type

        qtype = _explain_query_type(
            demo_driver,
            "MATCH (n:Movie) RETURN n LIMIT 1",
            DEMO_DB,
        )
        assert qtype == "r", f"Expected 'r', got {qtype!r}"

    def test_write_query_type(self, demo_driver):
        """EXPLAIN on a write (DELETE) must return 'w' or 'rw' without executing."""
        from ipython_neo4j.magic import _explain_query_type

        qtype = _explain_query_type(
            demo_driver,
            "MATCH (n:Movie) DETACH DELETE n",
            DEMO_DB,
        )
        assert qtype in ("w", "rw"), f"Expected write type, got {qtype!r}"

        # Verify no data was deleted
        records, _, _ = demo_driver.execute_query(
            "MATCH (n:Movie) RETURN count(n) AS c",
            database_=DEMO_DB,
        )
        assert records[0]["c"] > 0

    def test_schema_query_type(self, local_driver):
        """CREATE INDEX must be detected as schema type 's' via EXPLAIN."""
        from ipython_neo4j.magic import _explain_query_type

        # Use a unique name so this test is idempotent even if not cleaned up
        qtype = _explain_query_type(
            local_driver,
            "CREATE INDEX ipython_test_idx IF NOT EXISTS FOR (n:IpythonTestSchema) ON (n.name)",
            LOCAL_DB,
        )
        assert qtype == "s", f"Expected 's' for schema statement, got {qtype!r}"

    def test_drop_index_schema_type(self, local_driver):
        """DROP INDEX must also be detected as schema type 's'."""
        from ipython_neo4j.magic import _explain_query_type

        qtype = _explain_query_type(
            local_driver,
            "DROP INDEX ipython_test_idx IF EXISTS",
            LOCAL_DB,
        )
        assert qtype == "s", f"Expected 's' for DROP INDEX, got {qtype!r}"

    def test_create_constraint_schema_type(self, local_driver):
        """CREATE CONSTRAINT must be detected as schema type 's'."""
        from ipython_neo4j.magic import _explain_query_type

        qtype = _explain_query_type(
            local_driver,
            "CREATE CONSTRAINT ipython_test_cst IF NOT EXISTS "
            "FOR (n:IpythonTestSchema) REQUIRE n.id IS UNIQUE",
            LOCAL_DB,
        )
        assert qtype == "s", f"Expected 's' for CREATE CONSTRAINT, got {qtype!r}"

    def test_most_restrictive_type_order(self, demo_driver, local_driver):
        """Sanity-check: schema > write > read precedence via real EXPLAIN calls."""
        from ipython_neo4j.magic import _explain_query_type, _most_restrictive_type

        r_type = _explain_query_type(demo_driver, "MATCH (n:Movie) RETURN n LIMIT 1", DEMO_DB)
        w_type = _explain_query_type(demo_driver, "MATCH (n:Movie) DELETE n", DEMO_DB)
        s_type = _explain_query_type(
            local_driver,
            "CREATE INDEX ipython_mr_idx IF NOT EXISTS FOR (n:IpythonTestSchema) ON (n.id)",
            LOCAL_DB,
        )
        assert _most_restrictive_type([r_type, w_type]) in ("w", "rw")
        assert _most_restrictive_type([r_type, w_type, s_type]) == "s"


# ---------------------------------------------------------------------------
# Multi-statement integration tests
# ---------------------------------------------------------------------------

class TestMultiStatementIntegration:
    @pytest.fixture(autouse=True)
    def ip_shell(self):
        from IPython.core.interactiveshell import InteractiveShell
        from ipython_neo4j.magic import load_ipython_extension
        self.ip = InteractiveShell.instance()
        load_ipython_extension(self.ip)

    def test_two_read_statements(self):
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_cell_magic(
            "cypher",
            "-o last_result",
            "MATCH (m:Movie) RETURN m.title AS title LIMIT 3;\n"
            "MATCH (p:Person) RETURN p.name AS name LIMIT 5",
        )
        result = self.ip.user_ns.get("last_result")
        assert isinstance(result, CypherResult)
        # Last statement returns Person rows
        assert "name" in result.keys
        assert len(result) == 5

    def test_midline_semicolon_not_split(self):
        """WHERE n.name = 'O;Brien' must execute as a single statement."""
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_cell_magic(
            "cypher",
            "-o mid_result --no-preflight",
            # Cypher string literal with embedded semicolon — must NOT be split
            "MATCH (m:Movie) WHERE m.title = 'Semi;colon' OR 1=1 RETURN m.title AS title LIMIT 1",
        )
        result = self.ip.user_ns.get("mid_result")
        assert isinstance(result, CypherResult)

    def test_mixed_read_write_blocked(self):
        """Read + write multi-statement must be blocked without --write."""
        from ipython_neo4j.connection import Neo4jConnection
        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_cell_magic(
            "cypher",
            "",
            "MATCH (m:Movie) RETURN m.title LIMIT 1;\n"
            "CREATE (n:IpythonBlockedTest) RETURN n",
        )
        # The CREATE must NOT have happened (blocked by preflight)
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver(DEMO_URI, auth=(DEMO_USER, DEMO_PASS))
        try:
            recs, _, _ = drv.execute_query(
                "MATCH (n:IpythonBlockedTest) RETURN count(n) AS c",
                database_=DEMO_DB,
            )
            assert recs[0]["c"] == 0
        finally:
            drv.close()

    def test_schema_statements_on_local(self, local_driver):
        """CREATE INDEX + DROP INDEX as a two-statement cell via %%wcypher."""
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(LOCAL_URI, LOCAL_USER, LOCAL_PASS, LOCAL_DB)
        self.ip.run_cell_magic(
            "wcypher",
            "-o schema_result",
            "CREATE INDEX ipython_multi_idx IF NOT EXISTS FOR (n:IpythonTest) ON (n.name);\n"
            "DROP INDEX ipython_multi_idx IF EXISTS",
        )
        result = self.ip.user_ns.get("schema_result")
        assert isinstance(result, CypherResult)


# ---------------------------------------------------------------------------
# Magic integration tests (IPython shell)
# ---------------------------------------------------------------------------

class TestMagicIntegration:
    @pytest.fixture(autouse=True)
    def ip_shell(self):
        from IPython.core.interactiveshell import InteractiveShell
        from ipython_neo4j.magic import load_ipython_extension
        self.ip = InteractiveShell.instance()
        load_ipython_extension(self.ip)

    def test_neo4j_connect_magic(self):
        from ipython_neo4j.connection import Neo4jConnection
        self.ip.run_line_magic(
            "neo4j",
            f"{DEMO_URI} -u {DEMO_USER} -p {DEMO_PASS} -d {DEMO_DB}",
        )
        assert Neo4jConnection.is_connected()

    def test_cypher_read_magic_stores_result(self):
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_cell_magic(
            "cypher",
            "-o movies_result",
            "MATCH (m:Movie) RETURN m.title AS title LIMIT 5",
        )
        assert "movies_result" in self.ip.user_ns
        result = self.ip.user_ns["movies_result"]
        assert isinstance(result, CypherResult)
        assert len(result) == 5

    def test_cypher_df_magic_stores_dataframe(self):
        import pandas as pd
        from ipython_neo4j.connection import Neo4jConnection

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_cell_magic(
            "cypher",
            "--df -o movies_df",
            "MATCH (m:Movie) RETURN m.title AS title LIMIT 5",
        )
        assert "movies_df" in self.ip.user_ns
        df = self.ip.user_ns["movies_df"]
        assert isinstance(df, pd.DataFrame)
        assert "title" in df.columns

    def test_cypher_write_blocked_without_flag(self):
        """Write queries should be blocked and NOT modify the demo DB."""
        from ipython_neo4j.connection import Neo4jConnection

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        # This will be detected as 'w' by EXPLAIN and blocked
        self.ip.run_cell_magic(
            "cypher",
            "",
            "CREATE (n:IpythonTestNode {ts: datetime()}) RETURN n",
        )
        # The node should NOT have been created (write was blocked)
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver(DEMO_URI, auth=(DEMO_USER, DEMO_PASS))
        try:
            recs, _, _ = drv.execute_query(
                "MATCH (n:IpythonTestNode) RETURN count(n) AS c",
                database_=DEMO_DB,
            )
            assert recs[0]["c"] == 0
        finally:
            drv.close()

    def test_cypher_line_magic_read(self):
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        self.ip.run_line_magic(
            "cypher",
            "-o lm_result MATCH (m:Movie) RETURN m.title AS title LIMIT 3",
        )
        result = self.ip.user_ns.get("lm_result")
        assert isinstance(result, CypherResult)
        assert len(result) == 3

    def test_cypher_params_magic(self):
        from ipython_neo4j.connection import Neo4jConnection
        from ipython_neo4j.result import CypherResult

        Neo4jConnection.connect(DEMO_URI, DEMO_USER, DEMO_PASS, DEMO_DB)
        # Store the params dict as a namespace variable and reference it via -P
        self.ip.user_ns["_test_params"] = {"year": 2000}
        self.ip.run_cell_magic(
            "cypher",
            "-o param_result -P _test_params",
            "MATCH (m:Movie) WHERE m.released > $year RETURN m.title AS title LIMIT 5",
        )
        result = self.ip.user_ns.get("param_result")
        assert isinstance(result, CypherResult)


# ---------------------------------------------------------------------------
# Local write tests (skipped if no local Neo4j)
# ---------------------------------------------------------------------------

class TestLocalWriteIntegration:
    def test_wcypher_creates_and_deletes_node(self, local_driver):
        from ipython_neo4j.result import CypherResult
        from ipython_neo4j.magic import _explain_query_type

        # Verify EXPLAIN detects write
        qtype = _explain_query_type(
            local_driver,
            "CREATE (n:IpythonTest {id: 'pytest'}) RETURN n",
            LOCAL_DB,
        )
        assert qtype in ("w", "rw")

        # Execute write
        records, summary, keys = local_driver.execute_query(
            "CREATE (n:IpythonTest {id: 'pytest'}) RETURN n",
            database_=LOCAL_DB,
        )
        result = CypherResult(records=records, summary=summary, keys=keys)
        assert result.counters["nodes_created"] == 1

        # Cleanup
        local_driver.execute_query(
            "MATCH (n:IpythonTest {id: 'pytest'}) DELETE n",
            database_=LOCAL_DB,
        )

    def test_wcypher_magic_writes(self, local_driver):
        from IPython.core.interactiveshell import InteractiveShell
        from ipython_neo4j.magic import load_ipython_extension
        from ipython_neo4j.connection import Neo4jConnection

        ip = InteractiveShell.instance()
        load_ipython_extension(ip)
        Neo4jConnection.connect(LOCAL_URI, LOCAL_USER, LOCAL_PASS, LOCAL_DB)

        ip.run_cell_magic(
            "wcypher",
            "-o write_result",
            "CREATE (n:IpythonTest {id: 'magic_test'}) RETURN n",
        )

        # Cleanup
        local_driver.execute_query(
            "MATCH (n:IpythonTest {id: 'magic_test'}) DELETE n",
            database_=LOCAL_DB,
        )

"""IPython magic commands: %neo4j (connect) and %cypher / %%cypher (query)."""

from __future__ import annotations

import argparse
import os
import re
from typing import Optional

from IPython.core.magic import Magics, magics_class, line_magic, line_cell_magic
from IPython.core.magic_arguments import magic_arguments, argument, parse_argstring
from IPython.display import display, HTML

from .connection import Neo4jConnection, ConnectionConfig
from .result import CypherResult
from .display import render_error_html, render_connection_error_html, display_html

# Query types returned by Neo4j EXPLAIN summary
_WRITE_TYPES = {"w", "rw", "s"}   # write, read-write, schema
_READ_TYPES = {"r"}

# Split multi-statement cells on ; that is at the END of a line (possibly followed
# by trailing whitespace/newline). Semicolons embedded mid-line — e.g. inside string
# literals like WHERE name = 'O;Brien' — are NOT at end-of-line and are NOT split.
_STMT_SEP_RE = re.compile(r";\s*(?=\n|$)", re.MULTILINE)


def _split_statements(query: str) -> list[str]:
    """
    Split a Cypher query string into individual statements.

    Only splits on semicolons that sit at the **end of a line** (optionally
    followed by whitespace), which is the conventional Cypher multi-statement
    delimiter used in Cypher Shell and Neo4j Browser.  Mid-line semicolons —
    e.g. inside string literals — are left untouched.

    Examples
    --------
    >>> _split_statements("MATCH (n) RETURN n")
    ['MATCH (n) RETURN n']

    >>> _split_statements("CREATE INDEX idx FOR (n:Person) ON (n.name);\\nMATCH (n) RETURN n")
    ['CREATE INDEX idx FOR (n:Person) ON (n.name)', 'MATCH (n) RETURN n']

    >>> _split_statements("MATCH (n) WHERE n.name = 'O;Brien' RETURN n")
    ["MATCH (n) WHERE n.name = 'O;Brien' RETURN n"]
    """
    parts = _STMT_SEP_RE.split(query)
    return [p.strip() for p in parts if p.strip()]


def _explain_query_type(driver, query: str, database: str) -> str:
    """
    Run EXPLAIN on a **single** query (no execution) and return Neo4j's query
    type string: 'r' (read), 'w' (write), 'rw' (read-write), 's' (schema),
    or '' if EXPLAIN is unsupported / the query is not explainable.
    """
    try:
        _, summary, _ = driver.execute_query(
            f"EXPLAIN {query}",
            database_=database,
        )
        return (summary.query_type or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def _most_restrictive_type(types: list[str]) -> str:
    """
    Given a list of query type strings from multiple EXPLAIN calls, return the
    most restrictive one.  Precedence: s > rw > w > r > ''.
    """
    precedence = {"s": 4, "rw": 3, "w": 2, "r": 1, "": 0}
    return max(types, key=lambda t: precedence.get(t, 0), default="")


def _load_env_file(path: str) -> "dict[str, str] | None":
    """
    Load a dotenv-style file and return its key/value pairs.
    Returns None (and displays an error) if python-dotenv is not installed
    or the file cannot be found.
    """
    import pathlib

    try:
        from dotenv import dotenv_values
    except ImportError:
        display_html(
            "<div style='border-left:4px solid #e67e22;padding:8px 12px;background:#fff8f0'>"
            "<b style='color:#e67e22'>&#x26A0; python-dotenv not installed.</b><br>"
            "Install it with: <code>pip install python-dotenv</code>"
            "</div>"
        )
        return None

    env_path = pathlib.Path(path)
    if not env_path.exists():
        display_html(
            f"<div style='border-left:4px solid #c0392b;padding:8px 12px;background:#fff8f8'>"
            f"<b style='color:#c0392b'>&#x2717; File not found:</b> "
            f"<code>{path}</code>"
            f"</div>"
        )
        return None

    return dict(dotenv_values(env_path))


@magics_class
class Neo4jMagics(Magics):
    """Provides %neo4j, %cypher, %%cypher, and %wcypher magics."""

    # ------------------------------------------------------------------
    # %neo4j — connection management
    # ------------------------------------------------------------------

    @magic_arguments()
    @argument("uri", nargs="?", default=None, help="Connection URI (e.g. bolt://localhost:7687)")
    @argument("-u", "--username", default=None, help="Username (default: NEO4J_USERNAME env)")
    @argument("-p", "--password", default=None, help="Password (default: NEO4J_PASSWORD env)")
    @argument("-d", "--database", default=None, help="Database name (default: NEO4J_DATABASE env or 'neo4j')")
    @argument("--env-file", dest="env_file", default=None, metavar="FILE",
              help="Load connection settings from a .env file (requires python-dotenv). "
                   "Explicit flags (-u/-p/-d/URI) override values in the file.")
    @argument("--close", action="store_true", help="Close the current connection")
    @argument("--status", action="store_true", help="Show current connection status")
    @line_magic
    def neo4j(self, line: str) -> None:
        """Connect to a Neo4j instance.

        Connection is resolved in this priority order (highest first):
          1. Explicit flags on this magic call (-u / -p / -d / URI)
          2. --env-file <file>  (e.g. .env or secrets.env)
          3. Environment variables already set in the shell
             (NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE)

        Examples
        --------
        %neo4j                                         # auto-connect from env vars
        %neo4j bolt://localhost:7687
        %neo4j bolt://localhost:7687 -u neo4j -p secret
        %neo4j neo4j+s://demo.neo4jlabs.com -u movies -p movies -d movies
        %neo4j --env-file .env                         # load from .env file
        %neo4j --env-file secrets.env -d mydb          # .env + override database
        %neo4j --status
        %neo4j --close
        """
        args = parse_argstring(self.neo4j, line)

        if args.close:
            Neo4jConnection.close()
            display_html("<span style='color:#e74c3c'>&#x25CF; Disconnected from Neo4j.</span>")
            return

        if args.status:
            display_html(Neo4jConnection.status_html())
            return

        # --env-file: load values from a dotenv file, then let explicit flags override
        env_uri = env_user = env_pass = env_db = None
        if args.env_file:
            loaded = _load_env_file(args.env_file)
            if loaded is None:
                return  # error already displayed
            env_uri  = loaded.get("NEO4J_URI")
            env_user = loaded.get("NEO4J_USERNAME") or loaded.get("NEO4J_USER")
            env_pass = loaded.get("NEO4J_PASSWORD")
            env_db   = loaded.get("NEO4J_DATABASE")

        try:
            Neo4jConnection.connect(
                uri=args.uri or env_uri,
                username=args.username or env_user,
                password=args.password or env_pass,
                database=args.database or env_db,
            )
            display_html(
                "<span style='color:#27ae60'>&#x25CF; Connected.</span> "
                + Neo4jConnection.status_html()
            )
        except Exception as exc:  # noqa: BLE001
            display_html(render_connection_error_html(exc))

    # ------------------------------------------------------------------
    # %cypher / %%cypher — query execution
    # ------------------------------------------------------------------

    @magic_arguments()
    @argument("-u", "--uri", default=None, help="Override connection URI for this query")
    @argument("--username", default=None, help="Override username for this query")
    @argument("-p", "--password", default=None, help="Override password for this query")
    @argument("-d", "--database", default=None, help="Override database for this query")
    @argument("-o", "--output", default=None, metavar="VAR",
              help="Store CypherResult in this variable name in the notebook namespace")
    @argument("--df", dest="as_df", action="store_true",
              help="Store/return a DataFrame instead of a CypherResult")
    @argument("--viz", "--visualize", dest="visualize", action="store_true",
              help="Render an interactive graph visualization (requires neo4j-viz)")
    @argument("--params", "-P", default=None, metavar="EXPR",
              help="Python expression evaluating to a dict of query parameters")
    @argument("--write", "-w", dest="allow_write", action="store_true",
              help="Allow write/schema queries. Without this flag, write queries are blocked.")
    @argument("--no-preflight", dest="no_preflight", action="store_true",
              help="Skip the EXPLAIN pre-flight query-type check.")
    @argument("query_words", nargs=argparse.REMAINDER,
              help="Cypher query (line magic only; use cell body for cell magic)")
    @line_cell_magic
    def cypher(self, line: str, cell: Optional[str] = None) -> None:
        """Execute a Cypher query against Neo4j.

        By default only read queries ('r') are allowed.  Write / schema queries
        require the ``--write`` flag (or use ``%wcypher``).  A lightweight
        ``EXPLAIN`` pre-flight is run first to detect the query type before
        executing, unless ``--no-preflight`` is given.

        Line magic  — query on the same line as %cypher:
            %cypher MATCH (n:Person) RETURN n.name LIMIT 5

        Cell magic  — query in the cell body:
            %%cypher
            MATCH (n:Person)-[:KNOWS]->(m)
            RETURN n.name, m.name
            LIMIT 10

        Write query (explicit permission required):
            %%cypher --write
            CREATE (n:Person {name: 'Alice'})

        Options
        -------
        --write / -w        allow write or schema-change queries
        --no-preflight      skip EXPLAIN type detection
        -o / --output VAR   store result in VAR (namespace variable)
        --df                return / store as pandas DataFrame
        --viz               render interactive graph visualization
        -P / --params EXPR  dict expression for query parameters, e.g. -P "{'name': 'Alice'}"

        Connection override (for a single query):
            -u bolt://localhost:7687 --username neo4j -p secret -d mydb
        """
        self._run_cypher(line, cell, allow_write=False)

    @magic_arguments()
    @argument("-u", "--uri", default=None)
    @argument("--username", default=None)
    @argument("-p", "--password", default=None)
    @argument("-d", "--database", default=None)
    @argument("-o", "--output", default=None, metavar="VAR")
    @argument("--df", dest="as_df", action="store_true")
    @argument("--viz", "--visualize", dest="visualize", action="store_true")
    @argument("--params", "-P", default=None, metavar="EXPR")
    @argument("--no-preflight", dest="no_preflight", action="store_true")
    @argument("query_words", nargs=argparse.REMAINDER,
              help="Cypher query (line magic only; use cell body for cell magic)")
    @line_cell_magic
    def wcypher(self, line: str, cell: Optional[str] = None) -> None:
        """Execute a write Cypher query (CREATE, MERGE, DELETE, SET, REMOVE, schema DDL).

        Equivalent to ``%%cypher --write``.  Use this to make write intent
        explicit in your notebook cells.

        Examples
        --------
        %wcypher CREATE (n:Person {name: 'Alice'}) RETURN n

        %%wcypher
        MERGE (n:Person {name: $name})
        ON CREATE SET n.created = datetime()
        RETURN n
        """
        self._run_cypher(line, cell, allow_write=True)

    # ------------------------------------------------------------------
    # Shared execution engine
    # ------------------------------------------------------------------

    def _run_cypher(
        self,
        line: str,
        cell: Optional[str],
        allow_write: bool,
    ) -> None:
        # Re-parse args from the original line using the %cypher argument spec
        args = parse_argstring(self.cypher, line)

        # Honour explicit --write flag on %cypher
        if getattr(args, "allow_write", False):
            allow_write = True

        # Build the Cypher string
        if cell is not None:
            query = cell.strip()
        else:
            # args.query_words captures remaining tokens after flags (argparse.REMAINDER)
            query = " ".join(args.query_words) if args.query_words else ""

        if not query:
            display_html(
                "<span style='color:#e67e22'>&#x26A0; No Cypher query provided.</span>"
            )
            return

        # Resolve query parameters
        params: dict = {}
        if args.params:
            try:
                params = eval(args.params, self.shell.user_ns)  # noqa: S307
                if not isinstance(params, dict):
                    raise TypeError("--params expression must evaluate to a dict")
            except Exception as exc:  # noqa: BLE001
                display_html(render_error_html(exc, query))
                return

        # Determine driver
        try:
            if args.uri:
                from neo4j import GraphDatabase
                driver = GraphDatabase.driver(
                    args.uri,
                    auth=(
                        args.username or os.environ.get("NEO4J_USERNAME", "neo4j"),
                        args.password or os.environ.get("NEO4J_PASSWORD", "neo4j"),
                    ),
                )
                database = args.database or os.environ.get("NEO4J_DATABASE", "neo4j")
                own_driver = True
            else:
                driver = Neo4jConnection.get_driver()
                database = args.database or Neo4jConnection.get_database()
                own_driver = False
        except Exception as exc:  # noqa: BLE001
            display_html(render_connection_error_html(exc))
            return

        # Split into individual statements (handles multi-statement cells)
        statements = _split_statements(query)
        if not statements:
            display_html(
                "<span style='color:#e67e22'>&#x26A0; No Cypher query provided.</span>"
            )
            if own_driver:
                driver.close()
            return

        try:
            # --- Pre-flight EXPLAIN check (all statements) ---
            if not getattr(args, "no_preflight", False):
                qtypes = [
                    _explain_query_type(driver, stmt, database)
                    for stmt in statements
                ]
                overall_type = _most_restrictive_type(qtypes)
                if overall_type and overall_type in _WRITE_TYPES and not allow_write:
                    display_html(
                        _render_write_blocked_html(query, overall_type)
                    )
                    return

            # --- Execute each statement in sequence ---
            results: list[CypherResult] = []
            for stmt in statements:
                records, summary, keys = driver.execute_query(
                    stmt,
                    parameters_=params,
                    database_=database,
                )
                results.append(CypherResult(records=records, summary=summary, keys=keys))

        except Exception as exc:  # noqa: BLE001
            display_html(render_error_html(exc, query))
            return
        finally:
            if own_driver:
                driver.close()

        # For single statements keep the original single-result UX.
        # For multiple statements, display each result inline and store the last.
        if len(results) == 1:
            result = results[0]

            if args.visualize:
                try:
                    result.visualize()
                except ImportError as exc:
                    display_html(render_error_html(exc, query))
                return

            value = result.df if args.as_df else result
            if args.output:
                self.shell.user_ns[args.output] = value
                display_html(
                    f"<span style='color:#27ae60'>&#x2714; Stored in "
                    f"<code>{args.output}</code> ({len(result)} rows)</span>"
                )
                return
            display(value)

        else:
            # Multi-statement: display each result with a statement header
            for i, (stmt, result) in enumerate(zip(statements, results), 1):
                header = (
                    f"<div style='font-size:12px;color:#555;margin:8px 0 2px'>"
                    f"<b>Statement {i}/{len(results)}:</b> "
                    f"<code>{stmt[:80].replace('<','&lt;')}{'…' if len(stmt) > 80 else ''}</code>"
                    f"</div>"
                )
                display_html(header)
                display(result)

            last = results[-1]
            if args.output:
                value = last.df if args.as_df else last
                self.shell.user_ns[args.output] = value
                display_html(
                    f"<span style='color:#27ae60'>&#x2714; Last result stored in "
                    f"<code>{args.output}</code> ({len(last)} rows)</span>"
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_query_from_line(line: str) -> str:
    """
    Remove argument flags from the start of a magic line and return the
    remaining text as the Cypher query.
    """
    import shlex

    tokens = shlex.split(line)
    query_tokens: list[str] = []
    skip_next = False
    _FLAGS_WITH_VALUES = {
        "-u", "--uri", "--username", "-p", "--password",
        "-d", "--database", "-o", "--output", "--params", "-P",
    }
    _BOOL_FLAGS = {
        "--df", "--viz", "--visualize", "--write", "-w",
        "--no-preflight",
    }

    for tok in tokens:
        if skip_next:
            skip_next = False
            continue
        if tok in _FLAGS_WITH_VALUES:
            skip_next = True
            continue
        if tok in _BOOL_FLAGS:
            continue
        query_tokens.append(tok)

    return " ".join(query_tokens)


def _render_write_blocked_html(query: str, qtype: str) -> str:
    import html as html_mod

    label = {"w": "write", "rw": "read-write", "s": "schema"}.get(qtype, "write")
    return f"""
<div style="border-left:4px solid #e67e22;padding:10px 14px;margin:6px 0;
             background:#fff8f0;border-radius:0 4px 4px 0;font-family:sans-serif">
  <div style="font-weight:bold;color:#e67e22;font-size:14px">
    &#x26D4; Write query blocked
  </div>
  <div style="margin-top:4px;color:#333;font-size:13px">
    Neo4j detected this as a <code>{html_mod.escape(label)}</code> query.
    Use <code>%%cypher --write</code> or <code>%%wcypher</code> to allow
    write and schema operations explicitly.
  </div>
  <details style="margin-top:6px">
    <summary style="cursor:pointer;color:#555;font-size:12px">Query</summary>
    <pre style="background:#f8f8f8;padding:8px;border-radius:3px;
                font-size:12px;margin-top:4px">{html_mod.escape(query.strip())}</pre>
  </details>
</div>
""".strip()


def load_ipython_extension(ipython) -> None:
    """Register the magics when the extension is loaded."""
    ipython.register_magics(Neo4jMagics)


def unload_ipython_extension(ipython) -> None:
    """Called when the extension is unloaded (no-op)."""

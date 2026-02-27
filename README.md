# ipython-neo4j

IPython / Jupyter magic commands for [Neo4j](https://neo4j.com) Cypher queries.
A modern replacement for [icypher](https://pypi.org/project/icypher/) built on the
[neo4j-rust-ext](https://pypi.org/project/neo4j-rust-ext/) Bolt driver with first-class
graph visualization via [neo4j-viz](https://neo4j.com/docs/python-graph-visualization/current/).

```python
%load_ext ipython_neo4j

%neo4j bolt://localhost:7687 -u neo4j -p secret

%cypher MATCH (m:Movie) RETURN m.title AS title, m.released AS year LIMIT 10
```

---

## Installation

```bash
pip install ipython-neo4j
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add ipython-neo4j
```

To use `.env` file support via `%neo4j --env-file`, install the optional extra:

```bash
pip install "ipython-neo4j[dotenv]"
```

**Requirements**: Python ≥ 3.10, Neo4j 6.x.

---

## Quick start

```python
# 1. Load the extension
%load_ext ipython_neo4j

# 2. Connect
%neo4j bolt://localhost:7687 -u neo4j -p secret

# 3. Run a read query — renders as an HTML table
%cypher MATCH (m:Movie) RETURN m.title AS title LIMIT 5

# 4. Multi-line query in a cell
%%cypher
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
RETURN p.name AS actor, count(m) AS films
ORDER BY films DESC
LIMIT 10

# 5. Get a pandas DataFrame
%%cypher --df -o df
MATCH (m:Movie) RETURN m.title AS title, m.released AS year

df.describe()

# 6. Visualize a subgraph
%%cypher --viz
MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
RETURN p, r, m
LIMIT 30
```

---

## Magic commands

### `%neo4j` — connect / status / disconnect

```
%neo4j [URI] [-u USERNAME] [-p PASSWORD] [-d DATABASE]
       [--env-file FILE]
       [--status] [--close]
```

| Flag | Description |
|---|---|
| `URI` | Bolt URI, e.g. `bolt://localhost:7687` or `neo4j+s://host` |
| `-u / --username` | Neo4j username |
| `-p / --password` | Neo4j password |
| `-d / --database` | Target database (default: `neo4j`) |
| `--env-file FILE` | Load connection settings from a `.env` file |
| `--status` | Show current connection status |
| `--close` | Close the active connection |

### `%cypher` / `%%cypher` — read queries

```
%cypher [flags] QUERY
%%cypher [flags]
<query body>
```

### `%wcypher` / `%%wcypher` — write queries

Identical to `%cypher` / `%%cypher` but **write and schema operations are explicitly allowed**.
Use this to make write intent visible in notebooks.

### Common flags for `%cypher` / `%%cypher` / `%wcypher` / `%%wcypher`

| Flag | Description |
|---|---|
| `--write / -w` | Allow write / schema queries (alternative to `%%wcypher`) |
| `--no-preflight` | Skip the `EXPLAIN` query-type pre-flight check |
| `-o VAR` | Store the `CypherResult` in the notebook variable `VAR` |
| `--df` | Return / store a pandas `DataFrame` instead of a `CypherResult` |
| `--viz` | Render an interactive graph visualization (requires `neo4j-viz`) |
| `-P EXPR` | Query parameters — a Python expression evaluated in the notebook namespace |
| `-u / --uri` | Override connection URI for this query only |
| `--username` | Override username for this query only |
| `-p / --password` | Override password for this query only |
| `-d / --database` | Override database for this query only |

---

## Configuring the connection

Connection settings are resolved in this priority order (highest wins):

```
explicit flag on %neo4j  >  --env-file values  >  shell env vars  >  built-in defaults
```

### Option 1 — Direct flags (most explicit)

```python
%neo4j bolt://localhost:7687 -u neo4j -p secret -d mydb
```

### Option 2 — `.env` file

Create a `.env` file (and add it to `.gitignore`):

```ini
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=secret
NEO4J_DATABASE=mydb
```

Then in your notebook:

```python
%neo4j --env-file .env
%neo4j --env-file secrets.env -d override_db   # file + per-flag override
```

Requires [`python-dotenv`](https://pypi.org/project/python-dotenv/):
```bash
pip install python-dotenv
```

### Option 3 — IPython `%env` magic (no file needed)

```python
%env NEO4J_URI=bolt://localhost:7687
%env NEO4J_USERNAME=neo4j
%env NEO4J_PASSWORD=secret
%env NEO4J_DATABASE=mydb
%neo4j   # picks up the vars automatically
```

### Option 4 — Shell environment variables

Set variables before launching Jupyter, then call `%neo4j` with no arguments:

```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=secret
export NEO4J_DATABASE=mydb
jupyter lab
```

```python
%neo4j   # auto-connects from env
```

### Option 5 — `python-dotenv` directly in a cell

```python
from dotenv import load_dotenv
load_dotenv(".env")   # populates os.environ
%neo4j                # reads from env
```

### Option 6 — Per-query connection override

Override the global connection for a single query without changing it:

```python
%%cypher -u bolt://other-host:7687 --username analyst -p readonly -d reporting
MATCH (n:Report) RETURN n LIMIT 5
```

---

## Read / write safety

`%cypher` (and `%%cypher`) **block write and schema queries by default**.
Before executing, an `EXPLAIN` pre-flight call is made to detect the query type:

| Neo4j type | Meaning | Default behaviour |
|---|---|---|
| `r` | Read | ✅ Allowed |
| `w` | Write | ❌ Blocked |
| `rw` | Read + write | ❌ Blocked |
| `s` | Schema (DDL) | ❌ Blocked |

To allow writes, use `--write` or the dedicated `%%wcypher` magic:

```python
# Either of these works:
%%cypher --write
CREATE (n:Person {name: 'Alice'}) RETURN n

%%wcypher
CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)
```

Skip the pre-flight check when you know the query is safe (e.g. on slow connections):

```python
%%cypher --no-preflight
MATCH (n) RETURN n LIMIT 100
```

---

## Multi-statement cells

Statements can be separated by a **semicolon at the end of a line** — the same convention
used in Cypher Shell and Neo4j Browser:

```python
%%wcypher
CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name);
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE
```

Each statement is EXPLAIN'd individually. If **any** statement is a write or schema
operation the entire cell is blocked unless `--write` / `%%wcypher` is used.

> **Note:** Mid-line semicolons — e.g. inside string literals like
> `WHERE name = 'O;Brien'` — are **not** treated as statement separators.

When multiple statements are present, each result is displayed with a numbered header
and the **last result** is stored when `-o VAR` is used.

---

## Working with results

```python
%%cypher -o result
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
RETURN p.name AS name, m.title AS title
LIMIT 20
```

```python
# CypherResult renders as an HTML table in Jupyter automatically

result           # display HTML table
result.df        # pandas DataFrame shorthand
result.to_dataframe()   # same

len(result)      # number of rows
result.keys      # column names
result.counters  # write counters (nodes_created, etc.)
result.summary   # full Neo4j ResultSummary

result.visualize()               # render neo4j-viz graph
result.visualize(color_by="caption")   # with custom coloring
result.to_graph()                # VisualizationGraph object
```

### Query parameters

Pass a Python expression (evaluated in the notebook namespace) with `-P`:

```python
params = {"year": 2000, "name": "Tom Hanks"}

%%cypher -P params -o movies
MATCH (p:Person {name: $name})-[:ACTED_IN]->(m:Movie)
WHERE m.released >= $year
RETURN m.title AS title, m.released AS year
```

---

## Graph visualization

Return `Node` and `Relationship` objects and render them with `--viz`:

```python
%%cypher --viz
MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
RETURN p, r, m
LIMIT 40
```

Or call `.visualize()` programmatically on any `CypherResult`:

```python
%%cypher -o g
MATCH (p:Person)-[r:DIRECTED]->(m:Movie) RETURN p, r, m LIMIT 20

g.visualize(color_by="caption")
```

Visualization is powered by [neo4j-viz](https://neo4j.com/docs/python-graph-visualization/current/)
and is installed automatically as a dependency.

---

## Error rendering

Neo4j errors (syntax errors, type errors, auth failures, connectivity issues) are
rendered as styled HTML in the cell output rather than raw Python tracebacks:

```python
%cypher MATCH (n) RETURN @@bad@@
# → Cypher Syntax Error: rendered with error code and query context
```

---

## Reloading after code changes

When developing `ipython-neo4j` itself, reload the extension without restarting the kernel:

```python
%reload_ext ipython_neo4j
```

---

## Development

```bash
git clone https://github.com/neo4j-labs/ipython-neo4j
cd ipython-neo4j
uv sync --all-groups

# unit tests (no Neo4j required)
uv run pytest tests/test_connection.py tests/test_result.py tests/test_magic.py -v

# integration tests (requires Neo4j)
# credentials in integration.env
uv run pytest tests/test_integration.py -v

# run the demo notebook
uv run jupyter lab notebooks/demo.ipynb
```

Integration tests use:
- **Read-only demo** (`integration.env` default): `neo4j+s://demo.neo4jlabs.com` — user/db: `movies`
- **Local write tests**: `bolt://localhost:7687` — user: `neo4j`, password: `password`

---

## License

Apache 2.0

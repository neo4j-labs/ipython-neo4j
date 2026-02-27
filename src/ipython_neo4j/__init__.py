"""
ipython-neo4j
=============

IPython/Jupyter magic commands for Neo4j Cypher queries.

Usage
-----
Load the extension in a notebook::

    %load_ext ipython_neo4j

Connect::

    %neo4j bolt://localhost:7687 -u neo4j -p secret

Run queries::

    %cypher MATCH (n:Person) RETURN n.name LIMIT 5

    %%cypher
    MATCH (n:Person)-[:KNOWS]->(m:Person)
    RETURN n.name, m.name
    LIMIT 10

Return a DataFrame::

    %%cypher --df -o df
    MATCH (n:Movie) RETURN n.title, n.released

Visualize a subgraph::

    %%cypher --viz
    MATCH p=(n:Person)-[:ACTED_IN]->(m:Movie)
    RETURN p LIMIT 20
"""

from .connection import Neo4jConnection, ConnectionConfig
from .result import CypherResult
from .magic import load_ipython_extension, unload_ipython_extension

__all__ = [
    "Neo4jConnection",
    "ConnectionConfig",
    "CypherResult",
    "load_ipython_extension",
    "unload_ipython_extension",
]

__version__ = "0.1.0"

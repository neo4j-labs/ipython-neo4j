"""Neo4j connection management for IPython magic."""

from __future__ import annotations

import os
from typing import Optional

import neo4j
from neo4j import GraphDatabase, Driver, Auth


class ConnectionConfig:
    """Holds Neo4j connection parameters."""

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: Optional[str] = None,
    ) -> None:
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database or "neo4j"

    @classmethod
    def from_env(cls) -> "ConnectionConfig":
        """Build config from environment variables."""
        uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        username = os.environ.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USER", "neo4j")
        password = os.environ.get("NEO4J_PASSWORD", "neo4j")
        database = os.environ.get("NEO4J_DATABASE", "neo4j")
        return cls(uri=uri, username=username, password=password, database=database)

    @classmethod
    def from_uri_string(cls, uri_string: str) -> "ConnectionConfig":
        """Parse a connection URI of the form scheme://user:password@host:port/database."""
        # Support neo4j://user:pass@host:port/db and plain bolt://host:port
        from urllib.parse import urlparse

        parsed = urlparse(uri_string)
        username = parsed.username or os.environ.get("NEO4J_USERNAME", "neo4j")
        password = parsed.password or os.environ.get("NEO4J_PASSWORD", "neo4j")
        database = parsed.path.lstrip("/") or os.environ.get("NEO4J_DATABASE", "neo4j") or None

        # Reconstruct URI without credentials
        scheme = parsed.scheme
        host = parsed.hostname
        port = parsed.port
        netloc = f"{host}:{port}" if port else host
        clean_uri = f"{scheme}://{netloc}"

        return cls(uri=clean_uri, username=username, password=password, database=database or "neo4j")

    def __repr__(self) -> str:
        return f"ConnectionConfig(uri={self.uri!r}, username={self.username!r}, database={self.database!r})"


class Neo4jConnection:
    """Singleton-style connection manager for the IPython session."""

    _driver: Optional[Driver] = None
    _config: Optional[ConnectionConfig] = None

    @classmethod
    def connect(
        cls,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ) -> "Neo4jConnection":
        """Establish (or replace) the global Neo4j driver."""
        if uri is None:
            config = ConnectionConfig.from_env()
        else:
            config = ConnectionConfig(
                uri=uri,
                username=username or os.environ.get("NEO4J_USERNAME", "neo4j"),
                password=password or os.environ.get("NEO4J_PASSWORD", "neo4j"),
                database=database or os.environ.get("NEO4J_DATABASE", "neo4j"),
            )

        if cls._driver is not None:
            cls._driver.close()

        cls._driver = GraphDatabase.driver(
            config.uri,
            auth=(config.username, config.password),
        )
        cls._driver.verify_connectivity()
        cls._config = config
        return cls

    @classmethod
    def get_driver(cls) -> Driver:
        """Return existing driver, auto-connecting from env vars if needed."""
        if cls._driver is None:
            cls.connect()
        return cls._driver  # type: ignore[return-value]

    @classmethod
    def get_database(cls) -> str:
        if cls._config is None:
            return os.environ.get("NEO4J_DATABASE", "neo4j")
        return cls._config.database  # type: ignore[return-value]

    @classmethod
    def close(cls) -> None:
        if cls._driver is not None:
            cls._driver.close()
            cls._driver = None
            cls._config = None

    @classmethod
    def is_connected(cls) -> bool:
        return cls._driver is not None

    @classmethod
    def status_html(cls) -> str:
        if not cls.is_connected():
            return "<span style='color:#e74c3c'>&#x25CF; Not connected</span>"
        cfg = cls._config
        return (
            f"<span style='color:#27ae60'>&#x25CF; Connected</span> "
            f"to <code>{cfg.uri}</code> "  # type: ignore[union-attr]
            f"as <code>{cfg.username}</code> "  # type: ignore[union-attr]
            f"/ db: <code>{cfg.database}</code>"  # type: ignore[union-attr]
        )

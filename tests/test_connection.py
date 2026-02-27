"""Unit tests for Neo4jConnection and ConnectionConfig."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from ipython_neo4j.connection import ConnectionConfig, Neo4jConnection


class TestConnectionConfig:
    def test_from_env_defaults(self, monkeypatch):
        monkeypatch.delenv("NEO4J_URI", raising=False)
        monkeypatch.delenv("NEO4J_USERNAME", raising=False)
        monkeypatch.delenv("NEO4J_USER", raising=False)
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.delenv("NEO4J_DATABASE", raising=False)

        cfg = ConnectionConfig.from_env()
        assert cfg.uri == "bolt://localhost:7687"
        assert cfg.username == "neo4j"
        assert cfg.password == "neo4j"
        assert cfg.database == "neo4j"

    def test_from_env_custom(self, monkeypatch):
        monkeypatch.setenv("NEO4J_URI", "bolt://myhost:7687")
        monkeypatch.setenv("NEO4J_USERNAME", "admin")
        monkeypatch.setenv("NEO4J_PASSWORD", "secret")
        monkeypatch.setenv("NEO4J_DATABASE", "mydb")

        cfg = ConnectionConfig.from_env()
        assert cfg.uri == "bolt://myhost:7687"
        assert cfg.username == "admin"
        assert cfg.password == "secret"
        assert cfg.database == "mydb"

    def test_from_uri_string_with_credentials(self):
        cfg = ConnectionConfig.from_uri_string("bolt://alice:wonderland@localhost:7687/movies")
        assert cfg.uri == "bolt://localhost:7687"
        assert cfg.username == "alice"
        assert cfg.password == "wonderland"
        assert cfg.database == "movies"

    def test_from_uri_string_no_credentials(self, monkeypatch):
        monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
        monkeypatch.setenv("NEO4J_PASSWORD", "pass")
        cfg = ConnectionConfig.from_uri_string("bolt://localhost:7687")
        assert cfg.uri == "bolt://localhost:7687"
        assert cfg.username == "neo4j"

    def test_repr(self):
        cfg = ConnectionConfig("bolt://localhost:7687", "neo4j", "pw", "mydb")
        assert "bolt://localhost:7687" in repr(cfg)
        assert "neo4j" in repr(cfg)
        assert "pw" not in repr(cfg)  # password should NOT appear in repr


class TestNeo4jConnection:
    def setup_method(self):
        # Reset singleton state between tests
        Neo4jConnection._driver = None
        Neo4jConnection._config = None

    def test_connect_calls_driver(self):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw")
            mock_driver.verify_connectivity.assert_called_once()
            assert Neo4jConnection.is_connected()

    def test_get_driver_auto_connects(self):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            driver = Neo4jConnection.get_driver()
            assert driver is mock_driver

    def test_close(self):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw")
            Neo4jConnection.close()
            mock_driver.close.assert_called_once()
            assert not Neo4jConnection.is_connected()

    def test_status_html_connected(self):
        mock_driver = MagicMock()
        with patch("ipython_neo4j.connection.GraphDatabase.driver", return_value=mock_driver):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw", "mydb")
        html = Neo4jConnection.status_html()
        assert "Connected" in html
        assert "bolt://localhost:7687" in html
        assert "mydb" in html

    def test_status_html_not_connected(self):
        html = Neo4jConnection.status_html()
        assert "Not connected" in html

    def test_reconnect_closes_previous(self):
        mock_driver1 = MagicMock()
        mock_driver2 = MagicMock()
        drivers = iter([mock_driver1, mock_driver2])
        with patch("ipython_neo4j.connection.GraphDatabase.driver", side_effect=drivers):
            Neo4jConnection.connect("bolt://localhost:7687", "neo4j", "pw")
            Neo4jConnection.connect("bolt://localhost:7688", "neo4j", "pw")
            mock_driver1.close.assert_called_once()

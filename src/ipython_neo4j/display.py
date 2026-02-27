"""Helpers for rendering errors and status messages in Jupyter/IPython."""

from __future__ import annotations

import html
import traceback


def render_error_html(exc: Exception, query: str = "") -> str:
    """Return an HTML-formatted error block for display in a Jupyter cell."""
    from neo4j.exceptions import (
        CypherSyntaxError,
        CypherTypeError,
        ClientError,
        DatabaseError,
        ServiceUnavailable,
        AuthError,
    )

    title = type(exc).__name__
    message = html.escape(str(exc))

    # Extract Neo4j-specific metadata when available
    neo4j_code = ""
    if hasattr(exc, "code") and exc.code:
        neo4j_code = exc.code

    # Choose icon and colour by error category
    if isinstance(exc, (CypherSyntaxError,)):
        icon, color = "&#x2717;", "#c0392b"
        category = "Cypher Syntax Error"
    elif isinstance(exc, CypherTypeError):
        icon, color = "&#x26A0;", "#e67e22"
        category = "Cypher Type Error"
    elif isinstance(exc, AuthError):
        icon, color = "&#x1F512;", "#8e44ad"
        category = "Authentication Error"
    elif isinstance(exc, ServiceUnavailable):
        icon, color = "&#x1F534;", "#c0392b"
        category = "Service Unavailable"
    elif isinstance(exc, ClientError):
        icon, color = "&#x2717;", "#c0392b"
        category = "Client Error"
    elif isinstance(exc, DatabaseError):
        icon, color = "&#x26A0;", "#e67e22"
        category = "Database Error"
    else:
        icon, color = "&#x2717;", "#7f8c8d"
        category = "Error"

    query_block = ""
    if query.strip():
        escaped_query = html.escape(query.strip())
        query_block = (
            f"<details style='margin-top:6px'>"
            f"<summary style='cursor:pointer;color:#555;font-size:12px'>Query</summary>"
            f"<pre style='background:#f8f8f8;padding:8px;border-radius:3px;"
            f"font-size:12px;margin-top:4px'>{escaped_query}</pre>"
            f"</details>"
        )

    code_block = ""
    if neo4j_code:
        code_block = (
            f"<div style='font-family:monospace;font-size:11px;color:#555;"
            f"margin-top:4px'>Code: {html.escape(neo4j_code)}</div>"
        )

    return f"""
<div style="border-left:4px solid {color};padding:10px 14px;margin:6px 0;
             background:#fff8f8;border-radius:0 4px 4px 0;font-family:sans-serif">
  <div style="font-weight:bold;color:{color};font-size:14px">
    {icon} {category}: <span style='font-weight:normal'>{title}</span>
  </div>
  <div style="margin-top:4px;color:#333;font-size:13px">{message}</div>
  {code_block}
  {query_block}
</div>
""".strip()


def render_connection_error_html(exc: Exception) -> str:
    """Render a connection-specific error with setup hints."""
    from neo4j.exceptions import ServiceUnavailable, AuthError

    body = render_error_html(exc)

    hints: list[str] = []
    if isinstance(exc, ServiceUnavailable):
        hints = [
            "Ensure the Neo4j instance is running.",
            "Check <code>NEO4J_URI</code> environment variable (default: <code>bolt://localhost:7687</code>).",
            "Run <code>%neo4j bolt://localhost:7687 -u neo4j -p password</code> to connect explicitly.",
        ]
    elif isinstance(exc, AuthError):
        hints = [
            "Check username / password.",
            "Set <code>NEO4J_USERNAME</code> and <code>NEO4J_PASSWORD</code> environment variables.",
            "Run <code>%neo4j bolt://localhost:7687 -u neo4j -p &lt;password&gt;</code>.",
        ]

    if hints:
        items = "".join(f"<li>{h}</li>" for h in hints)
        body += (
            f"<ul style='font-size:12px;color:#555;margin:6px 0 0 14px'>{items}</ul>"
        )
    return body


def display_html(html_content: str) -> None:
    """Display raw HTML in the current IPython/Jupyter cell output."""
    try:
        from IPython.display import display, HTML
        display(HTML(html_content))
    except ImportError:
        print(html_content)

"""
Database layer — NeonDB (Postgres) connection and operations.
"""

import psycopg2
import psycopg2.extras
from config import DATABASE_URL

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS articles (
    url          TEXT PRIMARY KEY,
    title        TEXT,
    description  TEXT,
    outlet       TEXT,
    domain       TEXT,
    publish_date DATE,
    collected_at TIMESTAMPTZ DEFAULT now()
);
"""


def get_connection():
    """Return a new psycopg2 connection using DATABASE_URL."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Export it as an env var or add it to .env"
        )
    return psycopg2.connect(DATABASE_URL)


def ensure_schema():
    """Create the articles table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def upsert_articles(rows: list[dict]) -> int:
    """
    Bulk insert articles. Skips duplicates via ON CONFLICT DO NOTHING.
    Returns the number of newly inserted rows.
    """
    if not rows:
        return 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            values = [
                (
                    r["url"],
                    r.get("title"),
                    r.get("description"),
                    r.get("outlet"),
                    r.get("domain"),
                    r.get("publish_date"),
                )
                for r in rows
            ]
            sql = """
                INSERT INTO articles (url, title, description, outlet, domain, publish_date)
                VALUES %s
                ON CONFLICT (url) DO NOTHING
            """
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        return inserted
    finally:
        conn.close()


def query_articles(
    since: str | None = None,
    until: str | None = None,
    outlet: str | None = None,
) -> list[dict]:
    """
    Query articles with optional filters.
    since/until: 'YYYY-MM-DD' date strings for publish_date range.
    outlet: filter by outlet name.
    """
    conn = get_connection()
    try:
        conditions = []
        params = []

        if since:
            conditions.append("publish_date >= %s")
            params.append(since)
        if until:
            conditions.append("publish_date <= %s")
            params.append(until)
        if outlet:
            conditions.append("outlet = %s")
            params.append(outlet)

        sql = "SELECT url, title, description, outlet, domain, publish_date, collected_at FROM articles"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY publish_date DESC"

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()

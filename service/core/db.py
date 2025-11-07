# app/core/db.py
from contextlib import contextmanager
import psycopg2, psycopg2.extras
from typing import Iterator, Optional
from app.core.settings import settings

def get_conn(dsn: Optional[str] = None):
    return psycopg2.connect(dsn or settings.PG_DSN)

@contextmanager
def pg_conn(dsn: Optional[str] = None) -> Iterator[psycopg2.extensions.connection]:
    conn = get_conn(dsn)
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def pg_cursor(conn: Optional[psycopg2.extensions.connection] = None):
    own = False
    if conn is None:
        conn = get_conn()
        own = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        if own:
            conn.close()

def healthcheck() -> bool:
    try:
        with pg_conn() as c, c.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False

def acquire_advisory_lock(cur, key: int) -> bool:
    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
    return bool(cur.fetchone()[0])

def release_advisory_lock(cur, key: int) -> None:
    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))

def claim_rows_for_update_skip_locked(cur, sql: str, limit: int, params: tuple = ()) -> list:
    """
    Helper to claim work rows safely in concurrent workers.
    Example SQL:
      WITH c AS (
        SELECT person_id
        FROM bio_text
        WHERE text IS NULL AND qid IS NOT NULL
        FOR UPDATE SKIP LOCKED
        LIMIT %(limit)s
      )
      SELECT person_id FROM c;
    """
    cur.execute(sql, {**{ "limit": limit }, **{}})
    return cur.fetchall()

# Optional: SQLAlchemy (if you want ORM / Core)
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    _engine = create_engine(settings.PG_DSN, pool_pre_ping=True, future=True)
    SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)

    def get_engine():
        return _engine
except Exception:
    # SQLAlchemy not installed; ignore
    SessionLocal = None
    def get_engine(): return None

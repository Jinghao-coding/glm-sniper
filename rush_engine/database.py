import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "rush.db"

BEIJING_TZ = timezone(timedelta(hours=8))


def _now_iso() -> str:
    return datetime.now(BEIJING_TZ).isoformat()


def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            authorization TEXT NOT NULL,
            product_id TEXT NOT NULL DEFAULT 'product-1df3e1',
            invitation_code TEXT NOT NULL DEFAULT '',
            cookie_string TEXT NOT NULL DEFAULT '',
            turbo_concurrency INTEGER NOT NULL DEFAULT 10,
            normal_concurrency INTEGER NOT NULL DEFAULT 5,
            turbo_duration REAL NOT NULL DEFAULT 5.0,
            max_retry INTEGER NOT NULL DEFAULT 2000,
            rush_time TEXT NOT NULL DEFAULT '10:00:00',
            preheat_before INTEGER NOT NULL DEFAULT 3,
            request_timeout INTEGER NOT NULL DEFAULT 10,
            connection_pool_size INTEGER NOT NULL DEFAULT 50,
            warmup_count INTEGER NOT NULL DEFAULT 5,
            play_sound INTEGER NOT NULL DEFAULT 1,
            desktop_notify INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rush_sessions (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            started_at TEXT,
            finished_at TEXT,
            total_attempts INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            elapsed_ms REAL NOT NULL DEFAULT 0,
            result_biz_id TEXT,
            result_data TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );

        CREATE TABLE IF NOT EXISTS rush_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            attempt_num INTEGER NOT NULL,
            ok INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            biz_id TEXT,
            response_text TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES rush_sessions(id)
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_account ON rush_sessions(account_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON rush_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_sessions_created ON rush_sessions(created_at);
        CREATE INDEX IF NOT EXISTS idx_attempts_session ON rush_attempts(session_id);
    """)
    conn.commit()
    conn.close()


def create_account(data: dict) -> dict:
    conn = get_db()
    aid = data.get("id") or str(uuid.uuid4())[:8]
    now = _now_iso()
    conn.execute(
        """INSERT INTO accounts
           (id, name, authorization, product_id, invitation_code, cookie_string,
            turbo_concurrency, normal_concurrency, turbo_duration, max_retry,
            rush_time, preheat_before, request_timeout, connection_pool_size,
            warmup_count, play_sound, desktop_notify, is_active, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            aid, data.get("name", ""), data["authorization"],
            data.get("product_id", "product-1df3e1"),
            data.get("invitation_code", ""), data.get("cookie_string", ""),
            data.get("turbo_concurrency", 10), data.get("normal_concurrency", 5),
            data.get("turbo_duration", 5.0), data.get("max_retry", 2000),
            data.get("rush_time", "10:00:00"), data.get("preheat_before", 3),
            data.get("request_timeout", 10), data.get("connection_pool_size", 50),
            data.get("warmup_count", 5), int(data.get("play_sound", True)),
            int(data.get("desktop_notify", True)), int(data.get("is_active", True)),
            now, now,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM accounts WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row)


def list_accounts(active_only: bool = False) -> list[dict]:
    conn = get_db()
    if active_only:
        rows = conn.execute("SELECT * FROM accounts WHERE is_active=1 ORDER BY created_at DESC").fetchall()
    else:
        rows = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_account(account_id: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_account(account_id: str, data: dict) -> dict | None:
    conn = get_db()
    existing = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    if not existing:
        conn.close()
        return None

    fields = []
    values = []
    for key in [
        "name", "authorization", "product_id", "invitation_code", "cookie_string",
        "turbo_concurrency", "normal_concurrency", "turbo_duration", "max_retry",
        "rush_time", "preheat_before", "request_timeout", "connection_pool_size",
        "warmup_count", "play_sound", "desktop_notify", "is_active",
    ]:
        if key in data:
            fields.append(f"{key}=?")
            val = data[key]
            if key in ("play_sound", "desktop_notify", "is_active"):
                val = int(val)
            values.append(val)

    if not fields:
        conn.close()
        return dict(existing)

    fields.append("updated_at=?")
    values.append(_now_iso())
    values.append(account_id)

    conn.execute(f"UPDATE accounts SET {','.join(fields)} WHERE id=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_account(account_id: str) -> bool:
    conn = get_db()
    cursor = conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))
    conn.commit()
    deleted = cursor.rowcount > 0
    conn.close()
    return deleted


def create_session(account_id: str) -> dict:
    conn = get_db()
    sid = str(uuid.uuid4())[:12]
    now = _now_iso()
    conn.execute(
        """INSERT INTO rush_sessions
           (id, account_id, status, started_at, created_at)
           VALUES (?,?,?,?,?)""",
        (sid, account_id, "pending", now, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM rush_sessions WHERE id=?", (sid,)).fetchone()
    conn.close()
    return dict(row)


def update_session(session_id: str, data: dict) -> dict | None:
    conn = get_db()
    existing = conn.execute("SELECT * FROM rush_sessions WHERE id=?", (session_id,)).fetchone()
    if not existing:
        conn.close()
        return None

    fields = []
    values = []
    for key in [
        "status", "finished_at", "total_attempts", "success_count",
        "error_count", "elapsed_ms", "result_biz_id", "result_data",
    ]:
        if key in data:
            fields.append(f"{key}=?")
            values.append(data[key])

    if not fields:
        conn.close()
        return dict(existing)

    values.append(session_id)
    conn.execute(f"UPDATE rush_sessions SET {','.join(fields)} WHERE id=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM rush_sessions WHERE id=?", (session_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_sessions(account_id: str | None = None, limit: int = 50) -> list[dict]:
    conn = get_db()
    if account_id:
        rows = conn.execute(
            "SELECT * FROM rush_sessions WHERE account_id=? ORDER BY created_at DESC LIMIT ?",
            (account_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM rush_sessions ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session(session_id: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM rush_sessions WHERE id=?", (session_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_attempt(session_id: str, attempt_num: int, ok: bool, reason: str | None = None,
                   biz_id: str | None = None, response_text: str | None = None) -> dict:
    conn = get_db()
    now = _now_iso()
    cursor = conn.execute(
        """INSERT INTO rush_attempts
           (session_id, attempt_num, ok, reason, biz_id, response_text, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (session_id, attempt_num, int(ok), reason, biz_id, response_text, now),
    )
    conn.commit()
    aid = cursor.lastrowid
    row = conn.execute("SELECT * FROM rush_attempts WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row)


def list_attempts(session_id: str, limit: int = 200) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM rush_attempts WHERE session_id=? ORDER BY attempt_num DESC LIMIT ?",
        (session_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


init_db()

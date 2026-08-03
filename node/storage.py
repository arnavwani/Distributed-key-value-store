
import sqlite3
import threading
import time


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at REAL,
                deleted INTEGER DEFAULT 0
            )
        """)
        self._conn.commit()

    def put(self, key: str, value: str, updated_at: float = None):
        updated_at = updated_at if updated_at is not None else time.time()
        with self._lock:
            self._conn.execute(
                """INSERT INTO kv (key, value, updated_at, deleted)
                   VALUES (?, ?, ?, 0)
                   ON CONFLICT(key) DO UPDATE SET
                     value=excluded.value,
                     updated_at=excluded.updated_at,
                     deleted=0
                   WHERE excluded.updated_at >= kv.updated_at""",
                (key, value, updated_at),
            )
            self._conn.commit()
        return updated_at

    def delete(self, key: str, updated_at: float = None):
        updated_at = updated_at if updated_at is not None else time.time()
        with self._lock:
            self._conn.execute(
                """INSERT INTO kv (key, value, updated_at, deleted)
                   VALUES (?, NULL, ?, 1)
                   ON CONFLICT(key) DO UPDATE SET
                     value=NULL,
                     updated_at=excluded.updated_at,
                     deleted=1
                   WHERE excluded.updated_at >= kv.updated_at""",
                (key, updated_at),
            )
            self._conn.commit()
        return updated_at

    def get(self, key: str):
        with self._lock:
            cur = self._conn.execute(
                "SELECT value, updated_at, deleted FROM kv WHERE key=?", (key,)
            )
            row = cur.fetchone()
        if row is None or row[2] == 1:
            return None
        return row[0]

    def get_all_live(self) -> dict:
        with self._lock:
            cur = self._conn.execute(
                "SELECT key, value FROM kv WHERE deleted=0"
            )
            return {k: v for k, v in cur.fetchall()}

    def get_all_raw(self) -> dict:
        with self._lock:
            cur = self._conn.execute(
                "SELECT key, value, updated_at, deleted FROM kv"
            )
            return {r[0]: {"value": r[1], "updated_at": r[2], "deleted": r[3]} for r in cur.fetchall()}

    def apply_snapshot(self, snapshot: dict):
        for key, row in snapshot.items():
            if row["deleted"]:
                self.delete(key, updated_at=row["updated_at"])
            else:
                self.put(key, row["value"], updated_at=row["updated_at"])

    def count(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM kv WHERE deleted=0")
            return cur.fetchone()[0]

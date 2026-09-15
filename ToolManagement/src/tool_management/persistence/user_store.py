"""
User account store — durable signup/login identities with per-user API keys.

Backed by DuckDB in DATA_DIR (the Modal /data volume in production) so
accounts survive restarts. Passwords are PBKDF2-HMAC-SHA256; API keys are
opaque bearer tokens accepted by the auth gate alongside operator tokens.
"""

import hashlib
import logging
import os
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb

logger = logging.getLogger(__name__)

PBKDF2_ITERATIONS = 100_000
API_KEY_PREFIX = "sk-user-"


class UserStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "users.duckdb")
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id VARCHAR PRIMARY KEY,
                    email VARCHAR NOT NULL UNIQUE,
                    display_name VARCHAR,
                    password_hash VARCHAR NOT NULL,
                    salt VARCHAR NOT NULL,
                    api_key VARCHAR NOT NULL UNIQUE,
                    api_key_hash VARCHAR,
                    created_at TIMESTAMP,
                    last_login_at TIMESTAMP
                )
                """
            )
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS api_key_hash VARCHAR")
            rows = con.execute(
                "SELECT user_id, api_key, api_key_hash FROM users"
            ).fetchall()
            for user_id, stored_key, stored_hash in rows:
                if stored_hash:
                    continue
                digest = (
                    self._hash_api_key(stored_key)
                    if str(stored_key or "").startswith(API_KEY_PREFIX)
                    else str(stored_key or "")
                )
                con.execute(
                    "UPDATE users SET api_key = ?, api_key_hash = ? WHERE user_id = ?",
                    [digest, digest, user_id],
                )
            con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_api_key_hash "
                "ON users(api_key_hash)"
            )

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        return hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS
        ).hex()

    @staticmethod
    def _hash_api_key(api_key: str) -> str:
        return hashlib.sha256(api_key.encode("utf-8")).hexdigest()

    @staticmethod
    def _row_to_user(row: tuple, columns: list) -> Dict[str, Any]:
        user = dict(zip(columns, row))
        user.pop("password_hash", None)
        user.pop("salt", None)
        user.pop("api_key", None)
        user.pop("api_key_hash", None)
        return user

    _COLUMNS = [
        "user_id", "email", "display_name", "password_hash",
        "salt", "api_key", "api_key_hash", "created_at", "last_login_at",
    ]

    def create_user(
        self, email: str, password: str, display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        email = email.strip().lower()
        if not email or "@" not in email:
            raise ValueError("A valid email is required")
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters")

        user_id = "u_" + secrets.token_hex(12)
        salt = secrets.token_hex(16)
        api_key = API_KEY_PREFIX + secrets.token_urlsafe(24)
        api_key_hash = self._hash_api_key(api_key)
        now = datetime.now(timezone.utc)

        with self._lock, self._connect() as con:
            existing = con.execute(
                "SELECT user_id FROM users WHERE email = ?", [email]
            ).fetchone()
            if existing:
                raise ValueError(f"Account already exists for {email}")
            con.execute(
                """
                INSERT INTO users (
                    user_id, email, display_name, password_hash, salt,
                    api_key, api_key_hash, created_at, last_login_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    user_id, email, display_name,
                    self._hash_password(password, salt), salt,
                    api_key_hash, api_key_hash, now, now,
                ],
            )
        logger.info(f"Created user {user_id} ({email})")
        return {
            "user_id": user_id,
            "email": email,
            "display_name": display_name,
            "api_key": api_key,
            "created_at": now.isoformat(),
        }

    def authenticate(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        email = email.strip().lower()
        with self._lock, self._connect() as con:
            row = con.execute(
                f"SELECT {', '.join(self._COLUMNS)} FROM users WHERE email = ?",
                [email],
            ).fetchone()
            if not row:
                return None
            record = dict(zip(self._COLUMNS, row))
            candidate = self._hash_password(password, record["salt"])
            if not secrets.compare_digest(candidate, record["password_hash"]):
                return None
            con.execute(
                "UPDATE users SET last_login_at = ? WHERE user_id = ?",
                [datetime.now(timezone.utc), record["user_id"]],
            )
            return self._row_to_user(row, self._COLUMNS)

    def get_by_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        if not api_key or not api_key.startswith(API_KEY_PREFIX):
            return None
        api_key_hash = self._hash_api_key(api_key)
        with self._lock, self._connect() as con:
            row = con.execute(
                f"""
                SELECT {', '.join(self._COLUMNS)} FROM users
                WHERE api_key_hash = ? OR api_key = ?
                """,
                [api_key_hash, api_key_hash],
            ).fetchone()
        return self._row_to_user(row, self._COLUMNS) if row else None

    def get_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            row = con.execute(
                f"SELECT {', '.join(self._COLUMNS)} FROM users WHERE user_id = ?",
                [user_id],
            ).fetchone()
        return self._row_to_user(row, self._COLUMNS) if row else None

    def rotate_api_key(self, user_id: str) -> Optional[str]:
        new_key = API_KEY_PREFIX + secrets.token_urlsafe(24)
        new_key_hash = self._hash_api_key(new_key)
        with self._lock, self._connect() as con:
            changed = con.execute(
                "UPDATE users SET api_key = ?, api_key_hash = ? WHERE user_id = ?",
                [new_key_hash, new_key_hash, user_id],
            ).fetchone()
            _ = changed
            row = con.execute(
                "SELECT api_key_hash FROM users WHERE user_id = ?", [user_id]
            ).fetchone()
        return new_key if row and row[0] == new_key_hash else None

    def count(self) -> int:
        with self._lock, self._connect() as con:
            return con.execute("SELECT count(*) FROM users").fetchone()[0]

    def list_users(self, limit: int = 100, offset: int = 0) -> list:
        with self._lock, self._connect() as con:
            rows = con.execute(
                f"SELECT {', '.join(self._COLUMNS)} FROM users "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()
        return [self._row_to_user(r, self._COLUMNS) for r in rows]

    def change_password(
        self, user_id: str, current_password: str, new_password: str
    ) -> bool:
        if len(new_password) < 8:
            raise ValueError("New password must be at least 8 characters")
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT password_hash, salt FROM users WHERE user_id = ?", [user_id]
            ).fetchone()
            if not row:
                return False
            if not secrets.compare_digest(
                self._hash_password(current_password, row[1]), row[0]
            ):
                return False
            new_salt = secrets.token_hex(16)
            con.execute(
                "UPDATE users SET password_hash = ?, salt = ? WHERE user_id = ?",
                [self._hash_password(new_password, new_salt), new_salt, user_id],
            )
        return True


_user_store: Optional[UserStore] = None
_store_lock = threading.Lock()


def get_user_store() -> UserStore:
    global _user_store
    with _store_lock:
        if _user_store is None:
            _user_store = UserStore()
        return _user_store

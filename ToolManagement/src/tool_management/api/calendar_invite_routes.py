"""Tenant-safe, bidirectional iCalendar/iTIP invite planning.

The routes in this module own durable calendar *drafts* and RSVP state.  They
never call an email or calendar provider directly.  Dispatch produces an
explicit, approval-gated tool plan for Tool Management's governed execution
surface, so retries, policy, credentials, and side effects remain centralized.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..oauth import TOKEN_SECRET
from .tools_api import resolve_execution_identity


router = APIRouter(prefix="/api/v1/calendar", tags=["calendar-invites"])

_EMAIL_RE = re.compile(r"^[^\s@<>;,]+@[^\s@<>;,]+\.[^\s@<>;,]+$")
_SAFE_UID_RE = re.compile(r"^[^\x00-\x20\x7f]{8,255}$")
_MAX_ICS_BYTES = 256 * 1024
_OPERATOR_OWNER = "__operator__"
_PRODID = "-//Distributed Systems//Life Butler Calendar//EN"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if not _EMAIL_RE.fullmatch(email):
        raise ValueError("must be a valid email address")
    return email


def _reject_control_characters(value: Optional[str]) -> Optional[str]:
    if value is not None and any(ord(char) < 32 and char not in "\t" for char in value):
        raise ValueError("must not contain control characters")
    return value


class ParticipationStatus(str, Enum):
    NEEDS_ACTION = "NEEDS-ACTION"
    ACCEPTED = "ACCEPTED"
    DECLINED = "DECLINED"
    TENTATIVE = "TENTATIVE"
    DELEGATED = "DELEGATED"


class ParticipationRole(str, Enum):
    REQUIRED = "REQ-PARTICIPANT"
    OPTIONAL = "OPT-PARTICIPANT"
    NON_PARTICIPANT = "NON-PARTICIPANT"


class Organizer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str
    name: Optional[str] = Field(default=None, max_length=200)

    _email = field_validator("email")(_validate_email)
    _name = field_validator("name")(_reject_control_characters)


class Attendee(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str
    name: Optional[str] = Field(default=None, max_length=200)
    role: ParticipationRole = ParticipationRole.REQUIRED
    rsvp_required: bool = True
    participation_status: ParticipationStatus = ParticipationStatus.NEEDS_ACTION

    _email = field_validator("email")(_validate_email)
    _name = field_validator("name")(_reject_control_characters)


class InviteCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1, max_length=300)
    description: str = Field(default="", max_length=20_000)
    location: str = Field(default="", max_length=1_000)
    starts_at: datetime
    ends_at: datetime
    timezone: str = Field(default="UTC", min_length=1, max_length=100)
    organizer: Organizer
    attendees: List[Attendee] = Field(min_length=1, max_length=200)
    uid: Optional[str] = Field(default=None, min_length=8, max_length=255)

    @field_validator("summary", "description", "location")
    @classmethod
    def _safe_text(cls, value: str) -> str:
        return _reject_control_characters(value.strip()) or ""

    @field_validator("timezone")
    @classmethod
    def _known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("must be a valid IANA timezone") from exc
        return value

    @field_validator("uid")
    @classmethod
    def _safe_uid(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not _SAFE_UID_RE.fullmatch(value):
            raise ValueError("contains unsafe characters")
        return value

    @model_validator(mode="after")
    def _valid_event(self) -> "InviteCreateRequest":
        if self.starts_at.tzinfo is None or self.ends_at.tzinfo is None:
            raise ValueError("starts_at and ends_at must include UTC offsets")
        if self.ends_at <= self.starts_at:
            raise ValueError("ends_at must be after starts_at")
        emails = [item.email for item in self.attendees]
        if len(emails) != len(set(emails)):
            raise ValueError("attendee emails must be unique")
        return self


class InviteUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_sequence: int = Field(ge=0)
    summary: Optional[str] = Field(default=None, min_length=1, max_length=300)
    description: Optional[str] = Field(default=None, max_length=20_000)
    location: Optional[str] = Field(default=None, max_length=1_000)
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    timezone: Optional[str] = Field(default=None, min_length=1, max_length=100)
    organizer: Optional[Organizer] = None
    attendees: Optional[List[Attendee]] = Field(default=None, min_length=1, max_length=200)

    @field_validator("summary", "description", "location")
    @classmethod
    def _safe_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return _reject_control_characters(value.strip())

    @field_validator("starts_at", "ends_at")
    @classmethod
    def _aware_optional_datetime(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is not None and value.tzinfo is None:
            raise ValueError("must include a UTC offset")
        return value

    @field_validator("timezone")
    @classmethod
    def _known_optional_timezone(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            try:
                ZoneInfo(value)
            except ZoneInfoNotFoundError as exc:
                raise ValueError("must be a valid IANA timezone") from exc
        return value

    @field_validator("attendees")
    @classmethod
    def _unique_optional_attendees(cls, value: Optional[List[Attendee]]) -> Optional[List[Attendee]]:
        if value is not None:
            emails = [item.email for item in value]
            if len(emails) != len(set(emails)):
                raise ValueError("attendee emails must be unique")
        return value

    @model_validator(mode="after")
    def _has_change(self) -> "InviteUpdateRequest":
        supplied = self.model_fields_set - {"expected_sequence"}
        if not supplied:
            raise ValueError("at least one invite field must be supplied")
        non_nullable = {"summary", "starts_at", "ends_at", "timezone", "organizer", "attendees"}
        if any(getattr(self, field) is None for field in supplied & non_nullable):
            raise ValueError("summary, times, timezone, organizer, and attendees cannot be null")
        return self


class SequenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_sequence: int = Field(ge=0)


class DispatchChannel(str, Enum):
    EMAIL = "email"
    GOOGLE_CALENDAR = "google_calendar"
    OUTLOOK_CALENDAR = "outlook_calendar"


class DispatchPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_sequence: int = Field(ge=0)
    channel: DispatchChannel
    approved: bool = False
    approval_reference: Optional[str] = Field(default=None, min_length=3, max_length=200)
    provider_event_id: Optional[str] = Field(default=None, min_length=1, max_length=500)
    download_ttl_seconds: int = Field(default=900, ge=60, le=3600)

    @model_validator(mode="after")
    def _complete_approval(self) -> "DispatchPlanRequest":
        if self.approved and not self.approval_reference:
            raise ValueError("approval_reference is required when approved is true")
        return self


def _escape_ical_text(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\r\n", "\\n")
        .replace("\r", "\\n")
        .replace("\n", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


def _escape_ical_param(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "\\\"").replace("\r", "").replace("\n", "")


def _fold_ical_line(line: str, limit: int = 75) -> List[str]:
    """Fold a content line to RFC 5545's 75-octet recommendation."""
    chunks: List[str] = []
    remaining = line
    first = True
    while remaining:
        prefix = "" if first else " "
        budget = limit - len(prefix.encode("utf-8"))
        size = 0
        index = 0
        for index, char in enumerate(remaining, start=1):
            char_size = len(char.encode("utf-8"))
            if size + char_size > budget:
                index -= 1
                break
            size += char_size
        else:
            index = len(remaining)
        if index <= 0:
            index = 1
        chunks.append(prefix + remaining[:index])
        remaining = remaining[index:]
        first = False
    return chunks or [""]


def _format_ical_datetime(value: str) -> str:
    return _parse_iso(value).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_ical(record: Dict[str, Any]) -> str:
    """Render one durable record as an RFC 5545 iTIP VCALENDAR."""
    method = "CANCEL" if record["status"] == "cancelled" else "REQUEST"
    organizer = record["organizer"]
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{_PRODID}",
        "CALSCALE:GREGORIAN",
        f"METHOD:{method}",
        f"X-WR-TIMEZONE:{record['timezone']}",
        "BEGIN:VEVENT",
        f"UID:{record['uid']}",
        f"SEQUENCE:{record['sequence']}",
        f"DTSTAMP:{_format_ical_datetime(record['updated_at'])}",
        f"DTSTART:{_format_ical_datetime(record['starts_at'])}",
        f"DTEND:{_format_ical_datetime(record['ends_at'])}",
        f"SUMMARY:{_escape_ical_text(record['summary'])}",
    ]
    if record.get("description"):
        lines.append(f"DESCRIPTION:{_escape_ical_text(record['description'])}")
    if record.get("location"):
        lines.append(f"LOCATION:{_escape_ical_text(record['location'])}")
    if organizer.get("name"):
        lines.append(
            f'ORGANIZER;CN="{_escape_ical_param(organizer["name"])}":mailto:{organizer["email"]}'
        )
    else:
        lines.append(f"ORGANIZER:mailto:{organizer['email']}")
    for attendee in record["attendees"]:
        parameters = []
        if attendee.get("name"):
            parameters.append(f'CN="{_escape_ical_param(attendee["name"])}"')
        parameters.extend(
            [
                f"ROLE={attendee['role']}",
                f"PARTSTAT={attendee['participation_status']}",
                f"RSVP={'TRUE' if attendee['rsvp_required'] else 'FALSE'}",
            ]
        )
        lines.append(f"ATTENDEE;{';'.join(parameters)}:mailto:{attendee['email']}")
    lines.extend(
        [
            f"STATUS:{'CANCELLED' if method == 'CANCEL' else 'CONFIRMED'}",
            "TRANSP:OPAQUE",
            "END:VEVENT",
            "END:VCALENDAR",
        ]
    )
    folded = [folded_line for line in lines for folded_line in _fold_ical_line(line)]
    return "\r\n".join(folded) + "\r\n"


def _unfold_ical(text: str) -> List[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    lines: List[str] = []
    for line in normalized.split("\n"):
        if line.startswith((" ", "\t")) and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return lines


def _split_property(line: str) -> tuple[str, Dict[str, str], str]:
    if ":" not in line:
        raise ValueError("calendar property is missing a value delimiter")
    left, value = line.split(":", 1)
    parts = left.split(";")
    name = parts[0].upper()
    params: Dict[str, str] = {}
    for part in parts[1:]:
        if "=" in part:
            key, param_value = part.split("=", 1)
            params[key.upper()] = param_value.strip('"')
    return name, params, value.strip()


def parse_ical_reply(text: str) -> Dict[str, Any]:
    """Parse the minimal, security-sensitive subset required for iTIP RSVP."""
    method: Optional[str] = None
    in_event = False
    event_count = 0
    uid: Optional[str] = None
    sequence: Optional[int] = None
    uid_count = 0
    sequence_count = 0
    attendees: List[Dict[str, str]] = []
    for line in _unfold_ical(text):
        upper = line.upper()
        if upper == "BEGIN:VEVENT":
            event_count += 1
            in_event = True
            continue
        if upper == "END:VEVENT":
            in_event = False
            continue
        try:
            name, params, value = _split_property(line)
        except ValueError:
            continue
        if not in_event and name == "METHOD":
            method = value.upper()
        elif in_event and name == "UID":
            uid_count += 1
            uid = value
        elif in_event and name == "SEQUENCE":
            sequence_count += 1
            try:
                sequence = int(value)
            except ValueError as exc:
                raise ValueError("SEQUENCE must be an integer") from exc
        elif in_event and name == "ATTENDEE":
            email = value[7:] if value.lower().startswith("mailto:") else value
            attendees.append(
                {
                    "email": _validate_email(email),
                    "participation_status": params.get("PARTSTAT", "").upper(),
                }
            )
    if method != "REPLY":
        raise ValueError("METHOD must be REPLY")
    if event_count != 1:
        raise ValueError("exactly one VEVENT is required")
    if uid_count != 1 or not uid or not _SAFE_UID_RE.fullmatch(uid):
        raise ValueError("a valid UID is required")
    if sequence_count != 1 or sequence is None or sequence < 0:
        raise ValueError("a non-negative SEQUENCE is required")
    if len(attendees) != 1:
        raise ValueError("exactly one replying ATTENDEE is required")
    status = attendees[0]["participation_status"]
    if status not in {member.value for member in ParticipationStatus if member != ParticipationStatus.NEEDS_ACTION}:
        raise ValueError("ATTENDEE PARTSTAT is not a supported reply status")
    return {"uid": uid, "sequence": sequence, **attendees[0]}


class CalendarInviteStore:
    """Small durable SQLite store with per-owner idempotency and revisions."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS calendar_invites (
                    invite_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    uid TEXT NOT NULL UNIQUE,
                    sequence INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_calendar_invites_owner
                    ON calendar_invites(owner_id, updated_at DESC);
                CREATE TABLE IF NOT EXISTS calendar_invite_operations (
                    owner_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    invite_id TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(owner_id, idempotency_key),
                    FOREIGN KEY(invite_id) REFERENCES calendar_invites(invite_id)
                );
                CREATE TABLE IF NOT EXISTS calendar_invite_replies (
                    invite_id TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    attendee_email TEXT NOT NULL,
                    participation_status TEXT NOT NULL,
                    received_at REAL NOT NULL,
                    PRIMARY KEY(invite_id, payload_hash),
                    FOREIGN KEY(invite_id) REFERENCES calendar_invites(invite_id)
                );
                """
            )

    @staticmethod
    def _decode(row: sqlite3.Row) -> Dict[str, Any]:
        return json.loads(row["record_json"])

    @staticmethod
    def _authorize(record: Dict[str, Any], identity: Dict[str, Any]) -> None:
        if identity.get("kind") == "customer" and record["owner_id"] != identity.get("tenant_id"):
            raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})

    @staticmethod
    def _operation_replay(
        conn: sqlite3.Connection,
        owner_id: str,
        idempotency_key: str,
        operation: str,
        request_hash: str,
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM calendar_invite_operations WHERE owner_id = ? AND idempotency_key = ?",
            (owner_id, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["operation"] != operation or row["request_hash"] != request_hash:
            raise HTTPException(status_code=409, detail={"error": "idempotency_key_reused"})
        return json.loads(row["snapshot_json"])

    @staticmethod
    def _record_operation(
        conn: sqlite3.Connection,
        owner_id: str,
        idempotency_key: str,
        operation: str,
        request_hash: str,
        record: Dict[str, Any],
    ) -> None:
        conn.execute(
            """INSERT INTO calendar_invite_operations
               (owner_id, idempotency_key, operation, request_hash, invite_id, snapshot_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                owner_id,
                idempotency_key,
                operation,
                request_hash,
                record["invite_id"],
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                time.time(),
            ),
        )

    def create(
        self,
        owner_id: str,
        idempotency_key: str,
        request_hash: str,
        record: Dict[str, Any],
    ) -> tuple[Dict[str, Any], bool]:
        with self._lock, self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            replay = self._operation_replay(conn, owner_id, idempotency_key, "create", request_hash)
            if replay is not None:
                return replay, True
            try:
                conn.execute(
                    """INSERT INTO calendar_invites
                       (invite_id, owner_id, uid, sequence, status, record_json, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record["invite_id"],
                        owner_id,
                        record["uid"],
                        record["sequence"],
                        record["status"],
                        json.dumps(record, sort_keys=True, separators=(",", ":")),
                        time.time(),
                        time.time(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise HTTPException(status_code=409, detail={"error": "calendar_uid_exists"}) from exc
            self._record_operation(conn, owner_id, idempotency_key, "create", request_hash, record)
            return record, False

    def get(self, invite_id: str, identity: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT record_json FROM calendar_invites WHERE invite_id = ?", (invite_id,)
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})
        record = self._decode(row)
        self._authorize(record, identity)
        return record

    def get_signed(self, invite_id: str) -> Dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT record_json FROM calendar_invites WHERE invite_id = ?",
                (invite_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})
        return self._decode(row)

    def get_by_uid(self, uid: str, identity: Dict[str, Any]) -> Dict[str, Any]:
        query = "SELECT record_json FROM calendar_invites WHERE uid = ?"
        params: tuple[Any, ...] = (uid,)
        if identity.get("kind") == "customer":
            query += " AND owner_id = ?"
            params += (identity.get("tenant_id"),)
        with self._lock, self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})
        return self._decode(row)

    def update(
        self,
        invite_id: str,
        identity: Dict[str, Any],
        expected_sequence: int,
        idempotency_key: str,
        operation: str,
        request_hash: str,
        record: Dict[str, Any],
    ) -> tuple[Dict[str, Any], bool]:
        owner_id = _owner_id(identity)
        with self._lock, self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            replay = self._operation_replay(conn, owner_id, idempotency_key, operation, request_hash)
            if replay is not None:
                return replay, True
            row = conn.execute(
                "SELECT record_json FROM calendar_invites WHERE invite_id = ?", (invite_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})
            current = self._decode(row)
            self._authorize(current, identity)
            if operation == "cancel" and current["status"] == "cancelled":
                raise HTTPException(status_code=409, detail={"error": "calendar_invite_already_cancelled"})
            if current["sequence"] != expected_sequence:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "stale_sequence", "current_sequence": current["sequence"]},
                )
            conn.execute(
                """UPDATE calendar_invites
                   SET sequence = ?, status = ?, record_json = ?, updated_at = ?
                   WHERE invite_id = ? AND sequence = ?""",
                (
                    record["sequence"],
                    record["status"],
                    json.dumps(record, sort_keys=True, separators=(",", ":")),
                    time.time(),
                    invite_id,
                    expected_sequence,
                ),
            )
            if conn.total_changes < 1:
                raise HTTPException(status_code=409, detail={"error": "stale_sequence"})
            self._record_operation(conn, owner_id, idempotency_key, operation, request_hash, record)
            return record, False

    def apply_reply(
        self,
        identity: Dict[str, Any],
        parsed: Dict[str, Any],
        payload_hash: str,
    ) -> tuple[Dict[str, Any], bool]:
        with self._lock, self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            query = "SELECT record_json FROM calendar_invites WHERE uid = ?"
            params: tuple[Any, ...] = (parsed["uid"],)
            if identity.get("kind") == "customer":
                query += " AND owner_id = ?"
                params += (identity.get("tenant_id"),)
            row = conn.execute(query, params).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail={"error": "calendar_invite_not_found"})
            record = self._decode(row)
            duplicate = conn.execute(
                "SELECT 1 FROM calendar_invite_replies WHERE invite_id = ? AND payload_hash = ?",
                (record["invite_id"], payload_hash),
            ).fetchone()
            if duplicate:
                return record, True
            if parsed["sequence"] < record["sequence"]:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "stale_reply_sequence", "current_sequence": record["sequence"]},
                )
            if parsed["sequence"] > record["sequence"]:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "future_reply_sequence", "current_sequence": record["sequence"]},
                )
            if record["status"] == "cancelled":
                raise HTTPException(status_code=409, detail={"error": "calendar_invite_cancelled"})
            matched = False
            for attendee in record["attendees"]:
                if hmac.compare_digest(attendee["email"].lower(), parsed["email"].lower()):
                    attendee["participation_status"] = parsed["participation_status"]
                    matched = True
                    break
            if not matched:
                raise HTTPException(status_code=422, detail={"error": "reply_attendee_not_invited"})
            record["updated_at"] = _iso_utc(_utc_now())
            conn.execute(
                "UPDATE calendar_invites SET record_json = ?, updated_at = ? WHERE invite_id = ?",
                (
                    json.dumps(record, sort_keys=True, separators=(",", ":")),
                    time.time(),
                    record["invite_id"],
                ),
            )
            conn.execute(
                """INSERT INTO calendar_invite_replies
                   (invite_id, payload_hash, sequence, attendee_email, participation_status, received_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    record["invite_id"],
                    payload_hash,
                    parsed["sequence"],
                    parsed["email"],
                    parsed["participation_status"],
                    time.time(),
                ),
            )
            return record, False


_store_lock = threading.Lock()
_store: Optional[CalendarInviteStore] = None
_store_path: Optional[Path] = None


def get_calendar_invite_store() -> CalendarInviteStore:
    global _store, _store_path
    configured = os.getenv("CALENDAR_INVITE_DB_PATH")
    path = Path(configured) if configured else Path(os.getenv("DATA_DIR", "data")) / "calendar_invites.db"
    with _store_lock:
        if _store is None or _store_path != path:
            _store = CalendarInviteStore(path)
            _store_path = path
        return _store


def _owner_id(identity: Dict[str, Any]) -> str:
    if identity.get("kind") == "customer":
        tenant_id = str(identity.get("tenant_id") or "").strip()
        if not tenant_id:
            raise HTTPException(status_code=403, detail={"error": "tenant_identity_required"})
        return tenant_id
    return _OPERATOR_OWNER


def _signing_secret() -> bytes:
    return (os.getenv("CALENDAR_INVITE_SIGNING_SECRET") or TOKEN_SECRET).encode("utf-8")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _download_token(record: Dict[str, Any], ttl_seconds: int) -> tuple[str, int]:
    expires_at = int(time.time()) + ttl_seconds
    payload = {
        "v": 1,
        "invite_id": record["invite_id"],
        "sequence": record["sequence"],
        "exp": expires_at,
    }
    encoded = _b64url(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    signature = _b64url(hmac.new(_signing_secret(), encoded.encode("ascii"), hashlib.sha256).digest())
    return f"{encoded}.{signature}", expires_at


def _verify_download_token(token: str, invite_id: str) -> Dict[str, Any]:
    try:
        encoded, signature = token.split(".", 1)
        expected = _b64url(hmac.new(_signing_secret(), encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            raise ValueError("signature")
        payload = json.loads(_b64url_decode(encoded))
        if payload.get("v") != 1 or payload.get("invite_id") != invite_id:
            raise ValueError("claims")
        if int(payload.get("exp", 0)) <= int(time.time()):
            raise HTTPException(status_code=410, detail={"error": "calendar_download_expired"})
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=403, detail={"error": "calendar_download_token_invalid"}) from exc


def _base_url(request: Request) -> str:
    configured = os.getenv("API_BASE_URL")
    return configured.rstrip("/") if configured else str(request.base_url).rstrip("/")


def _invite_response(
    request: Request,
    record: Dict[str, Any],
    *,
    replayed: bool = False,
    ttl_seconds: int = 900,
) -> Dict[str, Any]:
    token, expires_at = _download_token(record, ttl_seconds)
    method = "CANCEL" if record["status"] == "cancelled" else "REQUEST"
    return {
        "invite": {key: value for key, value in record.items() if key != "owner_id"},
        "calendar": {
            "method": method,
            "content_type": f"text/calendar; charset=utf-8; method={method}",
            "filename": f"invite-{record['invite_id']}.ics",
            "download_url": f"{_base_url(request)}/api/v1/calendar/invites/{record['invite_id']}/calendar.ics?token={token}",
            "expires_at": datetime.fromtimestamp(expires_at, timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "replayed": replayed,
    }


def _new_record(payload: InviteCreateRequest, owner_id: str) -> Dict[str, Any]:
    now = _iso_utc(_utc_now())
    invite_id = f"cal_{uuid4().hex}"
    uid = payload.uid or f"{uuid4().hex}@tools.distributed.systems"
    return {
        "invite_id": invite_id,
        "owner_id": owner_id,
        "uid": uid,
        "sequence": 0,
        "status": "active",
        "summary": payload.summary,
        "description": payload.description,
        "location": payload.location,
        "starts_at": _iso_utc(payload.starts_at),
        "ends_at": _iso_utc(payload.ends_at),
        "timezone": payload.timezone,
        "organizer": payload.organizer.model_dump(mode="json"),
        "attendees": [item.model_dump(mode="json") for item in payload.attendees],
        "created_at": now,
        "updated_at": now,
    }


@router.post("/invites", status_code=201)
async def create_invite(
    payload: InviteCreateRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=8, max_length=200),
) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    owner_id = _owner_id(identity)
    request_hash = _canonical_hash(payload.model_dump(mode="json"))
    record, replayed = get_calendar_invite_store().create(
        owner_id,
        idempotency_key,
        request_hash,
        _new_record(payload, owner_id),
    )
    response = _invite_response(request, record, replayed=replayed)
    if replayed:
        response["created"] = False
    else:
        response["created"] = True
    return response


@router.get("/invites/{invite_id}")
async def get_invite(invite_id: str, request: Request) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    record = get_calendar_invite_store().get(invite_id, identity)
    return _invite_response(request, record)


@router.patch("/invites/{invite_id}")
async def update_invite(
    invite_id: str,
    payload: InviteUpdateRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=8, max_length=200),
) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    current = get_calendar_invite_store().get(invite_id, identity)
    if current["status"] == "cancelled":
        raise HTTPException(status_code=409, detail={"error": "calendar_invite_cancelled"})
    updated = dict(current)
    supplied = payload.model_fields_set - {"expected_sequence"}
    for field in supplied:
        value = getattr(payload, field)
        if field in {"starts_at", "ends_at"}:
            updated[field] = _iso_utc(value)
        elif field == "organizer":
            updated[field] = value.model_dump(mode="json")
        elif field == "attendees":
            updated[field] = [item.model_dump(mode="json") for item in value]
        else:
            updated[field] = value
    if _parse_iso(updated["ends_at"]) <= _parse_iso(updated["starts_at"]):
        raise HTTPException(status_code=422, detail={"error": "ends_at_must_follow_starts_at"})
    updated["sequence"] = payload.expected_sequence + 1
    updated["updated_at"] = _iso_utc(_utc_now())
    request_hash = _canonical_hash(
        {"invite_id": invite_id, "payload": payload.model_dump(mode="json", exclude_unset=True)}
    )
    record, replayed = get_calendar_invite_store().update(
        invite_id,
        identity,
        payload.expected_sequence,
        idempotency_key,
        "update",
        request_hash,
        updated,
    )
    return _invite_response(request, record, replayed=replayed)


@router.post("/invites/{invite_id}/cancel")
async def cancel_invite(
    invite_id: str,
    payload: SequenceRequest,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=8, max_length=200),
) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    current = get_calendar_invite_store().get(invite_id, identity)
    cancelled = dict(current)
    cancelled["status"] = "cancelled"
    cancelled["sequence"] = payload.expected_sequence + 1
    cancelled["updated_at"] = _iso_utc(_utc_now())
    request_hash = _canonical_hash({"invite_id": invite_id, "payload": payload.model_dump(mode="json")})
    record, replayed = get_calendar_invite_store().update(
        invite_id,
        identity,
        payload.expected_sequence,
        idempotency_key,
        "cancel",
        request_hash,
        cancelled,
    )
    return _invite_response(request, record, replayed=replayed)


@router.get("/invites/{invite_id}/calendar.ics", name="download_calendar_invite")
async def download_invite(invite_id: str, token: str = Query(...)) -> Response:
    claims = _verify_download_token(token, invite_id)
    record = get_calendar_invite_store().get_signed(invite_id)
    if record["sequence"] != claims["sequence"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "calendar_download_revision_changed", "current_sequence": record["sequence"]},
        )
    method = "CANCEL" if record["status"] == "cancelled" else "REQUEST"
    return Response(
        content=build_ical(record),
        media_type=f"text/calendar; charset=utf-8; method={method}",
        headers={
            "Content-Disposition": f'attachment; filename="invite-{invite_id}.ics"',
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.post("/replies")
async def receive_reply(request: Request) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "text/calendar":
        raise HTTPException(status_code=415, detail={"error": "text_calendar_required"})
    raw = await request.body()
    if not raw or len(raw) > _MAX_ICS_BYTES:
        raise HTTPException(status_code=413, detail={"error": "calendar_reply_size_invalid"})
    try:
        text = raw.decode("utf-8-sig")
        parsed = parse_ical_reply(text)
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail={"error": "calendar_reply_invalid"}) from exc
    payload_hash = hashlib.sha256(raw).hexdigest()
    record, duplicate = get_calendar_invite_store().apply_reply(identity, parsed, payload_hash)
    attendee = next(item for item in record["attendees"] if item["email"] == parsed["email"])
    return {
        "invite_id": record["invite_id"],
        "uid": record["uid"],
        "sequence": record["sequence"],
        "reply": {
            "attendee": attendee["email"],
            "participation_status": attendee["participation_status"],
        },
        "applied": not duplicate,
        "duplicate": duplicate,
    }


def _calendar_provider_step(record: Dict[str, Any], channel: DispatchChannel, provider_event_id: Optional[str]) -> Dict[str, Any]:
    method = "CANCEL" if record["status"] == "cancelled" else "REQUEST"
    backend = channel.value
    if method == "CANCEL":
        if not provider_event_id:
            raise HTTPException(
                status_code=422,
                detail={"error": "provider_event_id_required_for_cancel"},
            )
        return {
            "tool": f"{backend}:delete_event",
            "arguments": {"event_id": provider_event_id, "send_updates": "all"},
        }
    arguments = {
        "summary": record["summary"],
        "description": record["description"],
        "location": record["location"],
        "start": {"dateTime": record["starts_at"], "timeZone": record["timezone"]},
        "end": {"dateTime": record["ends_at"], "timeZone": record["timezone"]},
        "attendees": [{"email": item["email"]} for item in record["attendees"]],
        "send_updates": "all",
        "i_cal_uid": record["uid"],
        "sequence": record["sequence"],
    }
    if provider_event_id:
        arguments["event_id"] = provider_event_id
        tool = "update_event"
    else:
        tool = "create_event"
    return {"tool": f"{backend}:{tool}", "arguments": arguments}


@router.post("/invites/{invite_id}/dispatch-plan")
async def plan_invite_dispatch(
    invite_id: str,
    payload: DispatchPlanRequest,
    request: Request,
) -> Dict[str, Any]:
    identity = resolve_execution_identity(request)
    record = get_calendar_invite_store().get(invite_id, identity)
    if record["sequence"] != payload.expected_sequence:
        raise HTTPException(
            status_code=409,
            detail={"error": "stale_sequence", "current_sequence": record["sequence"]},
        )
    if not payload.approved:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "dispatch_approval_required",
                "message": "No provider call was made; submit approved=true with an approval reference to receive a plan.",
            },
        )
    token, expires_at = _download_token(record, payload.download_ttl_seconds)
    source_url = f"{_base_url(request)}/api/v1/calendar/invites/{invite_id}/calendar.ics?token={token}"
    method = "CANCEL" if record["status"] == "cancelled" else "REQUEST"
    side_effect_key = f"calendar:{record['uid']}:{record['sequence']}:{method.lower()}"
    if payload.channel == DispatchChannel.EMAIL:
        prefix = "Canceled" if method == "CANCEL" else "Invitation"
        steps = [
            {
                "tool": "distributed-email-mcp:send_with_attachment",
                "arguments": {
                    "to": attendee["email"],
                    "subject": f"{prefix}: {record['summary']}",
                    "source": source_url,
                    "filename": f"invite-{invite_id}.ics",
                    "body": "Please open the attached calendar invitation to respond.",
                    "idempotency_key": f"{side_effect_key}:{hashlib.sha256(attendee['email'].encode()).hexdigest()[:16]}",
                },
            }
            for attendee in record["attendees"]
        ]
    else:
        steps = [_calendar_provider_step(record, payload.channel, payload.provider_event_id)]
    return {
        "mode": "plan_only",
        "executed": False,
        "invite_id": invite_id,
        "sequence": record["sequence"],
        "method": method,
        "approval": {
            "required": True,
            "received": True,
            "reference": payload.approval_reference,
        },
        "steps": steps,
        "execution_handoff": {
            "endpoint": "/api/v1/jobs/submit",
            "instruction": "Submit each step through governed Tool Management execution; this route never sends externally.",
        },
        "reply_handoff": {
            "endpoint": "/api/v1/calendar/replies",
            "content_type": "text/calendar",
            "method": "REPLY",
        },
        "artifact": {
            "url": source_url,
            "expires_at": datetime.fromtimestamp(expires_at, timezone.utc).isoformat().replace("+00:00", "Z"),
        },
    }

"""
DataFrame Storage for Research MCP - Structured data collection over time.

Provides:
- Schema-driven data collection from task groups and FindAll
- Progressive row insertion as tasks complete
- Column type inference and validation
- Query, filter, and aggregation operations
- Export to CSV, JSON, Markdown tables
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ColumnType(str, Enum):
    """Supported column types."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    JSON = "json"
    DATETIME = "datetime"
    URL = "url"
    LIST = "list"


@dataclass
class Column:
    """DataFrame column definition."""
    name: str
    type: ColumnType = ColumnType.STRING
    description: Optional[str] = None
    required: bool = False
    default: Any = None


@dataclass
class DataFrame:
    """A structured collection of rows with typed columns."""
    id: str
    name: str
    description: Optional[str]
    columns: List[Column]
    source_type: str  # "task_group", "findall", "manual"
    source_id: Optional[str]  # task_group_id or findall_id
    created_at: datetime
    updated_at: datetime
    row_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataFrameRow:
    """A single row in a DataFrame."""
    id: str
    dataframe_id: str
    task_id: Optional[str]  # Source task that produced this row
    data: Dict[str, Any]
    confidence: Optional[float] = None
    citations: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DataFrameStorage:
    """
    SQLite storage for DataFrames - structured data from parallel tasks.

    Supports:
    - Creating DataFrames with typed schemas
    - Progressive row insertion from task completions
    - Query/filter/aggregate operations
    - Export to various formats
    """

    def __init__(self, db_path: str = "data/research_dataframes.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_conn() as conn:
            conn.executescript("""
                -- DataFrames table
                CREATE TABLE IF NOT EXISTS dataframes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    columns_json TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT,
                    metadata_json TEXT,
                    row_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- DataFrame rows
                CREATE TABLE IF NOT EXISTS dataframe_rows (
                    id TEXT PRIMARY KEY,
                    dataframe_id TEXT NOT NULL REFERENCES dataframes(id),
                    task_id TEXT,
                    data_json TEXT NOT NULL,
                    confidence REAL,
                    citations_json TEXT,
                    created_at TEXT NOT NULL
                );

                -- Task group tracking
                CREATE TABLE IF NOT EXISTS task_groups (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    dataframe_id TEXT REFERENCES dataframes(id),
                    total_tasks INTEGER NOT NULL,
                    completed_tasks INTEGER DEFAULT 0,
                    failed_tasks INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    strategy TEXT DEFAULT 'balanced',
                    parallel_group_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- Individual tasks in a group
                CREATE TABLE IF NOT EXISTS task_group_items (
                    id TEXT PRIMARY KEY,
                    group_id TEXT NOT NULL REFERENCES task_groups(id),
                    task_id TEXT NOT NULL,
                    parallel_task_id TEXT,
                    input TEXT NOT NULL,
                    processor TEXT DEFAULT 'lite',
                    status TEXT DEFAULT 'pending',
                    result_json TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                );

                -- FindAll runs
                CREATE TABLE IF NOT EXISTS findall_runs (
                    id TEXT PRIMARY KEY,
                    objective TEXT NOT NULL,
                    schema_json TEXT,
                    dataframe_id TEXT REFERENCES dataframes(id),
                    parallel_run_id TEXT,
                    max_results INTEGER DEFAULT 50,
                    status TEXT DEFAULT 'pending',
                    entities_found INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_rows_dataframe ON dataframe_rows(dataframe_id);
                CREATE INDEX IF NOT EXISTS idx_rows_task ON dataframe_rows(task_id);
                CREATE INDEX IF NOT EXISTS idx_group_items_group ON task_group_items(group_id);
                CREATE INDEX IF NOT EXISTS idx_group_items_task ON task_group_items(task_id);
            """)
            conn.commit()
            logger.info(f"DataFrame storage initialized at {self.db_path}")

    # =========================================================================
    # DATAFRAME OPERATIONS
    # =========================================================================

    def create_dataframe(
        self,
        name: str,
        columns: List[Dict[str, Any]],
        source_type: str = "manual",
        source_id: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        """Create a new DataFrame with schema."""
        df_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        # Parse columns
        parsed_columns = [
            Column(
                name=c.get("name", f"col_{i}"),
                type=ColumnType(c.get("type", "string")),
                description=c.get("description"),
                required=c.get("required", False),
                default=c.get("default"),
            )
            for i, c in enumerate(columns)
        ]

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO dataframes
                (id, name, description, columns_json, source_type, source_id,
                 metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                df_id, name, description,
                json.dumps([{
                    "name": c.name, "type": c.type.value,
                    "description": c.description, "required": c.required,
                    "default": c.default
                } for c in parsed_columns]),
                source_type, source_id,
                json.dumps(metadata or {}),
                now, now
            ))
            conn.commit()

        return DataFrame(
            id=df_id,
            name=name,
            description=description,
            columns=parsed_columns,
            source_type=source_type,
            source_id=source_id,
            created_at=datetime.fromisoformat(now),
            updated_at=datetime.fromisoformat(now),
            metadata=metadata or {},
        )

    def add_row(
        self,
        dataframe_id: str,
        data: Dict[str, Any],
        task_id: Optional[str] = None,
        confidence: Optional[float] = None,
        citations: Optional[List[Dict[str, Any]]] = None,
    ) -> DataFrameRow:
        """Add a row to a DataFrame."""
        row_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO dataframe_rows
                (id, dataframe_id, task_id, data_json, confidence, citations_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row_id, dataframe_id, task_id,
                json.dumps(data, default=str),
                confidence,
                json.dumps(citations or []),
                now
            ))

            # Update row count
            conn.execute("""
                UPDATE dataframes SET row_count = row_count + 1, updated_at = ?
                WHERE id = ?
            """, (now, dataframe_id))
            conn.commit()

        return DataFrameRow(
            id=row_id,
            dataframe_id=dataframe_id,
            task_id=task_id,
            data=data,
            confidence=confidence,
            citations=citations or [],
            created_at=datetime.fromisoformat(now),
        )

    def add_rows_batch(
        self,
        dataframe_id: str,
        rows: List[Dict[str, Any]],
        task_id: Optional[str] = None,
    ) -> int:
        """Add multiple rows efficiently."""
        now = datetime.now(timezone.utc).isoformat()
        added = 0

        with self._get_conn() as conn:
            for row_data in rows:
                row_id = str(uuid.uuid4())
                data = row_data.get("data", row_data)
                confidence = row_data.get("confidence")
                citations = row_data.get("citations", [])

                conn.execute("""
                    INSERT INTO dataframe_rows
                    (id, dataframe_id, task_id, data_json, confidence, citations_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row_id, dataframe_id, task_id,
                    json.dumps(data, default=str),
                    confidence,
                    json.dumps(citations),
                    now
                ))
                added += 1

            # Update row count
            conn.execute("""
                UPDATE dataframes SET row_count = row_count + ?, updated_at = ?
                WHERE id = ?
            """, (added, now, dataframe_id))
            conn.commit()

        return added

    def get_dataframe(self, dataframe_id: str) -> Optional[DataFrame]:
        """Get DataFrame by ID."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM dataframes WHERE id = ?", (dataframe_id,)
            ).fetchone()

            if not row:
                return None

            columns_data = json.loads(row["columns_json"])
            return DataFrame(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                columns=[Column(
                    name=c["name"],
                    type=ColumnType(c["type"]),
                    description=c.get("description"),
                    required=c.get("required", False),
                    default=c.get("default"),
                ) for c in columns_data],
                source_type=row["source_type"],
                source_id=row["source_id"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                row_count=row["row_count"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )

    def query_rows(
        self,
        dataframe_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at DESC",
    ) -> Tuple[List[DataFrameRow], int]:
        """Query rows from a DataFrame with optional filtering."""
        with self._get_conn() as conn:
            # Base query
            query = "SELECT * FROM dataframe_rows WHERE dataframe_id = ?"
            params: List[Any] = [dataframe_id]

            # Apply JSON filters (simplified - could be enhanced)
            if filters:
                for key, value in filters.items():
                    query += f" AND json_extract(data_json, '$.{key}') = ?"
                    params.append(json.dumps(value) if isinstance(value, (dict, list)) else value)

            # Count total
            count_query = query.replace("SELECT *", "SELECT COUNT(*)")
            total = conn.execute(count_query, params).fetchone()[0]

            # Get rows
            query += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = conn.execute(query, params).fetchall()

            return [
                DataFrameRow(
                    id=r["id"],
                    dataframe_id=r["dataframe_id"],
                    task_id=r["task_id"],
                    data=json.loads(r["data_json"]),
                    confidence=r["confidence"],
                    citations=json.loads(r["citations_json"] or "[]"),
                    created_at=datetime.fromisoformat(r["created_at"]),
                )
                for r in rows
            ], total

    def list_dataframes(
        self,
        source_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[DataFrame]:
        """List all DataFrames."""
        with self._get_conn() as conn:
            query = "SELECT * FROM dataframes"
            params: List[Any] = []

            if source_type:
                query += " WHERE source_type = ?"
                params.append(source_type)

            query += " ORDER BY updated_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()

            return [
                DataFrame(
                    id=r["id"],
                    name=r["name"],
                    description=r["description"],
                    columns=[Column(
                        name=c["name"],
                        type=ColumnType(c["type"]),
                        description=c.get("description"),
                        required=c.get("required", False),
                        default=c.get("default"),
                    ) for c in json.loads(r["columns_json"])],
                    source_type=r["source_type"],
                    source_id=r["source_id"],
                    created_at=datetime.fromisoformat(r["created_at"]),
                    updated_at=datetime.fromisoformat(r["updated_at"]),
                    row_count=r["row_count"],
                    metadata=json.loads(r["metadata_json"] or "{}"),
                )
                for r in rows
            ]

    # =========================================================================
    # TASK GROUP OPERATIONS
    # =========================================================================

    def create_task_group(
        self,
        name: str,
        tasks: List[Dict[str, Any]],
        output_schema: Optional[List[Dict[str, Any]]] = None,
        strategy: str = "balanced",
    ) -> Tuple[str, Optional[str]]:
        """Create a task group with optional DataFrame for results."""
        group_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        dataframe_id = None

        with self._get_conn() as conn:
            # Create DataFrame if schema provided
            if output_schema:
                dataframe_id = str(uuid.uuid4())
                conn.execute("""
                    INSERT INTO dataframes
                    (id, name, description, columns_json, source_type, source_id,
                     metadata_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dataframe_id,
                    f"{name}_results",
                    f"Results from task group: {name}",
                    json.dumps(output_schema),
                    "task_group",
                    group_id,
                    json.dumps({"strategy": strategy}),
                    now, now
                ))

            # Create task group
            conn.execute("""
                INSERT INTO task_groups
                (id, name, dataframe_id, total_tasks, strategy, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (group_id, name, dataframe_id, len(tasks), strategy, now, now))

            # Create individual task items
            for task_config in tasks:
                task_id = str(uuid.uuid4())
                conn.execute("""
                    INSERT INTO task_group_items
                    (id, group_id, task_id, input, processor, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()),
                    group_id,
                    task_id,
                    task_config.get("input", ""),
                    task_config.get("processor", "lite"),
                    now
                ))

            conn.commit()

        return group_id, dataframe_id

    def get_task_group(self, group_id: str) -> Optional[Dict[str, Any]]:
        """Get task group with all items."""
        with self._get_conn() as conn:
            group = conn.execute(
                "SELECT * FROM task_groups WHERE id = ?", (group_id,)
            ).fetchone()

            if not group:
                return None

            items = conn.execute(
                "SELECT * FROM task_group_items WHERE group_id = ? ORDER BY created_at",
                (group_id,)
            ).fetchall()

            return {
                "id": group["id"],
                "name": group["name"],
                "dataframe_id": group["dataframe_id"],
                "total_tasks": group["total_tasks"],
                "completed_tasks": group["completed_tasks"],
                "failed_tasks": group["failed_tasks"],
                "status": group["status"],
                "strategy": group["strategy"],
                "parallel_group_id": group["parallel_group_id"],
                "created_at": group["created_at"],
                "updated_at": group["updated_at"],
                "progress": (group["completed_tasks"] + group["failed_tasks"]) / max(1, group["total_tasks"]),
                "items": [
                    {
                        "id": item["id"],
                        "task_id": item["task_id"],
                        "parallel_task_id": item["parallel_task_id"],
                        "input": item["input"],
                        "processor": item["processor"],
                        "status": item["status"],
                        "result": json.loads(item["result_json"]) if item["result_json"] else None,
                        "error": item["error"],
                        "created_at": item["created_at"],
                        "completed_at": item["completed_at"],
                    }
                    for item in items
                ]
            }

    def update_task_group_item(
        self,
        group_id: str,
        task_id: str,
        status: str,
        parallel_task_id: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ):
        """Update a task item in a group."""
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            completed_at = now if status in ("completed", "failed") else None

            conn.execute("""
                UPDATE task_group_items
                SET status = ?, parallel_task_id = ?, result_json = ?,
                    error = ?, completed_at = ?
                WHERE group_id = ? AND task_id = ?
            """, (
                status, parallel_task_id,
                json.dumps(result, default=str) if result else None,
                error, completed_at,
                group_id, task_id
            ))

            # Update group counters
            if status == "completed":
                conn.execute("""
                    UPDATE task_groups
                    SET completed_tasks = completed_tasks + 1, updated_at = ?
                    WHERE id = ?
                """, (now, group_id))
            elif status == "failed":
                conn.execute("""
                    UPDATE task_groups
                    SET failed_tasks = failed_tasks + 1, updated_at = ?
                    WHERE id = ?
                """, (now, group_id))

            # Check if group is complete
            group = conn.execute(
                "SELECT total_tasks, completed_tasks, failed_tasks FROM task_groups WHERE id = ?",
                (group_id,)
            ).fetchone()

            if group:
                total = group["total_tasks"]
                done = group["completed_tasks"] + group["failed_tasks"]
                if done >= total:
                    new_status = "completed" if group["failed_tasks"] == 0 else "partial"
                    conn.execute(
                        "UPDATE task_groups SET status = ?, updated_at = ? WHERE id = ?",
                        (new_status, now, group_id)
                    )

            conn.commit()

    def update_task_group_parallel_id(self, group_id: str, parallel_group_id: str):
        """Update the Parallel AI group ID."""
        now = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE task_groups
                SET parallel_group_id = ?, status = 'running', updated_at = ?
                WHERE id = ?
            """, (parallel_group_id, now, group_id))
            conn.commit()

    # =========================================================================
    # FINDALL OPERATIONS
    # =========================================================================

    def create_findall_run(
        self,
        objective: str,
        schema: Optional[Dict[str, Any]] = None,
        max_results: int = 50,
    ) -> Tuple[str, str]:
        """Create a FindAll run with DataFrame for entities."""
        run_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        # Infer columns from schema or use defaults
        if schema and "properties" in schema:
            columns = [
                {"name": k, "type": self._infer_type(v), "description": v.get("description")}
                for k, v in schema["properties"].items()
            ]
        else:
            columns = [
                {"name": "name", "type": "string"},
                {"name": "description", "type": "string"},
                {"name": "url", "type": "url"},
                {"name": "metadata", "type": "json"},
            ]

        # Create DataFrame for entities
        dataframe_id = str(uuid.uuid4())

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO dataframes
                (id, name, description, columns_json, source_type, source_id,
                 metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dataframe_id,
                f"findall_{run_id[:8]}",
                f"Entities from: {objective[:100]}",
                json.dumps(columns),
                "findall",
                run_id,
                json.dumps({"objective": objective, "max_results": max_results}),
                now, now
            ))

            conn.execute("""
                INSERT INTO findall_runs
                (id, objective, schema_json, dataframe_id, max_results, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id, objective,
                json.dumps(schema) if schema else None,
                dataframe_id, max_results,
                now, now
            ))
            conn.commit()

        return run_id, dataframe_id

    def update_findall_run(
        self,
        run_id: str,
        status: str,
        parallel_run_id: Optional[str] = None,
        entities_found: Optional[int] = None,
    ):
        """Update FindAll run status."""
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            updates = ["status = ?", "updated_at = ?"]
            params: List[Any] = [status, now]

            if parallel_run_id:
                updates.append("parallel_run_id = ?")
                params.append(parallel_run_id)

            if entities_found is not None:
                updates.append("entities_found = ?")
                params.append(entities_found)

            params.append(run_id)
            conn.execute(
                f"UPDATE findall_runs SET {', '.join(updates)} WHERE id = ?",
                params
            )
            conn.commit()

    def get_findall_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get FindAll run details."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM findall_runs WHERE id = ?", (run_id,)
            ).fetchone()

            if not row:
                return None

            return {
                "id": row["id"],
                "objective": row["objective"],
                "schema": json.loads(row["schema_json"]) if row["schema_json"] else None,
                "dataframe_id": row["dataframe_id"],
                "parallel_run_id": row["parallel_run_id"],
                "max_results": row["max_results"],
                "status": row["status"],
                "entities_found": row["entities_found"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }

    def _infer_type(self, schema_prop: Dict[str, Any]) -> str:
        """Infer column type from JSON schema property."""
        schema_type = schema_prop.get("type", "string")
        if schema_type == "number" or schema_type == "integer":
            return "number"
        elif schema_type == "boolean":
            return "boolean"
        elif schema_type == "array":
            return "list"
        elif schema_type == "object":
            return "json"
        elif schema_prop.get("format") == "uri":
            return "url"
        elif schema_prop.get("format") == "date-time":
            return "datetime"
        return "string"

    # =========================================================================
    # EXPORT OPERATIONS
    # =========================================================================

    def export_to_csv(self, dataframe_id: str) -> str:
        """Export DataFrame to CSV string."""
        df = self.get_dataframe(dataframe_id)
        if not df:
            return ""

        rows, _ = self.query_rows(dataframe_id, limit=10000)

        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        column_names = [c.name for c in df.columns]
        writer.writerow(column_names)

        # Rows
        for row in rows:
            writer.writerow([
                json.dumps(row.data.get(c, "")) if isinstance(row.data.get(c), (dict, list)) else row.data.get(c, "")
                for c in column_names
            ])

        return output.getvalue()

    def export_to_json(self, dataframe_id: str) -> Dict[str, Any]:
        """Export DataFrame to JSON."""
        df = self.get_dataframe(dataframe_id)
        if not df:
            return {}

        rows, total = self.query_rows(dataframe_id, limit=10000)

        return {
            "dataframe": {
                "id": df.id,
                "name": df.name,
                "description": df.description,
                "columns": [{"name": c.name, "type": c.type.value} for c in df.columns],
                "row_count": total,
            },
            "rows": [row.data for row in rows],
        }

    def export_to_markdown(self, dataframe_id: str, max_rows: int = 50) -> str:
        """Export DataFrame to Markdown table."""
        df = self.get_dataframe(dataframe_id)
        if not df:
            return ""

        rows, total = self.query_rows(dataframe_id, limit=max_rows)

        # Build markdown table
        column_names = [c.name for c in df.columns]
        lines = [
            f"## {df.name}",
            f"_{df.description}_" if df.description else "",
            "",
            "| " + " | ".join(column_names) + " |",
            "| " + " | ".join(["---"] * len(column_names)) + " |",
        ]

        for row in rows:
            values = []
            for c in column_names:
                val = row.data.get(c, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)[:50] + "..."
                elif isinstance(val, str) and len(val) > 50:
                    val = val[:50] + "..."
                values.append(str(val).replace("|", "\\|").replace("\n", " "))
            lines.append("| " + " | ".join(values) + " |")

        if total > max_rows:
            lines.append(f"\n_...and {total - max_rows} more rows_")

        return "\n".join(lines)

    def aggregate(
        self,
        dataframe_id: str,
        group_by: Optional[str] = None,
        aggregations: Optional[List[Dict[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """Perform aggregations on DataFrame."""
        rows, _ = self.query_rows(dataframe_id, limit=10000)

        if not rows:
            return []

        if not group_by:
            # Global aggregations
            result: Dict[str, Any] = {"_count": len(rows)}
            if aggregations:
                for agg in aggregations:
                    field = agg.get("field")
                    func = agg.get("function", "count")
                    values = [r.data.get(field) for r in rows if r.data.get(field) is not None]

                    if func == "count":
                        result[f"{field}_count"] = len(values)
                    elif func == "sum" and all(isinstance(v, (int, float)) for v in values):
                        result[f"{field}_sum"] = sum(values)
                    elif func == "avg" and all(isinstance(v, (int, float)) for v in values):
                        result[f"{field}_avg"] = sum(values) / len(values) if values else 0
                    elif func == "min":
                        result[f"{field}_min"] = min(values) if values else None
                    elif func == "max":
                        result[f"{field}_max"] = max(values) if values else None
                    elif func == "distinct":
                        result[f"{field}_distinct"] = list(set(str(v) for v in values))

            return [result]

        # Group by field
        groups: Dict[Any, List[DataFrameRow]] = {}
        for row in rows:
            key = row.data.get(group_by, "_null")
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        results = []
        for key, group_rows in groups.items():
            result = {group_by: key, "_count": len(group_rows)}
            if aggregations:
                for agg in aggregations:
                    field = agg.get("field")
                    func = agg.get("function", "count")
                    values = [r.data.get(field) for r in group_rows if r.data.get(field) is not None]

                    if func == "sum" and all(isinstance(v, (int, float)) for v in values):
                        result[f"{field}_sum"] = sum(values)
                    elif func == "avg" and all(isinstance(v, (int, float)) for v in values):
                        result[f"{field}_avg"] = sum(values) / len(values) if values else 0

            results.append(result)

        return results

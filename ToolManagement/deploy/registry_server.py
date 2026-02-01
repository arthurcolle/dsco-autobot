#!/usr/bin/env python3
"""
Central Tool Registry - Single source of truth for all tools.

Provides:
- Tool registration from services
- Tool discovery for clients
- Execution routing (optional)
- Health monitoring
- 50 Compound Tools with WebSocket/SSE streaming

Clients query this registry to discover tools without seeing vendor details.

Run with:
    python -m deploy.registry_server
    # or
    uvicorn deploy.registry_server:app --port 8000

Environment variables:
    DB_PATH: Path to SQLite database (default: ./data/registry.db)
    REQUIRE_AUTH: Require API key for registration (default: false)
"""

import hashlib
import hmac
import json
import os
import secrets
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import aiosqlite
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Header, Depends, WebSocket, WebSocketDisconnect, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

from tool_management.compound import (
    get_compound_registry,
    PipelineEvent,
    PipelineEventType,
    CompoundToolRegistry,
)

# Intelligence module imports
try:
    from tool_management.intelligence import (
        ToolAlgebra,
        ToolIndex,
        LocalEmbeddingProvider,
        ExecutionMemory,
        ExecutionRecord,
        IntentDAGCompiler,
        AdaptiveRouter,
        RoutingContext,
        RoutingStrategy,
        ToolVariant,
    )
    INTELLIGENCE_AVAILABLE = True
except ImportError:
    INTELLIGENCE_AVAILABLE = False
from tool_management.compound.streaming import (
    StreamingExecutor,
    StreamConfig,
    ExecutionStatus,
    PipelineChain,
    PipelineChainStep,
    AdvancedConnectionManager,
    StreamSubscription,
)
from tool_management.compound.orchestration import (
    DAGPipeline,
    DAGNode,
    DAGNodeType,
    DAGExecutor,
    WebhookConfig,
    WebhookTrigger,
    WebhookManager,
    ResultCache,
    PriorityExecutionQueue,
    ExecutionPriority,
    CollaborationManager,
    ResourceLimits,
)


# =============================================================================
# AUTHENTICATION SYSTEM
# =============================================================================

# Auth Configuration
ORCHESTRATION_AUTH_ENABLED = os.getenv("ORCHESTRATION_AUTH_ENABLED", "true").lower() == "true"
JWT_SECRET = os.getenv("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "24"))
API_KEY_HEADER_NAME = "X-API-Key"
BEARER_TOKEN_HEADER = "Authorization"

# Master admin key (set via env var for production)
ORCHESTRATION_ADMIN_KEY = os.getenv("ORCHESTRATION_ADMIN_KEY", "")
# Comma-separated operator keys
ORCHESTRATION_OPERATOR_KEYS = os.getenv("ORCHESTRATION_OPERATOR_KEYS", "").split(",") if os.getenv("ORCHESTRATION_OPERATOR_KEYS") else []


class AuthRole(str, Enum):
    """Authentication roles with hierarchical permissions."""
    ADMIN = "admin"        # Full access to everything
    OPERATOR = "operator"  # Can execute, manage webhooks, queues
    VIEWER = "viewer"      # Read-only access
    ANONYMOUS = "anonymous"  # No auth provided


class AuthScope(str, Enum):
    """Fine-grained permission scopes."""
    # DAG
    DAG_EXECUTE = "dag:execute"
    DAG_RESUME = "dag:resume"
    DAG_READ = "dag:read"
    # Webhooks
    WEBHOOK_CREATE = "webhook:create"
    WEBHOOK_DELETE = "webhook:delete"
    WEBHOOK_READ = "webhook:read"
    # Cache
    CACHE_INVALIDATE = "cache:invalidate"
    CACHE_READ = "cache:read"
    # Queue
    QUEUE_ENQUEUE = "queue:enqueue"
    QUEUE_READ = "queue:read"
    # Rooms
    ROOM_CREATE = "room:create"
    ROOM_JOIN = "room:join"
    ROOM_READ = "room:read"
    # Admin
    ADMIN_ALL = "admin:all"


# Role to scopes mapping
ROLE_SCOPES: Dict[AuthRole, Set[AuthScope]] = {
    AuthRole.ADMIN: {s for s in AuthScope},  # All scopes
    AuthRole.OPERATOR: {
        AuthScope.DAG_EXECUTE, AuthScope.DAG_RESUME, AuthScope.DAG_READ,
        AuthScope.WEBHOOK_CREATE, AuthScope.WEBHOOK_DELETE, AuthScope.WEBHOOK_READ,
        AuthScope.CACHE_INVALIDATE, AuthScope.CACHE_READ,
        AuthScope.QUEUE_ENQUEUE, AuthScope.QUEUE_READ,
        AuthScope.ROOM_CREATE, AuthScope.ROOM_JOIN, AuthScope.ROOM_READ,
    },
    AuthRole.VIEWER: {
        AuthScope.DAG_READ,
        AuthScope.WEBHOOK_READ,
        AuthScope.CACHE_READ,
        AuthScope.QUEUE_READ,
        AuthScope.ROOM_READ,
    },
    AuthRole.ANONYMOUS: set(),
}


class AuthUser(BaseModel):
    """Authenticated user context."""
    id: str
    role: AuthRole
    scopes: Set[AuthScope] = set()
    api_key_id: Optional[str] = None
    token_id: Optional[str] = None
    issued_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


class APIKeyRecord(BaseModel):
    """API key storage record."""
    id: str
    key_hash: str
    name: str
    role: AuthRole
    scopes: Set[AuthScope] = set()
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    enabled: bool = True


class TokenPayload(BaseModel):
    """JWT token payload."""
    sub: str  # User/key ID
    role: str
    scopes: List[str]
    iat: datetime
    exp: datetime
    jti: str  # Token ID


class AuthManager:
    """Manages API keys and JWT tokens."""

    def __init__(self):
        self.api_keys: Dict[str, APIKeyRecord] = {}
        self._initialize_default_keys()

    def _initialize_default_keys(self):
        """Initialize from environment variables."""
        if ORCHESTRATION_ADMIN_KEY:
            self._add_key_internal(
                key=ORCHESTRATION_ADMIN_KEY,
                name="admin-env",
                role=AuthRole.ADMIN,
            )
        for idx, key in enumerate(ORCHESTRATION_OPERATOR_KEYS):
            if key.strip():
                self._add_key_internal(
                    key=key.strip(),
                    name=f"operator-env-{idx}",
                    role=AuthRole.OPERATOR,
                )

    def _hash_key(self, key: str) -> str:
        """Hash API key for storage."""
        return hashlib.sha256(key.encode()).hexdigest()

    def _add_key_internal(
        self,
        key: str,
        name: str,
        role: AuthRole,
        scopes: Optional[Set[AuthScope]] = None,
        expires_at: Optional[datetime] = None,
    ) -> str:
        """Add API key internally (no key generation)."""
        key_id = str(uuid4())
        key_hash = self._hash_key(key)

        self.api_keys[key_hash] = APIKeyRecord(
            id=key_id,
            key_hash=key_hash,
            name=name,
            role=role,
            scopes=scopes or ROLE_SCOPES.get(role, set()),
            created_at=datetime.utcnow(),
            expires_at=expires_at,
        )
        return key_id

    def create_api_key(
        self,
        name: str,
        role: AuthRole = AuthRole.OPERATOR,
        scopes: Optional[Set[AuthScope]] = None,
        expires_days: Optional[int] = None,
    ) -> tuple[str, str]:
        """Create a new API key. Returns (key_id, key)."""
        key = f"orch_{secrets.token_urlsafe(32)}"
        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)

        key_id = self._add_key_internal(key, name, role, scopes, expires_at)
        return key_id, key

    def validate_api_key(self, key: str) -> Optional[AuthUser]:
        """Validate API key and return user context."""
        key_hash = self._hash_key(key)
        record = self.api_keys.get(key_hash)

        if not record:
            return None
        if not record.enabled:
            return None
        if record.expires_at and record.expires_at < datetime.utcnow():
            return None

        # Update last used
        record.last_used = datetime.utcnow()

        return AuthUser(
            id=record.id,
            role=record.role,
            scopes=record.scopes,
            api_key_id=record.id,
            issued_at=record.created_at,
            expires_at=record.expires_at,
        )

    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke an API key."""
        for key_hash, record in self.api_keys.items():
            if record.id == key_id:
                record.enabled = False
                return True
        return False

    def create_jwt_token(
        self,
        user_id: str,
        role: AuthRole = AuthRole.OPERATOR,
        scopes: Optional[List[str]] = None,
        expires_hours: Optional[int] = None,
    ) -> str:
        """Create a JWT token."""
        if not JWT_AVAILABLE:
            raise HTTPException(500, "JWT support not available (install PyJWT)")

        now = datetime.utcnow()
        exp_hours = expires_hours or JWT_EXPIRATION_HOURS
        expires = now + timedelta(hours=exp_hours)

        token_scopes = scopes or [s.value for s in ROLE_SCOPES.get(role, set())]

        payload = {
            "sub": user_id,
            "role": role.value,
            "scopes": token_scopes,
            "iat": now,
            "exp": expires,
            "jti": str(uuid4()),
        }

        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    def validate_jwt_token(self, token: str) -> Optional[AuthUser]:
        """Validate JWT token and return user context."""
        if not JWT_AVAILABLE:
            return None

        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

            return AuthUser(
                id=payload["sub"],
                role=AuthRole(payload["role"]),
                scopes={AuthScope(s) for s in payload.get("scopes", []) if s in [e.value for e in AuthScope]},
                token_id=payload.get("jti"),
                issued_at=datetime.fromtimestamp(payload["iat"]) if isinstance(payload["iat"], (int, float)) else payload["iat"],
                expires_at=datetime.fromtimestamp(payload["exp"]) if isinstance(payload["exp"], (int, float)) else payload["exp"],
            )
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List all API keys (without the actual key)."""
        return [
            {
                "id": r.id,
                "name": r.name,
                "role": r.role.value,
                "scopes": [s.value for s in r.scopes],
                "created_at": r.created_at.isoformat(),
                "expires_at": r.expires_at.isoformat() if r.expires_at else None,
                "last_used": r.last_used.isoformat() if r.last_used else None,
                "enabled": r.enabled,
            }
            for r in self.api_keys.values()
        ]


# Global auth manager
auth_manager: Optional[AuthManager] = None

# Security schemes
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    api_key: Optional[str] = Security(api_key_header),
    bearer: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> AuthUser:
    """
    Authenticate user via API key or JWT token.

    Priority: API Key > Bearer Token
    """
    if not ORCHESTRATION_AUTH_ENABLED:
        # Auth disabled - return admin user
        return AuthUser(
            id="anonymous",
            role=AuthRole.ADMIN,
            scopes={s for s in AuthScope},
        )

    # Try API key first
    if api_key:
        user = auth_manager.validate_api_key(api_key)
        if user:
            return user

    # Try Bearer token
    if bearer:
        user = auth_manager.validate_jwt_token(bearer.credentials)
        if user:
            return user

    # No valid auth
    raise HTTPException(
        status_code=401,
        detail="Invalid or missing authentication",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_optional_user(
    api_key: Optional[str] = Security(api_key_header),
    bearer: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> AuthUser:
    """Get user or anonymous if auth disabled/not provided."""
    if not ORCHESTRATION_AUTH_ENABLED:
        return AuthUser(
            id="anonymous",
            role=AuthRole.ADMIN,
            scopes={s for s in AuthScope},
        )

    if api_key:
        user = auth_manager.validate_api_key(api_key)
        if user:
            return user

    if bearer:
        user = auth_manager.validate_jwt_token(bearer.credentials)
        if user:
            return user

    return AuthUser(id="anonymous", role=AuthRole.ANONYMOUS, scopes=set())


def require_scope(scope: AuthScope):
    """Dependency factory for requiring a specific scope."""
    async def check_scope(user: AuthUser = Depends(get_current_user)) -> AuthUser:
        if AuthScope.ADMIN_ALL in user.scopes or scope in user.scopes:
            return user
        raise HTTPException(
            status_code=403,
            detail=f"Missing required scope: {scope.value}",
        )
    return check_scope


def require_role(min_role: AuthRole):
    """Dependency factory for requiring minimum role."""
    role_hierarchy = [AuthRole.ANONYMOUS, AuthRole.VIEWER, AuthRole.OPERATOR, AuthRole.ADMIN]

    async def check_role(user: AuthUser = Depends(get_current_user)) -> AuthUser:
        user_level = role_hierarchy.index(user.role)
        required_level = role_hierarchy.index(min_role)
        if user_level >= required_level:
            return user
        raise HTTPException(
            status_code=403,
            detail=f"Requires role: {min_role.value} or higher",
        )
    return check_role


# =============================================================================
# MODELS
# =============================================================================

class ToolRegistration(BaseModel):
    """Tool registration request."""
    name: str
    description: str
    input_schema: Dict[str, Any]
    service_id: str
    service_url: str
    category: Optional[str] = None
    tags: List[str] = []


class RegisteredTool(BaseModel):
    """Tool stored in registry."""
    id: str
    name: str
    description: str
    input_schema: Dict[str, Any]
    service_id: str
    service_url: str
    category: Optional[str] = None
    tags: List[str] = []
    version: str = "1.0.0"
    enabled: bool = True
    healthy: bool = True
    created_at: datetime
    updated_at: datetime


class ServiceRegistration(BaseModel):
    """Service registration request."""
    service_id: str
    service_name: str
    service_url: str
    description: str = ""
    tools: List[ToolRegistration] = []


class RegisteredService(BaseModel):
    """Service stored in registry."""
    service_id: str
    service_name: str
    service_url: str
    description: str
    tool_count: int
    healthy: bool
    last_heartbeat: datetime
    created_at: datetime


class ExecuteRequest(BaseModel):
    """Execute tool request (routed through registry)."""
    tool_id: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None


class ExecuteResponse(BaseModel):
    """Execute response."""
    request_id: str
    tool_id: str
    service_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float


# =============================================================================
# REGISTRY STORAGE
# =============================================================================

class RegistryStorage:
    """SQLite storage for registry."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        """Initialize database."""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._create_tables()

    async def _create_tables(self):
        """Create tables."""
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS services (
                service_id TEXT PRIMARY KEY,
                service_name TEXT NOT NULL,
                service_url TEXT NOT NULL,
                description TEXT DEFAULT '',
                healthy INTEGER DEFAULT 1,
                last_heartbeat TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tools (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                input_schema TEXT NOT NULL,
                service_id TEXT NOT NULL,
                service_url TEXT NOT NULL,
                category TEXT,
                tags TEXT DEFAULT '[]',
                version TEXT DEFAULT '1.0.0',
                enabled INTEGER DEFAULT 1,
                healthy INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (service_id) REFERENCES services(service_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tools_service ON tools(service_id);
            CREATE INDEX IF NOT EXISTS idx_tools_category ON tools(category);
            CREATE INDEX IF NOT EXISTS idx_tools_name ON tools(name);
        """)
        await self._db.commit()

    async def close(self):
        """Close database."""
        if self._db:
            await self._db.close()

    # Services
    async def register_service(
        self,
        service_id: str,
        service_name: str,
        service_url: str,
        description: str = "",
    ) -> RegisteredService:
        """Register or update a service."""
        now = datetime.utcnow().isoformat()

        await self._db.execute("""
            INSERT INTO services (service_id, service_name, service_url, description, last_heartbeat, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(service_id) DO UPDATE SET
                service_name = excluded.service_name,
                service_url = excluded.service_url,
                description = excluded.description,
                last_heartbeat = excluded.last_heartbeat
        """, (service_id, service_name, service_url, description, now, now))
        await self._db.commit()

        return await self.get_service(service_id)

    async def get_service(self, service_id: str) -> Optional[RegisteredService]:
        """Get service by ID."""
        async with self._db.execute(
            "SELECT * FROM services WHERE service_id = ?",
            (service_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

        # Count tools
        async with self._db.execute(
            "SELECT COUNT(*) as count FROM tools WHERE service_id = ?",
            (service_id,)
        ) as cursor:
            count_row = await cursor.fetchone()
            tool_count = count_row["count"]

        return RegisteredService(
            service_id=row["service_id"],
            service_name=row["service_name"],
            service_url=row["service_url"],
            description=row["description"],
            tool_count=tool_count,
            healthy=bool(row["healthy"]),
            last_heartbeat=datetime.fromisoformat(row["last_heartbeat"]) if row["last_heartbeat"] else datetime.utcnow(),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    async def list_services(self) -> List[RegisteredService]:
        """List all services."""
        services = []
        async with self._db.execute("SELECT service_id FROM services") as cursor:
            async for row in cursor:
                service = await self.get_service(row["service_id"])
                if service:
                    services.append(service)
        return services

    async def heartbeat(self, service_id: str) -> bool:
        """Update service heartbeat."""
        now = datetime.utcnow().isoformat()
        cursor = await self._db.execute(
            "UPDATE services SET last_heartbeat = ?, healthy = 1 WHERE service_id = ?",
            (now, service_id)
        )
        await self._db.commit()
        return cursor.rowcount > 0

    async def delete_service(self, service_id: str):
        """Delete service and its tools."""
        await self._db.execute("DELETE FROM tools WHERE service_id = ?", (service_id,))
        await self._db.execute("DELETE FROM services WHERE service_id = ?", (service_id,))
        await self._db.commit()

    # Tools
    async def register_tool(self, tool: ToolRegistration) -> RegisteredTool:
        """Register a tool."""
        import json

        tool_id = f"{tool.service_id}:{tool.name}"
        now = datetime.utcnow().isoformat()

        await self._db.execute("""
            INSERT INTO tools (id, name, description, input_schema, service_id, service_url, category, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                description = excluded.description,
                input_schema = excluded.input_schema,
                service_url = excluded.service_url,
                category = excluded.category,
                tags = excluded.tags,
                updated_at = excluded.updated_at
        """, (
            tool_id,
            tool.name,
            tool.description,
            json.dumps(tool.input_schema),
            tool.service_id,
            tool.service_url,
            tool.category,
            json.dumps(tool.tags),
            now,
            now,
        ))
        await self._db.commit()

        return await self.get_tool(tool_id)

    async def get_tool(self, tool_id: str) -> Optional[RegisteredTool]:
        """Get tool by ID."""
        import json

        async with self._db.execute(
            "SELECT * FROM tools WHERE id = ?",
            (tool_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

        return RegisteredTool(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            input_schema=json.loads(row["input_schema"]),
            service_id=row["service_id"],
            service_url=row["service_url"],
            category=row["category"],
            tags=json.loads(row["tags"]) if row["tags"] else [],
            version=row["version"],
            enabled=bool(row["enabled"]),
            healthy=bool(row["healthy"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    async def list_tools(
        self,
        category: Optional[str] = None,
        service_id: Optional[str] = None,
        search: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> List[RegisteredTool]:
        """List tools with filters."""
        import json

        query = "SELECT * FROM tools WHERE enabled = 1"
        params = []

        if category:
            query += " AND category = ?"
            params.append(category)
        if service_id:
            query += " AND service_id = ?"
            params.append(service_id)
        if search:
            query += " AND (name LIKE ? OR description LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%"])

        query += " ORDER BY name"

        tools = []
        async with self._db.execute(query, params) as cursor:
            async for row in cursor:
                tool = RegisteredTool(
                    id=row["id"],
                    name=row["name"],
                    description=row["description"],
                    input_schema=json.loads(row["input_schema"]),
                    service_id=row["service_id"],
                    service_url=row["service_url"],
                    category=row["category"],
                    tags=json.loads(row["tags"]) if row["tags"] else [],
                    version=row["version"],
                    enabled=bool(row["enabled"]),
                    healthy=bool(row["healthy"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )

                # Filter by tags if specified
                if tags:
                    if not any(t in tool.tags for t in tags):
                        continue

                tools.append(tool)

        return tools

    async def delete_tool(self, tool_id: str):
        """Delete a tool."""
        await self._db.execute("DELETE FROM tools WHERE id = ?", (tool_id,))
        await self._db.commit()


# =============================================================================
# REGISTRY SERVER
# =============================================================================

# Configuration
DB_PATH = os.getenv("DB_PATH", "./data/registry.db")
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
REGISTRY_API_KEY = os.getenv("REGISTRY_API_KEY", "")

# Storage instance
storage: Optional[RegistryStorage] = None

# Compound tool registry
compound_registry: Optional[CompoundToolRegistry] = None

# Advanced streaming
streaming_executor: Optional[StreamingExecutor] = None
connection_manager: Optional[AdvancedConnectionManager] = None

# Orchestration
dag_executor: Optional[DAGExecutor] = None
webhook_manager: Optional[WebhookManager] = None
result_cache: Optional[ResultCache] = None
priority_queue: Optional[PriorityExecutionQueue] = None
collaboration_manager: Optional[CollaborationManager] = None

# Intelligence
tool_algebra: Optional['ToolAlgebra'] = None
tool_index: Optional['ToolIndex'] = None
execution_memory: Optional['ExecutionMemory'] = None
intent_compiler: Optional['IntentDAGCompiler'] = None
adaptive_router: Optional['AdaptiveRouter'] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown."""
    global storage, compound_registry, streaming_executor, connection_manager
    global dag_executor, webhook_manager, result_cache, priority_queue, collaboration_manager
    global auth_manager
    global tool_algebra, tool_index, execution_memory, intent_compiler, adaptive_router

    storage = RegistryStorage(DB_PATH)
    await storage.initialize()

    compound_registry = get_compound_registry()
    streaming_executor = StreamingExecutor(compound_registry)
    connection_manager = AdvancedConnectionManager()

    # Initialize auth manager
    auth_manager = AuthManager()

    # Orchestration components
    webhook_manager = WebhookManager()
    result_cache = ResultCache(max_size=1000, default_ttl_seconds=3600)
    priority_queue = PriorityExecutionQueue(max_concurrent=10)
    collaboration_manager = CollaborationManager()
    dag_executor = DAGExecutor(
        registry=compound_registry,
        webhook_manager=webhook_manager,
        cache=result_cache,
        collaboration_manager=collaboration_manager,
    )

    await streaming_executor.start()
    await connection_manager.start()
    await webhook_manager.start()

    # Initialize intelligence components
    if INTELLIGENCE_AVAILABLE:
        embedding_provider = LocalEmbeddingProvider()
        tool_algebra = ToolAlgebra()
        tool_index = ToolIndex(embedding_provider)
        execution_memory = ExecutionMemory()
        intent_compiler = IntentDAGCompiler(tool_index, tool_algebra, execution_memory)
        adaptive_router = AdaptiveRouter(execution_memory)
        print(f"Intelligence module initialized")
    else:
        print(f"Intelligence module not available (import error)")

    print(f"Registry initialized with database: {DB_PATH}")
    print(f"Compound tools registered: {len(compound_registry._pipelines)}")
    print(f"Advanced streaming enabled")
    print(f"DAG orchestration enabled")
    print(f"Webhooks, caching, priority queue, collaboration enabled")
    print(f"Authentication: {'ENABLED' if ORCHESTRATION_AUTH_ENABLED else 'DISABLED'}")
    if ORCHESTRATION_AUTH_ENABLED:
        print(f"  API keys loaded: {len(auth_manager.api_keys)}")
        print(f"  JWT support: {'available' if JWT_AVAILABLE else 'not available (install PyJWT)'}")

    yield

    await streaming_executor.stop()
    await connection_manager.stop()
    await webhook_manager.stop()
    await storage.close()


# Create app
app = FastAPI(
    title="Tool Registry",
    description="Central registry for tool discovery and routing",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Auth dependency
async def verify_admin(x_api_key: Optional[str] = Header(None)):
    """Verify admin API key for registration endpoints."""
    if REQUIRE_AUTH:
        if not x_api_key or x_api_key != REGISTRY_API_KEY:
            raise HTTPException(401, "Invalid API key")
    return True


# =============================================================================
# ROUTES
# =============================================================================

@app.get("/")
async def root():
    """Registry info."""
    services = await storage.list_services()
    tools = await storage.list_tools()

    return {
        "name": "Tool Registry",
        "version": "1.0.0",
        "services": len(services),
        "tools": len(tools),
        "endpoints": {
            "services": "/services",
            "tools": "/tools",
            "execute": "/execute",
        },
    }


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy"}


# Services
@app.get("/services", response_model=List[RegisteredService])
async def list_services():
    """List all registered services."""
    return await storage.list_services()


@app.post("/services", response_model=RegisteredService)
async def register_service(
    body: ServiceRegistration,
    _: bool = Depends(verify_admin),
):
    """Register a service and its tools."""
    # Register service
    service = await storage.register_service(
        service_id=body.service_id,
        service_name=body.service_name,
        service_url=body.service_url,
        description=body.description,
    )

    # Register tools
    for tool_reg in body.tools:
        tool_reg.service_id = body.service_id
        tool_reg.service_url = body.service_url
        await storage.register_tool(tool_reg)

    # Refresh service to get tool count
    return await storage.get_service(body.service_id)


@app.get("/services/{service_id}", response_model=RegisteredService)
async def get_service(service_id: str):
    """Get service by ID."""
    service = await storage.get_service(service_id)
    if not service:
        raise HTTPException(404, f"Service not found: {service_id}")
    return service


@app.post("/services/{service_id}/heartbeat")
async def service_heartbeat(service_id: str):
    """Update service heartbeat."""
    success = await storage.heartbeat(service_id)
    if not success:
        raise HTTPException(404, f"Service not found: {service_id}")
    return {"status": "ok"}


@app.delete("/services/{service_id}")
async def delete_service(
    service_id: str,
    _: bool = Depends(verify_admin),
):
    """Delete a service and all its tools."""
    await storage.delete_service(service_id)
    return {"status": "deleted"}


# Tools
@app.get("/tools", response_model=List[RegisteredTool])
async def list_tools(
    category: Optional[str] = None,
    service_id: Optional[str] = None,
    search: Optional[str] = None,
    tags: Optional[str] = None,
):
    """List all registered tools."""
    tag_list = tags.split(",") if tags else None
    return await storage.list_tools(
        category=category,
        service_id=service_id,
        search=search,
        tags=tag_list,
    )


@app.post("/tools", response_model=RegisteredTool)
async def register_tool(
    body: ToolRegistration,
    _: bool = Depends(verify_admin),
):
    """Register a single tool."""
    return await storage.register_tool(body)


@app.get("/tools/{tool_id}", response_model=RegisteredTool)
async def get_tool(tool_id: str):
    """Get tool by ID."""
    tool = await storage.get_tool(tool_id)
    if not tool:
        raise HTTPException(404, f"Tool not found: {tool_id}")
    return tool


@app.delete("/tools/{tool_id}")
async def delete_tool(
    tool_id: str,
    _: bool = Depends(verify_admin),
):
    """Delete a tool."""
    await storage.delete_tool(tool_id)
    return {"status": "deleted"}


# Execution (routes to appropriate service)
@app.post("/execute", response_model=ExecuteResponse)
async def execute_tool(body: ExecuteRequest):
    """
    Execute a tool by routing to the appropriate service.

    This is optional - clients can also call services directly.
    """
    import time

    tool = await storage.get_tool(body.tool_id)
    if not tool:
        raise HTTPException(404, f"Tool not found: {body.tool_id}")

    if not tool.enabled or not tool.healthy:
        raise HTTPException(503, f"Tool unavailable: {body.tool_id}")

    # Route to service
    start = time.time()
    request_id = str(uuid4())

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{tool.service_url}/execute",
                json={
                    "tool": tool.name,
                    "inputs": body.inputs,
                    "idempotency_key": body.idempotency_key,
                },
            )
            response.raise_for_status()
            data = response.json()

            elapsed = (time.time() - start) * 1000

            return ExecuteResponse(
                request_id=data.get("request_id", request_id),
                tool_id=body.tool_id,
                service_id=tool.service_id,
                status=data.get("status", "completed"),
                result=data.get("result"),
                error=data.get("error"),
                execution_time_ms=elapsed,
            )

    except httpx.HTTPError as e:
        elapsed = (time.time() - start) * 1000
        return ExecuteResponse(
            request_id=request_id,
            tool_id=body.tool_id,
            service_id=tool.service_id,
            status="failed",
            error=str(e),
            execution_time_ms=elapsed,
        )


# =============================================================================
# COMPOUND TOOLS
# =============================================================================

class CompoundExecuteRequest(BaseModel):
    """Request to execute a compound tool."""
    pipeline: str
    inputs: Dict[str, Any] = Field(default_factory=dict)


class CompoundToolInfo(BaseModel):
    """Compound tool metadata."""
    name: str
    description: str
    category: str
    step_count: int
    base_tools: List[str]


@app.get("/compound/tools", response_model=List[CompoundToolInfo])
async def list_compound_tools(
    category: Optional[str] = None,
    search: Optional[str] = None,
):
    """List available compound tools (DSPy pipelines)."""
    tools = []
    for metadata in compound_registry.list_all():
        if category and metadata.category != category:
            continue
        if search and search.lower() not in metadata.name.lower() and search.lower() not in metadata.description.lower():
            continue
        tools.append(CompoundToolInfo(
            name=metadata.name,
            description=metadata.description,
            category=metadata.category,
            step_count=metadata.step_count,
            base_tools=metadata.base_tools,
        ))
    return tools


@app.get("/compound/categories")
async def list_compound_categories():
    """List compound tool categories."""
    from tool_management.compound.registry import COMPOUND_TOOLS_BY_CATEGORY
    return {
        "categories": [
            {
                "id": cat_id,
                "name": cat["name"],
                "description": cat["description"],
                "tool_count": len(cat["tools"]),
            }
            for cat_id, cat in COMPOUND_TOOLS_BY_CATEGORY.items()
        ]
    }


@app.post("/compound/execute")
async def execute_compound_tool(body: CompoundExecuteRequest):
    """
    Execute a compound tool (non-streaming).

    For streaming, use the WebSocket endpoint: /compound/stream
    """
    pipeline = compound_registry.get(body.pipeline)
    if not pipeline:
        raise HTTPException(404, f"Compound tool not found: {body.pipeline}")

    state = await compound_registry.execute(body.pipeline, **body.inputs)

    return {
        "execution_id": state.id,
        "pipeline": body.pipeline,
        "status": state.status,
        "steps_completed": state.current_step,
        "total_steps": state.total_steps,
        "tool_calls": len(state.tool_calls),
        "result": state.final_result,
        "error": state.error,
    }


# =============================================================================
# ADVANCED STREAMING MODELS
# =============================================================================

class AdvancedStreamRequest(BaseModel):
    """Advanced streaming execution request."""
    pipeline: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    config: Optional[Dict[str, Any]] = None  # StreamConfig fields


class ParallelStreamRequest(BaseModel):
    """Request to execute multiple pipelines in parallel."""
    pipelines: List[Dict[str, Any]]  # [{pipeline, inputs}, ...]
    max_concurrent: int = 10


class ChainStreamRequest(BaseModel):
    """Request to execute a chain of pipelines."""
    name: str
    steps: List[Dict[str, Any]]  # PipelineChainStep dicts
    initial_inputs: Dict[str, Any] = Field(default_factory=dict)
    stop_on_failure: bool = True


class ReplayRequest(BaseModel):
    """Request to replay a past execution."""
    execution_id: str
    speed: float = 1.0


# =============================================================================
# ADVANCED STREAMING ENDPOINTS
# =============================================================================

@app.post("/compound/execute/advanced")
async def execute_advanced(body: AdvancedStreamRequest):
    """
    Execute with advanced features (cancellation, filtering, metrics).

    Returns execution_id for control operations.
    """
    config = StreamConfig(**(body.config or {}))

    async def event_generator():
        async for event in streaming_executor.execute(
            body.pipeline, body.inputs, config
        ):
            data = event.model_dump_json()
            yield f"data: {data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/compound/execute/parallel")
async def execute_parallel(body: ParallelStreamRequest):
    """
    Execute multiple pipelines in parallel.

    Streams events tagged with pipeline index.
    """
    requests = [
        (p["pipeline"], p.get("inputs", {}))
        for p in body.pipelines
    ]

    async def event_generator():
        async for idx, event in streaming_executor.execute_parallel(
            requests, max_concurrent=body.max_concurrent
        ):
            data = event.model_dump(mode="json")
            data["_parallel_index"] = idx
            yield f"data: {json.dumps(data)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@app.post("/compound/execute/chain")
async def execute_chain(body: ChainStreamRequest):
    """
    Execute a chain of pipelines where outputs feed into inputs.
    """
    chain = PipelineChain(
        name=body.name,
        steps=[PipelineChainStep(**s) for s in body.steps],
        stop_on_failure=body.stop_on_failure,
    )

    async def event_generator():
        async for event in streaming_executor.execute_chain(
            chain, body.initial_inputs
        ):
            data = event.model_dump_json()
            yield f"data: {data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@app.post("/compound/executions/{execution_id}/cancel")
async def cancel_execution(execution_id: str):
    """Cancel a running execution."""
    success = streaming_executor.cancel(execution_id)
    if not success:
        raise HTTPException(404, f"Execution not found or not running: {execution_id}")
    return {"status": "cancelled", "execution_id": execution_id}


@app.post("/compound/executions/{execution_id}/pause")
async def pause_execution(execution_id: str):
    """Pause a running execution."""
    success = streaming_executor.pause(execution_id)
    if not success:
        raise HTTPException(404, f"Execution not found or not running: {execution_id}")
    return {"status": "paused", "execution_id": execution_id}


@app.post("/compound/executions/{execution_id}/resume")
async def resume_execution(execution_id: str):
    """Resume a paused execution."""
    success = streaming_executor.resume(execution_id)
    if not success:
        raise HTTPException(404, f"Execution not found or not paused: {execution_id}")
    return {"status": "resumed", "execution_id": execution_id}


@app.post("/compound/executions/{execution_id}/replay")
async def replay_execution(execution_id: str, speed: float = 1.0):
    """
    Replay a past execution's events.

    Args:
        execution_id: ID of execution to replay
        speed: Playback speed (2.0 = 2x speed, 0.5 = half speed)
    """
    async def event_generator():
        try:
            async for event in streaming_executor.replay(execution_id, speed):
                data = event.model_dump_json()
                yield f"data: {data}\n\n"
        except ValueError as e:
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
    )


@app.get("/compound/executions")
async def list_executions(active_only: bool = False):
    """List all executions."""
    active = streaming_executor.list_active()

    if active_only:
        return {"executions": active}

    history = [
        {
            "id": ctx.id,
            "pipeline": ctx.pipeline_name,
            "status": ctx.status.value,
            "started_at": ctx.started_at.isoformat() if ctx.started_at else None,
            "completed_at": ctx.completed_at.isoformat() if ctx.completed_at else None,
            "events_emitted": ctx.events_emitted,
            "error": ctx.error,
        }
        for ctx in streaming_executor.execution_history.values()
    ]

    return {
        "active": active,
        "history": history[-100:],  # Last 100
    }


@app.get("/compound/executions/{execution_id}")
async def get_execution(execution_id: str):
    """Get execution details."""
    ctx = streaming_executor.get_execution(execution_id)
    if not ctx:
        raise HTTPException(404, f"Execution not found: {execution_id}")

    return {
        "id": ctx.id,
        "pipeline": ctx.pipeline_name,
        "status": ctx.status.value,
        "inputs": ctx.inputs,
        "started_at": ctx.started_at.isoformat() if ctx.started_at else None,
        "completed_at": ctx.completed_at.isoformat() if ctx.completed_at else None,
        "events_emitted": ctx.events_emitted,
        "bytes_emitted": ctx.bytes_emitted,
        "error": ctx.error,
        "final_result": ctx.final_state.final_result if ctx.final_state else None,
    }


@app.get("/compound/metrics")
async def get_metrics():
    """Get streaming executor metrics."""
    return {
        "executor": streaming_executor.get_metrics(),
        "connections": connection_manager.get_stats(),
        "pipelines": {
            "total": len(compound_registry._pipelines),
            "categories": compound_registry.list_categories(),
        },
        "cache": result_cache.stats(),
        "queue": priority_queue.stats(),
    }


# =============================================================================
# DAG ORCHESTRATION
# =============================================================================

class DAGExecuteRequest(BaseModel):
    """Request to execute a DAG pipeline."""
    dag: Dict[str, Any]  # DAGPipeline dict
    inputs: Dict[str, Any] = Field(default_factory=dict)
    limits: Optional[Dict[str, Any]] = None
    enable_caching: bool = True
    room_id: Optional[str] = None


class DAGResumeRequest(BaseModel):
    """Request to resume a DAG execution."""
    execution_id: str
    dag: Dict[str, Any]


@app.post("/orchestration/dag/execute")
async def execute_dag(
    body: DAGExecuteRequest,
    user: AuthUser = Depends(require_scope(AuthScope.DAG_EXECUTE)),
):
    """
    Execute a DAG-based pipeline.

    Requires: `dag:execute` scope

    DAGs support:
    - Parallel execution of independent nodes
    - Conditional branching
    - Data transformation between nodes
    - Checkpointing for recovery
    - Resource limits

    Example DAG:
    {
        "name": "research_pipeline",
        "nodes": {
            "research": {
                "name": "research",
                "type": "pipeline",
                "pipeline": "deep_research",
                "inputs": {"topic": "AI"}
            },
            "verify": {
                "name": "verify",
                "type": "pipeline",
                "pipeline": "claim_verifier",
                "depends_on": ["research"],
                "input_mappings": {"claim": "research.summary"}
            }
        },
        "entry_node": "research",
        "exit_nodes": ["verify"]
    }
    """
    dag = DAGPipeline(**body.dag)
    limits = ResourceLimits(**(body.limits or {})) if body.limits else None

    async def event_generator():
        async for event in dag_executor.execute(
            dag=dag,
            inputs=body.inputs,
            limits=limits,
            enable_caching=body.enable_caching,
            room_id=body.room_id,
        ):
            yield f"data: {event.model_dump_json()}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
    )


@app.post("/orchestration/dag/resume")
async def resume_dag(
    body: DAGResumeRequest,
    user: AuthUser = Depends(require_scope(AuthScope.DAG_RESUME)),
):
    """Resume a checkpointed DAG execution.

    Requires: `dag:resume` scope
    """
    dag = DAGPipeline(**body.dag)

    async def event_generator():
        async for event in dag_executor.execute(
            dag=dag,
            inputs={},
            resume_from=body.execution_id,
        ):
            yield f"data: {event.model_dump_json()}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
    )


@app.post("/orchestration/dag/estimate")
async def estimate_dag(
    body: Dict[str, Any],
    user: AuthUser = Depends(require_scope(AuthScope.DAG_READ)),
):
    """Estimate cost and duration for a DAG before execution.

    Requires: `dag:read` scope
    """
    dag = DAGPipeline(**body)
    return dag.estimate_cost()


# =============================================================================
# WEBHOOKS
# =============================================================================

class WebhookCreateRequest(BaseModel):
    """Create webhook request."""
    url: str
    triggers: List[str] = ["execution_completed"]
    headers: Dict[str, str] = Field(default_factory=dict)
    secret: Optional[str] = None


@app.get("/orchestration/webhooks")
async def list_webhooks(
    user: AuthUser = Depends(require_scope(AuthScope.WEBHOOK_READ)),
):
    """List registered webhooks.

    Requires: `webhook:read` scope
    """
    return {
        "webhooks": [
            {
                "id": w.id,
                "url": w.url,
                "triggers": [t.value for t in w.triggers],
                "enabled": w.enabled,
            }
            for w in webhook_manager.webhooks.values()
        ]
    }


@app.post("/orchestration/webhooks")
async def create_webhook(
    body: WebhookCreateRequest,
    user: AuthUser = Depends(require_scope(AuthScope.WEBHOOK_CREATE)),
):
    """Register a webhook.

    Requires: `webhook:create` scope
    """
    webhook = WebhookConfig(
        url=body.url,
        triggers=[WebhookTrigger(t) for t in body.triggers],
        headers=body.headers,
        secret=body.secret,
    )
    webhook_id = webhook_manager.register(webhook)
    return {"webhook_id": webhook_id, "created_by": user.id}


@app.delete("/orchestration/webhooks/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    user: AuthUser = Depends(require_scope(AuthScope.WEBHOOK_DELETE)),
):
    """Delete a webhook.

    Requires: `webhook:delete` scope
    """
    webhook_manager.unregister(webhook_id)
    return {"status": "deleted", "deleted_by": user.id}


@app.get("/orchestration/webhooks/deliveries")
async def list_webhook_deliveries(
    limit: int = 50,
    user: AuthUser = Depends(require_scope(AuthScope.WEBHOOK_READ)),
):
    """List recent webhook deliveries.

    Requires: `webhook:read` scope
    """
    return {
        "deliveries": [
            {
                "id": d.id,
                "webhook_id": d.webhook_id,
                "trigger": d.trigger.value,
                "status_code": d.status_code,
                "attempts": d.attempts,
                "delivered_at": d.delivered_at.isoformat() if d.delivered_at else None,
                "error": d.error,
            }
            for d in webhook_manager.deliveries[-limit:]
        ]
    }


# =============================================================================
# CACHING
# =============================================================================

@app.get("/orchestration/cache/stats")
async def get_cache_stats(
    user: AuthUser = Depends(require_scope(AuthScope.CACHE_READ)),
):
    """Get cache statistics.

    Requires: `cache:read` scope
    """
    return result_cache.stats()


@app.post("/orchestration/cache/invalidate")
async def invalidate_cache(
    pipeline: Optional[str] = None,
    user: AuthUser = Depends(require_scope(AuthScope.CACHE_INVALIDATE)),
):
    """Invalidate cache entries.

    Requires: `cache:invalidate` scope
    """
    result_cache.invalidate(pipeline)
    return {"status": "invalidated", "pipeline": pipeline or "all", "invalidated_by": user.id}


# =============================================================================
# PRIORITY QUEUE
# =============================================================================

class QueueExecuteRequest(BaseModel):
    """Request to queue an execution."""
    pipeline: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    priority: str = "normal"  # critical, high, normal, low, background


@app.post("/orchestration/queue/enqueue")
async def enqueue_execution(
    body: QueueExecuteRequest,
    user: AuthUser = Depends(require_scope(AuthScope.QUEUE_ENQUEUE)),
):
    """Add execution to priority queue.

    Requires: `queue:enqueue` scope
    """
    priority_map = {
        "critical": ExecutionPriority.CRITICAL,
        "high": ExecutionPriority.HIGH,
        "normal": ExecutionPriority.NORMAL,
        "low": ExecutionPriority.LOW,
        "background": ExecutionPriority.BACKGROUND,
    }

    priority = priority_map.get(body.priority, ExecutionPriority.NORMAL)

    execution_id = await priority_queue.enqueue(
        pipeline=body.pipeline,
        inputs=body.inputs,
        priority=priority,
    )

    return {
        "execution_id": execution_id,
        "priority": body.priority,
        "queue_position": len([
            i for i in priority_queue._queue
            if i.priority <= priority.value
        ]),
        "enqueued_by": user.id,
    }


@app.get("/orchestration/queue/stats")
async def get_queue_stats(
    user: AuthUser = Depends(require_scope(AuthScope.QUEUE_READ)),
):
    """Get queue statistics.

    Requires: `queue:read` scope
    """
    return priority_queue.stats()


# =============================================================================
# COLLABORATION ROOMS
# =============================================================================

@app.post("/orchestration/rooms/create")
async def create_collaboration_room(
    execution_id: str,
    user: AuthUser = Depends(require_scope(AuthScope.ROOM_CREATE)),
):
    """Create a collaboration room for an execution.

    Requires: `room:create` scope
    """
    room_id = collaboration_manager.create_room(execution_id)
    return {"room_id": room_id, "execution_id": execution_id, "created_by": user.id}


@app.get("/orchestration/rooms/{room_id}")
async def get_room_state(
    room_id: str,
    user: AuthUser = Depends(require_scope(AuthScope.ROOM_READ)),
):
    """Get collaboration room state.

    Requires: `room:read` scope
    """
    state = collaboration_manager.get_room_state(room_id)
    if not state:
        raise HTTPException(404, f"Room not found: {room_id}")
    return state


@app.websocket("/orchestration/rooms/{room_id}/join")
async def join_collaboration_room(
    websocket: WebSocket,
    room_id: str,
    name: str = "anonymous",
    api_key: Optional[str] = Query(None, alias="api_key"),
):
    """
    Join a collaboration room for real-time updates.

    Requires: `room:join` scope (pass api_key as query parameter)

    Features:
    - Receive all pipeline events in real-time
    - See other participants
    - Add annotations
    - Chat with other participants

    Protocol:
    1. Connect to /orchestration/rooms/{room_id}/join?api_key=YOUR_KEY
    2. Receive "joined" message with room state
    3. Receive pipeline events as they occur
    4. Send annotations: {"type": "annotate", "data": {...}}
    5. Send chat: {"type": "chat", "message": "..."}
    """
    # Validate auth for WebSocket
    if ORCHESTRATION_AUTH_ENABLED:
        if not api_key:
            await websocket.close(code=4001, reason="Authentication required: pass api_key query parameter")
            return
        user = auth_manager.validate_api_key(api_key)
        if not user:
            await websocket.close(code=4001, reason="Invalid API key")
            return
        if AuthScope.ROOM_JOIN not in user.scopes and AuthScope.ADMIN_ALL not in user.scopes:
            await websocket.close(code=4003, reason="Missing room:join scope")
            return
        participant_id = user.id
    else:
        participant_id = str(uuid4())

    await websocket.accept()

    success = await collaboration_manager.join(
        room_id=room_id,
        participant_id=participant_id,
        websocket=websocket,
        name=name,
    )

    if not success:
        await websocket.close(code=4004, reason="Room not found")
        return

    try:
        # Send join confirmation
        room_state = collaboration_manager.get_room_state(room_id)
        await websocket.send_json({
            "type": "joined",
            "participant_id": participant_id,
            "room": room_state,
        })

        # Handle incoming messages
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "annotate":
                await collaboration_manager.add_annotation(
                    room_id=room_id,
                    participant_id=participant_id,
                    annotation=data.get("data", {}),
                )

            elif msg_type == "chat":
                await collaboration_manager.send_chat(
                    room_id=room_id,
                    participant_id=participant_id,
                    message=data.get("message", ""),
                )

            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        await collaboration_manager.leave(room_id, participant_id)
    except Exception:
        await collaboration_manager.leave(room_id, participant_id)


# =============================================================================
# AUTHENTICATION MANAGEMENT
# =============================================================================

class CreateAPIKeyRequest(BaseModel):
    """Request to create an API key."""
    name: str
    role: str = "operator"  # admin, operator, viewer
    scopes: Optional[List[str]] = None
    expires_days: Optional[int] = None


class CreateTokenRequest(BaseModel):
    """Request to create a JWT token."""
    user_id: str
    role: str = "operator"
    scopes: Optional[List[str]] = None
    expires_hours: Optional[int] = None


@app.get("/orchestration/auth/status")
async def get_auth_status():
    """Get authentication status and configuration."""
    return {
        "enabled": ORCHESTRATION_AUTH_ENABLED,
        "jwt_available": JWT_AVAILABLE,
        "jwt_expiration_hours": JWT_EXPIRATION_HOURS,
        "roles": [r.value for r in AuthRole],
        "scopes": [s.value for s in AuthScope],
        "role_scopes": {
            role.value: [s.value for s in scopes]
            for role, scopes in ROLE_SCOPES.items()
        },
    }


@app.get("/orchestration/auth/me")
async def get_current_auth_user(
    user: AuthUser = Depends(get_current_user),
):
    """Get current authenticated user information."""
    return {
        "id": user.id,
        "role": user.role.value,
        "scopes": [s.value for s in user.scopes],
        "api_key_id": user.api_key_id,
        "token_id": user.token_id,
        "issued_at": user.issued_at.isoformat() if user.issued_at else None,
        "expires_at": user.expires_at.isoformat() if user.expires_at else None,
    }


@app.get("/orchestration/auth/keys")
async def list_api_keys(
    user: AuthUser = Depends(require_role(AuthRole.ADMIN)),
):
    """List all API keys (admin only).

    Requires: `admin` role
    """
    return {"keys": auth_manager.list_api_keys()}


@app.post("/orchestration/auth/keys")
async def create_api_key(
    body: CreateAPIKeyRequest,
    user: AuthUser = Depends(require_role(AuthRole.ADMIN)),
):
    """Create a new API key (admin only).

    Requires: `admin` role

    The key is only returned once - store it securely.
    """
    role = AuthRole(body.role) if body.role in [r.value for r in AuthRole] else AuthRole.OPERATOR
    scopes = None
    if body.scopes:
        scopes = {AuthScope(s) for s in body.scopes if s in [e.value for e in AuthScope]}

    key_id, key = auth_manager.create_api_key(
        name=body.name,
        role=role,
        scopes=scopes,
        expires_days=body.expires_days,
    )

    return {
        "key_id": key_id,
        "key": key,
        "name": body.name,
        "role": role.value,
        "expires_days": body.expires_days,
        "warning": "Store this key securely - it cannot be retrieved again",
    }


@app.delete("/orchestration/auth/keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    user: AuthUser = Depends(require_role(AuthRole.ADMIN)),
):
    """Revoke an API key (admin only).

    Requires: `admin` role
    """
    success = auth_manager.revoke_api_key(key_id)
    if not success:
        raise HTTPException(404, f"API key not found: {key_id}")
    return {"status": "revoked", "key_id": key_id, "revoked_by": user.id}


@app.post("/orchestration/auth/token")
async def create_jwt_token(
    body: CreateTokenRequest,
    user: AuthUser = Depends(require_role(AuthRole.ADMIN)),
):
    """Create a JWT token (admin only).

    Requires: `admin` role
    """
    if not JWT_AVAILABLE:
        raise HTTPException(500, "JWT support not available (install PyJWT)")

    role = AuthRole(body.role) if body.role in [r.value for r in AuthRole] else AuthRole.OPERATOR

    token = auth_manager.create_jwt_token(
        user_id=body.user_id,
        role=role,
        scopes=body.scopes,
        expires_hours=body.expires_hours,
    )

    return {
        "token": token,
        "token_type": "Bearer",
        "expires_hours": body.expires_hours or JWT_EXPIRATION_HOURS,
        "user_id": body.user_id,
        "role": role.value,
    }


@app.post("/orchestration/auth/validate")
async def validate_credentials(
    user: AuthUser = Depends(get_optional_user),
):
    """Validate current credentials without requiring auth."""
    if user.role == AuthRole.ANONYMOUS:
        return {
            "valid": False,
            "role": "anonymous",
            "message": "No valid credentials provided",
        }
    return {
        "valid": True,
        "id": user.id,
        "role": user.role.value,
        "scopes": [s.value for s in user.scopes],
    }


# =============================================================================
# ADVANCED WEBSOCKET
# =============================================================================

class ConnectionManager:
    """Manage WebSocket connections."""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        self.active_connections.pop(client_id, None)

    async def send_event(self, client_id: str, event: PipelineEvent):
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_json(
                event.model_dump(mode="json")
            )

    async def send_json(self, client_id: str, data: dict):
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_json(data)


manager = ConnectionManager()


@app.websocket("/compound/stream")
async def websocket_stream(websocket: WebSocket):
    """
    WebSocket endpoint for streaming compound tool execution.

    Protocol:
    1. Client connects
    2. Server sends: {"type": "connected", "client_id": "..."}
    3. Client sends: {"action": "execute", "pipeline": "deep_research", "inputs": {...}}
    4. Server streams PipelineEvent objects as JSON
    5. Server sends final event with type "pipeline_completed" or "pipeline_failed"

    Example client (JavaScript):
        const ws = new WebSocket('ws://localhost:8000/compound/stream');

        ws.onopen = () => {
            ws.send(JSON.stringify({
                action: 'execute',
                pipeline: 'deep_research',
                inputs: { topic: 'AI Safety' }
            }));
        };

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log(data.event_type, data.step_name, data.step_progress);
        };

    Example client (Python):
        import asyncio
        import websockets
        import json

        async def stream():
            async with websockets.connect('ws://localhost:8000/compound/stream') as ws:
                # Wait for connection confirmation
                msg = await ws.recv()
                print(json.loads(msg))

                # Execute pipeline
                await ws.send(json.dumps({
                    'action': 'execute',
                    'pipeline': 'deep_research',
                    'inputs': {'topic': 'AI Safety'}
                }))

                # Stream events
                async for msg in ws:
                    event = json.loads(msg)
                    print(f"{event['event_type']}: {event.get('step_name', '')}")
                    if event['event_type'] in ['pipeline_completed', 'pipeline_failed']:
                        break

        asyncio.run(stream())
    """
    client_id = str(uuid4())
    await manager.connect(websocket, client_id)

    try:
        # Send connection confirmation
        await manager.send_json(client_id, {
            "type": "connected",
            "client_id": client_id,
            "available_pipelines": list(compound_registry._pipelines.keys()),
        })

        while True:
            # Wait for execution request
            data = await websocket.receive_json()

            action = data.get("action")

            if action == "list":
                # List available pipelines
                await manager.send_json(client_id, {
                    "type": "pipeline_list",
                    "pipelines": [
                        {
                            "name": m.name,
                            "description": m.description,
                            "category": m.category,
                        }
                        for m in compound_registry.list_all()
                    ],
                })

            elif action == "execute":
                pipeline_name = data.get("pipeline")
                inputs = data.get("inputs", {})

                if not pipeline_name:
                    await manager.send_json(client_id, {
                        "type": "error",
                        "error": "Missing 'pipeline' field",
                    })
                    continue

                pipeline = compound_registry.get(pipeline_name)
                if not pipeline:
                    await manager.send_json(client_id, {
                        "type": "error",
                        "error": f"Pipeline not found: {pipeline_name}",
                    })
                    continue

                # Stream execution events
                try:
                    async for event in compound_registry.execute_stream(pipeline_name, **inputs):
                        await manager.send_event(client_id, event)

                except Exception as e:
                    await manager.send_json(client_id, {
                        "type": "error",
                        "error": str(e),
                    })

            elif action == "ping":
                await manager.send_json(client_id, {"type": "pong"})

            else:
                await manager.send_json(client_id, {
                    "type": "error",
                    "error": f"Unknown action: {action}. Use 'execute', 'list', or 'ping'.",
                })

    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        manager.disconnect(client_id)
        raise


@app.post("/compound/execute/stream")
async def execute_compound_stream_sse(body: CompoundExecuteRequest):
    """
    Execute a compound tool with Server-Sent Events (SSE) streaming.

    Alternative to WebSocket for HTTP-only environments.

    Example (curl):
        curl -N -X POST http://localhost:8000/compound/execute/stream \\
            -H "Content-Type: application/json" \\
            -d '{"pipeline": "deep_research", "inputs": {"topic": "AI Safety"}}'

    Example (JavaScript):
        const response = await fetch('/compound/execute/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pipeline: 'deep_research', inputs: { topic: 'AI' } })
        });

        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const lines = decoder.decode(value).split('\\n');
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    const event = JSON.parse(line.slice(6));
                    console.log(event.event_type, event.step_name);
                }
            }
        }

    Example (Python):
        import httpx

        with httpx.stream('POST', 'http://localhost:8000/compound/execute/stream',
                          json={'pipeline': 'deep_research', 'inputs': {'topic': 'AI'}}) as r:
            for line in r.iter_lines():
                if line.startswith('data: '):
                    event = json.loads(line[6:])
                    print(f"{event['event_type']}: {event.get('step_name', '')}")
    """
    pipeline = compound_registry.get(body.pipeline)
    if not pipeline:
        raise HTTPException(404, f"Compound tool not found: {body.pipeline}")

    async def event_generator():
        try:
            async for event in compound_registry.execute_stream(body.pipeline, **body.inputs):
                # Format as SSE
                data = event.model_dump_json()
                yield f"data: {data}\n\n"
        except Exception as e:
            error_data = json.dumps({"type": "error", "error": str(e)})
            yield f"data: {error_data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/compound/stream/{pipeline_name}")
async def websocket_stream_pipeline(websocket: WebSocket, pipeline_name: str):
    """
    Simplified WebSocket endpoint for streaming a specific pipeline.

    Just connect and send inputs as JSON to start streaming.

    Example:
        ws = new WebSocket('ws://localhost:8000/compound/stream/deep_research');
        ws.onopen = () => ws.send(JSON.stringify({ topic: 'AI Safety' }));
        ws.onmessage = (e) => console.log(JSON.parse(e.data));
    """
    pipeline = compound_registry.get(pipeline_name)
    if not pipeline:
        await websocket.close(code=4004, reason=f"Pipeline not found: {pipeline_name}")
        return

    client_id = str(uuid4())
    await manager.connect(websocket, client_id)

    try:
        # Send ready signal
        await manager.send_json(client_id, {
            "type": "ready",
            "pipeline": pipeline_name,
            "description": pipeline.description,
            "steps": len(pipeline.steps),
        })

        # Wait for inputs
        inputs = await websocket.receive_json()

        # Stream execution
        async for event in compound_registry.execute_stream(pipeline_name, **inputs):
            await manager.send_event(client_id, event)

    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        try:
            await manager.send_json(client_id, {
                "type": "error",
                "error": str(e),
            })
        except:
            pass
        manager.disconnect(client_id)


# =============================================================================
# INTELLIGENCE ENDPOINTS
# =============================================================================

class IntentCompileRequest(BaseModel):
    """Request to compile intent into DAG."""
    intent: str
    context: Optional[Dict[str, Any]] = None
    constraints: Optional[Dict[str, Any]] = None


class SemanticSearchRequest(BaseModel):
    """Request for semantic tool search."""
    query: str
    k: int = 10
    threshold: float = 0.0


class IndexToolRequest(BaseModel):
    """Request to index a tool."""
    tool_id: str
    name: str
    description: str
    category: Optional[str] = None
    tags: List[str] = []
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None


class RecordExecutionRequest(BaseModel):
    """Request to record tool execution."""
    tool_id: str
    tool_name: str = ""
    input_data: Dict[str, Any] = {}
    output_data: Optional[Any] = None
    success: bool = True
    latency_ms: float = 0.0
    cost: float = 0.0
    error: Optional[str] = None


class RegisterVariantRequest(BaseModel):
    """Request to register a tool variant."""
    variant_id: str
    tool_id: str
    provider: Optional[str] = None
    version: Optional[str] = None
    weight: float = 1.0


class RouteRequest(BaseModel):
    """Request to route a tool call."""
    tool_id: str
    strategy: Optional[str] = None
    max_latency_ms: Optional[float] = None
    max_cost: Optional[float] = None
    min_success_rate: Optional[float] = None
    latency_weight: float = 0.33
    cost_weight: float = 0.33
    reliability_weight: float = 0.34


@app.get("/intelligence/status")
async def intelligence_status():
    """Get intelligence module status."""
    return {
        "available": INTELLIGENCE_AVAILABLE,
        "components": {
            "tool_algebra": tool_algebra is not None,
            "tool_index": tool_index is not None,
            "execution_memory": execution_memory is not None,
            "intent_compiler": intent_compiler is not None,
            "adaptive_router": adaptive_router is not None,
        },
    }


@app.post("/intelligence/compile")
async def compile_intent(body: IntentCompileRequest):
    """
    Compile natural language intent into executable DAG.

    Example:
        POST /intelligence/compile
        {"intent": "get user data then validate and send notification"}

    Returns compiled DAG with nodes, edges, and execution plan.
    """
    if not INTELLIGENCE_AVAILABLE or not intent_compiler:
        raise HTTPException(503, "Intelligence module not available")

    dag = await intent_compiler.compile(
        intent=body.intent,
        context=body.context,
        constraints=body.constraints,
    )

    explanation = await intent_compiler.explain(dag)

    return {
        "dag": dag.to_dict(),
        "explanation": explanation,
    }


@app.post("/intelligence/compile/validate")
async def compile_and_validate_intent(body: IntentCompileRequest):
    """
    Compile intent with type validation.

    Returns DAG and any type compatibility errors.
    """
    if not INTELLIGENCE_AVAILABLE or not intent_compiler:
        raise HTTPException(503, "Intelligence module not available")

    dag, errors = await intent_compiler.compile_with_validation(
        intent=body.intent,
        context=body.context,
    )

    return {
        "dag": dag.to_dict(),
        "validation_errors": errors,
        "valid": len(errors) == 0,
    }


@app.post("/intelligence/tools/index")
async def index_tool(body: IndexToolRequest):
    """
    Index a tool for semantic search.

    Call this when registering new tools to enable semantic discovery.
    """
    if not INTELLIGENCE_AVAILABLE or not tool_index:
        raise HTTPException(503, "Intelligence module not available")

    await tool_index.add_tool(
        tool_id=body.tool_id,
        tool_name=body.name,
        description=body.description,
        input_schema=body.input_schema,
        output_schema=body.output_schema,
        metadata={"category": body.category, "tags": body.tags},
    )

    return {"status": "indexed", "tool_id": body.tool_id}


@app.post("/intelligence/tools/search")
async def semantic_search(body: SemanticSearchRequest):
    """
    Search for tools using natural language query.

    Example:
        POST /intelligence/tools/search
        {"query": "validate email address", "k": 5}

    Returns tools ranked by semantic similarity.
    """
    if not INTELLIGENCE_AVAILABLE or not tool_index:
        raise HTTPException(503, "Intelligence module not available")

    results = await tool_index.search(
        query=body.query,
        k=body.k,
        threshold=body.threshold,
    )

    return {
        "query": body.query,
        "results": [
            {
                "tool_id": r.tool_id,
                "tool_name": r.tool_name,
                "description": r.description,
                "score": r.score,
            }
            for r in results
        ],
    }


@app.get("/intelligence/tools/similar/{tool_id}")
async def find_similar_tools(tool_id: str, k: int = 5):
    """Find tools similar to the given tool."""
    if not INTELLIGENCE_AVAILABLE or not tool_index:
        raise HTTPException(503, "Intelligence module not available")

    results = await tool_index.find_similar(tool_id, k=k)

    return {
        "tool_id": tool_id,
        "similar": [
            {
                "tool_id": r.tool_id,
                "tool_name": r.tool_name,
                "score": r.score,
            }
            for r in results
        ],
    }


@app.get("/intelligence/tools/clusters")
async def get_tool_clusters(n_clusters: int = 5):
    """Get tool clusters based on semantic similarity."""
    if not INTELLIGENCE_AVAILABLE or not tool_index:
        raise HTTPException(503, "Intelligence module not available")

    clusters = await tool_index.cluster_tools(n_clusters=n_clusters)

    return {
        "n_clusters": n_clusters,
        "clusters": clusters,
    }


@app.post("/intelligence/memory/record")
async def record_execution_endpoint(body: RecordExecutionRequest):
    """
    Record a tool execution for learning.

    The execution memory learns from these records to:
    - Predict failure probability
    - Suggest next tools in sequences
    - Find cheaper alternatives
    - Cache semantically similar results
    """
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    import uuid
    from tool_management.intelligence.memory import ExecutionOutcome

    outcome = ExecutionOutcome.SUCCESS if body.success else ExecutionOutcome.FAILURE

    execution_memory.record_execution(
        execution_id=str(uuid.uuid4()),
        tool_id=body.tool_id,
        tool_name=body.tool_name or body.tool_id,
        input_data=body.input_data,
        output_data=body.output_data,
        outcome=outcome,
        latency_ms=body.latency_ms,
        cost=body.cost,
        error=body.error,
    )

    return {"status": "recorded", "tool_id": body.tool_id}


@app.get("/intelligence/memory/stats/{tool_id}")
async def get_tool_stats_endpoint(tool_id: str):
    """Get execution statistics for a tool."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    stats = execution_memory.get_tool_stats(tool_id)
    if not stats:
        raise HTTPException(404, f"No stats for tool: {tool_id}")

    return {
        "tool_id": tool_id,
        "total_executions": stats.total_executions,
        "success_count": stats.success_count,
        "failure_count": stats.failure_count,
        "ema_latency_ms": stats.ema_latency_ms,
        "ema_success_rate": stats.ema_success_rate,
        "total_cost": stats.total_cost,
    }


@app.get("/intelligence/memory/predict/{tool_id}")
async def predict_failure(tool_id: str):
    """Predict failure probability for a tool."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    prob = execution_memory.predict_failure_probability(tool_id)

    return {
        "tool_id": tool_id,
        "failure_probability": prob,
        "reliability": 1.0 - prob,
    }


@app.post("/intelligence/memory/suggest")
async def suggest_next_tools(
    current_tools: List[str],
    available_tools: List[str],
):
    """Suggest next tools based on execution patterns."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    suggestions = execution_memory.suggest_sequence(current_tools, available_tools)

    return {
        "current": current_tools,
        "suggestions": [
            {"tool_id": tid, "score": score}
            for tid, score in suggestions
        ],
    }


@app.get("/intelligence/memory/alternative/{tool_id}")
async def find_cheaper_alternative(tool_id: str):
    """Find a cheaper alternative to the given tool."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    alt = execution_memory.find_cheaper_alternative(tool_id)
    if not alt:
        return {"tool_id": tool_id, "alternative": None}

    alt_id, savings = alt
    return {
        "tool_id": tool_id,
        "alternative": alt_id,
        "cost_savings": savings,
    }


@app.get("/intelligence/memory/hot")
async def get_hot_tools(limit: int = 10):
    """Get most frequently used tools."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    hot = execution_memory.get_hot_tools(limit=limit)

    return {
        "hot_tools": [
            {"tool_id": tid, "executions": count}
            for tid, count in hot
        ],
    }


@app.get("/intelligence/memory/failing")
async def get_failing_tools(threshold: float = 0.3, limit: int = 10):
    """Get tools with high failure rates."""
    if not INTELLIGENCE_AVAILABLE or not execution_memory:
        raise HTTPException(503, "Intelligence module not available")

    failing = execution_memory.get_failing_tools(threshold=threshold, limit=limit)

    return {
        "threshold": threshold,
        "failing_tools": [
            {"tool_id": tid, "failure_rate": rate}
            for tid, rate in failing
        ],
    }


@app.post("/intelligence/router/variants")
async def register_variant(body: RegisterVariantRequest):
    """
    Register a tool variant for adaptive routing.

    Variants are different implementations of the same logical tool
    (e.g., different providers, versions, or regions).
    """
    if not INTELLIGENCE_AVAILABLE or not adaptive_router:
        raise HTTPException(503, "Intelligence module not available")

    variant = ToolVariant(
        variant_id=body.variant_id,
        tool_id=body.tool_id,
        provider=body.provider,
        version=body.version,
        weight=body.weight,
    )

    adaptive_router.register_variant(variant)

    return {"status": "registered", "variant_id": body.variant_id}


@app.post("/intelligence/router/route")
async def route_tool(body: RouteRequest):
    """
    Route a tool call to the best variant.

    Uses execution history to select the optimal variant based on:
    - Latency
    - Cost
    - Reliability
    - User preferences (weights)
    """
    if not INTELLIGENCE_AVAILABLE or not adaptive_router:
        raise HTTPException(503, "Intelligence module not available")

    strategy_map = {
        "lowest_latency": RoutingStrategy.LOWEST_LATENCY,
        "lowest_cost": RoutingStrategy.LOWEST_COST,
        "highest_success": RoutingStrategy.HIGHEST_SUCCESS,
        "balanced": RoutingStrategy.BALANCED,
        "round_robin": RoutingStrategy.ROUND_ROBIN,
        "random": RoutingStrategy.RANDOM,
        "weighted": RoutingStrategy.WEIGHTED,
        "epsilon_greedy": RoutingStrategy.EPSILON_GREEDY,
    }

    strategy = None
    if body.strategy:
        strategy = strategy_map.get(body.strategy.lower())

    context = RoutingContext(
        tool_id=body.tool_id,
        max_latency_ms=body.max_latency_ms,
        max_cost=body.max_cost,
        min_success_rate=body.min_success_rate,
        latency_weight=body.latency_weight,
        cost_weight=body.cost_weight,
        reliability_weight=body.reliability_weight,
    )

    try:
        decision = await adaptive_router.route(context, strategy)
    except ValueError as e:
        raise HTTPException(404, str(e))

    return {
        "selected_variant": {
            "variant_id": decision.selected_variant.variant_id,
            "tool_id": decision.selected_variant.tool_id,
            "provider": decision.selected_variant.provider,
            "avg_latency_ms": decision.selected_variant.avg_latency_ms,
            "avg_cost": decision.selected_variant.avg_cost,
            "success_rate": decision.selected_variant.success_rate,
        },
        "reason": decision.reason,
        "score": decision.score,
        "alternatives_count": len(decision.alternatives),
    }


@app.get("/intelligence/router/circuits")
async def get_circuit_states():
    """Get all circuit breaker states."""
    if not INTELLIGENCE_AVAILABLE or not adaptive_router:
        raise HTTPException(503, "Intelligence module not available")

    return {
        "circuits": adaptive_router.get_all_circuit_states(),
    }


@app.post("/intelligence/router/circuits/{variant_id}/reset")
async def reset_circuit_breaker(variant_id: str):
    """Manually reset a circuit breaker."""
    if not INTELLIGENCE_AVAILABLE or not adaptive_router:
        raise HTTPException(503, "Intelligence module not available")

    success = adaptive_router.reset_circuit_breaker(variant_id)
    if not success:
        raise HTTPException(404, f"Circuit breaker not found: {variant_id}")

    return {"status": "reset", "variant_id": variant_id}


@app.get("/intelligence/router/stats")
async def get_routing_stats():
    """Get routing statistics."""
    if not INTELLIGENCE_AVAILABLE or not adaptive_router:
        raise HTTPException(503, "Intelligence module not available")

    return {
        "routing_stats": adaptive_router.get_routing_stats(),
    }


@app.post("/intelligence/algebra/register")
async def register_tool_signature(
    tool_id: str,
    input_type: Dict[str, Any],
    output_type: Dict[str, Any],
):
    """Register a tool's type signature for composition checking."""
    if not INTELLIGENCE_AVAILABLE or not tool_algebra:
        raise HTTPException(503, "Intelligence module not available")

    from tool_management.intelligence import ToolType, ToolSignature

    in_type = ToolType.from_json_schema(input_type)
    out_type = ToolType.from_json_schema(output_type)

    sig = ToolSignature(
        tool_id=tool_id,
        input_type=in_type,
        output_type=out_type,
    )

    tool_algebra.register(sig)

    return {"status": "registered", "tool_id": tool_id}


@app.post("/intelligence/algebra/check")
async def check_composition(
    source_tool_id: str,
    target_tool_id: str,
):
    """Check if two tools can be composed (output→input compatibility)."""
    if not INTELLIGENCE_AVAILABLE or not tool_algebra:
        raise HTTPException(503, "Intelligence module not available")

    source_sig = tool_algebra.get_signature(source_tool_id)
    target_sig = tool_algebra.get_signature(target_tool_id)

    if not source_sig:
        raise HTTPException(404, f"Source tool not registered: {source_tool_id}")
    if not target_sig:
        raise HTTPException(404, f"Target tool not registered: {target_tool_id}")

    from tool_management.intelligence import TypeChecker

    checker = TypeChecker()
    result = checker.check_compatibility(source_sig.output_type, target_sig.input_type)

    return {
        "source": source_tool_id,
        "target": target_tool_id,
        "compatible": result.level.name != "INCOMPATIBLE",
        "compatibility_level": result.level.name,
        "coercion_required": result.coercion,
    }


@app.get("/intelligence/algebra/path")
async def find_composition_path(
    source_tool_id: str,
    target_tool_id: str,
    max_depth: int = 5,
):
    """Find a path of tools to compose from source to target type."""
    if not INTELLIGENCE_AVAILABLE or not tool_algebra:
        raise HTTPException(503, "Intelligence module not available")

    source_sig = tool_algebra.get_signature(source_tool_id)
    target_sig = tool_algebra.get_signature(target_tool_id)

    if not source_sig:
        raise HTTPException(404, f"Source tool not registered: {source_tool_id}")
    if not target_sig:
        raise HTTPException(404, f"Target tool not registered: {target_tool_id}")

    path = tool_algebra.find_path(
        source_sig.output_type,
        target_sig.input_type,
        max_depth=max_depth,
    )

    if not path:
        return {
            "source": source_tool_id,
            "target": target_tool_id,
            "path": None,
            "message": "No composition path found",
        }

    return {
        "source": source_tool_id,
        "target": target_tool_id,
        "path": [sig.tool_id for sig in path],
    }


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    auth_status = "ENABLED" if ORCHESTRATION_AUTH_ENABLED else "DISABLED"
    intel_status = "AVAILABLE" if INTELLIGENCE_AVAILABLE else "NOT AVAILABLE"
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                      Tool Registry                           ║
╠══════════════════════════════════════════════════════════════╣
║  Central registry for tool discovery and routing             ║
║  + 50 Compound Tools with WebSocket streaming                ║
║  + DAG Orchestration with authentication                     ║
║  + SOTA Intelligence (semantic search, learning, routing)    ║
╚══════════════════════════════════════════════════════════════╝

Authentication: {auth_status}
Intelligence: {intel_status}
  Set ORCHESTRATION_AUTH_ENABLED=false to disable
  Set ORCHESTRATION_ADMIN_KEY to configure admin API key
  Set ORCHESTRATION_OPERATOR_KEYS for operator keys (comma-separated)

Endpoints:
  Services:
    • GET  /services              List services
    • POST /services              Register service
    • GET  /services/{id}         Get service
    • POST /services/{id}/heartbeat  Heartbeat
    • DELETE /services/{id}       Remove service

  Tools:
    • GET  /tools                 List tools
    • POST /tools                 Register tool
    • GET  /tools/{id}            Get tool
    • DELETE /tools/{id}          Remove tool

  Execution:
    • POST /execute               Execute tool (routes to service)

  Compound Tools (DSPy Pipelines):
    • GET  /compound/tools        List compound tools
    • GET  /compound/categories   List categories
    • POST /compound/execute      Execute (non-streaming)

  Streaming:
    • POST /compound/execute/stream    SSE streaming (HTTP)
    • WS   /compound/stream            WebSocket streaming
    • WS   /compound/stream/{{name}}     WebSocket for specific pipeline

  Advanced Streaming:
    • POST /compound/execute/advanced  SSE with config (filtering, metrics)
    • POST /compound/execute/parallel  Run multiple pipelines in parallel
    • POST /compound/execute/chain     Chain pipelines (output→input)

  Execution Control:
    • GET  /compound/executions                  List executions
    • GET  /compound/executions/{{id}}             Get execution details
    • POST /compound/executions/{{id}}/cancel      Cancel execution
    • POST /compound/executions/{{id}}/pause       Pause execution
    • POST /compound/executions/{{id}}/resume      Resume execution
    • POST /compound/executions/{{id}}/replay      Replay past execution

  Metrics:
    • GET  /compound/metrics          Executor & connection metrics

  Orchestration (requires auth):
    • POST /orchestration/dag/execute       Execute DAG pipeline
    • POST /orchestration/dag/resume        Resume checkpointed DAG
    • POST /orchestration/dag/estimate      Estimate DAG cost
    • GET  /orchestration/webhooks          List webhooks
    • POST /orchestration/webhooks          Create webhook
    • DELETE /orchestration/webhooks/{{id}}   Delete webhook
    • GET  /orchestration/cache/stats       Cache statistics
    • POST /orchestration/cache/invalidate  Invalidate cache
    • POST /orchestration/queue/enqueue     Add to priority queue
    • GET  /orchestration/queue/stats       Queue statistics
    • POST /orchestration/rooms/create      Create collaboration room
    • GET  /orchestration/rooms/{{id}}        Get room state
    • WS   /orchestration/rooms/{{id}}/join   Join room (real-time)

  Authentication Management:
    • GET  /orchestration/auth/status       Auth configuration
    • GET  /orchestration/auth/me           Current user info
    • GET  /orchestration/auth/keys         List API keys (admin)
    • POST /orchestration/auth/keys         Create API key (admin)
    • DELETE /orchestration/auth/keys/{{id}}  Revoke API key (admin)
    • POST /orchestration/auth/token        Create JWT token (admin)
    • POST /orchestration/auth/validate     Validate credentials

  Intelligence (SOTA features):
    • GET  /intelligence/status             Module status
    • POST /intelligence/compile            Compile intent → DAG
    • POST /intelligence/compile/validate   Compile with type validation
    • POST /intelligence/tools/index        Index tool for semantic search
    • POST /intelligence/tools/search       Semantic tool search
    • GET  /intelligence/tools/similar/{{id}} Find similar tools
    • GET  /intelligence/tools/clusters     Get tool clusters
    • POST /intelligence/memory/record      Record execution
    • GET  /intelligence/memory/stats/{{id}}  Get tool stats
    • GET  /intelligence/memory/predict/{{id}} Predict failure
    • POST /intelligence/memory/suggest     Suggest next tools
    • GET  /intelligence/memory/hot         Get hot tools
    • GET  /intelligence/memory/failing     Get failing tools
    • POST /intelligence/router/variants    Register tool variant
    • POST /intelligence/router/route       Route to best variant
    • GET  /intelligence/router/circuits    Circuit breaker states
    • GET  /intelligence/router/stats       Routing statistics
    • POST /intelligence/algebra/register   Register type signature
    • POST /intelligence/algebra/check      Check composition
    • GET  /intelligence/algebra/path       Find composition path

Starting server on port 8000...
""")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )

"""
Ingestion Service API Server

FastAPI server for document ingestion.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .models import IngestionJob, IngestionStatus, DocumentType
from .pipeline import IngestionPipeline, PipelineConfig

logger = logging.getLogger(__name__)


# ============================================================================
# Pydantic Models
# ============================================================================

class IngestURLRequest(BaseModel):
    """Request to ingest from a URL."""
    url: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    doc_type: Optional[str] = None


class IngestTextRequest(BaseModel):
    """Request to ingest raw text."""
    text: str
    source: str = "text_input"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    doc_type: str = "text"


class JobResponse(BaseModel):
    """Response with job information."""
    job_id: str
    status: str
    progress: float
    source_path: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    document_id: Optional[str] = None
    chunk_count: int = 0
    entity_count: int = 0
    relation_count: int = 0


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    version: str = "0.1.0"


# ============================================================================
# Server Class
# ============================================================================

class IngestionServer:
    """FastAPI server for ingestion service."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.pipeline = IngestionPipeline(self.config)
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Create FastAPI application."""
        app = FastAPI(
            title="Ingestion Service",
            description="Document ingestion and processing pipeline",
            version="0.1.0",
        )

        # CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self._register_routes(app)
        return app

    def _register_routes(self, app: FastAPI):
        """Register all routes."""

        @app.get("/health", response_model=HealthResponse)
        async def health_check():
            """Health check endpoint."""
            return HealthResponse(
                status="healthy",
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        @app.post("/ingest/file", response_model=JobResponse)
        async def ingest_file(
            background_tasks: BackgroundTasks,
            file: UploadFile = File(...),
            metadata: Optional[str] = None,
        ):
            """
            Ingest an uploaded file.

            The file is processed asynchronously. Use GET /jobs/{job_id}
            to check progress.
            """
            # Read file content
            content = await file.read()

            # Parse metadata if provided
            meta = {}
            if metadata:
                import json
                try:
                    meta = json.loads(metadata)
                except json.JSONDecodeError:
                    raise HTTPException(400, "Invalid metadata JSON")

            # Create job
            job = IngestionJob(
                source_type="file",
                source_path=file.filename or "uploaded_file",
                config={"metadata": meta},
            )

            # Start background processing
            background_tasks.add_task(
                self._process_file,
                job,
                file.filename or "uploaded_file",
                content,
                meta,
            )

            return self._job_to_response(job)

        @app.post("/ingest/url", response_model=JobResponse)
        async def ingest_url(
            background_tasks: BackgroundTasks,
            request: IngestURLRequest,
        ):
            """
            Ingest from a URL.

            The URL is fetched and processed asynchronously.
            """
            # Create job
            job = IngestionJob(
                source_type="url",
                source_path=request.url,
                config={"metadata": request.metadata},
            )

            # Determine doc type
            doc_type = None
            if request.doc_type:
                try:
                    doc_type = DocumentType(request.doc_type)
                except ValueError:
                    pass

            # Start background processing
            background_tasks.add_task(
                self._process_url,
                job,
                request.url,
                request.metadata,
                doc_type,
            )

            return self._job_to_response(job)

        @app.post("/ingest/text", response_model=JobResponse)
        async def ingest_text(
            background_tasks: BackgroundTasks,
            request: IngestTextRequest,
        ):
            """
            Ingest raw text directly.

            Useful for processing text already extracted from documents.
            """
            # Create job
            job = IngestionJob(
                source_type="text",
                source_path=request.source,
                config={"metadata": request.metadata},
            )

            # Start background processing
            background_tasks.add_task(
                self._process_text,
                job,
                request.text,
                request.source,
                request.metadata,
            )

            return self._job_to_response(job)

        @app.get("/jobs/{job_id}", response_model=JobResponse)
        async def get_job(job_id: str):
            """Get job status by ID."""
            job = self.pipeline.get_job(job_id)
            if not job:
                raise HTTPException(404, f"Job {job_id} not found")
            return self._job_to_response(job)

        @app.get("/jobs", response_model=List[JobResponse])
        async def list_jobs(
            status: Optional[str] = Query(None, description="Filter by status"),
            limit: int = Query(100, ge=1, le=1000),
        ):
            """List all jobs with optional filtering."""
            status_filter = None
            if status:
                try:
                    status_filter = IngestionStatus(status)
                except ValueError:
                    raise HTTPException(400, f"Invalid status: {status}")

            jobs = self.pipeline.list_jobs(status_filter)
            jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)
            jobs = jobs[:limit]

            return [self._job_to_response(j) for j in jobs]

        @app.delete("/jobs/{job_id}")
        async def cancel_job(job_id: str):
            """Cancel a running job."""
            job = self.pipeline.get_job(job_id)
            if not job:
                raise HTTPException(404, f"Job {job_id} not found")

            if job.status in (IngestionStatus.COMPLETED, IngestionStatus.FAILED):
                raise HTTPException(400, f"Job {job_id} is already {job.status.value}")

            job.status = IngestionStatus.CANCELLED
            job.completed_at = datetime.now(timezone.utc)

            return {"job_id": job_id, "status": "cancelled"}

    async def _process_file(
        self,
        job: IngestionJob,
        filename: str,
        content: bytes,
        metadata: Dict[str, Any],
    ):
        """Process an uploaded file."""
        try:
            await self.pipeline.ingest(
                source=filename,
                content=content,
                metadata=metadata,
            )
        except Exception as e:
            logger.error(f"File ingestion failed: {e}")
            job.fail(str(e))

    async def _process_url(
        self,
        job: IngestionJob,
        url: str,
        metadata: Dict[str, Any],
        doc_type: Optional[DocumentType],
    ):
        """Process a URL."""
        try:
            await self.pipeline.ingest(
                source=url,
                doc_type=doc_type,
                metadata=metadata,
            )
        except Exception as e:
            logger.error(f"URL ingestion failed: {e}")
            job.fail(str(e))

    async def _process_text(
        self,
        job: IngestionJob,
        text: str,
        source: str,
        metadata: Dict[str, Any],
    ):
        """Process raw text."""
        try:
            await self.pipeline.ingest(
                source=source,
                content=text.encode('utf-8'),
                doc_type=DocumentType.TEXT,
                metadata=metadata,
            )
        except Exception as e:
            logger.error(f"Text ingestion failed: {e}")
            job.fail(str(e))

    def _job_to_response(self, job: IngestionJob) -> JobResponse:
        """Convert job to response model."""
        return JobResponse(
            job_id=job.id,
            status=job.status.value,
            progress=job.progress,
            source_path=job.source_path,
            created_at=job.created_at.isoformat(),
            started_at=job.started_at.isoformat() if job.started_at else None,
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
            error=job.error,
            document_id=job.document_id,
            chunk_count=job.chunk_count,
            entity_count=job.entity_count,
            relation_count=job.relation_count,
        )


def create_ingestion_app(config: Optional[PipelineConfig] = None) -> FastAPI:
    """Create the ingestion service FastAPI app."""
    server = IngestionServer(config)
    return server.app


# Default app instance
app = create_ingestion_app()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("INGESTION_PORT", "2015"))
    uvicorn.run(app, host="0.0.0.0", port=port)

"""
ChromaDB-backed vector store with explicit OpenAI embedding alignment.

CRITICAL FIX: The old code used ChromaDB's default 384-dim embedder for queries
but stored 3072-dim OpenAI embeddings at ingest time, causing dimension mismatch.
Now all embedding is done explicitly through OpenAI before hitting ChromaDB.
"""

import json
import time
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable

import chromadb
import openai

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embedding function factory
# ---------------------------------------------------------------------------

def make_openai_embed_fn(model: str = "text-embedding-3-large") -> Callable:
    """
    Create an embedding function backed by OpenAI.
    Returns a callable: embed(text) -> List[float]
    Also exposes .batch(texts) -> List[List[float]] and .model attribute.
    """
    client = openai.OpenAI()

    def embed_one(text: str) -> List[float]:
        resp = client.embeddings.create(model=model, input=[text])
        return resp.data[0].embedding

    def embed_batch(texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        all_embs: List[List[float]] = []
        for i in range(0, len(texts), 2048):
            batch = texts[i : i + 2048]
            resp = client.embeddings.create(model=model, input=batch)
            all_embs.extend([d.embedding for d in resp.data])
        return all_embs

    embed_one.batch = embed_batch  # type: ignore[attr-defined]
    embed_one.model = model  # type: ignore[attr-defined]
    return embed_one


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class VideoStore:
    """Manages ChromaDB collections for video content with aligned embeddings."""

    def __init__(
        self,
        persist_dir: str = "video_chroma",
        embedding_model: str = "text-embedding-3-large",
    ):
        self.persist_dir = persist_dir
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.embed_fn = make_openai_embed_fn(embedding_model)

        # Collections — no default EF; we always supply explicit embeddings
        self.transcripts = self.client.get_or_create_collection(
            name="transcripts", metadata={"hnsw:space": "cosine"}
        )
        self.frames = self.client.get_or_create_collection(
            name="frames", metadata={"hnsw:space": "cosine"}
        )
        self.videos = self.client.get_or_create_collection(
            name="videos", metadata={"hnsw:space": "cosine"}
        )
        self.annotations = self.client.get_or_create_collection(
            name="annotations", metadata={"hnsw:space": "cosine"}
        )

    # ---- ingest ------------------------------------------------------------

    def ingest(self, video) -> Dict[str, int]:
        """Ingest a ProcessedVideo into the store. Returns counts by type."""
        vid = video.metadata.video_id

        # --- video-level metadata ---
        self.videos.upsert(
            ids=[vid],
            documents=[
                json.dumps(
                    {
                        "title": video.metadata.title,
                        "channel": video.metadata.channel,
                        "duration_sec": video.metadata.duration_sec,
                        "url": video.metadata.url,
                        "upload_date": video.metadata.upload_date,
                        "description": video.metadata.description[:2000],
                        "output_dir": video.output_dir,
                        "full_transcript": video.full_transcript[:50000],
                    }
                )
            ],
            metadatas=[
                {
                    "video_id": vid,
                    "title": video.metadata.title,
                    "duration_sec": video.metadata.duration_sec,
                    "output_dir": video.output_dir,
                }
            ],
        )

        # --- transcript chunks ---
        chunk_ids, chunk_docs, chunk_metas, chunk_embs = [], [], [], []
        for c in video.transcript_chunks:
            cid = f"{vid}_chunk_{c.index}"
            chunk_ids.append(cid)
            chunk_docs.append(c.text)
            chunk_metas.append(
                {
                    "video_id": vid,
                    "chunk_index": c.index,
                    "start_sec": c.start_sec,
                    "end_sec": c.end_sec,
                    "type": "transcript",
                }
            )
            if c.embedding:
                chunk_embs.append(c.embedding)

        if chunk_ids:
            kwargs: Dict[str, Any] = dict(
                ids=chunk_ids, documents=chunk_docs, metadatas=chunk_metas
            )
            if chunk_embs and len(chunk_embs) == len(chunk_ids):
                kwargs["embeddings"] = chunk_embs
            else:
                logger.info("Generating transcript embeddings via OpenAI …")
                kwargs["embeddings"] = self.embed_fn.batch(chunk_docs)
            self.transcripts.upsert(**kwargs)

        # --- frame descriptions ---
        frame_ids, frame_docs, frame_metas, frame_embs = [], [], [], []
        for f in video.frames:
            desc = f.description or f"Frame at {f.timestamp_sec:.1f}s"
            fid = f"{vid}_frame_{f.index}"
            frame_ids.append(fid)
            doc_text = f"{f.timestamp_sec:.1f}s: {desc}"
            ocr = getattr(f, "ocr_text", "") or ""
            if ocr:
                doc_text += f" [OCR: {ocr}]"
            frame_docs.append(doc_text)
            frame_metas.append(
                {
                    "video_id": vid,
                    "frame_index": f.index,
                    "timestamp_sec": f.timestamp_sec,
                    "image_path": f.image_path,
                    "has_ocr": bool(ocr),
                    "is_scene_change": getattr(f, "is_scene_change", False),
                    "type": "frame",
                }
            )
            if f.embedding:
                frame_embs.append(f.embedding)

        if frame_ids:
            kwargs = dict(ids=frame_ids, documents=frame_docs, metadatas=frame_metas)
            if frame_embs and len(frame_embs) == len(frame_ids):
                kwargs["embeddings"] = frame_embs
            else:
                logger.info("Generating frame embeddings via OpenAI …")
                kwargs["embeddings"] = self.embed_fn.batch(frame_docs)
            self.frames.upsert(**kwargs)

        counts = {"transcript_chunks": len(chunk_ids), "frames": len(frame_ids)}
        logger.info(f"Ingested {vid}: {counts}")
        return counts

    # ---- search (FIXED: always use explicit OpenAI embeddings) -------------

    def search_transcript(
        self,
        query: str,
        video_id: Optional[str] = None,
        n_results: int = 5,
    ) -> List[Dict]:
        count = self.transcripts.count()
        if count == 0:
            return []
        query_emb = self.embed_fn(query)
        kwargs: Dict[str, Any] = {
            "query_embeddings": [query_emb],
            "n_results": min(n_results, count),
        }
        if video_id:
            kwargs["where"] = {"video_id": video_id}
        results = self.transcripts.query(**kwargs)
        return self._format_results(results)

    def search_frames(
        self,
        query: str,
        video_id: Optional[str] = None,
        n_results: int = 5,
    ) -> List[Dict]:
        count = self.frames.count()
        if count == 0:
            return []
        query_emb = self.embed_fn(query)
        kwargs: Dict[str, Any] = {
            "query_embeddings": [query_emb],
            "n_results": min(n_results, count),
        }
        if video_id:
            kwargs["where"] = {"video_id": video_id}
        results = self.frames.query(**kwargs)
        return self._format_results(results)

    def search_all(
        self,
        query: str,
        video_id: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, List[Dict]]:
        return {
            "transcript": self.search_transcript(query, video_id, n_results),
            "frames": self.search_frames(query, video_id, n_results),
        }

    # ---- range / positional retrieval -------------------------------------

    def get_chunks_in_range(
        self, video_id: str, start_sec: float, end_sec: float
    ) -> List[Dict]:
        results = self.transcripts.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return []
        out = []
        for i, doc in enumerate(results["documents"]):
            meta = results["metadatas"][i]
            cs, ce = meta.get("start_sec", 0), meta.get("end_sec", 0)
            if cs <= end_sec and ce >= start_sec:
                out.append({"text": doc, "metadata": meta, "id": results["ids"][i]})
        out.sort(key=lambda c: c["metadata"].get("start_sec", 0))
        return out

    def get_frames_in_range(
        self, video_id: str, start_sec: float, end_sec: float
    ) -> List[Dict]:
        results = self.frames.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return []
        out = []
        for i, doc in enumerate(results["documents"]):
            meta = results["metadatas"][i]
            ts = meta.get("timestamp_sec", 0)
            if start_sec <= ts <= end_sec:
                out.append({"text": doc, "metadata": meta, "id": results["ids"][i]})
        out.sort(key=lambda f: f["metadata"].get("timestamp_sec", 0))
        return out

    def get_frame_nearest(self, video_id: str, timestamp_sec: float) -> Optional[Dict]:
        results = self.frames.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return None
        best, best_dist = None, float("inf")
        for i, doc in enumerate(results["documents"]):
            meta = results["metadatas"][i]
            dist = abs(meta.get("timestamp_sec", 0) - timestamp_sec)
            if dist < best_dist:
                best_dist = dist
                best = {"text": doc, "metadata": meta, "id": results["ids"][i]}
        return best

    def get_frame_by_index(self, video_id: str, frame_index: int) -> Optional[Dict]:
        fid = f"{video_id}_frame_{frame_index}"
        results = self.frames.get(ids=[fid])
        if not results or not results.get("documents"):
            return None
        return {
            "text": results["documents"][0],
            "metadata": results["metadatas"][0] if results.get("metadatas") else {},
            "id": fid,
        }

    # ---- bulk getters ------------------------------------------------------

    def get_all_chunks(self, video_id: str) -> List[Dict]:
        results = self.transcripts.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return []
        chunks = []
        for i, doc in enumerate(results["documents"]):
            chunks.append({"text": doc, "metadata": results["metadatas"][i]})
        chunks.sort(key=lambda c: c["metadata"].get("start_sec", 0))
        return chunks

    def get_all_frames(self, video_id: str) -> List[Dict]:
        results = self.frames.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return []
        frames = []
        for i, doc in enumerate(results["documents"]):
            frames.append({"text": doc, "metadata": results["metadatas"][i]})
        frames.sort(key=lambda f: f["metadata"].get("timestamp_sec", 0))
        return frames

    # ---- video info --------------------------------------------------------

    def list_videos(self) -> List[Dict]:
        results = self.videos.get()
        videos = []
        for i, doc in enumerate(results.get("documents", [])):
            meta = results["metadatas"][i] if results.get("metadatas") else {}
            vid_id = results["ids"][i]
            try:
                data = json.loads(doc)
                data.pop("full_transcript", None)  # don't include in listing
            except (json.JSONDecodeError, TypeError):
                data = {}
            videos.append({"video_id": vid_id, **data, **meta})
        return videos

    def get_video_info(self, video_id: str) -> Optional[Dict]:
        results = self.videos.get(ids=[video_id])
        if not results or not results.get("documents"):
            return None
        data = json.loads(results["documents"][0])
        meta = results["metadatas"][0] if results.get("metadatas") else {}
        return {"video_id": video_id, **data, **meta}

    def get_video_transcript(self, video_id: str) -> str:
        info = self.get_video_info(video_id)
        return info.get("full_transcript", "") if info else ""

    # ---- annotations -------------------------------------------------------

    def add_annotation(
        self, video_id: str, timestamp_sec: float, note: str, tag: str = ""
    ) -> str:
        ann_id = f"{video_id}_ann_{int(timestamp_sec)}_{int(time.time())}"
        embedding = self.embed_fn(note)
        self.annotations.upsert(
            ids=[ann_id],
            documents=[note],
            embeddings=[embedding],
            metadatas=[
                {
                    "video_id": video_id,
                    "timestamp_sec": timestamp_sec,
                    "tag": tag,
                    "created_at": time.time(),
                }
            ],
        )
        return ann_id

    def get_annotations(self, video_id: str) -> List[Dict]:
        results = self.annotations.get(where={"video_id": video_id})
        if not results or not results.get("documents"):
            return []
        anns = []
        for i, doc in enumerate(results["documents"]):
            meta = results["metadatas"][i]
            anns.append({"note": doc, **meta, "id": results["ids"][i]})
        anns.sort(key=lambda a: a.get("timestamp_sec", 0))
        return anns

    def delete_annotation(self, annotation_id: str):
        self.annotations.delete(ids=[annotation_id])

    # ---- delete ------------------------------------------------------------

    def delete_video(self, video_id: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for name, col in [
            ("transcripts", self.transcripts),
            ("frames", self.frames),
            ("annotations", self.annotations),
        ]:
            try:
                results = col.get(where={"video_id": video_id})
                if results and results.get("ids"):
                    col.delete(ids=results["ids"])
                    counts[name] = len(results["ids"])
                else:
                    counts[name] = 0
            except Exception:
                counts[name] = 0
        try:
            self.videos.delete(ids=[video_id])
            counts["video"] = 1
        except Exception:
            counts["video"] = 0
        return counts

    # ---- helpers -----------------------------------------------------------

    def _format_results(self, results: Dict) -> List[Dict]:
        formatted = []
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        ids = results.get("ids", [[]])[0]
        for i in range(len(docs)):
            entry: Dict[str, Any] = {
                "id": ids[i],
                "text": docs[i],
                "metadata": metas[i] if i < len(metas) else {},
                "distance": distances[i] if i < len(distances) else None,
            }
            if entry["distance"] is not None:
                entry["relevance_score"] = round(1.0 - entry["distance"], 4)
            formatted.append(entry)
        return formatted

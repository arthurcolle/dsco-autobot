"""
Local Content Fetcher - Extract web content without external API costs.

Uses trafilatura for high-quality content extraction, with fallbacks
to other free methods (requests + basic parsing).

Install: pip install trafilatura httpx beautifulsoup4 lxml
"""

import asyncio
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# Try to import trafilatura (primary extraction method)
try:
    import trafilatura
    from trafilatura.settings import use_config
    TRAFILATURA_AVAILABLE = True

    # Configure trafilatura for better extraction
    TRAF_CONFIG = use_config()
    TRAF_CONFIG.set("DEFAULT", "EXTRACTION_TIMEOUT", "30")
    TRAF_CONFIG.set("DEFAULT", "MIN_OUTPUT_SIZE", "100")
except ImportError:
    TRAFILATURA_AVAILABLE = False
    logger.warning("trafilatura not available - install with: pip install trafilatura")

# Try to import beautifulsoup as fallback
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("beautifulsoup4 not available - install with: pip install beautifulsoup4 lxml")


@dataclass
class FetchResult:
    """Result of fetching and extracting content from a URL."""
    url: str
    success: bool
    title: Optional[str] = None
    content: Optional[str] = None
    description: Optional[str] = None
    content_hash: Optional[str] = None
    word_count: int = 0
    fetch_time_ms: int = 0
    method: Optional[str] = None  # 'trafilatura', 'bs4', 'raw'
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "success": self.success,
            "title": self.title,
            "content": self.content,
            "description": self.description,
            "content_hash": self.content_hash,
            "word_count": self.word_count,
            "fetch_time_ms": self.fetch_time_ms,
            "method": self.method,
            "error": self.error,
            "metadata": self.metadata,
        }


class ContentFetcher:
    """
    Fetch and extract content from URLs using local tools.

    Primary: trafilatura (best quality)
    Fallback: beautifulsoup4 (basic extraction)
    Last resort: raw text extraction
    """

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    # Domains to skip (paywalls, login walls, etc.)
    SKIP_DOMAINS = {
        "facebook.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "tiktok.com", "pinterest.com",
    }

    def __init__(
        self,
        timeout: float = 30.0,
        max_content_length: int = 500_000,  # 500KB max
        min_content_length: int = 100,
        executor: Optional[ThreadPoolExecutor] = None,
    ):
        self.timeout = timeout
        self.max_content_length = max_content_length
        self.min_content_length = min_content_length
        self._executor = executor or ThreadPoolExecutor(max_workers=4)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=self.DEFAULT_HEADERS,
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _should_skip(self, url: str) -> bool:
        """Check if URL should be skipped."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace("www.", "")
            return domain in self.SKIP_DOMAINS
        except Exception:
            return False

    def _content_hash(self, content: str) -> str:
        """Generate hash for content deduplication."""
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def fetch(self, url: str) -> FetchResult:
        """
        Fetch and extract content from a URL.

        Tries methods in order:
        1. trafilatura (best quality, handles most sites well)
        2. beautifulsoup4 (fallback, basic extraction)
        3. raw text (last resort)
        """
        start_time = datetime.now()

        if self._should_skip(url):
            return FetchResult(
                url=url,
                success=False,
                error=f"Domain skipped (social media/paywall)",
            )

        try:
            # Fetch raw HTML
            client = await self._get_client()
            response = await client.get(url)
            response.raise_for_status()

            html = response.text
            if len(html) > self.max_content_length:
                html = html[:self.max_content_length]

            # Try extraction methods
            result = await self._extract_content(url, html)
            result.fetch_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            return result

        except httpx.TimeoutException:
            return FetchResult(url=url, success=False, error="Timeout")
        except httpx.HTTPStatusError as e:
            return FetchResult(url=url, success=False, error=f"HTTP {e.response.status_code}")
        except Exception as e:
            return FetchResult(url=url, success=False, error=str(e))

    async def _extract_content(self, url: str, html: str) -> FetchResult:
        """Extract content from HTML using available methods."""

        # Try trafilatura first (best quality)
        if TRAFILATURA_AVAILABLE:
            result = await self._extract_with_trafilatura(url, html)
            if result.success and result.content:
                return result

        # Fallback to beautifulsoup
        if BS4_AVAILABLE:
            result = await self._extract_with_bs4(url, html)
            if result.success and result.content:
                return result

        # Last resort: basic text extraction
        return await self._extract_raw(url, html)

    async def _extract_with_trafilatura(self, url: str, html: str) -> FetchResult:
        """Extract content using trafilatura."""
        try:
            # Run trafilatura in thread pool (it's synchronous)
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(
                self._executor,
                lambda: trafilatura.extract(
                    html,
                    url=url,
                    include_comments=False,
                    include_tables=True,
                    no_fallback=False,
                    config=TRAF_CONFIG,
                )
            )

            if not content or len(content) < self.min_content_length:
                return FetchResult(url=url, success=False, error="Insufficient content")

            # Get metadata
            metadata = await loop.run_in_executor(
                self._executor,
                lambda: trafilatura.extract(
                    html,
                    url=url,
                    output_format="json",
                    include_comments=False,
                    config=TRAF_CONFIG,
                )
            )

            title = None
            description = None
            if metadata:
                import json
                try:
                    meta_dict = json.loads(metadata)
                    title = meta_dict.get("title")
                    description = meta_dict.get("description")
                except Exception:
                    pass

            # Fallback title extraction
            if not title:
                title = self._extract_title_from_html(html)

            return FetchResult(
                url=url,
                success=True,
                title=title,
                content=content,
                description=description,
                content_hash=self._content_hash(content),
                word_count=len(content.split()),
                method="trafilatura",
            )

        except Exception as e:
            logger.debug(f"trafilatura failed for {url}: {e}")
            return FetchResult(url=url, success=False, error=str(e))

    async def _extract_with_bs4(self, url: str, html: str) -> FetchResult:
        """Extract content using BeautifulSoup."""
        try:
            loop = asyncio.get_event_loop()

            def extract():
                soup = BeautifulSoup(html, "lxml")

                # Remove script, style, nav, footer, etc.
                for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript"]):
                    tag.decompose()

                # Get title
                title = None
                title_tag = soup.find("title")
                if title_tag:
                    title = title_tag.get_text(strip=True)

                # Get meta description
                description = None
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    description = meta_desc.get("content", "")

                # Try to find main content area
                main_content = None
                for selector in ["article", "main", '[role="main"]', ".content", "#content"]:
                    main_content = soup.select_one(selector)
                    if main_content:
                        break

                if main_content:
                    text = main_content.get_text(separator="\n", strip=True)
                else:
                    # Fallback to body
                    body = soup.find("body")
                    text = body.get_text(separator="\n", strip=True) if body else ""

                # Clean up whitespace
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                content = "\n".join(lines)

                return title, description, content

            title, description, content = await loop.run_in_executor(self._executor, extract)

            if not content or len(content) < self.min_content_length:
                return FetchResult(url=url, success=False, error="Insufficient content")

            return FetchResult(
                url=url,
                success=True,
                title=title,
                content=content,
                description=description,
                content_hash=self._content_hash(content),
                word_count=len(content.split()),
                method="bs4",
            )

        except Exception as e:
            logger.debug(f"bs4 failed for {url}: {e}")
            return FetchResult(url=url, success=False, error=str(e))

    async def _extract_raw(self, url: str, html: str) -> FetchResult:
        """Basic text extraction as last resort."""
        try:
            # Very basic: strip HTML tags
            import re
            text = re.sub(r'<[^>]+>', ' ', html)
            text = re.sub(r'\s+', ' ', text).strip()

            if len(text) < self.min_content_length:
                return FetchResult(url=url, success=False, error="Insufficient content")

            title = self._extract_title_from_html(html)

            return FetchResult(
                url=url,
                success=True,
                title=title,
                content=text[:50000],  # Limit raw extraction
                content_hash=self._content_hash(text),
                word_count=len(text.split()),
                method="raw",
            )

        except Exception as e:
            return FetchResult(url=url, success=False, error=str(e))

    def _extract_title_from_html(self, html: str) -> Optional[str]:
        """Extract title from HTML using regex."""
        import re
        match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    async def fetch_batch(
        self,
        urls: List[str],
        concurrency: int = 5,
    ) -> List[FetchResult]:
        """Fetch multiple URLs concurrently."""
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(url: str) -> FetchResult:
            async with semaphore:
                return await self.fetch(url)

        tasks = [fetch_with_semaphore(url) for url in urls]
        return await asyncio.gather(*tasks)


# Integration with URL Queue
async def process_pending_urls(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    limit: int = 10,
    concurrency: int = 3,
) -> Dict[str, Any]:
    """
    Process pending URLs in the queue, fetching content with trafilatura.

    Returns summary of processing results.
    """
    from .url_queue import get_url_queue, URLStatus

    queue = get_url_queue()
    fetcher = ContentFetcher()

    try:
        # Get pending URLs
        pending = queue.list_urls(
            user_id=user_id,
            session_id=session_id,
            status=URLStatus.PENDING,
            limit=limit,
        )

        if not pending:
            return {"processed": 0, "message": "No pending URLs"}

        results = {
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "details": [],
        }

        # Process in batches
        urls_to_fetch = [u for u in pending if not u.content]  # Skip already cached

        if not urls_to_fetch:
            results["skipped"] = len(pending)
            results["message"] = "All URLs already have cached content"
            return results

        # Fetch content
        fetch_results = await fetcher.fetch_batch(
            [u.url for u in urls_to_fetch],
            concurrency=concurrency,
        )

        # Update queue with results
        for queued_url, fetch_result in zip(urls_to_fetch, fetch_results):
            results["processed"] += 1

            if fetch_result.success:
                queue.update_status(
                    queued_url.id,
                    URLStatus.COMPLETED,
                    processed_data={
                        "content": fetch_result.content,
                        "title": fetch_result.title,
                        "description": fetch_result.description,
                        "word_count": fetch_result.word_count,
                        "method": fetch_result.method,
                        "content_hash": fetch_result.content_hash,
                    }
                )
                results["succeeded"] += 1
            else:
                queue.update_status(
                    queued_url.id,
                    URLStatus.FAILED,
                    error=fetch_result.error,
                )
                results["failed"] += 1

            results["details"].append({
                "url": queued_url.url[:60],
                "success": fetch_result.success,
                "method": fetch_result.method,
                "error": fetch_result.error,
                "word_count": fetch_result.word_count,
            })

        return results

    finally:
        await fetcher.close()


# Convenience function for single URL
async def fetch_url_content(url: str) -> FetchResult:
    """Fetch content from a single URL."""
    fetcher = ContentFetcher()
    try:
        return await fetcher.fetch(url)
    finally:
        await fetcher.close()

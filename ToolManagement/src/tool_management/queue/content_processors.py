"""
Dynamic Content Processors - Pluggable pipeline for processing web content.

Each processor handles one aspect of content processing:
- Text extraction
- Entity extraction (NER)
- Summarization
- Embedding generation
- Keyword/topic extraction
- Link extraction & categorization
- Structured data (JSON-LD, OpenGraph)
- Table/data extraction
- Sentiment analysis
- Q&A generation

Processors can be chained into pipelines and run selectively.
"""

import asyncio
import hashlib
import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class ProcessorType(str, Enum):
    """Categories of content processors."""
    EXTRACTION = "extraction"      # Extract raw content
    ENRICHMENT = "enrichment"      # Add metadata/annotations
    TRANSFORMATION = "transformation"  # Transform content format
    ANALYSIS = "analysis"          # Analyze content properties
    GENERATION = "generation"      # Generate new content from source


@dataclass
class ProcessorResult:
    """Result from a content processor."""
    processor: str
    success: bool
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    processing_time_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingContext:
    """Context passed through the processing pipeline."""
    url: str
    html: Optional[str] = None
    text: Optional[str] = None
    title: Optional[str] = None
    results: Dict[str, ProcessorResult] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_result(self, processor: str) -> Optional[ProcessorResult]:
        return self.results.get(processor)

    def add_result(self, result: ProcessorResult):
        self.results[result.processor] = result


class ContentProcessor(ABC):
    """Base class for content processors."""

    name: str = "base"
    description: str = "Base processor"
    processor_type: ProcessorType = ProcessorType.EXTRACTION
    requires: List[str] = []  # Other processors this depends on

    @abstractmethod
    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        """Process content and return result."""
        pass

    def can_run(self, ctx: ProcessingContext) -> bool:
        """Check if dependencies are satisfied."""
        for req in self.requires:
            result = ctx.get_result(req)
            if not result or not result.success:
                return False
        return True


# =============================================================================
# 1. TEXT EXTRACTION - Extract clean text from HTML
# =============================================================================

class TextExtractor(ContentProcessor):
    """Extract clean text content from HTML using trafilatura."""

    name = "text_extractor"
    description = "Extract clean text content from HTML"
    processor_type = ProcessorType.EXTRACTION

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        if not ctx.html:
            return ProcessorResult(
                processor=self.name,
                success=False,
                error="No HTML content"
            )

        try:
            # Try trafilatura first
            try:
                import trafilatura
                text = trafilatura.extract(ctx.html, url=ctx.url, include_tables=True)
                method = "trafilatura"
            except ImportError:
                # Fallback to basic extraction
                text = self._basic_extract(ctx.html)
                method = "basic"

            if not text:
                return ProcessorResult(
                    processor=self.name,
                    success=False,
                    error="No text extracted"
                )

            ctx.text = text  # Update context

            return ProcessorResult(
                processor=self.name,
                success=True,
                data={
                    "text": text,
                    "word_count": len(text.split()),
                    "char_count": len(text),
                    "method": method,
                },
                processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
            )

        except Exception as e:
            return ProcessorResult(
                processor=self.name,
                success=False,
                error=str(e)
            )

    def _basic_extract(self, html: str) -> str:
        """Basic HTML to text extraction."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


# =============================================================================
# 2. ENTITY EXTRACTION - Extract named entities (NER)
# =============================================================================

class EntityExtractor(ContentProcessor):
    """Extract named entities using spaCy or regex patterns."""

    name = "entity_extractor"
    description = "Extract named entities (people, places, organizations, dates)"
    processor_type = ProcessorType.ENRICHMENT
    requires = ["text_extractor"]

    # Regex patterns for basic entity extraction (fallback)
    PATTERNS = {
        "emails": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        "phones": r'\b(?:\+?1[-.]?)?\(?[0-9]{3}\)?[-.]?[0-9]{3}[-.]?[0-9]{4}\b',
        "urls": r'https?://[^\s<>"{}|\\^`\[\]]+',
        "dates": r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b',
        "money": r'\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|trillion))?',
        "percentages": r'\b\d+(?:\.\d+)?%',
    }

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        entities = {}

        # Try spaCy for NER
        try:
            import spacy
            nlp = spacy.load("en_core_web_sm")
            doc = nlp(text[:100000])  # Limit for performance

            spacy_entities = {}
            for ent in doc.ents:
                label = ent.label_
                if label not in spacy_entities:
                    spacy_entities[label] = []
                if ent.text not in spacy_entities[label]:
                    spacy_entities[label].append(ent.text)

            entities["spacy"] = spacy_entities
            method = "spacy"

        except (ImportError, OSError):
            method = "regex"

        # Always run regex patterns (catches things spaCy might miss)
        regex_entities = {}
        for name, pattern in self.PATTERNS.items():
            matches = list(set(re.findall(pattern, text, re.IGNORECASE)))
            if matches:
                regex_entities[name] = matches[:50]  # Limit results

        entities["patterns"] = regex_entities

        return ProcessorResult(
            processor=self.name,
            success=True,
            data={
                "entities": entities,
                "method": method,
                "entity_count": sum(
                    len(v) for d in entities.values()
                    for v in (d.values() if isinstance(d, dict) else [d])
                ),
            },
            processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
        )


# =============================================================================
# 3. SUMMARIZATION - Generate content summaries
# =============================================================================

class Summarizer(ContentProcessor):
    """Generate summaries using extractive or LLM-based methods."""

    name = "summarizer"
    description = "Generate content summaries"
    processor_type = ProcessorType.GENERATION
    requires = ["text_extractor"]

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        try:
            # Try sumy for extractive summarization (no API costs)
            try:
                from sumy.parsers.plaintext import PlaintextParser
                from sumy.nlp.tokenizers import Tokenizer
                from sumy.summarizers.lsa import LsaSummarizer
                from sumy.nlp.stemmers import Stemmer
                from sumy.utils import get_stop_words

                parser = PlaintextParser.from_string(text, Tokenizer("english"))
                stemmer = Stemmer("english")
                summarizer = LsaSummarizer(stemmer)
                summarizer.stop_words = get_stop_words("english")

                sentences = summarizer(parser.document, 3)  # 3 sentences
                summary = " ".join(str(s) for s in sentences)
                method = "sumy_lsa"

            except ImportError:
                # Fallback: first N sentences
                sentences = re.split(r'[.!?]+', text)
                summary = ". ".join(s.strip() for s in sentences[:3] if s.strip()) + "."
                method = "first_sentences"

            return ProcessorResult(
                processor=self.name,
                success=True,
                data={
                    "summary": summary,
                    "summary_word_count": len(summary.split()),
                    "compression_ratio": len(summary) / len(text) if text else 0,
                    "method": method,
                },
                processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
            )

        except Exception as e:
            return ProcessorResult(processor=self.name, success=False, error=str(e))


# =============================================================================
# 4. EMBEDDING GENERATION - Create vector embeddings
# =============================================================================

class EmbeddingGenerator(ContentProcessor):
    """Generate vector embeddings for semantic search."""

    name = "embedding_generator"
    description = "Generate vector embeddings for semantic search"
    processor_type = ProcessorType.TRANSFORMATION
    requires = ["text_extractor"]

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        try:
            # Try sentence-transformers (local, no API)
            try:
                from sentence_transformers import SentenceTransformer
                model = SentenceTransformer('all-MiniLM-L6-v2')

                # Chunk text for embedding
                chunks = self._chunk_text(text, max_chars=500)
                embeddings = model.encode(chunks).tolist()

                return ProcessorResult(
                    processor=self.name,
                    success=True,
                    data={
                        "embeddings": embeddings,
                        "chunk_count": len(chunks),
                        "embedding_dim": len(embeddings[0]) if embeddings else 0,
                        "model": "all-MiniLM-L6-v2",
                        "method": "sentence_transformers",
                    },
                    processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
                )

            except ImportError:
                # Fallback: simple hash-based pseudo-embedding (for testing)
                embedding = self._hash_embedding(text)
                return ProcessorResult(
                    processor=self.name,
                    success=True,
                    data={
                        "embeddings": [embedding],
                        "chunk_count": 1,
                        "embedding_dim": len(embedding),
                        "method": "hash_fallback",
                    },
                    processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
                )

        except Exception as e:
            return ProcessorResult(processor=self.name, success=False, error=str(e))

    def _chunk_text(self, text: str, max_chars: int = 500) -> List[str]:
        """Split text into chunks."""
        words = text.split()
        chunks = []
        current = []
        current_len = 0

        for word in words:
            if current_len + len(word) > max_chars and current:
                chunks.append(" ".join(current))
                current = []
                current_len = 0
            current.append(word)
            current_len += len(word) + 1

        if current:
            chunks.append(" ".join(current))

        return chunks[:20]  # Limit chunks

    def _hash_embedding(self, text: str, dim: int = 64) -> List[float]:
        """Generate pseudo-embedding from hash (fallback)."""
        h = hashlib.sha256(text.encode()).hexdigest()
        return [int(h[i:i+2], 16) / 255.0 for i in range(0, min(dim*2, len(h)), 2)]


# =============================================================================
# 5. KEYWORD/TOPIC EXTRACTION
# =============================================================================

class KeywordExtractor(ContentProcessor):
    """Extract keywords and topics from content."""

    name = "keyword_extractor"
    description = "Extract keywords, key phrases, and topics"
    processor_type = ProcessorType.ENRICHMENT
    requires = ["text_extractor"]

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        try:
            # Try YAKE for keyword extraction (no API)
            try:
                import yake
                kw_extractor = yake.KeywordExtractor(
                    lan="en", n=3, dedupLim=0.9, top=20
                )
                keywords = kw_extractor.extract_keywords(text)
                keywords = [{"keyword": kw, "score": round(score, 4)} for kw, score in keywords]
                method = "yake"

            except ImportError:
                # Fallback: TF-based extraction
                keywords = self._tf_keywords(text)
                method = "tf_fallback"

            return ProcessorResult(
                processor=self.name,
                success=True,
                data={
                    "keywords": keywords,
                    "keyword_count": len(keywords),
                    "method": method,
                },
                processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
            )

        except Exception as e:
            return ProcessorResult(processor=self.name, success=False, error=str(e))

    def _tf_keywords(self, text: str, top_n: int = 20) -> List[Dict[str, Any]]:
        """Simple term frequency keyword extraction."""
        # Tokenize and clean
        words = re.findall(r'\b[a-z]{3,}\b', text.lower())

        # Remove stopwords
        stopwords = {'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can',
                    'her', 'was', 'one', 'our', 'out', 'has', 'have', 'been', 'this',
                    'that', 'with', 'they', 'from', 'what', 'which', 'their', 'will',
                    'would', 'there', 'could', 'other', 'into', 'more', 'some', 'such'}
        words = [w for w in words if w not in stopwords]

        # Count frequencies
        freq = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1

        # Sort by frequency
        sorted_words = sorted(freq.items(), key=lambda x: -x[1])[:top_n]
        max_freq = sorted_words[0][1] if sorted_words else 1

        return [
            {"keyword": w, "score": round(f / max_freq, 4)}
            for w, f in sorted_words
        ]


# =============================================================================
# 6. LINK EXTRACTION & CATEGORIZATION
# =============================================================================

class LinkExtractor(ContentProcessor):
    """Extract and categorize all links from content."""

    name = "link_extractor"
    description = "Extract and categorize links for crawling"
    processor_type = ProcessorType.EXTRACTION

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        if not ctx.html:
            return ProcessorResult(processor=self.name, success=False, error="No HTML")

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(ctx.html, "lxml")
        except ImportError:
            # Regex fallback
            links = re.findall(r'href=["\']([^"\']+)["\']', ctx.html)
            return ProcessorResult(
                processor=self.name,
                success=True,
                data={"links": links[:100], "method": "regex"},
                processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
            )

        base_domain = urlparse(ctx.url).netloc

        links = {
            "internal": [],
            "external": [],
            "resources": [],  # js, css, images
            "documents": [],  # pdf, doc, etc.
            "social": [],
        }

        social_domains = {'twitter.com', 'facebook.com', 'linkedin.com', 'instagram.com',
                         'youtube.com', 'tiktok.com', 'x.com'}
        doc_extensions = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'}
        resource_extensions = {'.js', '.css', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico'}

        for tag in soup.find_all(['a', 'link', 'script', 'img']):
            href = tag.get('href') or tag.get('src')
            if not href:
                continue

            # Resolve relative URLs
            full_url = urljoin(ctx.url, href)
            parsed = urlparse(full_url)

            # Categorize
            ext = parsed.path.lower().split('.')[-1] if '.' in parsed.path else ''

            if f'.{ext}' in resource_extensions:
                links["resources"].append(full_url)
            elif f'.{ext}' in doc_extensions:
                links["documents"].append(full_url)
            elif parsed.netloc in social_domains:
                links["social"].append(full_url)
            elif parsed.netloc == base_domain:
                links["internal"].append(full_url)
            elif parsed.scheme in ('http', 'https'):
                links["external"].append(full_url)

        # Dedupe and limit
        for category in links:
            links[category] = list(set(links[category]))[:50]

        return ProcessorResult(
            processor=self.name,
            success=True,
            data={
                "links": links,
                "total_links": sum(len(v) for v in links.values()),
                "internal_count": len(links["internal"]),
                "external_count": len(links["external"]),
            },
            processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
        )


# =============================================================================
# 7. STRUCTURED DATA EXTRACTION (JSON-LD, OpenGraph, etc.)
# =============================================================================

class StructuredDataExtractor(ContentProcessor):
    """Extract structured data (JSON-LD, OpenGraph, microdata)."""

    name = "structured_data_extractor"
    description = "Extract JSON-LD, OpenGraph, and other structured data"
    processor_type = ProcessorType.EXTRACTION

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        if not ctx.html:
            return ProcessorResult(processor=self.name, success=False, error="No HTML")

        structured = {
            "json_ld": [],
            "open_graph": {},
            "twitter_cards": {},
            "meta": {},
        }

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(ctx.html, "lxml")

            # JSON-LD
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string)
                    structured["json_ld"].append(data)
                except (json.JSONDecodeError, TypeError):
                    pass

            # OpenGraph
            for meta in soup.find_all('meta', property=re.compile(r'^og:')):
                prop = meta.get('property', '').replace('og:', '')
                structured["open_graph"][prop] = meta.get('content', '')

            # Twitter Cards
            for meta in soup.find_all('meta', attrs={'name': re.compile(r'^twitter:')}):
                prop = meta.get('name', '').replace('twitter:', '')
                structured["twitter_cards"][prop] = meta.get('content', '')

            # General meta
            for meta in soup.find_all('meta', attrs={'name': True}):
                name = meta.get('name', '')
                if not name.startswith(('og:', 'twitter:')):
                    content = meta.get('content', '')
                    if content:
                        structured["meta"][name] = content

            # Title
            title_tag = soup.find('title')
            if title_tag:
                ctx.title = title_tag.get_text(strip=True)
                structured["meta"]["title"] = ctx.title

        except ImportError:
            # Regex fallback for JSON-LD
            json_ld_matches = re.findall(
                r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                ctx.html, re.DOTALL | re.IGNORECASE
            )
            for match in json_ld_matches:
                try:
                    structured["json_ld"].append(json.loads(match))
                except json.JSONDecodeError:
                    pass

        return ProcessorResult(
            processor=self.name,
            success=True,
            data={
                "structured_data": structured,
                "has_json_ld": len(structured["json_ld"]) > 0,
                "has_open_graph": len(structured["open_graph"]) > 0,
            },
            processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
        )


# =============================================================================
# 8. TABLE/DATA EXTRACTION
# =============================================================================

class TableExtractor(ContentProcessor):
    """Extract tables and structured data from HTML."""

    name = "table_extractor"
    description = "Extract tables as structured data"
    processor_type = ProcessorType.EXTRACTION

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        if not ctx.html:
            return ProcessorResult(processor=self.name, success=False, error="No HTML")

        tables = []

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(ctx.html, "lxml")

            for idx, table in enumerate(soup.find_all('table')[:10]):  # Limit tables
                rows = []
                headers = []

                # Get headers
                header_row = table.find('thead') or table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

                # Get data rows
                for tr in table.find_all('tr')[1:50]:  # Limit rows
                    cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                    if cells:
                        rows.append(cells)

                if rows:
                    tables.append({
                        "index": idx,
                        "headers": headers,
                        "rows": rows,
                        "row_count": len(rows),
                        "col_count": len(headers) if headers else (len(rows[0]) if rows else 0),
                    })

        except ImportError:
            pass  # No fallback for table extraction

        return ProcessorResult(
            processor=self.name,
            success=True,
            data={
                "tables": tables,
                "table_count": len(tables),
                "total_rows": sum(t["row_count"] for t in tables),
            },
            processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
        )


# =============================================================================
# 9. SENTIMENT ANALYSIS
# =============================================================================

class SentimentAnalyzer(ContentProcessor):
    """Analyze sentiment of content."""

    name = "sentiment_analyzer"
    description = "Analyze content sentiment (positive/negative/neutral)"
    processor_type = ProcessorType.ANALYSIS
    requires = ["text_extractor"]

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        try:
            # Try TextBlob (simple, no API)
            try:
                from textblob import TextBlob
                blob = TextBlob(text[:10000])  # Limit for performance

                polarity = blob.sentiment.polarity  # -1 to 1
                subjectivity = blob.sentiment.subjectivity  # 0 to 1

                if polarity > 0.1:
                    sentiment = "positive"
                elif polarity < -0.1:
                    sentiment = "negative"
                else:
                    sentiment = "neutral"

                return ProcessorResult(
                    processor=self.name,
                    success=True,
                    data={
                        "sentiment": sentiment,
                        "polarity": round(polarity, 4),
                        "subjectivity": round(subjectivity, 4),
                        "method": "textblob",
                    },
                    processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
                )

            except ImportError:
                # Fallback: simple word-based
                positive_words = {'good', 'great', 'excellent', 'amazing', 'wonderful',
                                 'fantastic', 'love', 'best', 'happy', 'positive'}
                negative_words = {'bad', 'terrible', 'awful', 'horrible', 'hate',
                                 'worst', 'sad', 'negative', 'poor', 'disappointing'}

                words = set(text.lower().split())
                pos_count = len(words & positive_words)
                neg_count = len(words & negative_words)

                if pos_count > neg_count:
                    sentiment = "positive"
                    polarity = min(pos_count / 10, 1.0)
                elif neg_count > pos_count:
                    sentiment = "negative"
                    polarity = -min(neg_count / 10, 1.0)
                else:
                    sentiment = "neutral"
                    polarity = 0.0

                return ProcessorResult(
                    processor=self.name,
                    success=True,
                    data={
                        "sentiment": sentiment,
                        "polarity": round(polarity, 4),
                        "method": "word_count_fallback",
                    },
                    processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
                )

        except Exception as e:
            return ProcessorResult(processor=self.name, success=False, error=str(e))


# =============================================================================
# 10. Q&A GENERATION
# =============================================================================

class QAGenerator(ContentProcessor):
    """Generate potential Q&A pairs from content."""

    name = "qa_generator"
    description = "Generate question-answer pairs from content"
    processor_type = ProcessorType.GENERATION
    requires = ["text_extractor"]

    async def process(self, ctx: ProcessingContext) -> ProcessorResult:
        start = datetime.now()

        text = ctx.text
        if not text:
            return ProcessorResult(processor=self.name, success=False, error="No text")

        qa_pairs = []

        # Simple heuristic-based Q&A generation
        sentences = re.split(r'[.!?]+', text)

        for sent in sentences[:50]:  # Limit
            sent = sent.strip()
            if len(sent) < 20 or len(sent) > 300:
                continue

            # Look for factual statements
            if re.match(r'^[A-Z][^.]*(?:is|are|was|were|has|have|can|will|should)\b', sent):
                # Generate a "What" question
                question = self._to_question(sent)
                if question:
                    qa_pairs.append({
                        "question": question,
                        "answer": sent,
                        "type": "factual"
                    })

            # Look for definitions
            if ' is ' in sent.lower() or ' are ' in sent.lower():
                parts = re.split(r'\s+(?:is|are)\s+', sent, maxsplit=1)
                if len(parts) == 2 and len(parts[0]) < 50:
                    qa_pairs.append({
                        "question": f"What is {parts[0].lower()}?",
                        "answer": sent,
                        "type": "definition"
                    })

        return ProcessorResult(
            processor=self.name,
            success=True,
            data={
                "qa_pairs": qa_pairs[:20],  # Limit output
                "qa_count": len(qa_pairs[:20]),
                "method": "heuristic",
            },
            processing_time_ms=int((datetime.now() - start).total_seconds() * 1000)
        )

    def _to_question(self, statement: str) -> Optional[str]:
        """Convert a statement to a question."""
        # Very simple transformation
        statement = statement.strip()
        if statement.startswith(('The ', 'A ', 'An ')):
            return f"What {statement[statement.index(' ')+1:].lower()}?"
        return None


# =============================================================================
# PROCESSOR REGISTRY & PIPELINE
# =============================================================================

class ProcessorRegistry:
    """Registry of available content processors."""

    _processors: Dict[str, Type[ContentProcessor]] = {}

    @classmethod
    def register(cls, processor_class: Type[ContentProcessor]):
        """Register a processor."""
        cls._processors[processor_class.name] = processor_class
        return processor_class

    @classmethod
    def get(cls, name: str) -> Optional[Type[ContentProcessor]]:
        """Get a processor by name."""
        return cls._processors.get(name)

    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        """List all registered processors."""
        return [
            {
                "name": p.name,
                "description": p.description,
                "type": p.processor_type.value,
                "requires": p.requires,
            }
            for p in cls._processors.values()
        ]

    @classmethod
    def get_by_type(cls, processor_type: ProcessorType) -> List[Type[ContentProcessor]]:
        """Get processors by type."""
        return [
            p for p in cls._processors.values()
            if p.processor_type == processor_type
        ]


# Register all processors
ProcessorRegistry.register(TextExtractor)
ProcessorRegistry.register(EntityExtractor)
ProcessorRegistry.register(Summarizer)
ProcessorRegistry.register(EmbeddingGenerator)
ProcessorRegistry.register(KeywordExtractor)
ProcessorRegistry.register(LinkExtractor)
ProcessorRegistry.register(StructuredDataExtractor)
ProcessorRegistry.register(TableExtractor)
ProcessorRegistry.register(SentimentAnalyzer)
ProcessorRegistry.register(QAGenerator)


class ProcessingPipeline:
    """Pipeline for running multiple processors on content."""

    def __init__(self, processors: Optional[List[str]] = None):
        """
        Initialize pipeline with processor names.

        If None, runs all processors in dependency order.
        """
        self.processor_names = processors

    async def process(
        self,
        url: str,
        html: str,
        processors: Optional[List[str]] = None,
    ) -> ProcessingContext:
        """Run the pipeline on content."""
        ctx = ProcessingContext(url=url, html=html)

        # Determine which processors to run
        to_run = processors or self.processor_names or list(ProcessorRegistry._processors.keys())

        # Build dependency order
        ordered = self._topological_sort(to_run)

        # Run processors
        for name in ordered:
            processor_class = ProcessorRegistry.get(name)
            if not processor_class:
                logger.warning(f"Unknown processor: {name}")
                continue

            processor = processor_class()

            if not processor.can_run(ctx):
                logger.debug(f"Skipping {name}: dependencies not met")
                continue

            try:
                result = await processor.process(ctx)
                ctx.add_result(result)
            except Exception as e:
                logger.error(f"Processor {name} failed: {e}")
                ctx.add_result(ProcessorResult(
                    processor=name,
                    success=False,
                    error=str(e)
                ))

        return ctx

    def _topological_sort(self, processors: List[str]) -> List[str]:
        """Sort processors by dependencies."""
        # Build dependency graph
        deps = {}
        for name in processors:
            proc_class = ProcessorRegistry.get(name)
            if proc_class:
                deps[name] = set(proc_class.requires) & set(processors)
            else:
                deps[name] = set()

        # Kahn's algorithm
        result = []
        no_deps = [n for n in processors if not deps[n]]

        while no_deps:
            n = no_deps.pop(0)
            result.append(n)
            for m in processors:
                if n in deps.get(m, set()):
                    deps[m].discard(n)
                    if not deps[m] and m not in result and m not in no_deps:
                        no_deps.append(m)

        # Add any remaining (circular deps or missing)
        for p in processors:
            if p not in result:
                result.append(p)

        return result


# Convenience function
async def process_url(
    url: str,
    html: str,
    processors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Process URL content through the pipeline."""
    pipeline = ProcessingPipeline()
    ctx = await pipeline.process(url, html, processors)

    return {
        "url": ctx.url,
        "title": ctx.title,
        "results": {
            name: result.data if result.success else {"error": result.error}
            for name, result in ctx.results.items()
        },
        "success_count": sum(1 for r in ctx.results.values() if r.success),
        "total_processors": len(ctx.results),
    }

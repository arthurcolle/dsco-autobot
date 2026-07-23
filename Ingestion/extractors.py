"""
Document Extractors

Extract text and metadata from various document formats.
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pathlib import Path

from .models import Document, DocumentType

logger = logging.getLogger(__name__)


class Extractor(ABC):
    """Base class for document extractors."""

    @abstractmethod
    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """
        Extract text and metadata from a document.

        Args:
            source: File path or URL
            content: Optional raw content bytes

        Returns:
            Document with extracted text and metadata
        """
        pass

    @abstractmethod
    def supports(self, source: str) -> bool:
        """Check if this extractor supports the given source."""
        pass


class PDFExtractor(Extractor):
    """Extract text from PDF documents."""

    def supports(self, source: str) -> bool:
        return source.lower().endswith('.pdf')

    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """Extract text from PDF."""
        logger.info(f"Extracting PDF: {source}")

        text = ""
        metadata: Dict[str, Any] = {}

        try:
            # Try PyMuPDF (fitz) first
            import fitz

            if content:
                doc = fitz.open(stream=content, filetype="pdf")
            else:
                doc = fitz.open(source)

            # Extract text from all pages
            pages = []
            for page_num, page in enumerate(doc):
                page_text = page.get_text()
                pages.append(page_text)

            text = "\n\n".join(pages)

            # Extract metadata
            pdf_metadata = doc.metadata
            if pdf_metadata:
                metadata["title"] = pdf_metadata.get("title", "")
                metadata["author"] = pdf_metadata.get("author", "")
                metadata["subject"] = pdf_metadata.get("subject", "")
                metadata["creator"] = pdf_metadata.get("creator", "")
                metadata["creation_date"] = pdf_metadata.get("creationDate", "")

            metadata["page_count"] = len(doc)
            doc.close()

        except ImportError:
            # Fallback to pdfplumber
            try:
                import pdfplumber

                if content:
                    import io
                    pdf_file = io.BytesIO(content)
                else:
                    pdf_file = source

                with pdfplumber.open(pdf_file) as pdf:
                    pages = []
                    for page in pdf.pages:
                        page_text = page.extract_text() or ""
                        pages.append(page_text)
                    text = "\n\n".join(pages)
                    metadata["page_count"] = len(pdf.pages)

            except ImportError:
                logger.error("No PDF library available. Install pymupdf or pdfplumber.")
                raise ImportError("PDF extraction requires pymupdf or pdfplumber")

        return Document(
            source=source,
            content=text,
            doc_type=DocumentType.PDF,
            metadata=metadata,
            title=metadata.get("title"),
            author=metadata.get("author"),
            word_count=len(text.split()),
        )


class HTMLExtractor(Extractor):
    """Extract text from HTML documents."""

    def supports(self, source: str) -> bool:
        return source.lower().endswith(('.html', '.htm')) or source.startswith('http')

    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """Extract text from HTML."""
        logger.info(f"Extracting HTML: {source}")

        html_content = ""
        metadata: Dict[str, Any] = {}

        # Get HTML content
        if content:
            html_content = content.decode('utf-8', errors='ignore')
        elif source.startswith('http'):
            import httpx
            response = httpx.get(source, timeout=30.0)
            response.raise_for_status()
            html_content = response.text
        else:
            with open(source, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read()

        # Parse HTML
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()

            # Extract metadata
            title_tag = soup.find('title')
            if title_tag:
                metadata["title"] = title_tag.get_text().strip()

            # Extract meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name', meta.get('property', ''))
                content_val = meta.get('content', '')
                if name and content_val:
                    metadata[name] = content_val

            # Get main text
            text = soup.get_text(separator='\n', strip=True)

            # Clean up whitespace
            text = re.sub(r'\n\s*\n', '\n\n', text)

        except ImportError:
            # Basic extraction without BeautifulSoup
            text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()

            # Try to extract title
            title_match = re.search(r'<title[^>]*>([^<]+)</title>', html_content, re.IGNORECASE)
            if title_match:
                metadata["title"] = title_match.group(1).strip()

        return Document(
            source=source,
            content=text,
            doc_type=DocumentType.HTML,
            metadata=metadata,
            title=metadata.get("title"),
            author=metadata.get("author"),
            word_count=len(text.split()),
        )


class MarkdownExtractor(Extractor):
    """Extract text from Markdown documents."""

    def supports(self, source: str) -> bool:
        return source.lower().endswith(('.md', '.markdown'))

    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """Extract text from Markdown."""
        logger.info(f"Extracting Markdown: {source}")

        if content:
            text = content.decode('utf-8', errors='ignore')
        else:
            with open(source, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()

        metadata: Dict[str, Any] = {}

        # Extract YAML frontmatter if present
        if text.startswith('---'):
            parts = text.split('---', 2)
            if len(parts) >= 3:
                frontmatter = parts[1].strip()
                text = parts[2].strip()

                # Parse YAML frontmatter
                try:
                    import yaml
                    metadata = yaml.safe_load(frontmatter) or {}
                except ImportError:
                    # Basic parsing without YAML
                    for line in frontmatter.split('\n'):
                        if ':' in line:
                            key, value = line.split(':', 1)
                            metadata[key.strip()] = value.strip()

        # Extract title from first heading
        title_match = re.search(r'^#\s+(.+)$', text, re.MULTILINE)
        if title_match and "title" not in metadata:
            metadata["title"] = title_match.group(1).strip()

        # Count headings
        headings = re.findall(r'^#+\s+.+$', text, re.MULTILINE)
        metadata["heading_count"] = len(headings)

        # Count code blocks
        code_blocks = re.findall(r'```[\s\S]*?```', text)
        metadata["code_block_count"] = len(code_blocks)

        return Document(
            source=source,
            content=text,
            doc_type=DocumentType.MARKDOWN,
            metadata=metadata,
            title=metadata.get("title"),
            author=metadata.get("author"),
            word_count=len(text.split()),
        )


class CodeExtractor(Extractor):
    """Extract text and structure from source code files."""

    LANGUAGE_EXTENSIONS = {
        '.py': 'python',
        '.js': 'javascript',
        '.ts': 'typescript',
        '.java': 'java',
        '.go': 'go',
        '.rs': 'rust',
        '.cpp': 'cpp',
        '.c': 'c',
        '.rb': 'ruby',
        '.php': 'php',
        '.swift': 'swift',
        '.kt': 'kotlin',
        '.scala': 'scala',
        '.cs': 'csharp',
    }

    def supports(self, source: str) -> bool:
        ext = Path(source).suffix.lower()
        return ext in self.LANGUAGE_EXTENSIONS

    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """Extract text and metadata from source code."""
        logger.info(f"Extracting code: {source}")

        if content:
            text = content.decode('utf-8', errors='ignore')
        else:
            with open(source, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()

        ext = Path(source).suffix.lower()
        language = self.LANGUAGE_EXTENSIONS.get(ext, 'unknown')

        metadata: Dict[str, Any] = {
            "language": language,
            "extension": ext,
        }

        # Count lines
        lines = text.split('\n')
        metadata["line_count"] = len(lines)

        # Count non-empty lines
        non_empty = [l for l in lines if l.strip()]
        metadata["non_empty_lines"] = len(non_empty)

        # Extract classes and functions based on language
        if language == 'python':
            classes = re.findall(r'^class\s+(\w+)', text, re.MULTILINE)
            functions = re.findall(r'^def\s+(\w+)', text, re.MULTILINE)
            imports = re.findall(r'^(?:from\s+[\w.]+\s+)?import\s+(.+)$', text, re.MULTILINE)
            metadata["classes"] = classes
            metadata["functions"] = functions
            metadata["import_count"] = len(imports)

        elif language in ('javascript', 'typescript'):
            classes = re.findall(r'class\s+(\w+)', text)
            functions = re.findall(r'(?:function\s+(\w+)|const\s+(\w+)\s*=\s*(?:async\s*)?\()', text)
            functions = [f[0] or f[1] for f in functions if f[0] or f[1]]
            metadata["classes"] = classes
            metadata["functions"] = functions

        elif language == 'go':
            functions = re.findall(r'func\s+(?:\(\w+\s+\*?\w+\)\s+)?(\w+)', text)
            types = re.findall(r'type\s+(\w+)\s+(?:struct|interface)', text)
            metadata["functions"] = functions
            metadata["types"] = types

        return Document(
            source=source,
            content=text,
            doc_type=DocumentType.CODE,
            metadata=metadata,
            language=language,
            word_count=len(text.split()),
        )


class TextExtractor(Extractor):
    """Extract plain text files."""

    def supports(self, source: str) -> bool:
        return source.lower().endswith(('.txt', '.text', '.log'))

    def extract(self, source: str, content: Optional[bytes] = None) -> Document:
        """Extract plain text."""
        logger.info(f"Extracting text: {source}")

        if content:
            text = content.decode('utf-8', errors='ignore')
        else:
            with open(source, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()

        return Document(
            source=source,
            content=text,
            doc_type=DocumentType.TEXT,
            word_count=len(text.split()),
        )


def get_extractor(source: str) -> Extractor:
    """Get the appropriate extractor for a source."""
    extractors = [
        PDFExtractor(),
        HTMLExtractor(),
        MarkdownExtractor(),
        CodeExtractor(),
        TextExtractor(),
    ]

    for extractor in extractors:
        if extractor.supports(source):
            return extractor

    # Default to text extractor
    return TextExtractor()

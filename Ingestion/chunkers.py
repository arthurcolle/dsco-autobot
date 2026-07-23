"""
Text Chunkers

Split documents into chunks for embedding and retrieval.
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass

from .models import Document, Chunk

logger = logging.getLogger(__name__)


@dataclass
class ChunkerConfig:
    """Configuration for chunking."""
    chunk_size: int = 512  # Target chunk size in tokens/chars
    chunk_overlap: int = 50  # Overlap between chunks
    min_chunk_size: int = 100  # Minimum chunk size
    max_chunk_size: int = 1024  # Maximum chunk size
    separator: str = "\n\n"  # Primary separator


class Chunker(ABC):
    """Base class for text chunkers."""

    def __init__(self, config: Optional[ChunkerConfig] = None):
        self.config = config or ChunkerConfig()

    @abstractmethod
    def chunk(self, document: Document) -> List[Chunk]:
        """
        Split a document into chunks.

        Args:
            document: Document to chunk

        Returns:
            List of chunks
        """
        pass


class SlidingWindowChunker(Chunker):
    """
    Simple sliding window chunker.

    Splits text into fixed-size chunks with overlap.
    """

    def chunk(self, document: Document) -> List[Chunk]:
        """Split document using sliding window."""
        logger.info(f"Chunking document {document.id} with sliding window")

        text = document.content
        chunks: List[Chunk] = []

        if not text:
            return chunks

        chunk_size = self.config.chunk_size
        overlap = self.config.chunk_overlap

        start = 0
        index = 0

        while start < len(text):
            end = start + chunk_size

            # Find a good break point (sentence or paragraph)
            if end < len(text):
                # Look for paragraph break
                para_break = text.rfind('\n\n', start + self.config.min_chunk_size, end)
                if para_break > start:
                    end = para_break + 2
                else:
                    # Look for sentence break
                    sentence_break = max(
                        text.rfind('. ', start + self.config.min_chunk_size, end),
                        text.rfind('? ', start + self.config.min_chunk_size, end),
                        text.rfind('! ', start + self.config.min_chunk_size, end),
                    )
                    if sentence_break > start:
                        end = sentence_break + 2

            chunk_text = text[start:end].strip()

            if chunk_text and len(chunk_text) >= self.config.min_chunk_size:
                chunks.append(Chunk(
                    document_id=document.id,
                    content=chunk_text,
                    index=index,
                    start_char=start,
                    end_char=end,
                    metadata={
                        "chunker": "sliding_window",
                        "chunk_size": chunk_size,
                        "overlap": overlap,
                    }
                ))
                index += 1

            # Move window
            start = end - overlap if end < len(text) else end

        logger.info(f"Created {len(chunks)} chunks from document {document.id}")
        return chunks


class SemanticChunker(Chunker):
    """
    Semantic-aware chunker.

    Splits text at semantic boundaries (paragraphs, sections, etc.).
    """

    def __init__(self, config: Optional[ChunkerConfig] = None):
        super().__init__(config)
        self.section_patterns = [
            r'^#{1,6}\s+.+$',  # Markdown headings
            r'^[A-Z][^.!?]*[.!?]\s*$',  # Sentence-ending lines
            r'^\s*$',  # Empty lines
        ]

    def chunk(self, document: Document) -> List[Chunk]:
        """Split document at semantic boundaries."""
        logger.info(f"Chunking document {document.id} with semantic chunker")

        text = document.content
        chunks: List[Chunk] = []

        if not text:
            return chunks

        # Split into paragraphs
        paragraphs = self._split_paragraphs(text)

        # Merge small paragraphs, split large ones
        current_chunk = ""
        current_start = 0
        index = 0
        char_pos = 0

        for para in paragraphs:
            para_len = len(para)

            # If adding this paragraph would exceed max size
            if current_chunk and len(current_chunk) + para_len > self.config.max_chunk_size:
                # Save current chunk
                if len(current_chunk) >= self.config.min_chunk_size:
                    chunks.append(Chunk(
                        document_id=document.id,
                        content=current_chunk.strip(),
                        index=index,
                        start_char=current_start,
                        end_char=char_pos,
                        metadata={"chunker": "semantic"}
                    ))
                    index += 1

                # Handle large paragraph
                if para_len > self.config.max_chunk_size:
                    sub_chunks = self._split_large_paragraph(
                        para, document.id, index, char_pos
                    )
                    for sub in sub_chunks:
                        chunks.append(sub)
                        index += 1
                    current_chunk = ""
                    current_start = char_pos + para_len
                else:
                    current_chunk = para
                    current_start = char_pos
            else:
                if current_chunk:
                    current_chunk += "\n\n" + para
                else:
                    current_chunk = para
                    current_start = char_pos

            char_pos += para_len + 2  # +2 for paragraph separator

        # Don't forget the last chunk
        if current_chunk and len(current_chunk) >= self.config.min_chunk_size:
            chunks.append(Chunk(
                document_id=document.id,
                content=current_chunk.strip(),
                index=index,
                start_char=current_start,
                end_char=len(text),
                metadata={"chunker": "semantic"}
            ))

        logger.info(f"Created {len(chunks)} chunks from document {document.id}")
        return chunks

    def _split_paragraphs(self, text: str) -> List[str]:
        """Split text into paragraphs."""
        # Split on double newlines
        paragraphs = re.split(r'\n\s*\n', text)
        return [p.strip() for p in paragraphs if p.strip()]

    def _split_large_paragraph(
        self,
        para: str,
        document_id: str,
        start_index: int,
        start_char: int
    ) -> List[Chunk]:
        """Split a large paragraph into smaller chunks."""
        chunks = []

        # Split on sentences
        sentences = re.split(r'(?<=[.!?])\s+', para)

        current = ""
        current_start = start_char
        char_pos = start_char

        for sentence in sentences:
            if len(current) + len(sentence) > self.config.chunk_size:
                if current:
                    chunks.append(Chunk(
                        document_id=document_id,
                        content=current.strip(),
                        index=start_index + len(chunks),
                        start_char=current_start,
                        end_char=char_pos,
                        metadata={"chunker": "semantic", "split": "sentence"}
                    ))
                current = sentence
                current_start = char_pos
            else:
                current = (current + " " + sentence).strip() if current else sentence

            char_pos += len(sentence) + 1

        if current:
            chunks.append(Chunk(
                document_id=document_id,
                content=current.strip(),
                index=start_index + len(chunks),
                start_char=current_start,
                end_char=char_pos,
                metadata={"chunker": "semantic", "split": "sentence"}
            ))

        return chunks


class CodeChunker(Chunker):
    """
    Code-aware chunker.

    Splits source code at function/class boundaries.
    """

    def chunk(self, document: Document) -> List[Chunk]:
        """Split code document at logical boundaries."""
        logger.info(f"Chunking code document {document.id}")

        text = document.content
        language = document.metadata.get("language", "unknown")
        chunks: List[Chunk] = []

        if not text:
            return chunks

        # Get patterns for the language
        patterns = self._get_language_patterns(language)

        if patterns:
            chunks = self._chunk_by_patterns(document, patterns)
        else:
            # Fallback to line-based chunking
            chunks = self._chunk_by_lines(document)

        logger.info(f"Created {len(chunks)} chunks from code document {document.id}")
        return chunks

    def _get_language_patterns(self, language: str) -> Optional[List[str]]:
        """Get regex patterns for code structure."""
        patterns = {
            'python': [
                r'^(?:async\s+)?def\s+\w+',  # Functions
                r'^class\s+\w+',  # Classes
            ],
            'javascript': [
                r'^(?:async\s+)?function\s+\w+',
                r'^(?:const|let|var)\s+\w+\s*=\s*(?:async\s*)?\(',
                r'^class\s+\w+',
            ],
            'typescript': [
                r'^(?:async\s+)?function\s+\w+',
                r'^(?:export\s+)?(?:const|let|var)\s+\w+\s*=',
                r'^(?:export\s+)?class\s+\w+',
                r'^(?:export\s+)?interface\s+\w+',
            ],
            'go': [
                r'^func\s+',
                r'^type\s+\w+\s+(?:struct|interface)',
            ],
            'java': [
                r'^\s*(?:public|private|protected)?\s*(?:static\s+)?(?:class|interface)\s+\w+',
                r'^\s*(?:public|private|protected)?\s*(?:static\s+)?\w+\s+\w+\s*\(',
            ],
        }
        return patterns.get(language)

    def _chunk_by_patterns(self, document: Document, patterns: List[str]) -> List[Chunk]:
        """Chunk code using language-specific patterns."""
        text = document.content
        lines = text.split('\n')
        chunks = []

        combined_pattern = '|'.join(f'({p})' for p in patterns)
        regex = re.compile(combined_pattern, re.MULTILINE)

        # Find all definition starts
        boundaries = []
        char_pos = 0
        for i, line in enumerate(lines):
            if regex.match(line.strip()):
                boundaries.append((i, char_pos))
            char_pos += len(line) + 1

        if not boundaries:
            return self._chunk_by_lines(document)

        # Create chunks between boundaries
        for i, (line_num, start_char) in enumerate(boundaries):
            if i + 1 < len(boundaries):
                end_line = boundaries[i + 1][0]
                end_char = boundaries[i + 1][1]
            else:
                end_line = len(lines)
                end_char = len(text)

            chunk_lines = lines[line_num:end_line]
            chunk_text = '\n'.join(chunk_lines).strip()

            if chunk_text and len(chunk_text) >= self.config.min_chunk_size:
                chunks.append(Chunk(
                    document_id=document.id,
                    content=chunk_text,
                    index=len(chunks),
                    start_char=start_char,
                    end_char=end_char,
                    metadata={
                        "chunker": "code",
                        "start_line": line_num,
                        "end_line": end_line,
                    }
                ))

        return chunks

    def _chunk_by_lines(self, document: Document) -> List[Chunk]:
        """Fallback: chunk by line count."""
        text = document.content
        lines = text.split('\n')
        chunks = []

        lines_per_chunk = self.config.chunk_size // 40  # Assume ~40 chars per line
        lines_per_chunk = max(10, lines_per_chunk)

        char_pos = 0
        for i in range(0, len(lines), lines_per_chunk):
            chunk_lines = lines[i:i + lines_per_chunk]
            chunk_text = '\n'.join(chunk_lines).strip()
            end_char = char_pos + len(chunk_text) + len(chunk_lines)

            if chunk_text:
                chunks.append(Chunk(
                    document_id=document.id,
                    content=chunk_text,
                    index=len(chunks),
                    start_char=char_pos,
                    end_char=end_char,
                    metadata={
                        "chunker": "code",
                        "start_line": i,
                        "end_line": min(i + lines_per_chunk, len(lines)),
                    }
                ))

            char_pos = end_char

        return chunks


def get_chunker(document: Document, config: Optional[ChunkerConfig] = None) -> Chunker:
    """Get the appropriate chunker for a document."""
    from .models import DocumentType

    if document.doc_type == DocumentType.CODE:
        return CodeChunker(config)
    else:
        return SemanticChunker(config)

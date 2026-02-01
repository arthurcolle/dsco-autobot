"""
Text Chunking Module - Split content into optimal chunks for embedding.

Supports multiple chunking strategies:
- Fixed size with overlap
- Sentence-based
- Paragraph-based
- Semantic (using sentence embeddings)
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
import hashlib


class ChunkingStrategy(str, Enum):
    """Available chunking strategies."""
    FIXED = "fixed"           # Fixed size with overlap
    SENTENCE = "sentence"     # Sentence boundaries
    PARAGRAPH = "paragraph"   # Paragraph boundaries
    SEMANTIC = "semantic"     # Semantic similarity boundaries
    RECURSIVE = "recursive"   # Recursive character splitting


@dataclass
class Chunk:
    """A chunk of text with metadata."""
    id: str
    text: str
    index: int                          # Position in sequence
    start_char: int                     # Start position in original text
    end_char: int                       # End position in original text
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None

    @property
    def char_count(self) -> int:
        return len(self.text)

    @property
    def word_count(self) -> int:
        return len(self.text.split())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "index": self.index,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "char_count": self.char_count,
            "word_count": self.word_count,
            "metadata": self.metadata,
            "has_embedding": self.embedding is not None,
        }


class BaseChunker(ABC):
    """Base class for text chunkers."""

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        min_chunk_size: int = 50,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    @abstractmethod
    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """Split text into chunks."""
        pass

    def _generate_chunk_id(self, text: str, index: int) -> str:
        """Generate unique ID for a chunk."""
        content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
        return f"chunk_{index}_{content_hash}"

    def _create_chunk(
        self,
        text: str,
        index: int,
        start_char: int,
        end_char: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Chunk:
        """Create a Chunk object."""
        return Chunk(
            id=self._generate_chunk_id(text, index),
            text=text.strip(),
            index=index,
            start_char=start_char,
            end_char=end_char,
            metadata=metadata or {},
        )


class FixedSizeChunker(BaseChunker):
    """
    Split text into fixed-size chunks with overlap.

    Simple and fast, but may split mid-sentence.
    """

    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        chunks = []
        start = 0
        index = 0

        while start < len(text):
            end = start + self.chunk_size

            # Don't go past end of text
            if end > len(text):
                end = len(text)

            chunk_text = text[start:end]

            # Skip if too small (unless it's the last chunk)
            if len(chunk_text.strip()) >= self.min_chunk_size or start + self.chunk_size >= len(text):
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=index,
                    start_char=start,
                    end_char=end,
                    metadata=metadata,
                ))
                index += 1

            # Move start position with overlap
            start = end - self.chunk_overlap
            if start >= len(text):
                break

        return chunks


class SentenceChunker(BaseChunker):
    """
    Split text at sentence boundaries, grouping sentences to target size.

    Better semantic coherence than fixed-size.
    """

    # Sentence boundary pattern
    SENTENCE_PATTERN = re.compile(r'(?<=[.!?])\s+(?=[A-Z])|(?<=[.!?])\s*\n')

    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        # Split into sentences
        sentences = self.SENTENCE_PATTERN.split(text)
        sentences = [s.strip() for s in sentences if s.strip()]

        chunks = []
        current_chunk = []
        current_length = 0
        chunk_start = 0
        index = 0
        char_pos = 0

        for sentence in sentences:
            sentence_len = len(sentence)

            # If adding this sentence exceeds chunk_size, save current chunk
            if current_length + sentence_len > self.chunk_size and current_chunk:
                chunk_text = " ".join(current_chunk)
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=index,
                    start_char=chunk_start,
                    end_char=char_pos,
                    metadata=metadata,
                ))
                index += 1

                # Start new chunk with overlap (last sentence)
                if self.chunk_overlap > 0 and current_chunk:
                    overlap_text = current_chunk[-1]
                    current_chunk = [overlap_text]
                    current_length = len(overlap_text)
                    chunk_start = char_pos - len(overlap_text)
                else:
                    current_chunk = []
                    current_length = 0
                    chunk_start = char_pos

            current_chunk.append(sentence)
            current_length += sentence_len + 1  # +1 for space
            char_pos += sentence_len + 1

        # Don't forget the last chunk
        if current_chunk:
            chunk_text = " ".join(current_chunk)
            if len(chunk_text) >= self.min_chunk_size:
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=index,
                    start_char=chunk_start,
                    end_char=len(text),
                    metadata=metadata,
                ))

        return chunks


class ParagraphChunker(BaseChunker):
    """
    Split text at paragraph boundaries.

    Best for structured documents with clear paragraphs.
    """

    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        # Split on double newlines or multiple newlines
        paragraphs = re.split(r'\n\s*\n', text)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        chunks = []
        current_chunk = []
        current_length = 0
        chunk_start = 0
        index = 0
        char_pos = 0

        for para in paragraphs:
            para_len = len(para)

            # If single paragraph exceeds chunk_size, use sentence chunker for it
            if para_len > self.chunk_size:
                # Save current chunk first
                if current_chunk:
                    chunk_text = "\n\n".join(current_chunk)
                    chunks.append(self._create_chunk(
                        text=chunk_text,
                        index=index,
                        start_char=chunk_start,
                        end_char=char_pos,
                        metadata=metadata,
                    ))
                    index += 1
                    current_chunk = []
                    current_length = 0

                # Split large paragraph by sentences
                sentence_chunker = SentenceChunker(
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                    min_chunk_size=self.min_chunk_size,
                )
                sub_chunks = sentence_chunker.chunk(para, metadata)
                for sc in sub_chunks:
                    sc.index = index
                    sc.start_char += char_pos
                    sc.end_char += char_pos
                    chunks.append(sc)
                    index += 1

                chunk_start = char_pos + para_len + 2
                char_pos += para_len + 2
                continue

            # If adding paragraph exceeds chunk_size, save current chunk
            if current_length + para_len > self.chunk_size and current_chunk:
                chunk_text = "\n\n".join(current_chunk)
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=index,
                    start_char=chunk_start,
                    end_char=char_pos,
                    metadata=metadata,
                ))
                index += 1
                current_chunk = []
                current_length = 0
                chunk_start = char_pos

            current_chunk.append(para)
            current_length += para_len + 2  # +2 for \n\n
            char_pos += para_len + 2

        # Last chunk
        if current_chunk:
            chunk_text = "\n\n".join(current_chunk)
            if len(chunk_text) >= self.min_chunk_size:
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=index,
                    start_char=chunk_start,
                    end_char=len(text),
                    metadata=metadata,
                ))

        return chunks


class RecursiveChunker(BaseChunker):
    """
    Recursively split text using a hierarchy of separators.

    Tries to keep semantically related content together.
    Similar to LangChain's RecursiveCharacterTextSplitter.
    """

    DEFAULT_SEPARATORS = ["\n\n", "\n", ". ", ", ", " ", ""]

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        min_chunk_size: int = 50,
        separators: Optional[List[str]] = None,
    ):
        super().__init__(chunk_size, chunk_overlap, min_chunk_size)
        self.separators = separators or self.DEFAULT_SEPARATORS

    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        chunks = self._split_text(text, self.separators)

        result = []
        for i, chunk_text in enumerate(chunks):
            # Calculate approximate positions
            start = text.find(chunk_text) if i == 0 else result[-1].end_char - self.chunk_overlap
            if start < 0:
                start = 0
            end = start + len(chunk_text)

            result.append(self._create_chunk(
                text=chunk_text,
                index=i,
                start_char=start,
                end_char=min(end, len(text)),
                metadata=metadata,
            ))

        return result

    def _split_text(self, text: str, separators: List[str]) -> List[str]:
        """Recursively split text."""
        final_chunks = []

        # Get the appropriate separator
        separator = separators[-1]  # Default to last (empty string)
        for sep in separators:
            if sep == "":
                separator = sep
                break
            if sep in text:
                separator = sep
                break

        # Split by separator
        if separator:
            splits = text.split(separator)
        else:
            splits = list(text)

        # Process splits
        good_splits = []
        current_split = []
        current_length = 0

        for split in splits:
            split_len = len(split)

            if split_len > self.chunk_size:
                # Need to split further
                if current_split:
                    merged = separator.join(current_split)
                    if len(merged) >= self.min_chunk_size:
                        good_splits.append(merged)
                    current_split = []
                    current_length = 0

                # Try next separator level
                if len(separators) > 1:
                    sub_splits = self._split_text(split, separators[1:])
                    good_splits.extend(sub_splits)
                else:
                    # Can't split further, just truncate
                    good_splits.append(split[:self.chunk_size])

            elif current_length + split_len + len(separator) > self.chunk_size:
                # Current batch is full
                if current_split:
                    merged = separator.join(current_split)
                    if len(merged) >= self.min_chunk_size:
                        good_splits.append(merged)

                # Start new batch with overlap
                overlap_count = 0
                overlap_splits = []
                for s in reversed(current_split):
                    if overlap_count + len(s) > self.chunk_overlap:
                        break
                    overlap_splits.insert(0, s)
                    overlap_count += len(s) + len(separator)

                current_split = overlap_splits + [split]
                current_length = sum(len(s) for s in current_split) + len(separator) * (len(current_split) - 1)

            else:
                current_split.append(split)
                current_length += split_len + len(separator)

        # Don't forget the last batch
        if current_split:
            merged = separator.join(current_split)
            if len(merged) >= self.min_chunk_size:
                good_splits.append(merged)

        return good_splits


class SemanticChunker(BaseChunker):
    """
    Split text based on semantic similarity between sentences.

    Groups semantically similar sentences together.
    Requires embeddings model.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        min_chunk_size: int = 50,
        similarity_threshold: float = 0.5,
        embedding_fn: Optional[Callable[[List[str]], List[List[float]]]] = None,
    ):
        super().__init__(chunk_size, chunk_overlap, min_chunk_size)
        self.similarity_threshold = similarity_threshold
        self.embedding_fn = embedding_fn

    def chunk(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        # If no embedding function, fall back to sentence chunker
        if self.embedding_fn is None:
            return SentenceChunker(
                self.chunk_size, self.chunk_overlap, self.min_chunk_size
            ).chunk(text, metadata)

        # Split into sentences first
        sentences = re.split(r'(?<=[.!?])\s+', text)
        sentences = [s.strip() for s in sentences if s.strip()]

        if len(sentences) <= 1:
            return [self._create_chunk(text, 0, 0, len(text), metadata)]

        # Get embeddings for all sentences
        embeddings = self.embedding_fn(sentences)

        # Find breakpoints based on similarity drops
        breakpoints = self._find_breakpoints(embeddings)

        # Create chunks from breakpoints
        chunks = []
        chunk_start_idx = 0
        char_pos = 0

        for i, bp in enumerate(breakpoints + [len(sentences)]):
            chunk_sentences = sentences[chunk_start_idx:bp]
            chunk_text = " ".join(chunk_sentences)

            if len(chunk_text) >= self.min_chunk_size:
                chunks.append(self._create_chunk(
                    text=chunk_text,
                    index=i,
                    start_char=char_pos,
                    end_char=char_pos + len(chunk_text),
                    metadata=metadata,
                ))

            char_pos += len(chunk_text) + 1
            chunk_start_idx = bp

        return chunks

    def _find_breakpoints(self, embeddings: List[List[float]]) -> List[int]:
        """Find semantic breakpoints based on embedding similarity."""
        import numpy as np

        breakpoints = []

        for i in range(1, len(embeddings)):
            # Cosine similarity between consecutive sentences
            sim = self._cosine_similarity(embeddings[i-1], embeddings[i])

            if sim < self.similarity_threshold:
                breakpoints.append(i)

        return breakpoints

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        import numpy as np
        a = np.array(a)
        b = np.array(b)
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8))


# =============================================================================
# Chunker Factory
# =============================================================================

def get_chunker(
    strategy: ChunkingStrategy = ChunkingStrategy.RECURSIVE,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    min_chunk_size: int = 50,
    **kwargs,
) -> BaseChunker:
    """
    Factory function to get a chunker by strategy.

    Args:
        strategy: Chunking strategy to use
        chunk_size: Target size of each chunk in characters
        chunk_overlap: Overlap between chunks
        min_chunk_size: Minimum chunk size
        **kwargs: Additional arguments for specific chunkers

    Returns:
        BaseChunker instance
    """
    chunkers = {
        ChunkingStrategy.FIXED: FixedSizeChunker,
        ChunkingStrategy.SENTENCE: SentenceChunker,
        ChunkingStrategy.PARAGRAPH: ParagraphChunker,
        ChunkingStrategy.RECURSIVE: RecursiveChunker,
        ChunkingStrategy.SEMANTIC: SemanticChunker,
    }

    chunker_class = chunkers.get(strategy, RecursiveChunker)
    return chunker_class(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        min_chunk_size=min_chunk_size,
        **kwargs,
    )


# =============================================================================
# Convenience Functions
# =============================================================================

def chunk_text(
    text: str,
    strategy: ChunkingStrategy = ChunkingStrategy.RECURSIVE,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Chunk]:
    """
    Convenience function to chunk text.

    Args:
        text: Text to chunk
        strategy: Chunking strategy
        chunk_size: Target chunk size
        chunk_overlap: Overlap between chunks
        metadata: Optional metadata to attach to chunks

    Returns:
        List of Chunk objects
    """
    chunker = get_chunker(strategy, chunk_size, chunk_overlap)
    return chunker.chunk(text, metadata)


def chunk_for_embedding(
    text: str,
    model_max_tokens: int = 512,
    overlap_tokens: int = 50,
) -> List[Chunk]:
    """
    Chunk text optimized for embedding models.

    Uses recursive chunking with sizes appropriate for embedding models.
    Assumes ~4 chars per token as rough estimate.
    """
    # Convert token limits to character limits (rough estimate)
    chunk_size = model_max_tokens * 4
    chunk_overlap = overlap_tokens * 4

    return chunk_text(
        text,
        strategy=ChunkingStrategy.RECURSIVE,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

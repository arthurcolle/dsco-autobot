"""
Semantic Chunking System
Intelligently chunks text based on semantic boundaries and importance
"""

import re
import hashlib
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import json
from collections import defaultdict


class ChunkType(Enum):
    """Types of semantic chunks"""
    DIALOGUE = "dialogue"
    CODE = "code"
    SEARCH_RESULT = "search_result"
    DOCUMENTATION = "documentation"
    ERROR_MESSAGE = "error"
    DATA = "data"
    NARRATIVE = "narrative"


@dataclass
class SemanticChunk:
    """Represents a semantic chunk of content"""
    content: str
    chunk_type: ChunkType
    importance_score: float  # 0.0 to 1.0
    tokens: int
    metadata: Dict[str, Any]
    chunk_id: str
    dependencies: List[str]  # IDs of chunks this depends on


class SemanticChunker:
    """
    Breaks content into semantic chunks for intelligent context management
    """

    def __init__(self):
        self.chunks: Dict[str, SemanticChunk] = {}
        self.chunk_graph: Dict[str, List[str]] = defaultdict(list)

        # Importance weights for different content types
        self.importance_weights = {
            ChunkType.ERROR_MESSAGE: 0.9,
            ChunkType.CODE: 0.8,
            ChunkType.DIALOGUE: 0.7,
            ChunkType.SEARCH_RESULT: 0.5,
            ChunkType.DOCUMENTATION: 0.6,
            ChunkType.DATA: 0.4,
            ChunkType.NARRATIVE: 0.6
        }

        # Patterns for identifying chunk boundaries
        self.boundary_patterns = {
            'code_block': r'```[\w]*\n.*?\n```',
            'error_block': r'(?:Error|Exception|Traceback).*?(?=\n\n|\Z)',
            'search_result': r'(?:Result \d+:|Search result:).*?(?=Result \d+:|\n\n|\Z)',
            'dialogue': r'(?:User:|Assistant:|System:).*?(?=User:|Assistant:|System:|\n\n|\Z)',
            'section': r'(?:^|\n)#+\s+.*?(?=\n#|\Z)',
            'paragraph': r'(?:^|\n\n)(?!#|```|User:|Assistant:).*?(?=\n\n|\Z)'
        }

    def chunk_text(self, text: str, context_type: str = "mixed") -> List[SemanticChunk]:
        """
        Break text into semantic chunks
        """
        chunks = []

        # Identify and extract different types of content
        remaining_text = text

        # Extract code blocks first
        code_chunks = self._extract_code_blocks(remaining_text)
        for chunk in code_chunks:
            chunks.append(chunk)
            remaining_text = remaining_text.replace(chunk.content, f"[CODE_CHUNK:{chunk.chunk_id}]")

        # Extract error messages
        error_chunks = self._extract_error_messages(remaining_text)
        for chunk in error_chunks:
            chunks.append(chunk)
            remaining_text = remaining_text.replace(chunk.content, f"[ERROR_CHUNK:{chunk.chunk_id}]")

        # Extract dialogue
        dialogue_chunks = self._extract_dialogue(remaining_text)
        for chunk in dialogue_chunks:
            chunks.append(chunk)
            remaining_text = remaining_text.replace(chunk.content, f"[DIALOGUE_CHUNK:{chunk.chunk_id}]")

        # Process remaining text as narrative/documentation
        if remaining_text.strip():
            narrative_chunks = self._chunk_narrative(remaining_text)
            chunks.extend(narrative_chunks)

        # Calculate importance scores
        for chunk in chunks:
            chunk.importance_score = self._calculate_importance(chunk)

        # Store chunks
        for chunk in chunks:
            self.chunks[chunk.chunk_id] = chunk

        return chunks

    def _extract_code_blocks(self, text: str) -> List[SemanticChunk]:
        """Extract code blocks from text"""
        chunks = []
        pattern = re.compile(self.boundary_patterns['code_block'], re.DOTALL)

        for match in pattern.finditer(text):
            content = match.group(0)
            chunk = SemanticChunk(
                content=content,
                chunk_type=ChunkType.CODE,
                importance_score=0.0,  # Will be calculated later
                tokens=self._estimate_tokens(content),
                metadata={'language': self._detect_language(content)},
                chunk_id=self._generate_chunk_id(content),
                dependencies=[]
            )
            chunks.append(chunk)

        return chunks

    def _extract_error_messages(self, text: str) -> List[SemanticChunk]:
        """Extract error messages and tracebacks"""
        chunks = []
        pattern = re.compile(self.boundary_patterns['error_block'], re.DOTALL | re.MULTILINE)

        for match in pattern.finditer(text):
            content = match.group(0)
            chunk = SemanticChunk(
                content=content,
                chunk_type=ChunkType.ERROR_MESSAGE,
                importance_score=0.0,
                tokens=self._estimate_tokens(content),
                metadata={'error_type': self._detect_error_type(content)},
                chunk_id=self._generate_chunk_id(content),
                dependencies=[]
            )
            chunks.append(chunk)

        return chunks

    def _extract_dialogue(self, text: str) -> List[SemanticChunk]:
        """Extract dialogue turns"""
        chunks = []
        pattern = re.compile(self.boundary_patterns['dialogue'], re.DOTALL)

        for match in pattern.finditer(text):
            content = match.group(0)
            speaker = "unknown"
            if content.startswith("User:"):
                speaker = "user"
            elif content.startswith("Assistant:"):
                speaker = "assistant"
            elif content.startswith("System:"):
                speaker = "system"

            chunk = SemanticChunk(
                content=content,
                chunk_type=ChunkType.DIALOGUE,
                importance_score=0.0,
                tokens=self._estimate_tokens(content),
                metadata={'speaker': speaker},
                chunk_id=self._generate_chunk_id(content),
                dependencies=[]
            )
            chunks.append(chunk)

        return chunks

    def _chunk_narrative(self, text: str) -> List[SemanticChunk]:
        """Chunk narrative text into paragraphs or sections"""
        chunks = []

        # Split by sections first
        sections = re.split(r'\n(?=#+\s)', text)

        for section in sections:
            if not section.strip():
                continue

            # Check if it's a section with heading
            if section.startswith('#'):
                lines = section.split('\n', 1)
                heading = lines[0] if lines else ""
                content = lines[1] if len(lines) > 1 else ""

                chunk = SemanticChunk(
                    content=section,
                    chunk_type=ChunkType.DOCUMENTATION,
                    importance_score=0.0,
                    tokens=self._estimate_tokens(section),
                    metadata={'heading': heading.strip('#').strip()},
                    chunk_id=self._generate_chunk_id(section),
                    dependencies=[]
                )
                chunks.append(chunk)
            else:
                # Split into paragraphs
                paragraphs = re.split(r'\n\n+', section)
                for para in paragraphs:
                    if para.strip():
                        chunk = SemanticChunk(
                            content=para,
                            chunk_type=ChunkType.NARRATIVE,
                            importance_score=0.0,
                            tokens=self._estimate_tokens(para),
                            metadata={},
                            chunk_id=self._generate_chunk_id(para),
                            dependencies=[]
                        )
                        chunks.append(chunk)

        return chunks

    def _calculate_importance(self, chunk: SemanticChunk) -> float:
        """Calculate importance score for a chunk"""
        base_score = self.importance_weights.get(chunk.chunk_type, 0.5)

        # Adjust based on content characteristics
        modifiers = []

        # Check for important keywords
        important_keywords = ['error', 'critical', 'important', 'todo', 'fixme', 'bug', 'security']
        keyword_count = sum(1 for kw in important_keywords if kw.lower() in chunk.content.lower())
        if keyword_count > 0:
            modifiers.append(min(0.2, keyword_count * 0.05))

        # Check for questions (likely important for context)
        if '?' in chunk.content:
            modifiers.append(0.1)

        # Recency bonus (would need timestamp in real implementation)
        # modifiers.append(recency_bonus)

        # Length penalty for very long chunks
        if chunk.tokens > 1000:
            modifiers.append(-0.1)

        final_score = base_score + sum(modifiers)
        return max(0.0, min(1.0, final_score))

    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count (simplified - use actual tokenizer in production)"""
        # Rough estimate: 1 token per 4 characters
        return len(text) // 4

    def _generate_chunk_id(self, content: str) -> str:
        """Generate unique ID for chunk"""
        return hashlib.md5(content.encode()).hexdigest()[:8]

    def _detect_language(self, code: str) -> str:
        """Detect programming language from code block"""
        if 'def ' in code or 'import ' in code:
            return 'python'
        elif 'function' in code or 'const ' in code or 'let ' in code:
            return 'javascript'
        elif '#include' in code or 'int main' in code:
            return 'c++'
        else:
            return 'unknown'

    def _detect_error_type(self, error_text: str) -> str:
        """Detect type of error from error message"""
        if 'SyntaxError' in error_text:
            return 'syntax'
        elif 'TypeError' in error_text:
            return 'type'
        elif 'ValueError' in error_text:
            return 'value'
        elif 'ImportError' in error_text:
            return 'import'
        else:
            return 'general'

    def compress_chunks(
        self,
        chunks: List[SemanticChunk],
        target_tokens: int
    ) -> List[SemanticChunk]:
        """
        Compress chunks to fit within token budget
        """
        # Sort by importance
        sorted_chunks = sorted(chunks, key=lambda x: x.importance_score, reverse=True)

        compressed_chunks = []
        total_tokens = 0

        for chunk in sorted_chunks:
            if total_tokens + chunk.tokens <= target_tokens:
                # Keep chunk as is
                compressed_chunks.append(chunk)
                total_tokens += chunk.tokens
            elif total_tokens < target_tokens:
                # Compress chunk to fit remaining space
                remaining_tokens = target_tokens - total_tokens
                compressed = self._compress_chunk(chunk, remaining_tokens)
                if compressed:
                    compressed_chunks.append(compressed)
                    total_tokens += compressed.tokens
                break

        return compressed_chunks

    def _compress_chunk(
        self,
        chunk: SemanticChunk,
        target_tokens: int
    ) -> Optional[SemanticChunk]:
        """Compress a single chunk to target token count"""
        if target_tokens <= 0:
            return None

        # Different compression strategies based on chunk type
        if chunk.chunk_type == ChunkType.CODE:
            compressed_content = self._compress_code(chunk.content, target_tokens)
        elif chunk.chunk_type == ChunkType.DIALOGUE:
            compressed_content = self._compress_dialogue(chunk.content, target_tokens)
        elif chunk.chunk_type == ChunkType.SEARCH_RESULT:
            compressed_content = self._compress_search_result(chunk.content, target_tokens)
        else:
            compressed_content = self._compress_text(chunk.content, target_tokens)

        if not compressed_content:
            return None

        return SemanticChunk(
            content=compressed_content,
            chunk_type=chunk.chunk_type,
            importance_score=chunk.importance_score * 0.8,  # Reduce importance of compressed
            tokens=self._estimate_tokens(compressed_content),
            metadata={**chunk.metadata, 'compressed': True},
            chunk_id=chunk.chunk_id + "_compressed",
            dependencies=chunk.dependencies
        )

    def _compress_code(self, code: str, target_tokens: int) -> str:
        """Compress code by removing comments and less important parts"""
        lines = code.split('\n')

        # Remove comments
        compressed_lines = []
        for line in lines:
            # Skip comment-only lines
            stripped = line.strip()
            if stripped.startswith('#') or stripped.startswith('//'):
                continue
            compressed_lines.append(line)

        # If still too long, keep only function signatures
        if self._estimate_tokens('\n'.join(compressed_lines)) > target_tokens:
            compressed_lines = [
                line for line in compressed_lines
                if any(kw in line for kw in ['def ', 'class ', 'function', 'interface'])
            ]

        return '\n'.join(compressed_lines)

    def _compress_dialogue(self, dialogue: str, target_tokens: int) -> str:
        """Compress dialogue by summarizing"""
        # In production, use LLM for summarization
        # For now, just truncate
        words = dialogue.split()
        target_words = target_tokens * 3  # Rough estimate
        if len(words) > target_words:
            return ' '.join(words[:target_words]) + "... [truncated]"
        return dialogue

    def _compress_search_result(self, result: str, target_tokens: int) -> str:
        """Compress search result to key information"""
        lines = result.split('\n')
        # Keep title and first few lines
        if len(lines) > 5:
            return '\n'.join(lines[:3]) + "\n... [compressed]"
        return result

    def _compress_text(self, text: str, target_tokens: int) -> str:
        """Generic text compression"""
        sentences = re.split(r'[.!?]+', text)
        compressed = []
        current_tokens = 0

        for sentence in sentences:
            sentence_tokens = self._estimate_tokens(sentence)
            if current_tokens + sentence_tokens <= target_tokens:
                compressed.append(sentence)
                current_tokens += sentence_tokens
            else:
                break

        return '. '.join(compressed) + '.'

    def get_relevant_chunks(
        self,
        query: str,
        max_chunks: int = 10
    ) -> List[SemanticChunk]:
        """Get chunks most relevant to a query"""
        # Simple keyword matching (use embeddings in production)
        query_words = set(query.lower().split())
        chunk_scores = []

        for chunk_id, chunk in self.chunks.items():
            chunk_words = set(chunk.content.lower().split())
            overlap = len(query_words & chunk_words)
            relevance = overlap / max(len(query_words), 1)

            # Combine with importance
            final_score = (relevance * 0.6) + (chunk.importance_score * 0.4)
            chunk_scores.append((chunk, final_score))

        # Sort by score and return top chunks
        chunk_scores.sort(key=lambda x: x[1], reverse=True)
        return [chunk for chunk, score in chunk_scores[:max_chunks]]


# Example usage
if __name__ == "__main__":
    chunker = SemanticChunker()

    sample_text = """
# Project Documentation

This is the main documentation for our project.

## Installation

To install the project, run:

```python
pip install my-project
```

## Usage

User: How do I use this library?
Assistant: Here's a simple example:

```python
def example_function():
    print("Hello, World!")
    return 42
```

Error: ImportError: No module named 'missing_module'
Traceback (most recent call last):
  File "main.py", line 10, in <module>
    import missing_module

## Troubleshooting

If you encounter issues, please check the documentation.

Search result: Found 3 matches for "configuration"
Result 1: Configuration options are in config.py
Result 2: Default configuration can be overridden
Result 3: See configuration guide for details
"""

    # Chunk the text
    chunks = chunker.chunk_text(sample_text)

    print(f"Created {len(chunks)} semantic chunks:\n")
    for chunk in chunks:
        print(f"Type: {chunk.chunk_type.value}")
        print(f"Importance: {chunk.importance_score:.2f}")
        print(f"Tokens: {chunk.tokens}")
        print(f"Content preview: {chunk.content[:100]}...")
        print("-" * 50)

    # Compress chunks to fit budget
    compressed = chunker.compress_chunks(chunks, target_tokens=500)
    print(f"\nCompressed to {len(compressed)} chunks within 500 token budget")

    # Get relevant chunks for a query
    relevant = chunker.get_relevant_chunks("error configuration", max_chunks=3)
    print(f"\nFound {len(relevant)} relevant chunks for 'error configuration'")
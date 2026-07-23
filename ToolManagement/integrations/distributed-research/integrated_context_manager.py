"""
Integrated Context Manager - Production Ready Implementation
Combines all context management strategies to prevent token limit errors
"""

import asyncio
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import tiktoken
from datetime import datetime

# Import our components (in production, these would be proper imports)
# from sliding_window_context import SlidingWindowContextManager
# from semantic_chunker import SemanticChunker
# from context_orchestrator import ContextOrchestrator, Priority


class IntegratedContextManager:
    """
    Production-ready context manager that integrates all strategies
    """

    def __init__(
        self,
        model: str = "gpt-4",
        max_tokens: int = 190000,  # Leave 10k buffer for 200k limit
        enable_semantic_chunking: bool = True,
        enable_compression: bool = True,
        enable_parallel_processing: bool = True
    ):
        self.model = model
        self.max_tokens = max_tokens
        self.enable_semantic_chunking = enable_semantic_chunking
        self.enable_compression = enable_compression
        self.enable_parallel_processing = enable_parallel_processing

        # Initialize tokenizer
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except:
            self.encoding = tiktoken.get_encoding("cl100k_base")

        # Token budgets for different operations
        self.budgets = {
            'system_prompt': 5000,
            'current_conversation': 60000,
            'search_results': 70000,
            'code_context': 35000,
            'documentation': 15000,
            'buffer': 5000
        }

        # Context storage by category
        self.context_categories = {
            'system': [],
            'conversation': [],
            'search': [],
            'code': [],
            'docs': []
        }

        # Monitoring
        self.metrics = {
            'api_calls': 0,
            'token_errors_prevented': 0,
            'compressions_performed': 0,
            'evictions_performed': 0,
            'average_response_time': 0
        }

    def count_tokens(self, text: str) -> int:
        """Accurate token counting"""
        return len(self.encoding.encode(text))

    async def process_request(
        self,
        messages: List[Dict[str, str]],
        search_results: Optional[List[str]] = None,
        code_context: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process a request with automatic context management
        """
        start_time = datetime.now()

        # Step 1: Analyze incoming content
        analysis = self._analyze_content({
            'messages': messages,
            'search_results': search_results or [],
            'code_context': code_context or []
        })

        # Step 2: Check if content fits within limits
        if analysis['total_tokens'] > self.max_tokens:
            # Apply management strategies
            managed_content = await self._manage_content(
                messages, search_results, code_context, analysis
            )
        else:
            managed_content = {
                'messages': messages,
                'search_results': search_results,
                'code_context': code_context
            }

        # Step 3: Prepare final context
        final_context = self._prepare_final_context(managed_content)

        # Step 4: Validate token count
        final_tokens = self._count_total_tokens(final_context)
        if final_tokens > self.max_tokens:
            # Emergency compression
            final_context = self._emergency_compress_context(final_context)
            self.metrics['token_errors_prevented'] += 1

        # Update metrics
        self.metrics['api_calls'] += 1
        response_time = (datetime.now() - start_time).total_seconds()
        self.metrics['average_response_time'] = (
            (self.metrics['average_response_time'] * (self.metrics['api_calls'] - 1) + response_time)
            / self.metrics['api_calls']
        )

        return {
            'context': final_context,
            'tokens_used': final_tokens,
            'management_applied': analysis['total_tokens'] > self.max_tokens,
            'metrics': self.get_metrics()
        }

    def _analyze_content(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze incoming content for token counts and priorities"""
        analysis = {
            'message_tokens': sum(
                self.count_tokens(msg['content'])
                for msg in content['messages']
            ),
            'search_tokens': sum(
                self.count_tokens(result)
                for result in content['search_results']
            ),
            'code_tokens': sum(
                self.count_tokens(code)
                for code in content['code_context']
            ),
            'total_tokens': 0
        }

        analysis['total_tokens'] = (
            analysis['message_tokens'] +
            analysis['search_tokens'] +
            analysis['code_tokens']
        )

        return analysis

    async def _manage_content(
        self,
        messages: List[Dict[str, str]],
        search_results: Optional[List[str]],
        code_context: Optional[List[str]],
        analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply management strategies to fit content within limits"""

        # Strategy 1: Compress search results first (lowest priority)
        if search_results and analysis['search_tokens'] > self.budgets['search_results']:
            search_results = await self._compress_search_results(
                search_results,
                self.budgets['search_results']
            )
            self.metrics['compressions_performed'] += 1

        # Strategy 2: Manage conversation history with sliding window
        if analysis['message_tokens'] > self.budgets['current_conversation']:
            messages = self._apply_sliding_window(
                messages,
                self.budgets['current_conversation']
            )
            self.metrics['evictions_performed'] += 1

        # Strategy 3: Semantic chunking for code context
        if code_context and analysis['code_tokens'] > self.budgets['code_context']:
            code_context = await self._chunk_and_prioritize_code(
                code_context,
                self.budgets['code_context']
            )

        return {
            'messages': messages,
            'search_results': search_results,
            'code_context': code_context
        }

    async def _compress_search_results(
        self,
        results: List[str],
        token_budget: int
    ) -> List[str]:
        """Compress search results to fit budget"""
        compressed = []
        current_tokens = 0

        for result in results:
            # Extract key information
            summary = self._extract_search_summary(result)
            summary_tokens = self.count_tokens(summary)

            if current_tokens + summary_tokens <= token_budget:
                compressed.append(summary)
                current_tokens += summary_tokens
            else:
                # Truncate and add indicator
                remaining_budget = token_budget - current_tokens
                if remaining_budget > 100:
                    truncated = self._truncate_to_tokens(summary, remaining_budget - 20)
                    compressed.append(truncated + "\n... [more results truncated]")
                break

        return compressed

    def _extract_search_summary(self, result: str) -> str:
        """Extract key information from search result"""
        lines = result.split('\n')
        summary_lines = []

        # Keep title/URL
        if lines:
            summary_lines.append(lines[0])

        # Extract key sentences (containing important keywords)
        keywords = ['conclusion', 'summary', 'result', 'finding', 'important']
        for line in lines[1:]:
            if any(kw in line.lower() for kw in keywords):
                summary_lines.append(line)
                if len(summary_lines) > 5:
                    break

        # If too few lines, add first few content lines
        if len(summary_lines) < 3 and len(lines) > 3:
            summary_lines.extend(lines[1:3])

        return '\n'.join(summary_lines)

    def _apply_sliding_window(
        self,
        messages: List[Dict[str, str]],
        token_budget: int
    ) -> List[Dict[str, str]]:
        """Apply sliding window to conversation history"""
        # Always keep system message if present
        system_messages = [m for m in messages if m.get('role') == 'system']
        other_messages = [m for m in messages if m.get('role') != 'system']

        # Count system tokens
        system_tokens = sum(self.count_tokens(m['content']) for m in system_messages)
        remaining_budget = token_budget - system_tokens

        # Keep most recent messages that fit
        kept_messages = []
        current_tokens = 0

        for message in reversed(other_messages):
            msg_tokens = self.count_tokens(message['content'])
            if current_tokens + msg_tokens <= remaining_budget:
                kept_messages.insert(0, message)
                current_tokens += msg_tokens
            else:
                break

        return system_messages + kept_messages

    async def _chunk_and_prioritize_code(
        self,
        code_context: List[str],
        token_budget: int
    ) -> List[str]:
        """Chunk and prioritize code context"""
        prioritized = []
        current_tokens = 0

        for code in code_context:
            # Extract important parts (function signatures, classes, etc.)
            important_parts = self._extract_code_essentials(code)
            part_tokens = self.count_tokens(important_parts)

            if current_tokens + part_tokens <= token_budget:
                prioritized.append(important_parts)
                current_tokens += part_tokens

        return prioritized

    def _extract_code_essentials(self, code: str) -> str:
        """Extract essential parts of code"""
        lines = code.split('\n')
        essential_lines = []

        for i, line in enumerate(lines):
            stripped = line.strip()
            # Keep function/class definitions
            if any(kw in stripped for kw in ['def ', 'class ', 'function', 'interface']):
                essential_lines.append(line)
                # Include docstring if present
                if i + 1 < len(lines) and '"""' in lines[i + 1]:
                    j = i + 1
                    while j < len(lines) and j < i + 10:
                        essential_lines.append(lines[j])
                        if lines[j].count('"""') >= 2 or (j > i + 1 and '"""' in lines[j]):
                            break
                        j += 1

        # If too few essential lines, include some context
        if len(essential_lines) < 10 and len(lines) > 10:
            essential_lines = lines[:5] + ["# ... code abbreviated ..."] + lines[-5:]

        return '\n'.join(essential_lines)

    def _prepare_final_context(self, content: Dict[str, Any]) -> List[Dict[str, str]]:
        """Prepare final context for API"""
        final_context = []

        # Add messages
        final_context.extend(content['messages'])

        # Add search results as assistant message if present
        if content['search_results']:
            search_content = "Search Results:\n" + "\n---\n".join(content['search_results'])
            final_context.append({
                'role': 'assistant',
                'content': search_content
            })

        # Add code context as assistant message if present
        if content['code_context']:
            code_content = "Code Context:\n```\n" + "\n---\n".join(content['code_context']) + "\n```"
            final_context.append({
                'role': 'assistant',
                'content': code_content
            })

        return final_context

    def _count_total_tokens(self, messages: List[Dict[str, str]]) -> int:
        """Count total tokens in message list"""
        return sum(self.count_tokens(msg['content']) for msg in messages)

    def _emergency_compress_context(
        self,
        context: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Emergency compression when still over limit"""
        compressed = []
        token_count = 0
        max_per_message = self.max_tokens // max(len(context), 1)

        for msg in context:
            msg_tokens = self.count_tokens(msg['content'])

            if msg_tokens > max_per_message:
                # Truncate message
                truncated_content = self._truncate_to_tokens(
                    msg['content'],
                    max_per_message
                )
                compressed.append({
                    'role': msg['role'],
                    'content': truncated_content
                })
                token_count += max_per_message
            else:
                compressed.append(msg)
                token_count += msg_tokens

            if token_count >= self.max_tokens * 0.95:
                break

        return compressed

    def _truncate_to_tokens(self, text: str, max_tokens: int) -> str:
        """Truncate text to specific token count"""
        tokens = self.encoding.encode(text)
        if len(tokens) <= max_tokens:
            return text

        truncated_tokens = tokens[:max_tokens]
        return self.encoding.decode(truncated_tokens) + "..."

    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        return {
            **self.metrics,
            'budgets': self.budgets,
            'max_tokens': self.max_tokens,
            'timestamp': datetime.now().isoformat()
        }

    def reset_metrics(self):
        """Reset performance metrics"""
        self.metrics = {
            'api_calls': 0,
            'token_errors_prevented': 0,
            'compressions_performed': 0,
            'evictions_performed': 0,
            'average_response_time': 0
        }


# Example usage
async def example_usage():
    """Demonstrate integrated context manager usage"""

    # Initialize manager
    manager = IntegratedContextManager(
        model="gpt-4",
        max_tokens=190000
    )

    # Simulate a complex request
    messages = [
        {"role": "system", "content": "You are a helpful coding assistant."},
        {"role": "user", "content": "Can you help me optimize my Python code?"},
        {"role": "assistant", "content": "I'd be happy to help optimize your Python code. Please share the code you'd like me to review."}
    ]

    # Add many historical messages
    for i in range(100):
        messages.append({"role": "user", "content": f"Question {i}: " + "x" * 500})
        messages.append({"role": "assistant", "content": f"Answer {i}: " + "y" * 1000})

    # Simulate large search results
    search_results = [
        f"Search result {i}: " + "z" * 5000
        for i in range(20)
    ]

    # Simulate code context
    code_context = [
        f"def function_{i}():\n    '''Docstring'''\n    " + "pass\n" * 100
        for i in range(10)
    ]

    # Process the request
    result = await manager.process_request(
        messages=messages,
        search_results=search_results,
        code_context=code_context
    )

    print("Processing Results:")
    print(f"  Final token count: {result['tokens_used']:,}")
    print(f"  Management applied: {result['management_applied']}")
    print(f"  Token errors prevented: {result['metrics']['token_errors_prevented']}")
    print(f"  Compressions performed: {result['metrics']['compressions_performed']}")
    print(f"  Evictions performed: {result['metrics']['evictions_performed']}")
    print(f"  Average response time: {result['metrics']['average_response_time']:.3f}s")
    print(f"  Final context messages: {len(result['context'])}")

    # The context is now ready for API call
    return result['context']


if __name__ == "__main__":
    # Run the example
    context = asyncio.run(example_usage())
    print(f"\nContext prepared successfully with {len(context)} messages")
    print("Ready for API call without token limit errors!")
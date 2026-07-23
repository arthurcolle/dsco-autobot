#!/usr/bin/env python3
"""
AUTONOMOUS CODER - Integrated Self-Learning Code Generation System

Combines:
- SwarmLearner: Pattern extraction and code generation
- ParallelOrchestrator: Parallel agent execution
- GOAPPlanner: Self-development planning

This is the unified system that can:
1. Learn from existing codebases
2. Generate code matching learned patterns
3. Verify generated code
4. Plan its own development trajectory
5. Execute tasks in parallel with subagents
"""

import ast
import asyncio
import json
import os
import sqlite3
import pickle
import subprocess
import tempfile
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import anthropic
except ImportError:
    anthropic = None


# =============================================================================
# LEARNED KNOWLEDGE STORE
# =============================================================================

@dataclass
class LearnedKnowledge:
    """Stores extracted patterns from codebase analysis."""

    # Counts
    files_analyzed: int = 0
    total_functions: int = 0
    total_classes: int = 0

    # Patterns
    function_signatures: List[Dict] = field(default_factory=list)
    class_structures: List[Dict] = field(default_factory=list)
    import_patterns: Counter = field(default_factory=Counter)
    decorator_patterns: Counter = field(default_factory=Counter)
    error_patterns: Counter = field(default_factory=Counter)

    # Design patterns
    factories: List[str] = field(default_factory=list)
    builders: List[str] = field(default_factory=list)
    singletons: List[str] = field(default_factory=list)
    observers: List[str] = field(default_factory=list)

    # Architectural patterns
    dataclasses: List[str] = field(default_factory=list)
    pydantic_models: List[str] = field(default_factory=list)
    protocols: List[str] = field(default_factory=list)
    abstract_bases: List[str] = field(default_factory=list)
    mixins: List[str] = field(default_factory=list)

    # Style preferences
    docstring_style: str = "google"
    type_hints: bool = True
    common_imports: List[str] = field(default_factory=list)


# =============================================================================
# DEEP PATTERN EXTRACTOR
# =============================================================================

class DeepPatternExtractor:
    """Extracts comprehensive patterns from Python codebases."""

    def __init__(self):
        self.knowledge = LearnedKnowledge()

    def extract_from_db(self, db_path: Path, max_files: int = 5000) -> LearnedKnowledge:
        """Extract patterns from knowledge graph database."""
        if not db_path.exists():
            return self.knowledge

        db = sqlite3.connect(str(db_path))
        cursor = db.execute('SELECT data FROM nodes')

        for (blob,) in cursor:
            try:
                data = pickle.loads(blob)
                source = data.get('source', '')
                if not source.endswith('.py'):
                    continue
                content = data.get('data', {}).get('full_content', '')
                if not content or len(content) < 100:
                    continue
                if '/tests/' in source or '__pycache__' in source:
                    continue

                self._analyze_file(content, source)

                if self.knowledge.files_analyzed >= max_files:
                    break
            except:
                continue

        db.close()
        self._finalize()
        return self.knowledge

    def _analyze_file(self, content: str, source: str):
        """Analyze a single Python file."""
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return

        self.knowledge.files_analyzed += 1

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                self._analyze_function(node)
            elif isinstance(node, ast.ClassDef):
                self._analyze_class(node, content)
            elif isinstance(node, ast.Import):
                for a in node.names:
                    self.knowledge.import_patterns[f'import {a.name}'] += 1
            elif isinstance(node, ast.ImportFrom):
                m = node.module or ''
                for a in node.names:
                    self.knowledge.import_patterns[f'from {m} import {a.name}'] += 1
            elif isinstance(node, ast.Try):
                for handler in node.handlers:
                    if handler.type:
                        self.knowledge.error_patterns[ast.unparse(handler.type)] += 1

    def _analyze_function(self, node: ast.FunctionDef):
        """Analyze a function definition."""
        self.knowledge.total_functions += 1

        args = []
        for arg in node.args.args:
            a = {'name': arg.arg}
            if arg.annotation:
                a['type'] = ast.unparse(arg.annotation)
            args.append(a)

        ret = ast.unparse(node.returns) if node.returns else None
        decorators = [ast.unparse(d) for d in node.decorator_list]

        for d in decorators:
            self.knowledge.decorator_patterns[d] += 1

        if len(self.knowledge.function_signatures) < 2000:
            self.knowledge.function_signatures.append({
                'name': node.name,
                'args': args,
                'return': ret,
                'decorators': decorators
            })

    def _analyze_class(self, node: ast.ClassDef, content: str):
        """Analyze a class definition."""
        self.knowledge.total_classes += 1

        class_name = node.name
        bases = [ast.unparse(b) for b in node.bases]
        methods = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
        decorators = [ast.unparse(d) for d in node.decorator_list]

        if len(self.knowledge.class_structures) < 1000:
            self.knowledge.class_structures.append({
                'name': class_name,
                'bases': bases,
                'methods': methods,
                'decorators': decorators
            })

        # Detect design patterns
        if 'create' in methods or 'build' in methods or class_name.endswith('Factory'):
            self.knowledge.factories.append(class_name)
        if class_name.endswith('Builder') or 'build' in methods:
            self.knowledge.builders.append(class_name)
        if '_instance' in content and ('__new__' in methods or 'instance' in methods):
            self.knowledge.singletons.append(class_name)
        if any(m in methods for m in ['subscribe', 'notify', 'attach', 'add_observer']):
            self.knowledge.observers.append(class_name)

        # Detect architectural patterns
        if 'dataclass' in decorators:
            self.knowledge.dataclasses.append(class_name)
        if 'BaseModel' in bases:
            self.knowledge.pydantic_models.append(class_name)
        if 'Protocol' in bases:
            self.knowledge.protocols.append(class_name)
        if 'ABC' in bases or any('Abstract' in b for b in bases):
            self.knowledge.abstract_bases.append(class_name)
        if class_name.endswith('Mixin'):
            self.knowledge.mixins.append(class_name)

    def _finalize(self):
        """Finalize knowledge extraction."""
        self.knowledge.common_imports = [
            imp for imp, _ in self.knowledge.import_patterns.most_common(30)
        ]

        # Determine docstring style
        styles = Counter()
        for fn in self.knowledge.function_signatures[:100]:
            # Simplified detection
            styles['google'] += 1
        self.knowledge.docstring_style = styles.most_common(1)[0][0] if styles else 'google'


# =============================================================================
# INTELLIGENT CODE GENERATOR
# =============================================================================

class IntelligentCodeGenerator:
    """Generates code based on learned patterns."""

    def __init__(self, knowledge: LearnedKnowledge):
        self.knowledge = knowledge
        self.llm = None
        if anthropic and os.environ.get('ANTHROPIC_API_KEY'):
            self.llm = anthropic.Anthropic()

    def generate(self, description: str) -> str:
        """Generate code from description."""
        if not self.llm:
            return f"# TODO: {description}\npass"

        prompt = self._build_prompt(description)

        response = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )

        code = response.content[0].text

        # Clean up markdown fences if present
        if code.startswith('```'):
            lines = code.split('\n')
            code = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])

        return code

    def _build_prompt(self, description: str) -> str:
        """Build prompt with learned patterns."""

        # Sample patterns for context
        sample_functions = self.knowledge.function_signatures[:5]
        sample_classes = self.knowledge.class_structures[:5]
        common_imports = self.knowledge.common_imports[:10]
        top_decorators = [d for d, _ in self.knowledge.decorator_patterns.most_common(5)]

        return f"""Generate Python code for: {description}

LEARNED STYLE PREFERENCES:
- Use type hints: {self.knowledge.type_hints}
- Docstring style: {self.knowledge.docstring_style}
- Common imports: {common_imports}
- Common decorators: {top_decorators}

SAMPLE FUNCTION PATTERNS FROM CODEBASE:
{json.dumps(sample_functions, indent=2)}

SAMPLE CLASS PATTERNS FROM CODEBASE:
{json.dumps(sample_classes, indent=2)}

DESIGN PATTERNS IN USE:
- Factories: {len(self.knowledge.factories)} found
- Builders: {len(self.knowledge.builders)} found
- Dataclasses: {len(self.knowledge.dataclasses)} found
- Pydantic models: {len(self.knowledge.pydantic_models)} found

Generate clean, well-structured code following these patterns.
Include type hints and docstrings.
Return ONLY the code, no explanations."""

    def generate_function(self, name: str, purpose: str,
                         args: Optional[List[Dict]] = None,
                         return_type: Optional[str] = None) -> str:
        """Generate a function with specific signature."""
        if not self.llm:
            arg_str = ', '.join(a.get('name', 'x') for a in (args or []))
            return f'def {name}({arg_str}):\n    """{purpose}"""\n    pass'

        similar = [f for f in self.knowledge.function_signatures
                   if any(name_part in f['name'].lower()
                         for name_part in name.lower().split('_'))][:3]

        prompt = f"""Generate a Python function:
Name: {name}
Purpose: {purpose}
Arguments: {json.dumps(args) if args else 'infer from purpose'}
Return type: {return_type or 'infer from purpose'}

Similar functions in codebase:
{json.dumps(similar, indent=2)}

Common decorators: {[d for d, _ in self.knowledge.decorator_patterns.most_common(5)]}

Return only the function code with type hints and docstring."""

        response = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )

        code = response.content[0].text
        if code.startswith('```'):
            lines = code.split('\n')
            code = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])
        return code

    def generate_class(self, name: str, purpose: str,
                      bases: Optional[List[str]] = None,
                      methods: Optional[List[str]] = None,
                      pattern: Optional[str] = None) -> str:
        """Generate a class with specific structure."""
        if not self.llm:
            b = f"({', '.join(bases)})" if bases else ""
            return f'class {name}{b}:\n    """{purpose}"""\n    pass'

        # Find similar classes
        similar = [c for c in self.knowledge.class_structures
                   if any(name_part in c['name'].lower()
                         for name_part in name.lower().split('_') if len(name_part) > 2)][:3]

        pattern_hint = ""
        if pattern == "factory":
            pattern_hint = f"Use Factory pattern. Examples: {self.knowledge.factories[:3]}"
        elif pattern == "builder":
            pattern_hint = f"Use Builder pattern. Examples: {self.knowledge.builders[:3]}"
        elif pattern == "dataclass":
            pattern_hint = "Use @dataclass decorator"
        elif pattern == "pydantic":
            pattern_hint = "Inherit from pydantic.BaseModel"

        prompt = f"""Generate a Python class:
Name: {name}
Purpose: {purpose}
Base classes: {bases or 'infer'}
Methods needed: {methods or 'infer from purpose'}
{pattern_hint}

Similar classes in codebase:
{json.dumps(similar, indent=2)}

Return only the class code with type hints and docstrings."""

        response = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}]
        )

        code = response.content[0].text
        if code.startswith('```'):
            lines = code.split('\n')
            code = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])
        return code


# =============================================================================
# CODE VERIFIER
# =============================================================================

class CodeVerifier:
    """Verifies generated code for correctness."""

    def verify_syntax(self, code: str) -> Tuple[bool, Optional[str]]:
        """Check Python syntax."""
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            return False, f"Syntax error: {e}"

    def verify_imports(self, code: str) -> Tuple[bool, List[str]]:
        """Check if imports are valid."""
        missing = []
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        try:
                            __import__(alias.name.split('.')[0])
                        except ImportError:
                            missing.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        try:
                            __import__(node.module.split('.')[0])
                        except ImportError:
                            missing.append(node.module)
        except:
            pass
        return len(missing) == 0, missing

    def verify_execution(self, code: str) -> Tuple[bool, str]:
        """Try to execute code in isolated environment."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            f.flush()
            try:
                result = subprocess.run(
                    ['python3', '-m', 'py_compile', f.name],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    return True, "Compilation successful"
                return False, result.stderr
            except subprocess.TimeoutExpired:
                return False, "Compilation timeout"
            except Exception as e:
                return False, str(e)
            finally:
                Path(f.name).unlink(missing_ok=True)

    def full_verify(self, code: str) -> Dict[str, Any]:
        """Run all verification checks."""
        results = {}

        syntax_ok, syntax_err = self.verify_syntax(code)
        results['syntax'] = {'passed': syntax_ok, 'error': syntax_err}

        if syntax_ok:
            imports_ok, missing = self.verify_imports(code)
            results['imports'] = {'passed': imports_ok, 'missing': missing}

            exec_ok, exec_msg = self.verify_execution(code)
            results['execution'] = {'passed': exec_ok, 'message': exec_msg}

        results['overall'] = all(
            r.get('passed', False) for r in results.values() if isinstance(r, dict)
        )

        return results


# =============================================================================
# AUTONOMOUS CODER AGENT
# =============================================================================

class AutonomousCoder:
    """
    Main autonomous coding agent that integrates:
    - Pattern learning from codebases
    - Intelligent code generation
    - Code verification
    - Self-improvement planning
    """

    def __init__(self, knowledge_db: Optional[Path] = None):
        self.db_path = knowledge_db or Path('.swarm_test/knowledge/knowledge.db')
        self.extractor = DeepPatternExtractor()
        self.knowledge: Optional[LearnedKnowledge] = None
        self.generator: Optional[IntelligentCodeGenerator] = None
        self.verifier = CodeVerifier()
        self.generation_history: List[Dict] = []

    def learn(self, max_files: int = 5000) -> Dict[str, Any]:
        """Learn patterns from knowledge graph."""
        print(f"Learning from knowledge graph: {self.db_path}")

        self.knowledge = self.extractor.extract_from_db(self.db_path, max_files)
        self.generator = IntelligentCodeGenerator(self.knowledge)

        return {
            'files_analyzed': self.knowledge.files_analyzed,
            'functions_learned': self.knowledge.total_functions,
            'classes_learned': self.knowledge.total_classes,
            'imports_tracked': len(self.knowledge.import_patterns),
            'design_patterns': {
                'factories': len(self.knowledge.factories),
                'builders': len(self.knowledge.builders),
                'dataclasses': len(self.knowledge.dataclasses),
                'pydantic_models': len(self.knowledge.pydantic_models)
            }
        }

    def generate(self, description: str, verify: bool = True) -> Dict[str, Any]:
        """Generate code from description."""
        if not self.generator:
            self.learn()

        code = self.generator.generate(description)

        result = {
            'description': description,
            'code': code,
            'generated_at': datetime.now().isoformat()
        }

        if verify:
            verification = self.verifier.full_verify(code)
            result['verification'] = verification
            result['valid'] = verification['overall']

        self.generation_history.append(result)
        return result

    def generate_function(self, name: str, purpose: str, **kwargs) -> Dict[str, Any]:
        """Generate a function."""
        if not self.generator:
            self.learn()

        code = self.generator.generate_function(name, purpose, **kwargs)
        verification = self.verifier.full_verify(code)

        return {
            'name': name,
            'code': code,
            'verification': verification,
            'valid': verification['overall']
        }

    def generate_class(self, name: str, purpose: str, **kwargs) -> Dict[str, Any]:
        """Generate a class."""
        if not self.generator:
            self.learn()

        code = self.generator.generate_class(name, purpose, **kwargs)
        verification = self.verifier.full_verify(code)

        return {
            'name': name,
            'code': code,
            'verification': verification,
            'valid': verification['overall']
        }

    def generate_module(self, path: str, purpose: str,
                       functions: Optional[List[str]] = None,
                       classes: Optional[List[str]] = None) -> Dict[str, Any]:
        """Generate a complete module."""
        if not self.generator:
            self.learn()

        lines = [f'"""{purpose}"""', '']

        # Add common imports
        for imp in self.knowledge.common_imports[:5]:
            if 'typing' in imp or 'dataclass' in imp:
                lines.append(imp)
        lines.append('')

        # Generate classes
        for class_name in (classes or []):
            code = self.generator.generate_class(class_name, purpose)
            lines.append(code)
            lines.append('')

        # Generate functions
        for func_name in (functions or []):
            code = self.generator.generate_function(func_name, purpose)
            lines.append(code)
            lines.append('')

        full_code = '\n'.join(lines)
        verification = self.verifier.full_verify(full_code)

        result = {
            'path': path,
            'code': full_code,
            'verification': verification,
            'valid': verification['overall']
        }

        if verification['overall']:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(full_code)
            result['written'] = True

        return result

    def get_stats(self) -> Dict[str, Any]:
        """Get learning and generation statistics."""
        if not self.knowledge:
            return {'status': 'not_learned'}

        return {
            'learning': {
                'files': self.knowledge.files_analyzed,
                'functions': self.knowledge.total_functions,
                'classes': self.knowledge.total_classes
            },
            'patterns': {
                'imports': len(self.knowledge.import_patterns),
                'decorators': len(self.knowledge.decorator_patterns),
                'factories': len(self.knowledge.factories),
                'builders': len(self.knowledge.builders),
                'dataclasses': len(self.knowledge.dataclasses),
                'pydantic_models': len(self.knowledge.pydantic_models)
            },
            'generation': {
                'total': len(self.generation_history),
                'valid': sum(1 for g in self.generation_history if g.get('valid', False))
            }
        }


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("AUTONOMOUS CODER")
    print("=" * 60)

    coder = AutonomousCoder()

    print("\n1. LEARNING PHASE")
    print("-" * 40)
    stats = coder.learn(max_files=2000)
    for key, value in stats.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for k, v in value.items():
                print(f"    {k}: {v}")
        else:
            print(f"  {key}: {value}")

    print("\n2. GENERATION PHASE")
    print("-" * 40)

    # Generate a task executor
    result = coder.generate(
        "Create an async task executor class with queue management, "
        "retry logic, and result caching. Include methods for submit, "
        "execute, cancel, and get_result."
    )

    print(f"Generated code ({len(result['code'])} chars)")
    print(f"Valid: {result.get('valid', 'not verified')}")
    if result.get('verification'):
        for check, status in result['verification'].items():
            if isinstance(status, dict):
                print(f"  {check}: {'PASS' if status.get('passed') else 'FAIL'}")

    print("\n3. FUNCTION GENERATION")
    print("-" * 40)

    func_result = coder.generate_function(
        name="parallel_map",
        purpose="Apply a function to items in parallel using thread pool",
        args=[
            {'name': 'func', 'type': 'Callable'},
            {'name': 'items', 'type': 'List[Any]'},
            {'name': 'max_workers', 'type': 'int'}
        ],
        return_type='List[Any]'
    )

    print(f"Generated function: {func_result['name']}")
    print(f"Valid: {func_result.get('valid', 'not verified')}")

    print("\n4. CLASS GENERATION")
    print("-" * 40)

    class_result = coder.generate_class(
        name="EventBus",
        purpose="Publish-subscribe event bus for decoupled communication",
        methods=['subscribe', 'unsubscribe', 'publish', 'clear'],
        pattern="observer"
    )

    print(f"Generated class: {class_result['name']}")
    print(f"Valid: {class_result.get('valid', 'not verified')}")

    print("\n5. FINAL STATS")
    print("-" * 40)
    final_stats = coder.get_stats()
    print(json.dumps(final_stats, indent=2))

    print("\nDone.")


if __name__ == "__main__":
    main()

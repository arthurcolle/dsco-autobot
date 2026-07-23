#!/usr/bin/env python3
"""
SWARM LEARNER - Self-Learning Code Generation

The swarm learns patterns from digested code and generates new code.
"""

import ast
import json
import os
import re
import sqlite3
import pickle
import hashlib
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    import anthropic
except ImportError:
    anthropic = None


@dataclass
class CodePattern:
    id: str
    pattern_type: str
    template: str
    examples: List[str] = field(default_factory=list)
    frequency: int = 0
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LearnedStyle:
    indent: str = "    "
    quote_style: str = '"'
    type_hints: bool = True
    docstrings: bool = True
    common_imports: List[str] = field(default_factory=list)
    common_decorators: List[str] = field(default_factory=list)


class PatternExtractor:
    def __init__(self):
        self.patterns: Dict[str, CodePattern] = {}
        self.style = LearnedStyle()
        self.function_signatures: List[Dict] = []
        self.class_structures: List[Dict] = []
        self.import_patterns: Counter = Counter()
        self.decorator_patterns: Counter = Counter()

    def extract_from_code(self, code: str, source: str = "") -> List[CodePattern]:
        patterns = []
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return patterns

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                p = self._extract_function(node, code)
                if p:
                    patterns.append(p)
            elif isinstance(node, ast.ClassDef):
                p = self._extract_class(node)
                if p:
                    patterns.append(p)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                self._record_import(node)

        self._learn_style(code)
        return patterns

    def _extract_function(self, node: ast.FunctionDef, code: str) -> Optional[CodePattern]:
        args = []
        for arg in node.args.args:
            a = {'name': arg.arg}
            if arg.annotation:
                a['type'] = ast.unparse(arg.annotation)
            args.append(a)

        ret = ast.unparse(node.returns) if node.returns else None
        decs = [ast.unparse(d) for d in node.decorator_list]

        for d in decs:
            self.decorator_patterns[d] += 1

        self.function_signatures.append({
            'name': node.name, 'args': args, 'return': ret, 'decorators': decs
        })

        pid = hashlib.md5(f"{node.name}{args}".encode()).hexdigest()[:12]
        return CodePattern(
            id=pid, pattern_type='function', template="",
            frequency=1, context={'name': node.name, 'args': args, 'return': ret}
        )

    def _extract_class(self, node: ast.ClassDef) -> Optional[CodePattern]:
        bases = [ast.unparse(b) for b in node.bases]
        methods = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]

        self.class_structures.append({
            'name': node.name, 'bases': bases, 'methods': methods
        })

        pid = hashlib.md5(f"{node.name}{bases}".encode()).hexdigest()[:12]
        return CodePattern(
            id=pid, pattern_type='class', template="",
            frequency=1, context={'name': node.name, 'bases': bases, 'methods': methods}
        )

    def _record_import(self, node):
        if isinstance(node, ast.Import):
            for a in node.names:
                self.import_patterns[f"import {a.name}"] += 1
        elif isinstance(node, ast.ImportFrom):
            m = node.module or ''
            for a in node.names:
                self.import_patterns[f"from {m} import {a.name}"] += 1

    def _learn_style(self, code: str):
        if '    ' in code:
            self.style.indent = "    "
        self.style.type_hints = ' -> ' in code
        self.style.common_imports = [i for i, _ in self.import_patterns.most_common(20)]
        self.style.common_decorators = [d for d, _ in self.decorator_patterns.most_common(10)]


class CodeGenerator:
    def __init__(self, extractor: PatternExtractor):
        self.extractor = extractor
        self.llm = None
        if anthropic and os.environ.get('ANTHROPIC_API_KEY'):
            self.llm = anthropic.Anthropic()

    def generate(self, description: str) -> str:
        if not self.llm:
            return f"# TODO: {description}\npass"

        prompt = f"""Generate Python code for: {description}

Style to match:
- Indent: {self.extractor.style.indent}
- Type hints: {self.extractor.style.type_hints}
- Common imports: {self.extractor.style.common_imports[:5]}

Function patterns from codebase:
{json.dumps(self.extractor.function_signatures[:5], indent=2)}

Class patterns:
{json.dumps(self.extractor.class_structures[:5], indent=2)}

Return only code, no explanation."""

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text

    def generate_function(self, name: str, purpose: str,
                          args: Optional[List[Dict]] = None,
                          return_type: Optional[str] = None) -> str:
        if not self.llm:
            arg_str = ', '.join(a.get('name', 'x') for a in (args or []))
            return f"def {name}({arg_str}):\n    \"\"\"{purpose}\"\"\"\n    pass"

        prompt = f"""Generate a Python function:
Name: {name}
Purpose: {purpose}
Args: {json.dumps(args) if args else 'infer'}
Return: {return_type or 'infer'}

Match this style:
{json.dumps(self.extractor.function_signatures[:3], indent=2)}

Return only the function code."""

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text

    def generate_class(self, name: str, purpose: str,
                       bases: Optional[List[str]] = None,
                       methods: Optional[List[str]] = None) -> str:
        if not self.llm:
            b = f"({', '.join(bases)})" if bases else ""
            return f"class {name}{b}:\n    \"\"\"{purpose}\"\"\"\n    pass"

        prompt = f"""Generate a Python class:
Name: {name}
Purpose: {purpose}
Bases: {bases or []}
Methods: {methods or 'infer'}

Match this style:
{json.dumps(self.extractor.class_structures[:3], indent=2)}

Return only the class code."""

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text


class SwarmLearner:
    def __init__(self, knowledge_db: Path):
        self.db_path = knowledge_db
        self.extractor = PatternExtractor()
        self.generator = CodeGenerator(self.extractor)
        self.learned = False
        self.stats = {}

    def learn(self, max_files: int = 500) -> Dict:
        if not self.db_path.exists():
            return {'error': 'DB not found'}

        db = sqlite3.connect(str(self.db_path))
        cursor = db.execute('SELECT data FROM nodes LIMIT ?', (max_files * 10,))

        files_learned = 0
        patterns = 0

        for (blob,) in cursor:
            try:
                data = pickle.loads(blob)
                content = data.get('data', {}).get('full_content', '')
                source = data.get('source', '')

                if content and source.endswith('.py') and len(content) > 50:
                    extracted = self.extractor.extract_from_code(content, source)
                    for p in extracted:
                        if p.id in self.extractor.patterns:
                            self.extractor.patterns[p.id].frequency += 1
                        else:
                            self.extractor.patterns[p.id] = p
                    patterns += len(extracted)
                    files_learned += 1

                    if files_learned >= max_files:
                        break
            except:
                continue

        db.close()
        self.learned = True

        self.stats = {
            'files': files_learned,
            'patterns': patterns,
            'functions': len(self.extractor.function_signatures),
            'classes': len(self.extractor.class_structures),
            'imports': len(self.extractor.import_patterns)
        }
        return self.stats

    def generate(self, desc: str) -> str:
        if not self.learned:
            self.learn()
        return self.generator.generate(desc)

    def generate_function(self, name: str, purpose: str, **kw) -> str:
        if not self.learned:
            self.learn()
        return self.generator.generate_function(name, purpose, **kw)

    def generate_class(self, name: str, purpose: str, **kw) -> str:
        if not self.learned:
            self.learn()
        return self.generator.generate_class(name, purpose, **kw)


class SwarmWriteTools:
    def __init__(self, learner: SwarmLearner):
        self.learner = learner
        self.writes = []

    def write_file(self, path: str, content: str) -> Dict:
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
            self.writes.append({'action': 'write', 'path': str(p), 'time': datetime.now().isoformat()})
            return {'status': 'ok', 'path': str(p), 'bytes': len(content)}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def edit_file(self, path: str, old: str, new: str) -> Dict:
        try:
            p = Path(path)
            if not p.exists():
                return {'status': 'error', 'error': 'not found'}
            content = p.read_text()
            if old not in content:
                return {'status': 'error', 'error': 'string not found'}
            p.write_text(content.replace(old, new, 1))
            self.writes.append({'action': 'edit', 'path': str(p), 'time': datetime.now().isoformat()})
            return {'status': 'ok', 'path': str(p)}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def generate_and_write(self, path: str, desc: str) -> Dict:
        code = self.learner.generate(desc)
        return self.write_file(path, code)

    def create_module(self, path: str, purpose: str,
                      functions: List[str] = None,
                      classes: List[str] = None) -> Dict:
        if not self.learner.learned:
            self.learner.learn()

        lines = [f'"""{purpose}"""', '']

        for imp in self.learner.extractor.style.common_imports[:3]:
            if 'typing' in imp or 'dataclass' in imp:
                lines.append(imp)
        lines.append('')

        for c in (classes or []):
            lines.append(self.learner.generate_class(c, purpose))
            lines.append('')

        for f in (functions or []):
            lines.append(self.learner.generate_function(f, purpose))
            lines.append('')

        return self.write_file(path, '\n'.join(lines))


class AutonomousCoder:
    def __init__(self, knowledge_db: Path):
        self.learner = SwarmLearner(knowledge_db)
        self.tools = SwarmWriteTools(self.learner)
        self.llm = None
        if anthropic and os.environ.get('ANTHROPIC_API_KEY'):
            self.llm = anthropic.Anthropic()

    def learn(self, max_files: int = 500) -> Dict:
        return self.learner.learn(max_files)

    def code(self, task: str) -> str:
        if not self.learner.learned:
            self.learn()

        t = task.lower()

        if 'create file' in t or 'write to' in t:
            match = re.search(r'(\S+\.py)', task)
            path = match.group(1) if match else 'generated.py'
            result = self.tools.generate_and_write(path, task)
            if result['status'] == 'ok':
                return f"Created {path}:\n\n{Path(path).read_text()}"
            return f"Error: {result.get('error')}"

        elif 'function' in t:
            match = re.search(r'function\s+(\w+)', t)
            name = match.group(1) if match else 'generated_fn'
            return self.learner.generate_function(name, task)

        elif 'class' in t:
            match = re.search(r'class\s+(\w+)', t)
            name = match.group(1) if match else 'GeneratedClass'
            return self.learner.generate_class(name, task)

        else:
            return self.learner.generate(task)


def main():
    import sys

    db = Path('.swarm_test/knowledge/knowledge.db')
    if not db.exists():
        db = Path('.swarm_data/knowledge/knowledge.db')

    coder = AutonomousCoder(db)

    print("Learning from codebase...")
    stats = coder.learn(500)
    print(f"Learned: {stats}")
    print("\nReady. Enter tasks:")

    while True:
        try:
            task = input("\ntask> ").strip()
        except (KeyboardInterrupt, EOFError):
            break
        if task.lower() in ('quit', 'exit', 'q'):
            break
        if task:
            print(coder.code(task))


if __name__ == "__main__":
    main()

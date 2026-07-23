"""Reflexive ontology for world model grounding.

Everything is a node. Edges are nodes. Types reference types.
Full extraction of entities AND relationships with extended type system.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

logger = logging.getLogger(__name__)

# JSONL logger for parseable request logs
_jsonl_log_file = None
_jsonl_log_lock = asyncio.Lock() if hasattr(asyncio, 'Lock') else None

def _get_jsonl_logger():
    """Get or create the JSONL log file handler."""
    global _jsonl_log_file
    if _jsonl_log_file is None:
        from pathlib import Path
        log_path = Path(__file__).parent / "extraction_logs.jsonl"
        _jsonl_log_file = open(log_path, "a", buffering=1)  # Line buffered
    return _jsonl_log_file

async def log_jsonl(record: Dict[str, Any]):
    """Log a record to the JSONL file (thread-safe)."""
    global _jsonl_log_lock
    if _jsonl_log_lock is None:
        _jsonl_log_lock = asyncio.Lock()
    async with _jsonl_log_lock:
        f = _get_jsonl_logger()
        f.write(json.dumps(record, default=str) + "\n")

# =============================================================================
# EXTENDED ONTOLOGY - All types from the world model spec
# =============================================================================

# Entity types by grounding category
ENTITY_TYPES = {
    # Physical Grounding
    "person": "Named individual humans",
    "organization": "Companies, institutions, agencies, groups",
    "group": "Informal collections of people",
    "location": "Cities, countries, geographic places, addresses",
    "facility": "Buildings, structures, installations",
    "object": "Physical artifacts, tools, devices",
    "substance": "Materials, chemicals, compounds",
    "organism": "Plants, animals, microorganisms",

    # Abstract Grounding
    "concept": "Ideas, theories, abstract notions",
    "category": "Classifications, taxonomies, types",
    "property": "Attributes, qualities, characteristics",
    "metric": "Measurements, KPIs, benchmarks",
    "pattern": "Recurring structures, templates, motifs",
    "process": "Methods, procedures, workflows",

    # Temporal Grounding
    "event": "Named events, occurrences, incidents",
    "state": "Conditions, situations, configurations",
    "interval": "Time periods, durations, spans",
    "instant": "Specific moments, timestamps",
    "schedule": "Planned timings, calendars",
    "deadline": "Due dates, cutoffs, expirations",

    # Symbolic Grounding
    "product": "Named products, services, offerings",
    "work": "Creative works, publications, artifacts",
    "law": "Legal documents, regulations, policies",
    "language": "Programming languages, natural languages, protocols",
    "protocol": "Standards, specifications, interfaces",
    "specification": "Technical specs, requirements, contracts",
    "identifier": "IDs, codes, references, URIs",

    # Quantitative Grounding
    "quantity": "Numbers with units, measurements",
    "money": "Monetary amounts, prices, valuations",
    "duration": "Time lengths, periods",
    "distance": "Spatial measurements",
    "probability": "Likelihoods, chances, odds",
    "ratio": "Proportions, percentages, fractions",

    # Mathematical Structure Grounding (FORMULAS AS STRUCTURE)
    "variable": "Symbolic variable in formula (x, y, k, etc.)",
    "constant": "Numeric constant or named constant (π, e, 2)",
    "expression": "Compound mathematical expression",
    "equation": "Equality statement (A = B)",
    "inequality": "Comparison statement (A > B, A < B, A ≥ B)",
    "formula": "Named mathematical formula with semantic meaning",
    "threshold": "Critical value that triggers behavior change",
    "parameter": "Tunable value in a model or system",
    "coefficient": "Multiplicative factor in expression",
    "exponent": "Power/exponent in expression",
    "operand": "Value operated on by operator",
    "term": "Additive component of expression",
    "factor": "Multiplicative component of expression",

    # Execution Semantics (for agent grounding)
    "action": "Verbs grounded as executable operations",
    "capability": "What an entity can do",
    "intention": "Inferred goals, plans, purposes",
    "constraint": "Conditions that must hold",
    "outcome": "Expected results of actions",
    "resource": "Things that can be consumed/produced",

    # Epistemic Grounding
    "claim": "Assertions with confidence/source",
    "evidence": "Supporting observations, data",
    "uncertainty": "Explicit unknowns, ambiguities",
}

# Relationship types by category
RELATION_TYPES = {
    # Compositional Relations
    "part_of": "Component relationship",
    "contains": "Containment relationship",
    "member_of": "Membership in group/org",
    "instance_of": "Type instantiation",
    "subtype_of": "Type hierarchy",

    # Causal Relations
    "causes": "Direct causation",
    "enables": "Makes possible",
    "prevents": "Blocks or inhibits",
    "triggers": "Initiates or activates",
    "inhibits": "Slows or reduces",
    "modulates": "Adjusts or regulates",

    # Temporal Relations (Allen interval algebra)
    "precedes": "Happens before",
    "overlaps": "Partially concurrent",
    "during": "Contained within",
    "starts": "Begins at same time",
    "finishes": "Ends at same time",
    "meets": "Immediately follows",

    # Spatial Relations
    "located_in": "Physical location",
    "near": "Proximity",
    "adjacent_to": "Neighboring",
    "contains_spatially": "Spatial containment",
    "path_between": "Route or connection",

    # Epistemic Relations
    "supports": "Evidence for",
    "contradicts": "Evidence against",
    "elaborates": "Provides detail",
    "summarizes": "Condenses",
    "cites": "References",
    "inferred_from": "Derived via reasoning",

    # Agentive Relations
    "owns": "Ownership",
    "controls": "Authority over",
    "authored": "Created by",
    "responsible_for": "Accountability",
    "delegated_to": "Assigned to",

    # Execution Relations (for agent grounding)
    "requires": "Precondition",
    "produces": "Output/effect",
    "satisfies": "Goal achievement",
    "uses": "Instrument/tool relationship",
    "depends_on": "Dependency",

    # Generic
    "related_to": "Unspecified association",
    "co_occurs": "Appears together",

    # MATHEMATICAL RELATIONS (formulas as structure)
    # Arithmetic operations as edges
    "divided_by": "Division: numerator → denominator",
    "multiplied_by": "Multiplication: factor → factor",
    "added_to": "Addition: term + term",
    "subtracted_from": "Subtraction: minuend - subtrahend",
    "raised_to_power": "Exponentiation: base^exponent",
    "sqrt_of": "Square root operation",
    "log_of": "Logarithm operation",

    # Comparison relations
    "equals": "Equality: A = B",
    "greater_than": "Strict inequality: A > B",
    "less_than": "Strict inequality: A < B",
    "greater_or_equal": "Non-strict inequality: A ≥ B",
    "less_or_equal": "Non-strict inequality: A ≤ B",
    "approximately_equals": "Approximate equality: A ≈ B",
    "proportional_to": "Proportionality: A ∝ B",

    # Structural formula relations
    "has_numerator": "Expression has this as numerator",
    "has_denominator": "Expression has this as denominator",
    "has_term": "Expression contains this additive term",
    "has_factor": "Expression contains this multiplicative factor",
    "has_exponent": "Expression has this as exponent",
    "has_coefficient": "Term has this coefficient",
    "has_variable": "Expression contains this variable",
    "has_constant": "Expression contains this constant",

    # Semantic formula relations
    "symbolizes": "Symbol represents this concept (b → benefit)",
    "threshold_for": "Value is threshold for this behavior",
    "determines": "Formula determines this outcome",
    "bounded_by": "Value is bounded by this constraint",
    "converges_to": "Expression converges to this value",
    "diverges_when": "Expression diverges under this condition",
    "valid_when": "Formula valid under this condition",
    "derived_from": "Formula derived from this formula",
}

# Meta-types for reflexive references
META_TYPES = {
    "type_node": "A node that defines a type",
    "relation_node": "A node that defines a relation type",
    "schema_node": "A node that defines schema structure",
    "frame_node": "An execution context frame",
    "summary_node": "A compressed representation",
    "index_node": "An index into graph structure",
    "shard_node": "A partition of distributed graph",
}

# Grounding categories (what kind of reality the entity refers to)
GROUNDING_CATEGORIES = {
    "physical": "Entities with physical existence in the world",
    "abstract": "Conceptual entities without physical form",
    "temporal": "Time-bound entities and events",
    "symbolic": "Representational entities (text, code, specs)",
    "quantitative": "Numeric and measurable entities",
    "execution": "Agent-executable operations and goals",
    "epistemic": "Knowledge and belief-related entities",
    "meta": "Self-referential graph structure entities",
}

# Relation categories (what kind of connection)
RELATION_CATEGORIES = {
    "compositional": "Part-whole and containment relations",
    "causal": "Cause-effect and influence relations",
    "temporal": "Time-ordering relations (Allen algebra)",
    "spatial": "Location and proximity relations",
    "epistemic": "Evidence and knowledge relations",
    "agentive": "Ownership and responsibility relations",
    "execution": "Dependency and flow relations",
    "generic": "Unspecified associations",
}

# Context window levels for hierarchical compression
CONTEXT_LEVELS = {
    0: {"name": "immediate", "tokens": 8000, "compression": 1.0, "desc": "Full detail, current focus"},
    1: {"name": "recent", "tokens": 16000, "compression": 0.5, "desc": "Summarized recent context"},
    2: {"name": "session", "tokens": 32000, "compression": 0.25, "desc": "Session-level summaries"},
    3: {"name": "persistent", "tokens": 64000, "compression": 0.1, "desc": "Long-term compressed memory"},
    4: {"name": "archive", "tokens": 128000, "compression": 0.02, "desc": "Highly compressed archive"},
}

# Graph operation types for execution semantics
GRAPH_OPS = {
    "query": "Read nodes/edges matching pattern",
    "traverse": "Walk graph following edges",
    "aggregate": "Compute over node/edge sets",
    "mutate": "Add/update/delete nodes or edges",
    "merge": "Combine graphs or resolve conflicts",
    "shard": "Partition graph across storage",
    "replicate": "Copy graph structure for redundancy",
    "compress": "Reduce graph to summary form",
    "expand": "Restore detail from summary",
    "validate": "Check graph consistency/constraints",
}

# Confidence calibration levels
CONFIDENCE_LEVELS = {
    "certain": {"min": 0.95, "desc": "High confidence, multiple sources agree"},
    "likely": {"min": 0.75, "desc": "Probable, good evidence"},
    "possible": {"min": 0.50, "desc": "Plausible but uncertain"},
    "speculative": {"min": 0.25, "desc": "Low confidence, limited evidence"},
    "unknown": {"min": 0.0, "desc": "No confidence, pure inference"},
}

# Extraction modes
EXTRACTION_MODES = {
    "entity_only": "Extract just named entities",
    "entity_relation": "Extract entities and relationships",
    "full_ontology": "Extract with full type system",
    "incremental": "Add to existing graph",
    "differential": "Find changes from previous state",
}

# Model provider configuration
MODEL_PROVIDERS = {
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "env_key": "OPENROUTER_API_KEY",
        "rate_limit": 200,  # requests per minute
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "env_key": "TOGETHER_API_KEY",
        "rate_limit": 100,
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "env_key": "GROQ_API_KEY",
        "rate_limit": 30,
    },
    "anthropic": {
        "base_url": "https://api.anthropic.com/v1",
        "env_key": "ANTHROPIC_API_KEY",
        "rate_limit": 50,
    },
}


# =============================================================================
# DYNAMIC TYPE REGISTRY - Runtime-extensible type system
# =============================================================================

class DynamicTypeRegistry:
    """Runtime-extensible registry for entity types, relations, and mathematical structures.

    Allows the system to discover and define new types on-the-fly from:
    - SymPy expression types (auto-discovery)
    - User-defined types at runtime
    - LLM-suggested types during extraction
    - Imported type schemas
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        # Core registries
        self._entity_types: Dict[str, Dict[str, Any]] = {}
        self._relation_types: Dict[str, Dict[str, Any]] = {}
        self._type_hierarchy: Dict[str, Set[str]] = defaultdict(set)  # parent -> children
        self._type_parents: Dict[str, str] = {}  # child -> parent

        # SymPy class -> ontology type mapping (auto-populated)
        self._sympy_type_map: Dict[str, str] = {}

        # Type inference rules: pattern -> type
        self._inference_rules: List[Tuple[callable, str, float]] = []

        # Statistics
        self._type_usage: Dict[str, int] = defaultdict(int)
        self._auto_discovered: Set[str] = set()

        # Initialize from static definitions
        self._load_static_types()
        self._discover_sympy_types()

    def _load_static_types(self):
        """Load types from the static ENTITY_TYPES and RELATION_TYPES dicts."""
        for type_name, description in ENTITY_TYPES.items():
            self.register_entity_type(type_name, description, source="static")

        for rel_name, description in RELATION_TYPES.items():
            self.register_relation_type(rel_name, description, source="static")

        # Set up mathematical type hierarchy
        math_hierarchy = {
            "mathematical": ["expression", "equation", "inequality", "formula"],
            "expression": ["term", "factor", "operand"],
            "operand": ["variable", "constant", "parameter", "coefficient"],
            "comparison": ["equation", "inequality"],
        }
        for parent, children in math_hierarchy.items():
            for child in children:
                self._type_hierarchy[parent].add(child)
                self._type_parents[child] = parent

    def _discover_sympy_types(self):
        """Auto-discover SymPy expression types and create mappings."""
        try:
            import sympy
            from sympy.core.basic import Basic
            from sympy.core.expr import Expr

            # Map SymPy classes to ontology types
            sympy_mappings = {
                # Core types
                "Symbol": "variable",
                "Dummy": "variable",
                "Wild": "variable",
                "Integer": "constant",
                "Float": "constant",
                "Rational": "constant",
                "Pi": "constant",
                "E": "constant",
                "I": "constant",
                "oo": "constant",
                "zoo": "constant",
                "nan": "constant",

                # Operations become expression subtypes
                "Add": "sum_expression",
                "Mul": "product_expression",
                "Pow": "power_expression",
                "log": "logarithm",
                "exp": "exponential",
                "sin": "trigonometric",
                "cos": "trigonometric",
                "tan": "trigonometric",
                "sqrt": "radical",
                "Abs": "absolute_value",
                "sign": "signum",
                "floor": "floor_function",
                "ceiling": "ceiling_function",
                "Min": "minimum",
                "Max": "maximum",

                # Calculus
                "Derivative": "derivative",
                "Integral": "integral",
                "Limit": "limit",
                "Sum": "summation",
                "Product": "product_series",
                "Subs": "substitution",

                # Relations
                "Eq": "equation",
                "Ne": "inequality",
                "Lt": "inequality",
                "Le": "inequality",
                "Gt": "inequality",
                "Ge": "inequality",
                "StrictLessThan": "inequality",
                "StrictGreaterThan": "inequality",
                "LessThan": "inequality",
                "GreaterThan": "inequality",

                # Logic
                "And": "logical_conjunction",
                "Or": "logical_disjunction",
                "Not": "logical_negation",
                "Implies": "logical_implication",
                "Equivalent": "logical_equivalence",
                "Xor": "logical_xor",

                # Set operations
                "Union": "set_union",
                "Intersection": "set_intersection",
                "Complement": "set_complement",
                "SymmetricDifference": "set_symmetric_diff",
                "EmptySet": "empty_set",
                "UniversalSet": "universal_set",
                "FiniteSet": "finite_set",
                "Interval": "interval",
                "ImageSet": "image_set",
                "ConditionSet": "condition_set",

                # Linear algebra
                "Matrix": "matrix",
                "ImmutableMatrix": "matrix",
                "MutableMatrix": "matrix",
                "MatrixSymbol": "matrix_symbol",
                "Determinant": "determinant",
                "Trace": "trace",
                "Transpose": "transpose",
                "Inverse": "matrix_inverse",

                # Special functions
                "gamma": "gamma_function",
                "factorial": "factorial",
                "binomial": "binomial_coeff",
                "fibonacci": "fibonacci",
                "bernoulli": "bernoulli",
                "catalan": "catalan",
                "bell": "bell_number",

                # Probability
                "Probability": "probability",
                "Expectation": "expectation",
                "Variance": "variance",
                "Covariance": "covariance",
                "RandomSymbol": "random_variable",

                # Piecewise
                "Piecewise": "piecewise",
                "ExprCondPair": "conditional_expr",

                # Functions
                "Function": "function",
                "Lambda": "lambda_expression",
                "FunctionClass": "function_class",
            }

            for sympy_name, onto_type in sympy_mappings.items():
                self._sympy_type_map[sympy_name] = onto_type

                # Auto-register types that don't exist yet
                if onto_type not in self._entity_types:
                    self.register_entity_type(
                        onto_type,
                        f"Auto-discovered from SymPy {sympy_name}",
                        source="sympy_auto",
                        parent="expression" if "expression" not in onto_type else "mathematical",
                    )
                    self._auto_discovered.add(onto_type)

            # Register corresponding relation types for operations
            operation_relations = {
                "sum_expression": ("has_addend", "adds"),
                "product_expression": ("has_multiplicand", "multiplies"),
                "power_expression": ("has_base", "has_exponent"),
                "derivative": ("differentiates", "with_respect_to"),
                "integral": ("integrates", "over_domain"),
                "limit": ("approaches", "as_tends_to"),
                "summation": ("sums_over", "from_to"),
                "logarithm": ("log_base", "log_argument"),
            }

            for expr_type, (rel1, rel2) in operation_relations.items():
                if rel1 not in self._relation_types:
                    self.register_relation_type(
                        rel1, f"Structural relation for {expr_type}",
                        source="sympy_auto", category="mathematical"
                    )
                if rel2 not in self._relation_types:
                    self.register_relation_type(
                        rel2, f"Structural relation for {expr_type}",
                        source="sympy_auto", category="mathematical"
                    )

            logger.info(f"Auto-discovered {len(self._auto_discovered)} types from SymPy")

        except ImportError:
            logger.warning("SymPy not available for type auto-discovery")

    def register_entity_type(
        self,
        type_name: str,
        description: str,
        source: str = "user",
        parent: Optional[str] = None,
        grounding_category: str = "abstract",
        properties: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Register a new entity type."""
        type_name = type_name.lower().replace(" ", "_")

        self._entity_types[type_name] = {
            "name": type_name,
            "description": description,
            "source": source,
            "parent": parent,
            "grounding_category": grounding_category,
            "properties": properties or {},
            "registered_at": datetime.utcnow().isoformat(),
        }

        if parent:
            self._type_hierarchy[parent].add(type_name)
            self._type_parents[type_name] = parent

        return True

    def register_relation_type(
        self,
        rel_name: str,
        description: str,
        source: str = "user",
        category: str = "generic",
        inverse: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Register a new relation type."""
        rel_name = rel_name.lower().replace(" ", "_")

        self._relation_types[rel_name] = {
            "name": rel_name,
            "description": description,
            "source": source,
            "category": category,
            "inverse": inverse,
            "properties": properties or {},
            "registered_at": datetime.utcnow().isoformat(),
        }

        # Auto-register inverse if specified
        if inverse and inverse not in self._relation_types:
            self._relation_types[inverse] = {
                "name": inverse,
                "description": f"Inverse of {rel_name}",
                "source": source,
                "category": category,
                "inverse": rel_name,
                "properties": {},
                "registered_at": datetime.utcnow().isoformat(),
            }

        return True

    def register_inference_rule(
        self,
        predicate: callable,
        inferred_type: str,
        confidence: float = 0.8,
    ):
        """Register a type inference rule.

        Args:
            predicate: Function(expr) -> bool that tests if rule applies
            inferred_type: Type to assign if predicate returns True
            confidence: Confidence score for this inference
        """
        self._inference_rules.append((predicate, inferred_type, confidence))
        self._inference_rules.sort(key=lambda x: -x[2])  # Highest confidence first

    def infer_type(self, expr) -> Tuple[str, float]:
        """Infer the ontology type for an expression.

        Returns (type_name, confidence)
        """
        # First try SymPy class mapping
        class_name = type(expr).__name__
        if class_name in self._sympy_type_map:
            self._type_usage[self._sympy_type_map[class_name]] += 1
            return self._sympy_type_map[class_name], 1.0

        # Try inference rules
        for predicate, inferred_type, confidence in self._inference_rules:
            try:
                if predicate(expr):
                    self._type_usage[inferred_type] += 1
                    return inferred_type, confidence
            except:
                continue

        # Fallback: auto-discover new type from SymPy class
        if class_name not in self._sympy_type_map:
            new_type = f"sympy_{class_name.lower()}"
            self.register_entity_type(
                new_type,
                f"Auto-discovered SymPy type: {class_name}",
                source="runtime_discovery",
                parent="expression",
            )
            self._sympy_type_map[class_name] = new_type
            self._auto_discovered.add(new_type)
            logger.info(f"Auto-discovered new type: {new_type} from {class_name}")
            self._type_usage[new_type] += 1
            return new_type, 0.7

        self._type_usage["expression"] += 1
        return "expression", 0.5

    def get_type_info(self, type_name: str) -> Optional[Dict[str, Any]]:
        """Get full information about a type."""
        type_name = type_name.lower()
        if type_name in self._entity_types:
            return self._entity_types[type_name]
        if type_name in self._relation_types:
            return self._relation_types[type_name]
        return None

    def is_subtype(self, child: str, parent: str) -> bool:
        """Check if child is a subtype of parent."""
        current = child.lower()
        parent = parent.lower()

        while current:
            if current == parent:
                return True
            current = self._type_parents.get(current)
        return False

    def get_subtypes(self, parent: str) -> Set[str]:
        """Get all subtypes of a type (recursive)."""
        parent = parent.lower()
        result = set()
        to_process = [parent]

        while to_process:
            current = to_process.pop()
            children = self._type_hierarchy.get(current, set())
            for child in children:
                if child not in result:
                    result.add(child)
                    to_process.append(child)

        return result

    def export_schema(self) -> Dict[str, Any]:
        """Export the complete type schema."""
        return {
            "entity_types": self._entity_types,
            "relation_types": self._relation_types,
            "type_hierarchy": {k: list(v) for k, v in self._type_hierarchy.items()},
            "sympy_mappings": self._sympy_type_map,
            "auto_discovered": list(self._auto_discovered),
            "type_usage": dict(self._type_usage),
            "exported_at": datetime.utcnow().isoformat(),
        }

    def import_schema(self, schema: Dict[str, Any]):
        """Import types from an exported schema."""
        for type_name, info in schema.get("entity_types", {}).items():
            if type_name not in self._entity_types:
                self.register_entity_type(
                    type_name,
                    info.get("description", ""),
                    source="import",
                    parent=info.get("parent"),
                    grounding_category=info.get("grounding_category", "abstract"),
                    properties=info.get("properties"),
                )

        for rel_name, info in schema.get("relation_types", {}).items():
            if rel_name not in self._relation_types:
                self.register_relation_type(
                    rel_name,
                    info.get("description", ""),
                    source="import",
                    category=info.get("category", "generic"),
                    inverse=info.get("inverse"),
                    properties=info.get("properties"),
                )

    @property
    def entity_types(self) -> List[str]:
        return list(self._entity_types.keys())

    @property
    def relation_types(self) -> List[str]:
        return list(self._relation_types.keys())

    def stats(self) -> Dict[str, Any]:
        return {
            "entity_type_count": len(self._entity_types),
            "relation_type_count": len(self._relation_types),
            "auto_discovered_count": len(self._auto_discovered),
            "inference_rules": len(self._inference_rules),
            "top_used_types": sorted(
                self._type_usage.items(),
                key=lambda x: -x[1]
            )[:20],
        }


# Global registry instance
def get_type_registry() -> DynamicTypeRegistry:
    """Get the global type registry singleton."""
    return DynamicTypeRegistry()


# Convenience functions for type operations
def register_type(
    type_name: str,
    description: str,
    kind: str = "entity",
    **kwargs
) -> bool:
    """Register a new type in the global registry."""
    registry = get_type_registry()
    if kind == "entity":
        return registry.register_entity_type(type_name, description, **kwargs)
    else:
        return registry.register_relation_type(type_name, description, **kwargs)


def infer_type_from_expr(expr) -> Tuple[str, float]:
    """Infer ontology type from a SymPy expression."""
    return get_type_registry().infer_type(expr)

# Fast models - dynamically populated from OpenRouter API
# Will be filled by fetch_mid_tier_models() at runtime
FAST_MODELS = []

async def fetch_mid_tier_models(
    min_percentile: float = 0.075,  # Skip bottom 7.5% cheapest
    max_percentile: float = 0.925,  # Skip top 7.5% most expensive
    max_models: int = 12,
    exclude_free: bool = True,
) -> List[str]:
    """Fetch models from OpenRouter API and select mid-tier by price."""
    import httpx

    api_key = os.getenv("OPENROUTER_API_KEY")
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://openrouter.ai/api/v1/models",
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
        )
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch models: {resp.status_code}")
            return [
                "google/gemini-2.0-flash-lite-001",
                "meta-llama/llama-3.1-8b-instruct",
                "qwen/qwen-2.5-7b-instruct",
                "deepseek/deepseek-chat",
            ]

        data = resp.json()
        models = data.get("data", [])

        # Filter for chat-capable models with pricing
        valid_models = []
        for m in models:
            pricing = m.get("pricing", {})
            prompt_price = float(pricing.get("prompt", 0) or 0)

            # Skip free models if requested
            if exclude_free and prompt_price == 0:
                continue

            # Skip models without proper pricing
            if prompt_price <= 0:
                continue

            # Must support chat/completion
            arch = m.get("architecture", {})
            if arch.get("modality") not in (None, "text", "text->text"):
                continue

            valid_models.append({
                "id": m["id"],
                "price": prompt_price,
                "context": m.get("context_length", 0),
            })

        if not valid_models:
            return ["deepseek/deepseek-chat"]

        # Sort by price
        valid_models.sort(key=lambda x: x["price"])

        # Select mid-tier (skip bottom and top percentiles)
        n = len(valid_models)
        start_idx = int(n * min_percentile)
        end_idx = int(n * max_percentile)

        mid_tier = valid_models[start_idx:end_idx]

        # Diversify by provider - pick from different providers
        providers_seen = set()
        selected = []

        for m in mid_tier:
            provider = m["id"].split("/")[0]
            # Allow up to 2 models per provider for diversity
            provider_count = sum(1 for s in selected if s.startswith(provider + "/"))
            if provider_count < 2:
                selected.append(m["id"])
                providers_seen.add(provider)
                if len(selected) >= max_models:
                    break

        logger.info(f"Selected {len(selected)} mid-tier models from {len(providers_seen)} providers")
        return selected


def fetch_mid_tier_models_sync(**kwargs) -> List[str]:
    """Synchronous wrapper for fetch_mid_tier_models."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Already in async context, create new loop in thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, fetch_mid_tier_models(**kwargs))
                return future.result()
        else:
            return loop.run_until_complete(fetch_mid_tier_models(**kwargs))
    except RuntimeError:
        return asyncio.run(fetch_mid_tier_models(**kwargs))

# Smart models for complex reasoning
SMART_MODELS = [
    "anthropic/claude-3.5-sonnet",
    "openai/gpt-4o",
    "google/gemini-1.5-pro",
    "deepseek/deepseek-r1",
]

# Parallelism settings
PARALLELISM = {
    "max_concurrent_chunks": 64,
    "max_concurrent_models": 8,
    "batch_size": 32,
    "httpx_connections": 200,
    "httpx_keepalive": 100,
    "request_timeout": 60.0,
    "connect_timeout": 15.0,
}

# Storage backends
STORAGE_BACKENDS = {
    "sqlite": {"type": "embedded", "max_size_gb": 10, "concurrent_writes": False},
    "duckdb": {"type": "embedded", "max_size_gb": 100, "concurrent_writes": True},
    "lmdb": {"type": "embedded", "max_size_gb": 1000, "concurrent_writes": False},
    "rocksdb": {"type": "embedded", "max_size_gb": 10000, "concurrent_writes": True},
    "mmap": {"type": "memory", "max_size_gb": 64, "concurrent_writes": True},
}

# =============================================================================
# FUNDAMENTAL SYSTEM TYPES - Data flow, computation, timing, agents
# =============================================================================

# Data lifecycle events - when data hits the system
DATA_EVENTS = {
    "ingestion": "Data entering the system from external source",
    "reception": "Data received at an endpoint",
    "parsing": "Data being parsed/deserialized",
    "validation": "Data being validated against schema",
    "normalization": "Data being normalized/canonicalized",
    "enrichment": "Data being enriched with additional info",
    "indexing": "Data being indexed for retrieval",
    "persistence": "Data being written to storage",
    "replication": "Data being copied to replica",
    "expiration": "Data TTL reached, marked for deletion",
    "eviction": "Data removed from cache/memory",
    "archival": "Data moved to cold storage",
}

# Computation lifecycle - when we start/stop computing
COMPUTE_EVENTS = {
    "invocation": "Computation requested/triggered",
    "scheduling": "Computation queued for execution",
    "dispatch": "Computation assigned to worker",
    "execution_start": "Computation begins processing",
    "checkpoint": "Computation state saved mid-execution",
    "execution_end": "Computation completes processing",
    "result_emission": "Computation outputs result",
    "error_emission": "Computation outputs error",
    "cancellation": "Computation cancelled before completion",
    "timeout": "Computation exceeded time limit",
    "retry": "Computation being retried after failure",
    "completion": "Computation fully finished including cleanup",
}

# System timing primitives
TIMING_TYPES = {
    "timestamp": "Absolute point in time (epoch)",
    "duration": "Elapsed time between two points",
    "latency": "Time from request to response",
    "deadline": "Time by which something must complete",
    "timeout": "Maximum allowed duration",
    "ttl": "Time-to-live before expiration",
    "backoff": "Wait time before retry",
    "interval": "Regular time between events",
    "jitter": "Random variation in timing",
    "skew": "Difference between clocks",
    "drift": "Gradual timing deviation",
    "epoch": "Reference point for time measurement",
}

# Message passing primitives
MESSAGE_TYPES = {
    "request": "Message asking for action/data",
    "response": "Message answering a request",
    "event": "Message signaling something happened",
    "command": "Message ordering an action",
    "query": "Message asking for information",
    "notification": "Message informing of state change",
    "acknowledgment": "Message confirming receipt",
    "heartbeat": "Message signaling liveness",
    "broadcast": "Message to all receivers",
    "multicast": "Message to group of receivers",
    "unicast": "Message to single receiver",
    "stream": "Continuous flow of messages",
}

# State machine primitives
STATE_TYPES = {
    "initial": "Starting state before any transitions",
    "pending": "Waiting for something to happen",
    "active": "Currently being processed",
    "paused": "Temporarily suspended",
    "blocked": "Waiting on external dependency",
    "completed": "Successfully finished",
    "failed": "Finished with error",
    "cancelled": "Terminated by request",
    "expired": "Timed out",
    "tombstone": "Soft-deleted, awaiting cleanup",
    "archived": "Moved to long-term storage",
    "unknown": "State cannot be determined",
}

# State transitions
TRANSITION_TYPES = {
    "create": "Entity comes into existence",
    "update": "Entity properties modified",
    "delete": "Entity removed from system",
    "restore": "Entity recovered from deletion",
    "promote": "Entity elevated in hierarchy",
    "demote": "Entity lowered in hierarchy",
    "merge": "Multiple entities combined",
    "split": "Single entity divided",
    "clone": "Entity duplicated",
    "migrate": "Entity moved between stores",
    "transform": "Entity type changed",
    "version": "New version of entity created",
}

# Resource types
RESOURCE_TYPES = {
    "cpu": "Compute cycles",
    "memory": "RAM allocation",
    "disk": "Storage space",
    "network": "Bandwidth",
    "gpu": "Graphics/tensor compute",
    "token": "LLM tokens (input/output)",
    "connection": "Network/DB connection",
    "thread": "Execution thread",
    "process": "OS process",
    "handle": "File/socket handle",
    "lock": "Synchronization primitive",
    "quota": "Rate limit allowance",
}

# Agent primitives - for agent execution semantics
AGENT_TYPES = {
    "agent": "Autonomous entity that can act",
    "tool": "Capability an agent can invoke",
    "environment": "Context in which agent operates",
    "observation": "Agent's perception of state",
    "action": "Agent's effect on environment",
    "goal": "Desired end state",
    "plan": "Sequence of actions to achieve goal",
    "belief": "Agent's model of world state",
    "intention": "Committed plan being executed",
    "capability": "What agent can do",
    "permission": "What agent is allowed to do",
    "delegation": "Transfer of responsibility",
}

# Provenance - tracking where things come from
PROVENANCE_TYPES = {
    "source": "Where data originated",
    "derivation": "How data was computed from other data",
    "attribution": "Who/what created the data",
    "lineage": "Chain of transformations",
    "version": "Which version of source",
    "citation": "Reference to external source",
    "annotation": "Metadata added to data",
    "audit_trail": "Log of access/modifications",
    "signature": "Cryptographic verification",
    "checksum": "Integrity verification",
    "timestamp_proof": "Proof of existence at time",
    "witness": "Third-party attestation",
}

# Error/exception types
ERROR_TYPES = {
    "validation_error": "Data failed validation",
    "parse_error": "Data could not be parsed",
    "timeout_error": "Operation exceeded time limit",
    "resource_error": "Resource unavailable/exhausted",
    "permission_error": "Operation not authorized",
    "conflict_error": "Concurrent modification conflict",
    "not_found_error": "Requested entity doesn't exist",
    "rate_limit_error": "Too many requests",
    "upstream_error": "Dependency failed",
    "internal_error": "Unexpected system error",
    "transient_error": "Temporary failure, may retry",
    "permanent_error": "Unrecoverable failure",
}

# Consistency/replication semantics
CONSISTENCY_TYPES = {
    "strong": "All reads see latest write",
    "eventual": "Reads will eventually see write",
    "causal": "Causally related ops ordered",
    "read_your_writes": "Writer sees own writes",
    "monotonic_reads": "No going backwards",
    "monotonic_writes": "Writes applied in order",
    "linearizable": "Single global order",
    "serializable": "Equivalent to serial execution",
    "snapshot": "Point-in-time consistent view",
    "bounded_staleness": "Max age of reads",
    "quorum": "Majority agreement",
    "lease": "Time-bounded ownership",
}

# Coordination primitives
COORDINATION_TYPES = {
    "lock": "Exclusive access to resource",
    "semaphore": "Counted access to resource",
    "barrier": "Synchronization point",
    "latch": "One-time synchronization",
    "fence": "Memory ordering constraint",
    "transaction": "Atomic group of operations",
    "saga": "Distributed transaction with compensation",
    "lease": "Time-bounded ownership",
    "election": "Leader selection",
    "consensus": "Agreement among nodes",
    "gossip": "Epidemic information spread",
    "vector_clock": "Partial ordering of events",
}

# Stream processing primitives
STREAM_TYPES = {
    "source": "Origin of stream data",
    "sink": "Destination of stream data",
    "transform": "Stateless stream transformation",
    "aggregate": "Stateful stream aggregation",
    "window": "Time/count bounded view",
    "watermark": "Progress tracking marker",
    "checkpoint": "Recovery point in stream",
    "partition": "Parallel stream segment",
    "join": "Combining multiple streams",
    "split": "Dividing stream into multiple",
    "filter": "Selecting from stream",
    "dedupe": "Removing duplicates",
}

# Graph-specific primitives
GRAPH_PRIMITIVES = {
    "node": "Vertex in graph",
    "edge": "Connection between nodes",
    "property": "Key-value on node/edge",
    "label": "Type tag on node/edge",
    "path": "Sequence of edges",
    "subgraph": "Subset of graph",
    "component": "Connected subgraph",
    "clique": "Fully connected subgraph",
    "hub": "Highly connected node",
    "bridge": "Edge connecting components",
    "cycle": "Path that returns to start",
    "tree": "Acyclic connected subgraph",
}

# Embedding/vector primitives
EMBEDDING_TYPES = {
    "dense_vector": "Full floating-point embedding",
    "sparse_vector": "Mostly-zero embedding",
    "binary_vector": "Bit-packed embedding",
    "quantized_vector": "Reduced-precision embedding",
    "centroid": "Cluster center vector",
    "projection": "Dimensionality-reduced vector",
    "similarity": "Distance/similarity score",
    "index": "Vector search index",
    "shard": "Partition of vector index",
    "recall": "Search completeness metric",
    "latency": "Search time metric",
    "throughput": "Search rate metric",
}

# LLM-specific primitives
LLM_PRIMITIVES = {
    "prompt": "Input to language model",
    "completion": "Output from language model",
    "token": "Atomic unit of text",
    "context_window": "Available input space",
    "temperature": "Sampling randomness",
    "top_p": "Nucleus sampling threshold",
    "stop_sequence": "Generation terminator",
    "logprobs": "Token probabilities",
    "embedding": "Text vector representation",
    "fine_tune": "Model adaptation",
    "system_prompt": "Behavior-setting context",
    "few_shot": "Example-based prompting",
}

# Caching primitives
CACHE_TYPES = {
    "hit": "Data found in cache",
    "miss": "Data not in cache",
    "eviction": "Data removed from cache",
    "invalidation": "Data marked stale",
    "refresh": "Data updated in cache",
    "prefetch": "Data loaded proactively",
    "write_through": "Write to cache and store",
    "write_back": "Write to cache, defer store",
    "write_around": "Write to store, skip cache",
    "read_through": "Load to cache on miss",
    "ttl": "Time-based expiration",
    "lru": "Least-recently-used eviction",
}

# Observability primitives
OBSERVABILITY_TYPES = {
    "metric": "Numeric measurement",
    "log": "Structured event record",
    "trace": "Distributed operation path",
    "span": "Single operation in trace",
    "baggage": "Context propagated in trace",
    "exemplar": "Representative sample",
    "histogram": "Distribution of values",
    "counter": "Monotonic count",
    "gauge": "Point-in-time value",
    "alert": "Condition-triggered notification",
    "dashboard": "Visualization of metrics",
    "slo": "Service level objective",
}

# =============================================================================
# PROCESS & STREAM DESCRIPTORS - Top-level process and stream event framing
# =============================================================================

# Top-level process descriptor - the long-running process itself
PROCESS_DESCRIPTOR = {
    "process_id": "Unique identifier for this process instance",
    "process_type": "Classification of process (batch, stream, daemon, job)",
    "process_name": "Human-readable name",
    "process_version": "Semantic version of process code",
    "parent_process_id": "ID of spawning process (if any)",
    "root_process_id": "ID of top-level ancestor",
    "start_timestamp": "When process began",
    "expected_duration": "Estimated runtime",
    "deadline": "Hard deadline for completion",
    "priority": "Scheduling priority (0-100)",
    "resource_class": "Resource allocation tier",
    "isolation_level": "Sandboxing/isolation mode",
    "owner": "User/service that owns this process",
    "tags": "Arbitrary key-value labels",
    "config": "Process configuration snapshot",
    "entrypoint": "Function/command that started process",
    "working_directory": "Filesystem context",
    "environment": "Environment variables snapshot",
}

# Process lifecycle states
PROCESS_STATES = {
    "initializing": "Process is starting up, loading resources",
    "ready": "Process is ready to begin work",
    "running": "Process is actively executing",
    "waiting": "Process is waiting for external input",
    "suspended": "Process is paused, can be resumed",
    "checkpointing": "Process is saving state for recovery",
    "draining": "Process is finishing current work, no new work",
    "terminating": "Process is shutting down gracefully",
    "terminated": "Process has stopped normally",
    "crashed": "Process stopped due to error",
    "killed": "Process was forcibly terminated",
    "zombie": "Process finished but not yet reaped",
}

# Stream descriptor - individual data stream within process
STREAM_DESCRIPTOR = {
    "stream_id": "Unique identifier for this stream",
    "stream_name": "Human-readable stream name",
    "stream_type": "Classification (input, output, internal, control)",
    "parent_process_id": "Process that owns this stream",
    "source_id": "Where stream data originates",
    "sink_id": "Where stream data goes",
    "schema_id": "Schema describing stream records",
    "encoding": "Serialization format (json, protobuf, arrow)",
    "compression": "Compression algorithm if any",
    "partitioning": "How stream is partitioned",
    "ordering": "Ordering guarantees (none, partial, total)",
    "delivery": "Delivery semantics (at_most_once, at_least_once, exactly_once)",
    "backpressure": "Backpressure handling strategy",
    "buffer_size": "Buffer capacity",
    "rate_limit": "Max throughput",
    "watermark_strategy": "How watermarks are generated",
}

# Stream event types - individual events within a stream
STREAM_EVENT_TYPES = {
    # Data events
    "record": "Regular data record in stream",
    "batch": "Group of records processed together",
    "window_open": "New processing window started",
    "window_close": "Processing window completed",
    "watermark": "Progress marker in event time",
    "barrier": "Checkpoint barrier in stream",

    # Control events
    "start_of_stream": "Stream beginning marker",
    "end_of_stream": "Stream completion marker",
    "pause": "Stream temporarily stopped",
    "resume": "Stream restarted after pause",
    "reconfigure": "Stream configuration changed",
    "repartition": "Stream partitioning changed",

    # Error events
    "invalid_record": "Record failed validation",
    "late_record": "Record arrived after watermark",
    "dropped_record": "Record intentionally discarded",
    "duplicate_record": "Duplicate record detected",
    "out_of_order": "Record arrived out of sequence",
    "backpressure_triggered": "Consumer can't keep up",

    # Metric events
    "throughput_sample": "Records per second measurement",
    "latency_sample": "Processing latency measurement",
    "buffer_level": "Current buffer utilization",
    "lag_sample": "Consumer lag measurement",
}

# Existence frame - contextual framing for entity existence
EXISTENCE_FRAME = {
    "created_at": "When entity came into existence",
    "created_by": "Who/what created the entity",
    "creation_reason": "Why entity was created",
    "creation_context": "Surrounding context at creation",
    "valid_from": "When entity becomes valid",
    "valid_until": "When entity stops being valid",
    "updated_at": "Last modification time",
    "updated_by": "Who/what last modified",
    "update_count": "Number of modifications",
    "accessed_at": "Last access time",
    "access_count": "Number of accesses",
    "expires_at": "When entity will be auto-deleted",
    "deleted_at": "When entity was deleted (if soft-delete)",
    "deleted_by": "Who/what deleted the entity",
    "deletion_reason": "Why entity was deleted",
    "archived_at": "When entity was archived",
    "restored_at": "When entity was restored from archive",
}

# Context frame - the surrounding context of an operation
CONTEXT_FRAME = {
    "request_id": "Unique ID for this request chain",
    "trace_id": "Distributed trace identifier",
    "span_id": "Current span in trace",
    "parent_span_id": "Parent span in trace",
    "session_id": "User/client session",
    "correlation_id": "Business correlation identifier",
    "causation_id": "ID of causing event",
    "conversation_id": "Multi-turn conversation ID",
    "tenant_id": "Multi-tenant isolation key",
    "user_id": "Authenticated user",
    "principal": "Security principal (user or service)",
    "permissions": "Effective permissions in context",
    "locale": "Language/region settings",
    "timezone": "Timezone for interpretation",
    "idempotency_key": "Key for idempotent operations",
    "deadline_remaining": "Time left before deadline",
    "budget_remaining": "Resource budget remaining",
    "priority": "Request priority",
    "retry_count": "Number of retries so far",
    "origin": "Where request originated",
    "user_agent": "Client identifier",
}

# Execution frame - runtime execution context
EXECUTION_FRAME = {
    "executor_id": "ID of executing worker/thread",
    "executor_type": "Type of executor (thread, process, actor, fiber)",
    "node_id": "Physical/virtual node ID",
    "zone": "Availability zone",
    "region": "Geographic region",
    "cluster": "Cluster identifier",
    "namespace": "Logical namespace",
    "deployment": "Deployment/release identifier",
    "environment": "Environment (dev, staging, prod)",
    "feature_flags": "Active feature flags",
    "experiment_arms": "A/B test assignments",
    "runtime_version": "Language runtime version",
    "library_versions": "Key library versions",
    "config_version": "Configuration version",
    "schema_version": "Data schema version",
}

# Causal frame - cause and effect tracking
CAUSAL_FRAME = {
    "trigger_event_id": "Event that triggered this",
    "trigger_type": "Type of trigger (schedule, message, api, manual)",
    "trigger_source": "Source of trigger",
    "trigger_timestamp": "When trigger occurred",
    "cause_chain": "Chain of causal events",
    "effect_type": "Type of effect (create, update, delete, notify)",
    "affected_entities": "Entities affected by this operation",
    "side_effects": "Known side effects produced",
    "compensating_action": "Action to undo if needed",
    "depends_on": "Prerequisites for this operation",
    "blocks": "Operations waiting on this",
    "deadline_source": "Where deadline came from",
    "priority_source": "Where priority came from",
}

# Checkpoint frame - for process recovery
CHECKPOINT_FRAME = {
    "checkpoint_id": "Unique checkpoint identifier",
    "checkpoint_type": "Type (full, incremental, differential)",
    "checkpoint_timestamp": "When checkpoint was taken",
    "checkpoint_trigger": "What triggered checkpoint (time, count, size, barrier)",
    "state_size": "Size of checkpointed state",
    "state_hash": "Hash of checkpointed state",
    "watermarks": "Watermark positions at checkpoint",
    "offsets": "Stream offsets at checkpoint",
    "pending_messages": "Unprocessed messages at checkpoint",
    "in_flight_count": "Operations in progress at checkpoint",
    "recovery_point": "Point from which to resume",
    "parent_checkpoint_id": "Previous checkpoint in chain",
    "retention_policy": "How long to keep checkpoint",
    "storage_location": "Where checkpoint is stored",
}

# Progress frame - tracking operation progress
PROGRESS_FRAME = {
    "total_units": "Total units of work",
    "completed_units": "Units completed so far",
    "failed_units": "Units that failed",
    "skipped_units": "Units intentionally skipped",
    "pending_units": "Units not yet started",
    "in_progress_units": "Units currently processing",
    "percentage_complete": "Completion percentage",
    "estimated_remaining": "Estimated time remaining",
    "throughput_current": "Current processing rate",
    "throughput_average": "Average processing rate",
    "throughput_peak": "Peak processing rate",
    "last_unit_completed": "Most recently completed unit",
    "last_error": "Most recent error",
    "bottleneck": "Current performance bottleneck",
}

# Quality frame - data/processing quality tracking
QUALITY_FRAME = {
    "records_processed": "Total records processed",
    "records_valid": "Records passing validation",
    "records_invalid": "Records failing validation",
    "records_enriched": "Records successfully enriched",
    "records_deduplicated": "Duplicate records removed",
    "null_rate": "Percentage of null values",
    "error_rate": "Percentage of errors",
    "latency_p50": "Median latency",
    "latency_p95": "95th percentile latency",
    "latency_p99": "99th percentile latency",
    "freshness": "Age of most recent data",
    "completeness": "Percentage of expected data present",
    "accuracy_score": "Accuracy measurement if available",
    "confidence_mean": "Mean confidence of extractions",
}

# Resource frame - resource consumption tracking
RESOURCE_FRAME = {
    "cpu_seconds": "CPU time consumed",
    "memory_bytes_peak": "Peak memory usage",
    "memory_bytes_avg": "Average memory usage",
    "disk_bytes_read": "Bytes read from disk",
    "disk_bytes_written": "Bytes written to disk",
    "network_bytes_in": "Bytes received",
    "network_bytes_out": "Bytes sent",
    "tokens_input": "LLM input tokens consumed",
    "tokens_output": "LLM output tokens generated",
    "api_calls": "External API calls made",
    "db_queries": "Database queries executed",
    "cache_hits": "Cache hit count",
    "cache_misses": "Cache miss count",
    "cost_estimate": "Estimated cost in dollars",
}

# =============================================================================
# FEEDBACK & ERROR TAXONOMY - For AI-generated feedback and actionable items
# =============================================================================

# Epistemic errors - errors in knowledge/belief
EPISTEMIC_ERRORS = {
    "missing_information": "Required information not available",
    "incomplete_context": "Context insufficient to make determination",
    "ambiguous_reference": "Reference could resolve to multiple entities",
    "contradictory_evidence": "Evidence points in conflicting directions",
    "stale_knowledge": "Information may be outdated",
    "unverified_claim": "Claim lacks supporting evidence",
    "circular_reasoning": "Reasoning depends on its own conclusion",
    "unfounded_inference": "Inference lacks sufficient premises",
    "confidence_miscalibration": "Confidence doesn't match evidence strength",
    "unknown_unknown": "Awareness of potential blind spots",
    "hallucination_risk": "High risk of confabulated content",
    "source_unreliability": "Source has known reliability issues",
}

# Ontological errors - errors in type/structure/existence
ONTOLOGICAL_ERRORS = {
    "type_mismatch": "Entity assigned wrong type",
    "category_error": "Predicate applied to wrong category",
    "nonexistent_entity": "Reference to entity that doesn't exist",
    "duplicate_entity": "Same entity represented multiple times",
    "missing_relationship": "Expected relationship not found",
    "invalid_relationship": "Relationship violates type constraints",
    "schema_violation": "Data doesn't conform to schema",
    "cardinality_violation": "Wrong number of related entities",
    "temporal_inconsistency": "Timeline doesn't make sense",
    "spatial_inconsistency": "Locations don't make sense",
    "identity_confusion": "Unclear if same or different entity",
    "reification_error": "Abstract treated as concrete or vice versa",
}

# Processing errors - errors during computation
PROCESSING_ERRORS = {
    "extraction_failure": "Failed to extract expected information",
    "parsing_ambiguity": "Multiple valid parse interpretations",
    "normalization_conflict": "Conflicting normalization rules",
    "merge_conflict": "Entities/values can't be cleanly merged",
    "resolution_failure": "Reference couldn't be resolved",
    "timeout_partial": "Timed out with partial results",
    "rate_limit_degraded": "Rate limited, using fallback",
    "model_disagreement": "Models produced conflicting outputs",
    "low_consensus": "Insufficient agreement among models",
    "grounding_failure": "Couldn't ground entity in source text",
    "context_overflow": "Input exceeded context window",
    "output_truncated": "Output was cut off",
}

# Data quality issues - problems with data itself
DATA_QUALITY_ISSUES = {
    "noise": "Data contains irrelevant content",
    "sparsity": "Too little information to process",
    "redundancy": "Excessive repetition",
    "inconsistency": "Self-contradictory data",
    "incompleteness": "Missing expected fields/sections",
    "encoding_error": "Character encoding problems",
    "format_error": "Unexpected data format",
    "corruption": "Data appears damaged",
    "truncation": "Data appears cut off",
    "injection": "Data may contain injection attacks",
    "pii_detected": "Personally identifiable information found",
    "sensitive_content": "Content may be sensitive/harmful",
}

# Improvement suggestions - actionable items for future
IMPROVEMENT_SUGGESTIONS = {
    "add_example": "More examples would improve extraction",
    "clarify_schema": "Schema definition is ambiguous",
    "add_constraint": "Additional constraint would prevent errors",
    "merge_types": "These types should be consolidated",
    "split_type": "This type should be subdivided",
    "add_relationship_type": "New relationship type needed",
    "improve_prompt": "Prompt could be more specific",
    "add_validation": "Additional validation rule needed",
    "cache_result": "This result should be cached",
    "parallelize": "This could be parallelized",
    "batch_optimize": "Batching would improve throughput",
    "index_field": "This field should be indexed",
}

# Feedback categories - types of feedback AI can provide
FEEDBACK_CATEGORIES = {
    "error_report": "Report of an error encountered",
    "warning": "Potential issue that may need attention",
    "suggestion": "Recommendation for improvement",
    "observation": "Neutral observation about data/process",
    "question": "Clarifying question for human",
    "uncertainty": "Expression of low confidence",
    "success": "Confirmation of successful operation",
    "partial_success": "Some objectives met, others not",
    "blocker": "Issue preventing further progress",
    "dependency": "External dependency needed",
    "performance": "Performance-related feedback",
    "cost": "Cost-related feedback",
}

# Feedback priority levels
FEEDBACK_PRIORITY = {
    "critical": "Must be addressed immediately",
    "high": "Should be addressed soon",
    "medium": "Address when convenient",
    "low": "Nice to address eventually",
    "informational": "For awareness only",
    "deferred": "Explicitly postponed",
}

# Feedback actionability
FEEDBACK_ACTIONABILITY = {
    "actionable_now": "Can be acted on immediately",
    "actionable_with_info": "Needs more information to act",
    "actionable_by_human": "Requires human decision",
    "actionable_by_system": "Can be auto-remediated",
    "not_actionable": "For information only",
    "requires_research": "Needs investigation first",
    "requires_approval": "Needs authorization to proceed",
    "blocked_by_dependency": "Waiting on external factor",
}

# Learning signals - what can be learned from feedback
LEARNING_SIGNALS = {
    "positive_example": "This worked well, do more like this",
    "negative_example": "This failed, avoid this pattern",
    "edge_case": "Unusual case that needs special handling",
    "pattern_discovered": "New pattern identified in data",
    "rule_violated": "Existing rule was broken",
    "rule_confirmed": "Existing rule was validated",
    "preference_expressed": "User preference recorded",
    "correction_received": "Human corrected AI output",
    "new_entity_type": "Previously unseen entity type",
    "new_relation_type": "Previously unseen relation type",
    "schema_evolution": "Schema needs to change",
    "model_drift": "Model behavior has changed",
}

# Remediation actions - what can be done about feedback
REMEDIATION_ACTIONS = {
    "retry": "Attempt the operation again",
    "retry_with_backoff": "Retry with exponential backoff",
    "fallback": "Use alternative approach",
    "skip": "Skip this item, continue with others",
    "abort": "Stop processing entirely",
    "escalate": "Escalate to human operator",
    "queue_for_review": "Add to human review queue",
    "auto_fix": "Apply automatic fix",
    "log_and_continue": "Log issue but proceed",
    "cache_invalidate": "Clear cached data",
    "reprocess": "Reprocess from earlier stage",
    "rollback": "Undo recent changes",
}

# Feedback context - where feedback applies
FEEDBACK_SCOPE = {
    "record_level": "Applies to single record",
    "batch_level": "Applies to batch of records",
    "document_level": "Applies to entire document",
    "chunk_level": "Applies to document chunk",
    "entity_level": "Applies to specific entity",
    "relation_level": "Applies to specific relationship",
    "field_level": "Applies to specific field",
    "process_level": "Applies to entire process",
    "system_level": "Applies to system as a whole",
    "model_level": "Applies to specific model",
    "schema_level": "Applies to schema definition",
    "cross_cutting": "Applies across multiple scopes",
}

# Work item types - actionable items generated from feedback
WORK_ITEM_TYPES = {
    "bug": "Something is broken and needs fixing",
    "data_fix": "Data needs to be corrected",
    "schema_change": "Schema needs modification",
    "config_change": "Configuration needs adjustment",
    "prompt_update": "Prompt needs refinement",
    "model_retrain": "Model needs retraining",
    "validation_rule": "New validation rule needed",
    "documentation": "Documentation needs update",
    "investigation": "Issue needs investigation",
    "optimization": "Performance can be improved",
    "cleanup": "Technical debt to address",
    "monitoring": "Need better observability",
}

# Work item states
WORK_ITEM_STATES = {
    "discovered": "Just identified, not yet triaged",
    "triaged": "Assessed and prioritized",
    "assigned": "Assigned to owner",
    "in_progress": "Actively being worked",
    "blocked": "Waiting on dependency",
    "in_review": "Work complete, under review",
    "done": "Successfully completed",
    "wont_fix": "Decided not to address",
    "duplicate": "Same as another item",
    "invalid": "Not actually an issue",
    "deferred": "Postponed to later",
    "automated": "Handled by automation",
}

# Confidence qualifiers - how confident is the feedback
CONFIDENCE_QUALIFIERS = {
    "certain": "Very high confidence in this feedback",
    "likely": "Probably correct",
    "possible": "Might be correct",
    "speculative": "Uncertain, needs verification",
    "heuristic": "Based on rules of thumb",
    "statistical": "Based on aggregate patterns",
    "model_output": "Direct model output, not verified",
    "human_verified": "Confirmed by human",
    "machine_verified": "Confirmed by automated check",
    "consensus": "Multiple sources agree",
    "minority_view": "Some but not all sources agree",
    "novel": "First time seeing this pattern",
}

# Impact assessment - what's the impact of this feedback
IMPACT_ASSESSMENT = {
    "data_loss_risk": "Could result in data loss",
    "correctness_risk": "Could produce incorrect results",
    "performance_impact": "Affects processing speed",
    "cost_impact": "Affects resource costs",
    "user_impact": "Affects end users",
    "security_impact": "Has security implications",
    "compliance_impact": "Has regulatory implications",
    "cascade_risk": "Could trigger additional failures",
    "isolated": "Contained, won't spread",
    "widespread": "Affects many records/users",
    "intermittent": "Happens sometimes",
    "systematic": "Happens consistently",
}


@dataclass
class ExtractedEntity:
    """Entity with full ontology typing."""
    name: str
    entity_type: str
    grounding_category: str  # physical, abstract, temporal, etc.
    confidence: float
    grounded: bool
    models: List[str]
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "type": self.entity_type,
            "grounding_category": self.grounding_category,
            "confidence": round(self.confidence, 4),
            "grounded": self.grounded,
            "models": self.models,
            "properties": self.properties,
        }


@dataclass
class ExtractedRelation:
    """Relationship between entities."""
    source: str  # entity name
    target: str  # entity name
    relation_type: str
    relation_category: str  # compositional, causal, temporal, etc.
    confidence: float
    context: str  # text snippet where relation was found
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "source": self.source,
            "target": self.target,
            "relation_type": self.relation_type,
            "relation_category": self.relation_category,
            "confidence": round(self.confidence, 4),
            "context": self.context[:200] if self.context else "",
            "properties": self.properties,
        }


def get_grounding_category(entity_type: str) -> str:
    """Determine grounding category for an entity type."""
    physical = {"person", "organization", "group", "location", "facility", "object", "substance", "organism"}
    abstract = {"concept", "category", "property", "metric", "pattern", "process"}
    temporal = {"event", "state", "interval", "instant", "schedule", "deadline"}
    symbolic = {"product", "work", "law", "language", "protocol", "specification", "identifier"}
    quantitative = {"quantity", "money", "duration", "distance", "probability", "ratio"}
    execution = {"action", "capability", "intention", "constraint", "outcome", "resource"}
    epistemic = {"claim", "evidence", "uncertainty"}

    if entity_type in physical:
        return "physical"
    elif entity_type in abstract:
        return "abstract"
    elif entity_type in temporal:
        return "temporal"
    elif entity_type in symbolic:
        return "symbolic"
    elif entity_type in quantitative:
        return "quantitative"
    elif entity_type in execution:
        return "execution"
    elif entity_type in epistemic:
        return "epistemic"
    return "other"


def get_relation_category(relation_type: str) -> str:
    """Determine category for a relation type."""
    compositional = {"part_of", "contains", "member_of", "instance_of", "subtype_of"}
    causal = {"causes", "enables", "prevents", "triggers", "inhibits", "modulates"}
    temporal = {"precedes", "overlaps", "during", "starts", "finishes", "meets"}
    spatial = {"located_in", "near", "adjacent_to", "contains_spatially", "path_between"}
    epistemic = {"supports", "contradicts", "elaborates", "summarizes", "cites", "inferred_from"}
    agentive = {"owns", "controls", "authored", "responsible_for", "delegated_to"}
    execution = {"requires", "produces", "satisfies", "uses", "depends_on"}

    if relation_type in compositional:
        return "compositional"
    elif relation_type in causal:
        return "causal"
    elif relation_type in temporal:
        return "temporal"
    elif relation_type in spatial:
        return "spatial"
    elif relation_type in epistemic:
        return "epistemic"
    elif relation_type in agentive:
        return "agentive"
    elif relation_type in execution:
        return "execution"
    return "generic"


class ReflexiveOntologyExtractor:
    """Extract entities AND relationships with full reflexive ontology."""

    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

    # Fast Chinese models - quorum voting (all cheap & fast)
    CHINESE_MODELS = [
        "moonshotai/kimi-k2",              # Kimi K2 - linear attention, very fast
        "deepseek/deepseek-chat-v3-0324",  # DeepSeek V3 - excellent quality
        "qwen/qwen3-235b-a22b",            # Qwen3 235B MoE - latest
        "qwen/qwen-2.5-72b-instruct",      # Qwen 2.5 72B - stable
        "deepseek/deepseek-r1",            # DeepSeek R1 - reasoning
    ]

    def __init__(
        self,
        models: Optional[List[str]] = None,
        max_concurrent: int = 500,  # Parallelism on CHUNKS
        min_consensus: int = 2,     # Need 2+ models to agree
    ):
        self.models = models or self.CHINESE_MODELS
        self.max_concurrent = max_concurrent
        self.min_consensus = min_consensus
        self._semaphore = asyncio.Semaphore(max_concurrent)

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            limits=httpx.Limits(max_connections=600, max_keepalive_connections=300),
        )
        self._headers = {
            "Authorization": f"Bearer {self.OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://parallel-ai.local",
        }


    async def _call_model(
        self,
        model_id: str,
        prompt: str,
        max_tokens: int = 2000,
    ) -> Tuple[str, Optional[str], float]:
        """Call model, return (model_id, response, latency_ms)."""
        payload = {
            "model": model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }

        async with self._semaphore:
            start = time.perf_counter()
            try:
                resp = await self.client.post(
                    f"{self.OPENROUTER_BASE_URL}/chat/completions",
                    headers=self._headers,
                    json=payload,
                )
                latency = (time.perf_counter() - start) * 1000

                if resp.status_code != 200:
                    logger.warning(f"{model_id} returned {resp.status_code}")
                    return model_id, None, latency

                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                # JSONL logging for parseable request details
                usage = data.get("usage", {})
                await log_jsonl({
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": "llm_request",
                    "model": model_id,
                    "status": resp.status_code,
                    "latency_ms": round(latency, 2),
                    "prompt_tokens": usage.get("prompt_tokens"),
                    "completion_tokens": usage.get("completion_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                    "response_length": len(content) if content else 0,
                    "success": True,
                })

                return model_id, content, latency

            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                logger.warning(f"{model_id} error: {e}")

                # Log errors too
                await log_jsonl({
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": "llm_request",
                    "model": model_id,
                    "status": "error",
                    "latency_ms": round(latency, 2),
                    "error": str(e),
                    "success": False,
                })

                return model_id, None, latency

    def _parse_json(self, response: str) -> Dict:
        """Parse JSON from model response."""
        if not response:
            return {}

        response = response.strip()

        # Direct parse
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # Extract from markdown
        if "```" in response:
            try:
                start = response.find("```")
                end = response.rfind("```")
                if start != end:
                    inner = response[start+3:end].strip()
                    if inner.startswith("json"):
                        inner = inner[4:].strip()
                    return json.loads(inner)
            except:
                pass

        # Find JSON boundaries
        for start_char, end_char in [("{", "}"), ("[", "]")]:
            start = response.find(start_char)
            end = response.rfind(end_char)
            if start != -1 and end > start:
                try:
                    return json.loads(response[start:end+1])
                except:
                    pass

        return {}

    def _entity_in_text(self, entity_name: str, text: str) -> bool:
        """Check if entity appears in text (grounding)."""
        text_lower = text.lower()
        entity_lower = entity_name.lower().strip()

        if not entity_lower or len(entity_lower) < 2:
            return False

        if entity_lower in text_lower:
            return True

        # Word boundary match
        pattern = r'\b' + re.escape(entity_lower) + r'\b'
        if re.search(pattern, text_lower):
            return True

        # Multi-word partial match
        words = entity_lower.split()
        if len(words) > 1:
            found = sum(1 for w in words if len(w) > 2 and w in text_lower)
            if found >= len(words) * 0.7:
                return True

        return False

    async def extract_entities_and_relations(
        self,
        text: str,
    ) -> Dict[str, Any]:
        """Extract both entities and relationships from text using Chinese model quorum."""
        # Build the extraction prompt with full ontology
        entity_types_str = ", ".join(sorted(ENTITY_TYPES.keys()))
        relation_types_str = ", ".join(sorted(RELATION_TYPES.keys()))

        prompt = f"""Extract entities AND relationships from this text. Return JSON with both.

ENTITY TYPES (use exactly these):
{entity_types_str}

RELATION TYPES (use exactly these):
{relation_types_str}

RULES:
1. Extract ONLY entities explicitly mentioned in the text
2. Each entity name must be an exact or near-exact quote from the text
3. Extract relationships BETWEEN the entities you found
4. For relationships, include the text snippet showing the relationship

Text:
---
{text[:5000]}
---

Return JSON format:
{{
  "entities": [
    {{"name": "exact name", "type": "person|organization|...", "confidence": 0.0-1.0}}
  ],
  "relationships": [
    {{"source": "entity name", "target": "entity name", "type": "causes|enables|...", "context": "text snippet showing relation", "confidence": 0.0-1.0}}
  ]
}}

JSON only:"""

        # Call all models in parallel
        tasks = [self._call_model(m, prompt) for m in self.models]
        results = await asyncio.gather(*tasks)

        # Aggregate entities
        entity_votes: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "names": [],
            "types": defaultdict(int),
            "confidences": [],
            "models": [],
        })

        # Aggregate relationships
        relation_votes: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "types": defaultdict(int),
            "confidences": [],
            "contexts": [],
            "models": [],
        })

        for model_id, response, latency in results:
            if not response:
                continue

            parsed = self._parse_json(response)
            short_name = model_id.split("/")[-1]

            # Process entities
            entities = parsed.get("entities", [])
            if isinstance(parsed, list):
                entities = parsed

            for e in entities:
                if not isinstance(e, dict):
                    continue

                name = str(e.get("name", e.get("entity", ""))).strip()
                if not name or len(name) < 2:
                    continue

                key = name.lower()
                entity_votes[key]["names"].append(name)
                etype = e.get("type", e.get("entity_type", "concept")).lower()
                entity_votes[key]["types"][etype] += 1
                entity_votes[key]["confidences"].append(float(e.get("confidence", 0.5)))
                entity_votes[key]["models"].append(short_name)

            # Process relationships
            relations = parsed.get("relationships", parsed.get("relations", []))
            for r in relations:
                if not isinstance(r, dict):
                    continue

                source = str(r.get("source", "")).strip()
                target = str(r.get("target", "")).strip()
                rtype = r.get("type", r.get("relation_type", "related_to")).lower()

                if not source or not target:
                    continue

                # Normalize relation key
                key = f"{source.lower()}|{target.lower()}"
                relation_votes[key]["source"] = source
                relation_votes[key]["target"] = target
                relation_votes[key]["types"][rtype] += 1
                relation_votes[key]["confidences"].append(float(r.get("confidence", 0.5)))
                relation_votes[key]["contexts"].append(r.get("context", "")[:200])
                relation_votes[key]["models"].append(short_name)

        # Build consensus entities
        validated_entities: List[ExtractedEntity] = []
        for key, data in entity_votes.items():
            vote_count = len(set(data["models"]))
            if vote_count < self.min_consensus:
                continue

            name = max(set(data["names"]), key=data["names"].count)

            # Check grounding
            if not self._entity_in_text(name, text):
                continue

            etype = max(data["types"], key=data["types"].get)
            # Normalize to valid type
            if etype not in ENTITY_TYPES:
                etype = "concept"

            avg_conf = sum(data["confidences"]) / len(data["confidences"])
            grounding_cat = get_grounding_category(etype)

            # LOG EACH ENTITY AS EXTRACTED
            logger.info(f"✓ ENTITY: {name} [{etype}/{grounding_cat}] conf={avg_conf:.2f} votes={vote_count}")

            validated_entities.append(ExtractedEntity(
                name=name,
                entity_type=etype,
                grounding_category=grounding_cat,
                confidence=avg_conf,
                grounded=True,
                models=list(set(data["models"])),
            ))

        # Build consensus relationships
        validated_relations: List[ExtractedRelation] = []
        entity_names = {e.name.lower() for e in validated_entities}

        for key, data in relation_votes.items():
            vote_count = len(set(data["models"]))
            if vote_count < self.min_consensus:
                continue

            source = data["source"]
            target = data["target"]

            # Check both entities exist
            if source.lower() not in entity_names or target.lower() not in entity_names:
                continue

            rtype = max(data["types"], key=data["types"].get)
            # Normalize to valid relation
            if rtype not in RELATION_TYPES:
                rtype = "related_to"

            avg_conf = sum(data["confidences"]) / len(data["confidences"])
            context = max(data["contexts"], key=len) if data["contexts"] else ""
            relation_cat = get_relation_category(rtype)

            # LOG EACH RELATIONSHIP AS EXTRACTED
            logger.info(f"✓ RELATION: {source} --[{rtype}/{relation_cat}]--> {target} conf={avg_conf:.2f} votes={vote_count}")

            validated_relations.append(ExtractedRelation(
                source=source,
                target=target,
                relation_type=rtype,
                relation_category=relation_cat,
                confidence=avg_conf,
                context=context,
            ))

        # Sort by confidence
        validated_entities.sort(key=lambda x: -x.confidence)
        validated_relations.sort(key=lambda x: -x.confidence)

        return {
            "entities": [e.to_dict() for e in validated_entities],
            "relationships": [r.to_dict() for r in validated_relations],
            "entity_count": len(validated_entities),
            "relationship_count": len(validated_relations),
            "stats": {
                "total_raw_entities": sum(len(d["names"]) for d in entity_votes.values()),
                "total_raw_relations": sum(len(d["models"]) for d in relation_votes.values()),
                "models_responded": sum(1 for r in results if r[1]),
            },
        }

    async def close(self):
        await self.client.aclose()


# Global instance
_extractor: Optional[ReflexiveOntologyExtractor] = None


def get_reflexive_extractor(models: Optional[List[str]] = None) -> ReflexiveOntologyExtractor:
    """Get or create reflexive ontology extractor."""
    global _extractor
    if _extractor is None:
        _extractor = ReflexiveOntologyExtractor(models=models)
    return _extractor


async def extract_world_model(text: str, models: Optional[List[str]] = None) -> Dict[str, Any]:
    """High-level function to extract full world model from text."""
    extractor = get_reflexive_extractor(models)
    return await extractor.extract_entities_and_relations(text)


# =============================================================================
# FORMULA DECOMPOSITION - Unfold mathematical formulas into graph structures
# =============================================================================

# Common mathematical constants
MATH_CONSTANTS = {
    "π": "pi",
    "pi": "pi",
    "e": "euler_number",
    "∞": "infinity",
    "inf": "infinity",
    "φ": "golden_ratio",
    "i": "imaginary_unit",
}

# Operator precedence for parsing
OPERATORS = {
    "+": {"name": "addition", "relation": "added_to", "precedence": 1},
    "-": {"name": "subtraction", "relation": "subtracted_from", "precedence": 1},
    "*": {"name": "multiplication", "relation": "multiplied_by", "precedence": 2},
    "×": {"name": "multiplication", "relation": "multiplied_by", "precedence": 2},
    "/": {"name": "division", "relation": "divided_by", "precedence": 2},
    "÷": {"name": "division", "relation": "divided_by", "precedence": 2},
    "^": {"name": "exponentiation", "relation": "raised_to_power", "precedence": 3},
    "**": {"name": "exponentiation", "relation": "raised_to_power", "precedence": 3},
    "=": {"name": "equality", "relation": "equals", "precedence": 0},
    "==": {"name": "equality", "relation": "equals", "precedence": 0},
    ">": {"name": "greater_than", "relation": "greater_than", "precedence": 0},
    "<": {"name": "less_than", "relation": "less_than", "precedence": 0},
    ">=": {"name": "greater_or_equal", "relation": "greater_or_equal", "precedence": 0},
    "≥": {"name": "greater_or_equal", "relation": "greater_or_equal", "precedence": 0},
    "<=": {"name": "less_or_equal", "relation": "less_or_equal", "precedence": 0},
    "≤": {"name": "less_or_equal", "relation": "less_or_equal", "precedence": 0},
    "≈": {"name": "approximately_equals", "relation": "approximately_equals", "precedence": 0},
    "∝": {"name": "proportional_to", "relation": "proportional_to", "precedence": 0},
}

# Known functions
MATH_FUNCTIONS = {
    "sqrt": "square_root",
    "log": "logarithm",
    "ln": "natural_logarithm",
    "exp": "exponential",
    "sin": "sine",
    "cos": "cosine",
    "tan": "tangent",
    "abs": "absolute_value",
    "max": "maximum",
    "min": "minimum",
    "sum": "summation",
    "prod": "product",
    "lim": "limit",
    "d/d": "derivative",
    "∫": "integral",
    "∂": "partial_derivative",
    "∇": "gradient",
    "Δ": "difference",
}


@dataclass
class FormulaNode:
    """A node in the formula graph."""
    id: str
    node_type: str  # variable, constant, expression, equation, inequality, etc.
    value: str  # the actual symbol/value
    semantic_meaning: Optional[str] = None  # what it represents (e.g., "benefit")
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "type": self.node_type,
            "value": self.value,
            "semantic_meaning": self.semantic_meaning,
            "properties": self.properties,
        }


@dataclass
class FormulaEdge:
    """An edge in the formula graph."""
    source_id: str
    target_id: str
    relation_type: str
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "relation": self.relation_type,
            "properties": self.properties,
        }


class FormulaDecomposer:
    """Decompose mathematical formulas into structured graph representations.

    Transforms opaque formula strings like "b/c > k" into queryable graphs:

    Input: "b/c > k"
    Output:
        Nodes:
            - b (variable, meaning: "benefit")
            - c (variable, meaning: "cost")
            - k (variable, meaning: "average_neighbors")
            - b/c (expression, meaning: "benefit_cost_ratio")
            - "b/c > k" (inequality, meaning: "cooperation_condition")

        Edges:
            - b --divided_by--> c (produces: b/c)
            - b/c --greater_than--> k
            - inequality --determines--> cooperation_emerges
    """

    def __init__(self, use_llm_semantics: bool = True):
        self.use_llm_semantics = use_llm_semantics
        self._node_counter = 0
        self.client = None
        self._headers = None
        if use_llm_semantics:
            self.client = httpx.AsyncClient(timeout=30.0)
            api_key = os.getenv("OPENROUTER_API_KEY")
            self._headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

    def _gen_id(self, prefix: str = "node") -> str:
        """Generate unique node ID."""
        self._node_counter += 1
        return f"{prefix}_{self._node_counter}"

    def _tokenize(self, formula: str) -> List[str]:
        """Tokenize a formula into components."""
        # Normalize unicode
        formula = formula.replace("≥", ">=").replace("≤", "<=")
        formula = formula.replace("×", "*").replace("÷", "/")

        tokens = []
        i = 0
        while i < len(formula):
            c = formula[i]

            # Skip whitespace
            if c.isspace():
                i += 1
                continue

            # Multi-char operators
            if i + 1 < len(formula):
                two_char = formula[i:i+2]
                if two_char in (">=", "<=", "==", "**", "!="):
                    tokens.append(two_char)
                    i += 2
                    continue

            # Single char operators or parens
            if c in "+-*/^=><()[]{}":
                tokens.append(c)
                i += 1
                continue

            # Numbers (including decimals)
            if c.isdigit() or (c == '.' and i + 1 < len(formula) and formula[i+1].isdigit()):
                num = ""
                while i < len(formula) and (formula[i].isdigit() or formula[i] == '.'):
                    num += formula[i]
                    i += 1
                tokens.append(num)
                continue

            # Identifiers (variables, function names)
            if c.isalpha() or c in "αβγδεζηθικλμνξοπρστυφχψωΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩ_":
                ident = ""
                while i < len(formula) and (formula[i].isalnum() or formula[i] in "_'"):
                    ident += formula[i]
                    i += 1
                tokens.append(ident)
                continue

            # Greek letters and special symbols
            if c in "πφ∞∝≈∇∂∫∑∏":
                tokens.append(c)
                i += 1
                continue

            i += 1

        return tokens

    def _is_number(self, s: str) -> bool:
        """Check if string is a number."""
        try:
            float(s)
            return True
        except:
            return s in MATH_CONSTANTS

    def _classify_token(self, token: str) -> str:
        """Classify a token as variable, constant, operator, or function."""
        if token in OPERATORS:
            return "operator"
        if token in "()[]{}":
            return "bracket"
        if self._is_number(token):
            return "constant"
        if token.lower() in MATH_CONSTANTS:
            return "constant"
        if token.lower() in MATH_FUNCTIONS:
            return "function"
        return "variable"

    def decompose(
        self,
        formula: str,
        context: str = "",
        symbol_meanings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Decompose a formula into a graph structure.

        Args:
            formula: The formula string (e.g., "b/c > k")
            context: Surrounding text for semantic inference
            symbol_meanings: Pre-defined meanings for symbols (e.g., {"b": "benefit"})

        Returns:
            Dictionary with nodes, edges, and metadata
        """
        symbol_meanings = symbol_meanings or {}
        tokens = self._tokenize(formula)

        nodes: List[FormulaNode] = []
        edges: List[FormulaEdge] = []
        node_map: Dict[str, FormulaNode] = {}  # value -> node

        # First pass: create nodes for all variables and constants
        for token in tokens:
            token_type = self._classify_token(token)
            if token_type in ("variable", "constant") and token not in node_map:
                node_id = self._gen_id(token_type[:3])
                semantic = symbol_meanings.get(token)

                # Check if it's a known constant
                if token.lower() in MATH_CONSTANTS:
                    semantic = MATH_CONSTANTS[token.lower()]

                node = FormulaNode(
                    id=node_id,
                    node_type=token_type,
                    value=token,
                    semantic_meaning=semantic,
                )
                nodes.append(node)
                node_map[token] = node

        # Second pass: identify structure and create edges
        # Look for binary operators and create expression nodes
        i = 0
        while i < len(tokens):
            token = tokens[i]

            if token in OPERATORS:
                op_info = OPERATORS[token]

                # Get left and right operands
                left_token = tokens[i-1] if i > 0 else None
                right_token = tokens[i+1] if i + 1 < len(tokens) else None

                if left_token and right_token and left_token in node_map and right_token in node_map:
                    left_node = node_map[left_token]
                    right_node = node_map[right_token]

                    # For comparison operators, create inequality/equation node
                    if op_info["precedence"] == 0:
                        expr_type = "equation" if "equal" in op_info["name"] else "inequality"
                        expr_id = self._gen_id(expr_type[:3])
                        expr_value = f"{left_token} {token} {right_token}"

                        expr_node = FormulaNode(
                            id=expr_id,
                            node_type=expr_type,
                            value=expr_value,
                            properties={"operator": token, "operator_name": op_info["name"]},
                        )
                        nodes.append(expr_node)
                        node_map[expr_value] = expr_node

                        # Edge from left to right with comparison relation
                        edges.append(FormulaEdge(
                            source_id=left_node.id,
                            target_id=right_node.id,
                            relation_type=op_info["relation"],
                        ))

                        # Expression has both sides
                        edges.append(FormulaEdge(
                            source_id=expr_node.id,
                            target_id=left_node.id,
                            relation_type="has_operand",
                            properties={"position": "left"},
                        ))
                        edges.append(FormulaEdge(
                            source_id=expr_node.id,
                            target_id=right_node.id,
                            relation_type="has_operand",
                            properties={"position": "right"},
                        ))

                    # For arithmetic operators
                    else:
                        expr_id = self._gen_id("exp")
                        expr_value = f"{left_token}{token}{right_token}"

                        expr_node = FormulaNode(
                            id=expr_id,
                            node_type="expression",
                            value=expr_value,
                            properties={"operator": token, "operator_name": op_info["name"]},
                        )
                        nodes.append(expr_node)
                        node_map[expr_value] = expr_node

                        # Create the operational edge
                        edges.append(FormulaEdge(
                            source_id=left_node.id,
                            target_id=right_node.id,
                            relation_type=op_info["relation"],
                            properties={"produces": expr_node.id},
                        ))

                        # Expression structure
                        if token in "/*÷":
                            edges.append(FormulaEdge(
                                source_id=expr_node.id,
                                target_id=left_node.id,
                                relation_type="has_numerator" if token in "/÷" else "has_factor",
                            ))
                            edges.append(FormulaEdge(
                                source_id=expr_node.id,
                                target_id=right_node.id,
                                relation_type="has_denominator" if token in "/÷" else "has_factor",
                            ))
                        else:
                            edges.append(FormulaEdge(
                                source_id=expr_node.id,
                                target_id=left_node.id,
                                relation_type="has_term" if token in "+-" else "has_operand",
                            ))
                            edges.append(FormulaEdge(
                                source_id=expr_node.id,
                                target_id=right_node.id,
                                relation_type="has_term" if token in "+-" else "has_operand",
                            ))

            i += 1

        # Create top-level formula node
        formula_node = FormulaNode(
            id=self._gen_id("formula"),
            node_type="formula",
            value=formula,
            properties={
                "original": formula,
                "token_count": len(tokens),
                "variable_count": sum(1 for n in nodes if n.node_type == "variable"),
                "constant_count": sum(1 for n in nodes if n.node_type == "constant"),
            },
        )
        nodes.append(formula_node)

        # Link formula to its components
        for node in nodes:
            if node.id != formula_node.id and node.node_type in ("variable", "constant"):
                edges.append(FormulaEdge(
                    source_id=formula_node.id,
                    target_id=node.id,
                    relation_type="has_variable" if node.node_type == "variable" else "has_constant",
                ))

        return {
            "formula": formula,
            "nodes": [n.to_dict() for n in nodes],
            "edges": [e.to_dict() for e in edges],
            "stats": {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "variables": [n.value for n in nodes if n.node_type == "variable"],
                "constants": [n.value for n in nodes if n.node_type == "constant"],
                "operators": [t for t in tokens if t in OPERATORS],
            },
        }

    async def decompose_with_semantics(
        self,
        formula: str,
        context: str,
        existing_entities: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """Decompose formula AND infer semantic meanings from context using LLM.

        This is the full pipeline - parses the formula structure and uses an LLM
        to infer what each symbol represents based on surrounding text.
        """
        # First do structural decomposition
        result = self.decompose(formula, context)

        if not self.use_llm_semantics or not self.client:
            return result

        # Get variables that need semantic meaning
        variables = result["stats"]["variables"]
        if not variables:
            return result

        # Build prompt for semantic inference
        existing_names = []
        if existing_entities:
            existing_names = [e.get("name", "") for e in existing_entities[:20]]

        prompt = f"""Given this mathematical formula and its context, infer what each variable represents.

Formula: {formula}

Context:
{context[:2000]}

Variables to identify: {', '.join(variables)}

{"Known entities in this text: " + ', '.join(existing_names) if existing_names else ""}

Return JSON mapping each variable to its semantic meaning:
{{"b": "benefit", "c": "cost", "k": "average_degree"}}

JSON only:"""

        try:
            resp = await self.client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=self._headers,
                json={
                    "model": "deepseek/deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 300,
                    "temperature": 0.0,
                },
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                # Parse the semantic mappings
                try:
                    if "{" in content:
                        start = content.find("{")
                        end = content.rfind("}") + 1
                        semantics = json.loads(content[start:end])

                        # Update nodes with semantic meanings
                        for node in result["nodes"]:
                            if node["type"] == "variable" and node["value"] in semantics:
                                node["semantic_meaning"] = semantics[node["value"]]

                                # Add symbolizes edge to link symbol to meaning
                                result["edges"].append({
                                    "source": node["id"],
                                    "target": semantics[node["value"]],
                                    "relation": "symbolizes",
                                })
                except:
                    pass

        except Exception as e:
            logger.warning(f"LLM semantic inference failed: {e}")

        return result

    async def close(self):
        """Close HTTP client."""
        if self.client:
            await self.client.aclose()


# =============================================================================
# CANONICAL MATHEMATICAL MODELS - SymPy-defined formulas for multi-agent systems
# =============================================================================

def _build_canonical_models():
    """Build canonical models using SymPy for proper symbolic representation."""
    try:
        from sympy import (
            symbols, Symbol, Function, Derivative, Sum, Product, Integral,
            pi, E, oo, sqrt, exp, log, sin, cos,
            Eq, Gt, Lt, Ge, Le, And, Or,
            Matrix, IndexedBase, Idx,
        )

        # Define common symbols
        t, dt = symbols('t dt', real=True)  # time
        i, j, k, n, m = symbols('i j k n m', integer=True)  # indices
        x, y, z = symbols('x y z', real=True)  # generic variables
        alpha, beta, gamma, delta, epsilon = symbols('alpha beta gamma delta epsilon', real=True, positive=True)
        lambda_, mu, sigma, tau = symbols('lambda mu sigma tau', real=True, positive=True)

        return {
            # ===== GAME THEORY =====
            "cooperation_threshold": {
                "domain": "evolutionary_game_theory",
                "name": "Network Reciprocity Condition",
                "formula_str": "b/c > k",
                "sympy_expr": Gt(Symbol('b')/Symbol('c'), Symbol('k')),
                "symbols": {
                    "b": {"name": "benefit", "type": "quantity", "meaning": "benefit_to_recipient"},
                    "c": {"name": "cost", "type": "quantity", "meaning": "cost_to_donor"},
                    "k": {"name": "avg_degree", "type": "parameter", "meaning": "average_network_degree"},
                },
                "interpretation": "Cooperation evolves when benefit-cost ratio exceeds average neighbors",
                "source": "Nowak & May 1992, Ohtsuki et al. 2006",
            },

            "nash_equilibrium": {
                "domain": "game_theory",
                "name": "Nash Equilibrium Condition",
                "formula_str": "u_i(s_i*, s_{-i}*) >= u_i(s_i, s_{-i}*)",
                "sympy_expr": Ge(
                    Function('u_i')(Symbol('s_i_star'), Symbol('s_neg_i_star')),
                    Function('u_i')(Symbol('s_i'), Symbol('s_neg_i_star'))
                ),
                "symbols": {
                    "u_i": {"name": "utility_i", "type": "function", "meaning": "utility_function_player_i"},
                    "s_i_star": {"name": "optimal_strategy_i", "type": "variable", "meaning": "best_response_strategy"},
                    "s_neg_i_star": {"name": "others_strategies", "type": "variable", "meaning": "other_players_equilibrium"},
                    "s_i": {"name": "any_strategy", "type": "variable", "meaning": "arbitrary_deviation"},
                },
                "interpretation": "No player can improve by unilateral deviation",
                "source": "Nash 1950",
            },

            "payoff_matrix_game": {
                "domain": "game_theory",
                "name": "Two-Player Symmetric Game Payoff",
                "formula_str": "π = p_C * p_C * R + p_C * p_D * S + p_D * p_C * T + p_D * p_D * P",
                "sympy_expr": Eq(
                    Symbol('pi'),
                    Symbol('p_C')**2 * Symbol('R') +
                    Symbol('p_C') * Symbol('p_D') * Symbol('S') +
                    Symbol('p_D') * Symbol('p_C') * Symbol('T') +
                    Symbol('p_D')**2 * Symbol('P')
                ),
                "symbols": {
                    "pi": {"name": "expected_payoff", "type": "quantity", "meaning": "expected_payoff"},
                    "p_C": {"name": "prob_cooperate", "type": "probability", "meaning": "cooperation_probability"},
                    "p_D": {"name": "prob_defect", "type": "probability", "meaning": "defection_probability"},
                    "R": {"name": "reward", "type": "quantity", "meaning": "mutual_cooperation_payoff"},
                    "S": {"name": "sucker", "type": "quantity", "meaning": "exploited_cooperator_payoff"},
                    "T": {"name": "temptation", "type": "quantity", "meaning": "defector_against_cooperator_payoff"},
                    "P": {"name": "punishment", "type": "quantity", "meaning": "mutual_defection_payoff"},
                },
                "interpretation": "Expected payoff in symmetric 2x2 game",
                "source": "Axelrod 1984",
            },

            # ===== EVOLUTIONARY DYNAMICS =====
            "replicator_dynamics": {
                "domain": "evolutionary_dynamics",
                "name": "Replicator Equation",
                "formula_str": "dx_i/dt = x_i * (f_i - φ)",
                "sympy_expr": Eq(
                    Derivative(Function('x_i')(t), t),
                    Function('x_i')(t) * (Symbol('f_i') - Symbol('phi'))
                ),
                "symbols": {
                    "x_i": {"name": "strategy_fraction", "type": "variable", "meaning": "population_share_strategy_i"},
                    "f_i": {"name": "fitness_i", "type": "quantity", "meaning": "fitness_of_strategy_i"},
                    "phi": {"name": "avg_fitness", "type": "quantity", "meaning": "average_population_fitness"},
                    "t": {"name": "time", "type": "variable", "meaning": "continuous_time"},
                },
                "interpretation": "Strategies with above-average fitness grow; below-average shrink",
                "source": "Taylor & Jonker 1978",
            },

            "price_equation": {
                "domain": "evolutionary_dynamics",
                "name": "Price Equation",
                "formula_str": "Δz = Cov(w, z)/w̄ + E(w·Δz)/w̄",
                "sympy_expr": Eq(
                    Symbol('Delta_z'),
                    Symbol('Cov_wz')/Symbol('w_bar') + Symbol('E_w_Delta_z')/Symbol('w_bar')
                ),
                "symbols": {
                    "Delta_z": {"name": "trait_change", "type": "quantity", "meaning": "change_in_mean_trait"},
                    "Cov_wz": {"name": "fitness_trait_cov", "type": "quantity", "meaning": "covariance_fitness_trait"},
                    "w_bar": {"name": "mean_fitness", "type": "quantity", "meaning": "population_mean_fitness"},
                    "E_w_Delta_z": {"name": "transmission_bias", "type": "quantity", "meaning": "expected_transmission_bias"},
                },
                "interpretation": "Trait change = selection + transmission bias",
                "source": "Price 1970",
            },

            # ===== SYNCHRONIZATION =====
            "kuramoto_critical": {
                "domain": "synchronization",
                "name": "Kuramoto Critical Coupling",
                "formula_str": "K_c = 2 / (π * g(0))",
                "sympy_expr": Eq(
                    Symbol('K_c'),
                    2 / (pi * Function('g')(0))
                ),
                "symbols": {
                    "K_c": {"name": "critical_coupling", "type": "threshold", "meaning": "onset_synchronization_coupling"},
                    "g": {"name": "freq_distribution", "type": "function", "meaning": "natural_frequency_density"},
                },
                "interpretation": "Minimum coupling strength for synchronization onset",
                "source": "Kuramoto 1984",
            },

            "kuramoto_order": {
                "domain": "synchronization",
                "name": "Kuramoto Order Parameter",
                "formula_str": "r * exp(i*ψ) = (1/N) * Σ exp(i*θ_j)",
                "sympy_expr": Eq(
                    Symbol('r') * exp(Symbol('I') * Symbol('psi')),
                    Symbol('mean_exp_theta')
                ),
                "symbols": {
                    "r": {"name": "order_parameter", "type": "metric", "meaning": "coherence_magnitude"},
                    "psi": {"name": "mean_phase", "type": "variable", "meaning": "collective_phase"},
                    "theta_j": {"name": "oscillator_phase", "type": "variable", "meaning": "individual_oscillator_phase"},
                    "N": {"name": "population_size", "type": "constant", "meaning": "number_of_oscillators"},
                },
                "interpretation": "r=0 means incoherent, r=1 means fully synchronized",
                "source": "Kuramoto 1984",
            },

            # ===== CONSENSUS & DISTRIBUTED SYSTEMS =====
            "consensus_update": {
                "domain": "distributed_consensus",
                "name": "Consensus Protocol Update",
                "formula_str": "x_i(t+1) = Σ_j a_ij * x_j(t)",
                "sympy_expr": Eq(
                    Function('x_i')(t + 1),
                    Symbol('sum_aij_xj')
                ),
                "symbols": {
                    "x_i": {"name": "agent_state", "type": "variable", "meaning": "state_of_agent_i"},
                    "a_ij": {"name": "weight", "type": "parameter", "meaning": "influence_weight_j_on_i"},
                    "t": {"name": "time_step", "type": "variable", "meaning": "discrete_time"},
                },
                "interpretation": "Agent state becomes weighted average of neighbors",
                "source": "DeGroot 1974, Olfati-Saber 2004",
            },

            "laplacian_consensus": {
                "domain": "distributed_consensus",
                "name": "Laplacian Consensus Dynamics",
                "formula_str": "dx/dt = -L * x",
                "sympy_expr": Eq(
                    Derivative(Symbol('x'), t),
                    -Symbol('L') * Symbol('x')
                ),
                "symbols": {
                    "x": {"name": "state_vector", "type": "variable", "meaning": "agent_states_vector"},
                    "L": {"name": "laplacian", "type": "matrix", "meaning": "graph_laplacian_matrix"},
                },
                "interpretation": "Continuous-time consensus with Laplacian dynamics",
                "source": "Olfati-Saber & Murray 2004",
            },

            # ===== REINFORCEMENT LEARNING =====
            "bellman_equation": {
                "domain": "reinforcement_learning",
                "name": "Bellman Optimality Equation",
                "formula_str": "V*(s) = max_a [R(s,a) + γ * Σ P(s'|s,a) * V*(s')]",
                "sympy_expr": Eq(
                    Function('V_star')(Symbol('s')),
                    Symbol('max_a_expr')
                ),
                "symbols": {
                    "V_star": {"name": "optimal_value", "type": "function", "meaning": "optimal_state_value"},
                    "s": {"name": "state", "type": "variable", "meaning": "environment_state"},
                    "a": {"name": "action", "type": "variable", "meaning": "agent_action"},
                    "R": {"name": "reward", "type": "function", "meaning": "immediate_reward"},
                    "gamma": {"name": "discount", "type": "parameter", "meaning": "discount_factor"},
                    "P": {"name": "transition", "type": "function", "meaning": "transition_probability"},
                },
                "interpretation": "Optimal value = best action's immediate + discounted future reward",
                "source": "Bellman 1957",
            },

            "q_learning_update": {
                "domain": "reinforcement_learning",
                "name": "Q-Learning Update",
                "formula_str": "Q(s,a) <- Q(s,a) + α * [r + γ * max_a' Q(s',a') - Q(s,a)]",
                "sympy_expr": Eq(
                    Function('Q_new')(Symbol('s'), Symbol('a')),
                    Function('Q')(Symbol('s'), Symbol('a')) +
                    Symbol('alpha') * (Symbol('r') + Symbol('gamma') * Symbol('max_Q_next') - Function('Q')(Symbol('s'), Symbol('a')))
                ),
                "symbols": {
                    "Q": {"name": "action_value", "type": "function", "meaning": "state_action_value"},
                    "alpha": {"name": "learning_rate", "type": "parameter", "meaning": "step_size"},
                    "gamma": {"name": "discount", "type": "parameter", "meaning": "discount_factor"},
                    "r": {"name": "reward", "type": "quantity", "meaning": "observed_reward"},
                },
                "interpretation": "Update Q towards observed reward + best future estimate",
                "source": "Watkins 1989",
            },

            # ===== EPIDEMICS / INFORMATION SPREAD =====
            "sir_model": {
                "domain": "epidemic_dynamics",
                "name": "SIR Epidemic Model",
                "formula_str": "dI/dt = β*S*I - γ*I",
                "sympy_expr": Eq(
                    Derivative(Symbol('I'), t),
                    Symbol('beta') * Symbol('S') * Symbol('I') - Symbol('gamma_r') * Symbol('I')
                ),
                "symbols": {
                    "S": {"name": "susceptible", "type": "variable", "meaning": "susceptible_population"},
                    "I": {"name": "infected", "type": "variable", "meaning": "infected_population"},
                    "beta": {"name": "transmission_rate", "type": "parameter", "meaning": "infection_rate"},
                    "gamma_r": {"name": "recovery_rate", "type": "parameter", "meaning": "recovery_rate"},
                },
                "interpretation": "Infection grows with contact, decreases with recovery",
                "source": "Kermack & McKendrick 1927",
            },

            "basic_reproduction": {
                "domain": "epidemic_dynamics",
                "name": "Basic Reproduction Number",
                "formula_str": "R_0 = β / γ",
                "sympy_expr": Eq(
                    Symbol('R_0'),
                    Symbol('beta') / Symbol('gamma_r')
                ),
                "symbols": {
                    "R_0": {"name": "reproduction_number", "type": "threshold", "meaning": "expected_secondary_infections"},
                    "beta": {"name": "transmission_rate", "type": "parameter", "meaning": "infection_rate"},
                    "gamma_r": {"name": "recovery_rate", "type": "parameter", "meaning": "recovery_rate"},
                },
                "interpretation": "R_0 > 1 means epidemic spreads; R_0 < 1 means dies out",
                "source": "Kermack & McKendrick 1927",
            },

            # ===== NETWORK DYNAMICS =====
            "preferential_attachment": {
                "domain": "network_growth",
                "name": "Barabási-Albert Preferential Attachment",
                "formula_str": "P(k) ~ k^(-γ), γ ≈ 3",
                "sympy_expr": Eq(
                    Function('P')(Symbol('k')),
                    Symbol('A') * Symbol('k')**(-Symbol('gamma_exp'))
                ),
                "symbols": {
                    "P": {"name": "degree_distribution", "type": "function", "meaning": "probability_of_degree_k"},
                    "k": {"name": "degree", "type": "variable", "meaning": "node_degree"},
                    "gamma_exp": {"name": "exponent", "type": "parameter", "meaning": "power_law_exponent"},
                },
                "interpretation": "Rich-get-richer leads to scale-free degree distribution",
                "source": "Barabási & Albert 1999",
            },

            # ===== CONTROL THEORY =====
            "lyapunov_stability": {
                "domain": "control_theory",
                "name": "Lyapunov Stability Condition",
                "formula_str": "dV/dt = ∇V · f(x) < 0",
                "sympy_expr": Lt(
                    Derivative(Function('V')(Symbol('x')), t),
                    0
                ),
                "symbols": {
                    "V": {"name": "lyapunov_function", "type": "function", "meaning": "energy_like_function"},
                    "x": {"name": "state", "type": "variable", "meaning": "system_state"},
                    "f": {"name": "dynamics", "type": "function", "meaning": "system_dynamics"},
                },
                "interpretation": "If V decreases along trajectories, equilibrium is stable",
                "source": "Lyapunov 1892",
            },

            # ===== INFORMATION THEORY =====
            "shannon_entropy": {
                "domain": "information_theory",
                "name": "Shannon Entropy",
                "formula_str": "H(X) = -Σ p(x) * log(p(x))",
                "sympy_expr": Eq(
                    Function('H')(Symbol('X')),
                    -Symbol('sum_p_log_p')
                ),
                "symbols": {
                    "H": {"name": "entropy", "type": "metric", "meaning": "information_entropy"},
                    "p": {"name": "probability", "type": "function", "meaning": "probability_mass_function"},
                    "X": {"name": "random_variable", "type": "variable", "meaning": "discrete_random_variable"},
                },
                "interpretation": "Expected information content / uncertainty of distribution",
                "source": "Shannon 1948",
            },

            "mutual_information": {
                "domain": "information_theory",
                "name": "Mutual Information",
                "formula_str": "I(X;Y) = H(X) + H(Y) - H(X,Y)",
                "sympy_expr": Eq(
                    Function('I')(Symbol('X'), Symbol('Y')),
                    Function('H')(Symbol('X')) + Function('H')(Symbol('Y')) - Function('H_joint')(Symbol('X'), Symbol('Y'))
                ),
                "symbols": {
                    "I": {"name": "mutual_info", "type": "metric", "meaning": "mutual_information"},
                    "H": {"name": "entropy", "type": "function", "meaning": "entropy_function"},
                    "X": {"name": "var_X", "type": "variable", "meaning": "first_random_variable"},
                    "Y": {"name": "var_Y", "type": "variable", "meaning": "second_random_variable"},
                },
                "interpretation": "Shared information between two variables",
                "source": "Shannon 1948",
            },
        }
    except ImportError:
        # Return string-only version if SymPy not available
        return {}


# Build canonical models at module load
CANONICAL_MODELS = _build_canonical_models()


def get_formula_decomposer(use_llm: bool = True) -> FormulaDecomposer:
    """Get formula decomposer instance."""
    return FormulaDecomposer(use_llm_semantics=use_llm)


class SympyFormulaDecomposer:
    """Decompose formulas using SymPy for proper AST parsing.

    This version correctly handles operator precedence and nested expressions.
    """

    def __init__(self, use_llm_semantics: bool = True):
        self.use_llm_semantics = use_llm_semantics
        self._node_counter = 0
        self.client = None
        self._headers = None

        # Try to import sympy
        try:
            from sympy import Symbol, Expr, Rel, Eq, Gt, Lt, Ge, Le
            from sympy.parsing.sympy_parser import parse_expr, standard_transformations, implicit_multiplication_application
            from sympy.core.numbers import Number, Pi, E
            from sympy.core.mul import Mul
            from sympy.core.add import Add
            from sympy.core.power import Pow
            self._sympy_available = True
        except ImportError:
            self._sympy_available = False
            logger.warning("SymPy not available - falling back to regex parser")

        if use_llm_semantics:
            self.client = httpx.AsyncClient(timeout=30.0)
            api_key = os.getenv("OPENROUTER_API_KEY")
            self._headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

    def _gen_id(self, prefix: str = "node") -> str:
        self._node_counter += 1
        return f"{prefix}_{self._node_counter}"

    def _sympy_to_graph(
        self,
        expr,
        nodes: List[FormulaNode],
        edges: List[FormulaEdge],
        node_map: Dict[str, FormulaNode],
        symbol_meanings: Dict[str, str],
    ) -> FormulaNode:
        """Recursively convert SymPy expression tree to graph nodes/edges."""
        from sympy import Symbol, Number, Mul, Add, Pow, Rational, pi, E
        from sympy.core.relational import Relational, Eq, Gt, Lt, Ge, Le, Ne
        from sympy.core.numbers import Pi as PiClass  # Pi class for isinstance check

        expr_str = str(expr)

        # Already processed this exact expression
        if expr_str in node_map:
            return node_map[expr_str]

        # Symbol (variable)
        if isinstance(expr, Symbol):
            node = FormulaNode(
                id=self._gen_id("var"),
                node_type="variable",
                value=str(expr),
                semantic_meaning=symbol_meanings.get(str(expr)),
            )
            nodes.append(node)
            node_map[expr_str] = node
            return node

        # Number (constant)
        if isinstance(expr, (Number, int, float)):
            node = FormulaNode(
                id=self._gen_id("con"),
                node_type="constant",
                value=str(expr),
            )
            nodes.append(node)
            node_map[expr_str] = node
            return node

        # Pi, E
        if isinstance(expr, PiClass):
            node = FormulaNode(
                id=self._gen_id("con"),
                node_type="constant",
                value="π",
                semantic_meaning="pi",
            )
            nodes.append(node)
            node_map[expr_str] = node
            return node

        # Relational (=, >, <, etc.)
        if isinstance(expr, Relational):
            # Process LHS and RHS
            lhs_node = self._sympy_to_graph(expr.lhs, nodes, edges, node_map, symbol_meanings)
            rhs_node = self._sympy_to_graph(expr.rhs, nodes, edges, node_map, symbol_meanings)

            # Determine relation type
            if isinstance(expr, Eq):
                rel_type = "equals"
                node_type = "equation"
            elif isinstance(expr, Gt):
                rel_type = "greater_than"
                node_type = "inequality"
            elif isinstance(expr, Lt):
                rel_type = "less_than"
                node_type = "inequality"
            elif isinstance(expr, Ge):
                rel_type = "greater_or_equal"
                node_type = "inequality"
            elif isinstance(expr, Le):
                rel_type = "less_or_equal"
                node_type = "inequality"
            else:
                rel_type = "related_to"
                node_type = "relation"

            # Create relation node
            node = FormulaNode(
                id=self._gen_id(node_type[:3]),
                node_type=node_type,
                value=expr_str,
                properties={"relation_type": rel_type},
            )
            nodes.append(node)
            node_map[expr_str] = node

            # Add edges
            edges.append(FormulaEdge(
                source_id=lhs_node.id,
                target_id=rhs_node.id,
                relation_type=rel_type,
            ))
            edges.append(FormulaEdge(
                source_id=node.id,
                target_id=lhs_node.id,
                relation_type="has_operand",
                properties={"position": "left"},
            ))
            edges.append(FormulaEdge(
                source_id=node.id,
                target_id=rhs_node.id,
                relation_type="has_operand",
                properties={"position": "right"},
            ))

            return node

        # Multiplication - but first check if it's actually division (b * c^-1 pattern)
        if isinstance(expr, Mul):
            args = list(expr.args)

            # Detect division pattern: Mul with Pow(..., -1) factors
            # SymPy represents b/c as Mul(b, Pow(c, -1))
            numerator_args = []
            denominator_args = []

            for arg in args:
                if isinstance(arg, Pow) and arg.args[1] == -1:
                    # This is x^-1, so x goes in denominator
                    denominator_args.append(arg.args[0])
                else:
                    numerator_args.append(arg)

            # If we have denominators, this is division - create clean structure
            if denominator_args:
                # Build numerator expression (or just the single var)
                if len(numerator_args) == 1:
                    num_node = self._sympy_to_graph(numerator_args[0], nodes, edges, node_map, symbol_meanings)
                elif len(numerator_args) > 1:
                    # Multiple numerator factors - create Mul for them
                    from sympy import Mul as SMul
                    num_expr = SMul(*numerator_args)
                    num_node = self._sympy_to_graph(num_expr, nodes, edges, node_map, symbol_meanings)
                else:
                    # No numerator args means implicit 1 (like 1/x)
                    num_node = FormulaNode(
                        id=self._gen_id("con"),
                        node_type="constant",
                        value="1",
                    )
                    nodes.append(num_node)
                    node_map["1"] = num_node

                # Build denominator expression
                if len(denominator_args) == 1:
                    denom_node = self._sympy_to_graph(denominator_args[0], nodes, edges, node_map, symbol_meanings)
                else:
                    from sympy import Mul as SMul
                    denom_expr = SMul(*denominator_args)
                    denom_node = self._sympy_to_graph(denom_expr, nodes, edges, node_map, symbol_meanings)

                # Create the division expression node
                node = FormulaNode(
                    id=self._gen_id("exp"),
                    node_type="expression",
                    value=expr_str,
                    properties={"operator": "/", "operator_name": "division"},
                )
                nodes.append(node)
                node_map[expr_str] = node

                # Clean edges: numerator and denominator
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=num_node.id,
                    relation_type="has_numerator",
                ))
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=denom_node.id,
                    relation_type="has_denominator",
                ))
                edges.append(FormulaEdge(
                    source_id=num_node.id,
                    target_id=denom_node.id,
                    relation_type="divided_by",
                ))

                return node

            # Pure multiplication (no division) - process normally
            factor_nodes = [
                self._sympy_to_graph(arg, nodes, edges, node_map, symbol_meanings)
                for arg in args
            ]

            node = FormulaNode(
                id=self._gen_id("exp"),
                node_type="expression",
                value=expr_str,
                properties={"operator": "*", "operator_name": "multiplication"},
            )
            nodes.append(node)
            node_map[expr_str] = node

            for i, factor in enumerate(factor_nodes):
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=factor.id,
                    relation_type="has_factor",
                    properties={"position": i},
                ))
                if i > 0:
                    edges.append(FormulaEdge(
                        source_id=factor_nodes[i-1].id,
                        target_id=factor.id,
                        relation_type="multiplied_by",
                    ))

            return node

        # Addition
        if isinstance(expr, Add):
            args = expr.args
            term_nodes = [
                self._sympy_to_graph(arg, nodes, edges, node_map, symbol_meanings)
                for arg in args
            ]

            node = FormulaNode(
                id=self._gen_id("exp"),
                node_type="expression",
                value=expr_str,
                properties={"operator": "+", "operator_name": "addition"},
            )
            nodes.append(node)
            node_map[expr_str] = node

            for i, term in enumerate(term_nodes):
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=term.id,
                    relation_type="has_term",
                    properties={"position": i},
                ))
                if i > 0:
                    edges.append(FormulaEdge(
                        source_id=term_nodes[i-1].id,
                        target_id=term.id,
                        relation_type="added_to",
                    ))

            return node

        # Power/Division (Pow with -1 exponent = division: x^-1 = 1/x)
        if isinstance(expr, Pow):
            base = expr.args[0]
            exp = expr.args[1]

            # Check if this is division (x^-1) BEFORE creating nodes
            is_division = str(exp) == "-1"

            if is_division:
                # x^-1 = 1/x - create clean division structure with implicit 1 numerator
                denom_node = self._sympy_to_graph(base, nodes, edges, node_map, symbol_meanings)

                # Create implicit "1" numerator
                num_node = FormulaNode(
                    id=self._gen_id("con"),
                    node_type="constant",
                    value="1",
                    semantic_meaning="unity",
                )
                nodes.append(num_node)
                node_map["_implicit_1"] = num_node

                # Create the reciprocal/division expression
                node = FormulaNode(
                    id=self._gen_id("exp"),
                    node_type="expression",
                    value=f"1/{base}",  # Display as 1/x not x^-1
                    properties={"operator": "/", "operator_name": "reciprocal"},
                )
                nodes.append(node)
                node_map[expr_str] = node

                # Clean division edges
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=num_node.id,
                    relation_type="has_numerator",
                ))
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=denom_node.id,
                    relation_type="has_denominator",
                ))
                edges.append(FormulaEdge(
                    source_id=num_node.id,
                    target_id=denom_node.id,
                    relation_type="divided_by",
                ))

                return node
            else:
                # Regular exponentiation
                base_node = self._sympy_to_graph(base, nodes, edges, node_map, symbol_meanings)
                exp_node = self._sympy_to_graph(exp, nodes, edges, node_map, symbol_meanings)

                node = FormulaNode(
                    id=self._gen_id("exp"),
                    node_type="expression",
                    value=expr_str,
                    properties={"operator": "^", "operator_name": "exponentiation"},
                )
                nodes.append(node)
                node_map[expr_str] = node

                edges.append(FormulaEdge(
                    source_id=base_node.id,
                    target_id=exp_node.id,
                    relation_type="raised_to_power",
                ))
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=base_node.id,
                    relation_type="has_base",
                ))
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=exp_node.id,
                    relation_type="has_exponent",
                ))

                return node

        # Generic expression - create node for it
        node = FormulaNode(
            id=self._gen_id("exp"),
            node_type="expression",
            value=expr_str,
        )
        nodes.append(node)
        node_map[expr_str] = node

        # Process args if any
        if hasattr(expr, 'args'):
            for i, arg in enumerate(expr.args):
                arg_node = self._sympy_to_graph(arg, nodes, edges, node_map, symbol_meanings)
                edges.append(FormulaEdge(
                    source_id=node.id,
                    target_id=arg_node.id,
                    relation_type="has_operand",
                    properties={"position": i},
                ))

        return node

    def decompose(
        self,
        formula: str,
        context: str = "",
        symbol_meanings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Decompose formula using SymPy's proper parser."""
        if not self._sympy_available:
            # Fall back to regex parser
            fallback = FormulaDecomposer(use_llm_semantics=False)
            return fallback.decompose(formula, context, symbol_meanings)

        from sympy.parsing.sympy_parser import parse_expr, standard_transformations
        from sympy import symbols, Gt, Lt, Ge, Le, Eq

        symbol_meanings = symbol_meanings or {}
        nodes: List[FormulaNode] = []
        edges: List[FormulaEdge] = []
        node_map: Dict[str, FormulaNode] = {}

        # Handle comparison operators (SymPy's parse_expr doesn't handle these directly)
        comparison_ops = {
            ">=": (Ge, "greater_or_equal"),
            "<=": (Le, "less_or_equal"),
            ">": (Gt, "greater_than"),
            "<": (Lt, "less_than"),
            "=": (Eq, "equals"),
            "==": (Eq, "equals"),
        }

        parsed_expr = None

        # Try to find and handle comparison operator
        for op_str, (op_class, _) in sorted(comparison_ops.items(), key=lambda x: -len(x[0])):
            if op_str in formula:
                parts = formula.split(op_str, 1)
                if len(parts) == 2:
                    try:
                        lhs = parse_expr(parts[0].strip(), transformations=standard_transformations)
                        rhs = parse_expr(parts[1].strip(), transformations=standard_transformations)
                        parsed_expr = op_class(lhs, rhs)
                        break
                    except:
                        continue

        # If no comparison, parse as plain expression
        if parsed_expr is None:
            try:
                parsed_expr = parse_expr(formula, transformations=standard_transformations)
            except Exception as e:
                logger.warning(f"SymPy parse failed for '{formula}': {e}")
                # Fall back to regex
                fallback = FormulaDecomposer(use_llm_semantics=False)
                return fallback.decompose(formula, context, symbol_meanings)

        # Convert to graph
        root_node = self._sympy_to_graph(parsed_expr, nodes, edges, node_map, symbol_meanings)

        # Create top-level formula node if not already a relation/equation
        if root_node.node_type not in ("equation", "inequality"):
            formula_node = FormulaNode(
                id=self._gen_id("formula"),
                node_type="formula",
                value=formula,
                properties={"root_expression": root_node.id},
            )
            nodes.append(formula_node)
            edges.append(FormulaEdge(
                source_id=formula_node.id,
                target_id=root_node.id,
                relation_type="has_expression",
            ))

        # Collect stats
        variables = [n.value for n in nodes if n.node_type == "variable"]
        constants = [n.value for n in nodes if n.node_type == "constant"]

        return {
            "formula": formula,
            "nodes": [n.to_dict() for n in nodes],
            "edges": [e.to_dict() for e in edges],
            "stats": {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "variables": variables,
                "constants": constants,
            },
            "parser": "sympy",
        }

    async def close(self):
        if self.client:
            await self.client.aclose()


async def decompose_formula(
    formula: str,
    context: str = "",
    symbol_meanings: Optional[Dict[str, str]] = None,
    use_llm: bool = True,
) -> Dict[str, Any]:
    """High-level function to decompose a formula into graph structure.

    Examples:
        >>> result = await decompose_formula("b/c > k", context="cooperation emerges when...")
        >>> print(result["nodes"])  # [var:b, var:c, var:k, expr:b/c, ineq:b/c>k]
        >>> print(result["edges"])  # [divided_by, greater_than, has_numerator, ...]
    """
    decomposer = get_formula_decomposer(use_llm=use_llm)

    if use_llm and context:
        result = await decomposer.decompose_with_semantics(formula, context)
    else:
        result = decomposer.decompose(formula, context, symbol_meanings)

    await decomposer.close()
    return result

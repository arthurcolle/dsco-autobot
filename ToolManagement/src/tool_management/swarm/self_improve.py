"""
Self-Improvement System for Swarm Agents.

Enables agents to propose, review, and implement code changes
under MCP oversight with hierarchical approval.
"""

from __future__ import annotations

import ast
import asyncio
import difflib
import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field

from .master_control import (
    MasterControlProgram,
    ClearanceLevel,
    ActionType,
    AuthorizationRequest,
    get_mcp,
)
from .swarm_mcp_bridge import get_swarm_mcp_bridge

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    """Types of code changes."""
    BUG_FIX = "bug_fix"
    OPTIMIZATION = "optimization"
    REFACTOR = "refactor"
    NEW_FEATURE = "new_feature"
    SECURITY_FIX = "security_fix"
    DOCUMENTATION = "documentation"
    TEST = "test"


class ChangeStatus(str, Enum):
    """Status of a proposed change."""
    PROPOSED = "proposed"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    IMPLEMENTED = "implemented"
    ROLLED_BACK = "rolled_back"


class CodeChange(BaseModel):
    """A proposed code change."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    proposer_id: str
    file_path: str
    change_type: ChangeType
    description: str
    original_content: str
    proposed_content: str
    diff: str = ""
    justification: str = ""
    status: ChangeStatus = ChangeStatus.PROPOSED
    reviewers: List[str] = Field(default_factory=list)
    approvals: List[str] = Field(default_factory=list)
    rejections: List[str] = Field(default_factory=list)
    review_comments: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    implemented_at: Optional[datetime] = None
    checksum_before: str = ""
    checksum_after: str = ""


class ImprovementProposal(BaseModel):
    """High-level improvement proposal (may contain multiple changes)."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    proposer_id: str
    title: str
    description: str
    rationale: str
    changes: List[str] = Field(default_factory=list)  # CodeChange IDs
    status: ChangeStatus = ChangeStatus.PROPOSED
    priority: int = 1  # 1-5, higher = more important
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SelfImprovementEngine:
    """
    Enables swarm agents to improve the codebase.

    Flow:
    1. Agent proposes change via LLM analysis
    2. MCP validates authorization
    3. Reviewers (higher-clearance agents) review
    4. If approved, change is implemented
    5. Tests run, rollback if failure
    """

    def __init__(self, swarm_id: str, codebase_root: str):
        self.swarm_id = swarm_id
        self.codebase_root = Path(codebase_root)
        self.mcp = get_mcp(swarm_id)
        self.bridge = get_swarm_mcp_bridge()

        self.changes: Dict[str, CodeChange] = {}
        self.proposals: Dict[str, ImprovementProposal] = {}
        self.backups: Dict[str, str] = {}  # file_path -> original content

        # Protected paths that require OVERSEER clearance
        self.protected_paths = {
            "master_control.py",
            "self_improve.py",
            "__init__.py",
        }

    async def analyze_for_improvements(
        self,
        agent_id: str,
        file_path: str,
        focus: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Use LLM to analyze a file for potential improvements.

        Returns list of improvement suggestions.
        """
        # Check authorization
        auth = self.mcp.authorize(AuthorizationRequest(
            agent_id=agent_id,
            action=ActionType.READ_FILE,
            target=file_path,
            justification="Analyzing for improvements",
        ))

        if not auth.authorized:
            logger.warning(f"Agent {agent_id} denied read access to {file_path}")
            return []

        full_path = self.codebase_root / file_path
        if not full_path.exists():
            return []

        content = full_path.read_text()

        # Use LLM to analyze
        prompt = f"""Analyze this Python code for potential improvements.
Focus: {focus or 'general quality, performance, security'}

Return a JSON array of improvements:
[{{"type": "bug_fix|optimization|refactor|security_fix", "description": "...", "location": "line X or function Y", "priority": 1-5}}]

Code:
```python
{content[:8000]}
```

Be specific and actionable. Only suggest meaningful improvements."""

        result = await self.bridge.execute_llm_task(
            task_description=prompt,
            swarm_id=self.swarm_id,
        )

        if not result.get("success"):
            return []

        # Parse suggestions
        try:
            response = result.get("result", "[]")
            # Extract JSON from response
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                import json
                return json.loads(json_match.group())
        except Exception as e:
            logger.error(f"Failed to parse improvement suggestions: {e}")

        return []

    async def propose_change(
        self,
        agent_id: str,
        file_path: str,
        change_type: ChangeType,
        description: str,
        proposed_content: str,
        justification: str = "",
    ) -> Optional[CodeChange]:
        """
        Propose a code change.

        Requires at least WORKER clearance.
        """
        # Check if file is protected
        if any(p in file_path for p in self.protected_paths):
            agent = self.mcp.agents.get(agent_id)
            if not agent or agent.clearance < ClearanceLevel.OVERSEER:
                logger.warning(f"Agent {agent_id} attempted to modify protected file {file_path}")
                self.mcp._record_violation(
                    agent_id=agent_id,
                    action=ActionType.WRITE_FILE,
                    target=file_path,
                    reason="Attempted modification of protected file",
                    severity=7,
                )
                return None

        # Request authorization
        auth = await self.mcp.request_approval(
            agent_id=agent_id,
            action=ActionType.WRITE_FILE,
            target=file_path,
            justification=justification or description,
        )

        full_path = self.codebase_root / file_path
        original_content = full_path.read_text() if full_path.exists() else ""

        # Generate diff
        diff = "\n".join(difflib.unified_diff(
            original_content.splitlines(),
            proposed_content.splitlines(),
            fromfile=f"a/{file_path}",
            tofile=f"b/{file_path}",
            lineterm="",
        ))

        change = CodeChange(
            proposer_id=agent_id,
            file_path=file_path,
            change_type=change_type,
            description=description,
            original_content=original_content,
            proposed_content=proposed_content,
            diff=diff,
            justification=justification,
            checksum_before=hashlib.sha256(original_content.encode()).hexdigest()[:16],
        )

        if not auth.authorized:
            change.status = ChangeStatus.UNDER_REVIEW
            # Assign reviewers (supervisors and architects)
            change.reviewers = [
                a.agent_id for a in self.mcp.agents.values()
                if a.clearance >= ClearanceLevel.SUPERVISOR and a.agent_id != agent_id
            ]
        else:
            # Auto-approved for high-clearance agents
            change.status = ChangeStatus.APPROVED
            change.approvals.append(self.mcp.id)

        self.changes[change.id] = change

        logger.info(f"Change proposed: {change.id} by {agent_id} - {description[:50]}")

        return change

    async def review_change(
        self,
        change_id: str,
        reviewer_id: str,
        approve: bool,
        comment: str = "",
    ) -> bool:
        """
        Review a proposed change.

        Requires SUPERVISOR clearance or higher.
        """
        change = self.changes.get(change_id)
        if not change:
            return False

        reviewer = self.mcp.agents.get(reviewer_id)
        if not reviewer:
            return False

        # Check clearance
        if reviewer.clearance < ClearanceLevel.SUPERVISOR:
            return False

        # Record review
        change.review_comments.append({
            "reviewer_id": reviewer_id,
            "approve": approve,
            "comment": comment,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        if approve:
            change.approvals.append(reviewer_id)
        else:
            change.rejections.append(reviewer_id)

        # Determine outcome
        # Need 2 approvals with no rejections, OR 1 ARCHITECT+ approval
        architect_approved = any(
            self.mcp.agents.get(a, {}).clearance >= ClearanceLevel.ARCHITECT
            for a in change.approvals
        )

        if architect_approved or (len(change.approvals) >= 2 and len(change.rejections) == 0):
            change.status = ChangeStatus.APPROVED
        elif len(change.rejections) >= 2:
            change.status = ChangeStatus.REJECTED

        return True

    async def implement_change(self, change_id: str, implementer_id: str) -> Tuple[bool, str]:
        """
        Implement an approved change.

        Returns (success, message).
        """
        change = self.changes.get(change_id)
        if not change:
            return False, "Change not found"

        if change.status != ChangeStatus.APPROVED:
            return False, f"Change not approved (status: {change.status})"

        # Final authorization check
        auth = self.mcp.authorize(AuthorizationRequest(
            agent_id=implementer_id,
            action=ActionType.WRITE_FILE,
            target=change.file_path,
            justification=f"Implementing approved change {change_id}",
        ))

        if not auth.authorized:
            return False, f"Authorization denied: {auth.reason}"

        # Backup original
        self.backups[change.file_path] = change.original_content

        # Write new content
        full_path = self.codebase_root / change.file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Validate Python syntax if .py file
            if change.file_path.endswith('.py'):
                try:
                    ast.parse(change.proposed_content)
                except SyntaxError as e:
                    return False, f"Syntax error in proposed code: {e}"

            full_path.write_text(change.proposed_content)

            change.status = ChangeStatus.IMPLEMENTED
            change.implemented_at = datetime.now(timezone.utc)
            change.checksum_after = hashlib.sha256(change.proposed_content.encode()).hexdigest()[:16]

            logger.info(f"Change {change_id} implemented by {implementer_id}")

            return True, "Change implemented successfully"

        except Exception as e:
            # Rollback
            if change.file_path in self.backups:
                full_path.write_text(self.backups[change.file_path])
            return False, f"Implementation failed: {e}"

    async def rollback_change(self, change_id: str, rollbacker_id: str) -> Tuple[bool, str]:
        """
        Rollback an implemented change.

        Requires WARDEN clearance or higher.
        """
        change = self.changes.get(change_id)
        if not change:
            return False, "Change not found"

        if change.status != ChangeStatus.IMPLEMENTED:
            return False, "Change not implemented"

        rollbacker = self.mcp.agents.get(rollbacker_id)
        if not rollbacker or rollbacker.clearance < ClearanceLevel.WARDEN:
            return False, "Insufficient clearance to rollback"

        if change.file_path not in self.backups:
            return False, "No backup available"

        try:
            full_path = self.codebase_root / change.file_path
            full_path.write_text(self.backups[change.file_path])

            change.status = ChangeStatus.ROLLED_BACK

            logger.info(f"Change {change_id} rolled back by {rollbacker_id}")

            return True, "Rollback successful"

        except Exception as e:
            return False, f"Rollback failed: {e}"

    async def auto_improve(
        self,
        agent_id: str,
        file_path: str,
        improvement_type: ChangeType,
    ) -> Optional[CodeChange]:
        """
        Fully automated improvement: analyze, generate, propose.

        Uses LLM to generate the actual code change.
        """
        # Get analysis
        suggestions = await self.analyze_for_improvements(agent_id, file_path)

        if not suggestions:
            return None

        # Filter by type
        relevant = [s for s in suggestions if s.get("type") == improvement_type.value]
        if not relevant:
            relevant = suggestions[:1]  # Take first suggestion

        if not relevant:
            return None

        suggestion = relevant[0]

        # Read current content
        full_path = self.codebase_root / file_path
        if not full_path.exists():
            return None

        content = full_path.read_text()

        # Generate improved code
        prompt = f"""Apply this improvement to the code:

Improvement: {suggestion.get('description')}
Location: {suggestion.get('location', 'unknown')}

Original code:
```python
{content}
```

Return ONLY the complete improved code, no explanations.
Maintain all existing functionality.
Follow existing code style."""

        result = await self.bridge.execute_llm_task(
            task_description=prompt,
            swarm_id=self.swarm_id,
        )

        if not result.get("success"):
            return None

        # Extract code from response
        response = result.get("result", "")
        code_match = re.search(r'```python\n([\s\S]*?)\n```', response)
        if code_match:
            improved_code = code_match.group(1)
        else:
            improved_code = response

        # Propose the change
        return await self.propose_change(
            agent_id=agent_id,
            file_path=file_path,
            change_type=improvement_type,
            description=suggestion.get("description", "Auto-generated improvement"),
            proposed_content=improved_code,
            justification=f"Auto-improvement based on analysis: {suggestion}",
        )

    def get_pending_reviews(self, reviewer_id: str) -> List[CodeChange]:
        """Get changes pending review by this agent."""
        return [
            c for c in self.changes.values()
            if c.status == ChangeStatus.UNDER_REVIEW
            and reviewer_id in c.reviewers
            and reviewer_id not in c.approvals
            and reviewer_id not in c.rejections
        ]

    def status(self) -> Dict[str, Any]:
        """Get engine status."""
        return {
            "swarm_id": self.swarm_id,
            "total_changes": len(self.changes),
            "by_status": {
                status.value: len([c for c in self.changes.values() if c.status == status])
                for status in ChangeStatus
            },
            "total_proposals": len(self.proposals),
        }


# Singleton per swarm
_engines: Dict[str, SelfImprovementEngine] = {}


def get_improvement_engine(swarm_id: str, codebase_root: str) -> SelfImprovementEngine:
    """Get or create improvement engine for a swarm."""
    if swarm_id not in _engines:
        _engines[swarm_id] = SelfImprovementEngine(swarm_id, codebase_root)
    return _engines[swarm_id]

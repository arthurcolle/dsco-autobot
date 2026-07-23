#!/usr/bin/env python3
"""
Swarm CLI - Command-line interface for managing swarm state, costs, and agents.

Usage:
    swarm status                    Show overall swarm status
    swarm agents [swarm_id]         List all agents and their status
    swarm costs [swarm_id]          Show cost breakdown and spending
    swarm tasks [swarm_id]          List tasks and their status
    swarm mcp [swarm_id]            Show MCP oversight status
    swarm violations [swarm_id]     List violations
    swarm wallets [swarm_id]        Show wallet balances
    swarm changes [swarm_id]        List pending/recent code changes
    swarm logs [swarm_id] [-n N]    Show audit logs
    swarm health                    Check MCP backend health
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add src to path for imports
_src_path = str(Path(__file__).parent.parent.parent)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from tool_management.swarm.master_control import get_mcp, ClearanceLevel
from tool_management.swarm.persistence import get_swarm_db
from tool_management.swarm.swarm_mcp_bridge import get_swarm_mcp_bridge, DEFAULT_MODEL


class Colors:
    """ANSI color codes for terminal output."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    @classmethod
    def disable(cls):
        for attr in dir(cls):
            if not attr.startswith('_') and attr != 'disable':
                setattr(cls, attr, "")


def fmt_decimal(val: Any) -> str:
    """Format decimal/float values."""
    if val is None:
        return "-"
    try:
        return f"{float(val):,.4f}"
    except (ValueError, TypeError):
        return str(val)


def fmt_status(status: str) -> str:
    """Color-code status values."""
    status_colors = {
        "active": Colors.GREEN,
        "idle": Colors.YELLOW,
        "busy": Colors.CYAN,
        "terminated": Colors.RED,
        "pending": Colors.YELLOW,
        "in_progress": Colors.CYAN,
        "completed": Colors.GREEN,
        "failed": Colors.RED,
        "approved": Colors.GREEN,
        "rejected": Colors.RED,
        "under_review": Colors.YELLOW,
    }
    color = status_colors.get(status.lower(), Colors.WHITE)
    return f"{color}{status}{Colors.RESET}"


def fmt_clearance(level: int) -> str:
    """Format clearance level with color."""
    names = {0: "DRONE", 1: "WORKER", 2: "SUPERVISOR", 3: "ARCHITECT",
             4: "WARDEN", 5: "OVERSEER", 10: "MCP"}
    colors = {0: Colors.DIM, 1: Colors.WHITE, 2: Colors.CYAN,
              3: Colors.BLUE, 4: Colors.YELLOW, 5: Colors.MAGENTA, 10: Colors.RED}
    name = names.get(level, f"L{level}")
    color = colors.get(level, Colors.WHITE)
    return f"{color}{name}{Colors.RESET}"


def print_header(title: str):
    """Print a section header."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  {title}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'═' * 60}{Colors.RESET}")


def print_table(headers: List[str], rows: List[List[str]], widths: Optional[List[int]] = None):
    """Print a formatted table."""
    if not widths:
        widths = [max(len(str(row[i])) for row in [headers] + rows) + 2
                  for i in range(len(headers))]

    # Header
    header_line = "".join(f"{Colors.BOLD}{h:<{widths[i]}}{Colors.RESET}"
                          for i, h in enumerate(headers))
    print(f"\n  {header_line}")
    print(f"  {Colors.DIM}{'─' * sum(widths)}{Colors.RESET}")

    # Rows
    for row in rows:
        row_line = "".join(f"{str(c):<{widths[i]}}" for i, c in enumerate(row))
        print(f"  {row_line}")


class SwarmCLI:
    """Command-line interface for swarm management."""

    def __init__(self, no_color: bool = False):
        if no_color:
            Colors.disable()
        self.db = get_swarm_db()

    def cmd_status(self, args):
        """Show overall swarm status."""
        print_header("SWARM SYSTEM STATUS")

        # List all swarms
        swarms = self.db.list_swarms()

        if not swarms:
            print(f"\n  {Colors.YELLOW}No swarms found. Create one with the API.{Colors.RESET}")
            return

        print(f"\n  {Colors.BOLD}Active Swarms:{Colors.RESET} {len(swarms)}")
        print(f"  {Colors.BOLD}LLM Model:{Colors.RESET} {DEFAULT_MODEL}")

        rows = []
        for swarm in swarms:
            swarm_id = swarm.get("id", "?")
            name = swarm.get("name", "unnamed")[:20]
            status = swarm.get("status", "unknown")
            agent_count = swarm.get("agent_count", 0)

            rows.append([
                swarm_id[:12] + "..." if len(swarm_id) > 12 else swarm_id,
                name,
                fmt_status(status),
                str(agent_count),
            ])

        print_table(["ID", "Name", "Status", "Agents"], rows, [16, 22, 14, 8])

    def cmd_agents(self, args):
        """List agents and their status."""
        swarm_id = args.swarm_id or "default"
        print_header(f"AGENTS - Swarm: {swarm_id}")

        # Get MCP for this swarm
        mcp = get_mcp(swarm_id)

        if not mcp.agents:
            print(f"\n  {Colors.YELLOW}No agents registered.{Colors.RESET}")
            return

        rows = []
        for agent_id, profile in mcp.agents.items():
            rows.append([
                agent_id[:20],
                fmt_clearance(profile.clearance.value),
                f"{profile.trust_score:.2f}",
                str(len(profile.violations)),
                profile.supervisor_id[:15] if profile.supervisor_id else "-",
            ])

        print_table(
            ["Agent ID", "Clearance", "Trust", "Violations", "Supervisor"],
            rows,
            [22, 12, 8, 12, 16]
        )

        print(f"\n  {Colors.DIM}Total: {len(mcp.agents)} agents{Colors.RESET}")

    def cmd_costs(self, args):
        """Show cost breakdown and spending."""
        swarm_id = args.swarm_id or "default"
        print_header(f"COST MANAGEMENT - Swarm: {swarm_id}")

        try:
            # Get LLM usage stats from DB
            llm_stats = self.db.get_llm_usage_stats(swarm_id if swarm_id != "default" else None, days=30)

            print(f"\n  {Colors.BOLD}LLM API Usage (30 days):{Colors.RESET}")
            print(f"    Model: {DEFAULT_MODEL}")
            print(f"    Total Requests: {llm_stats.get('total_requests', 0)}")
            print(f"    Total Tokens: {llm_stats.get('total_tokens', 0):,}")
            print(f"    Prompt Tokens: {llm_stats.get('total_prompt_tokens', 0):,}")
            print(f"    Completion Tokens: {llm_stats.get('total_completion_tokens', 0):,}")
            print(f"    Total Cost: {Colors.YELLOW}${llm_stats.get('total_cost_usd', 0):.4f}{Colors.RESET}")
            print(f"    Avg Latency: {llm_stats.get('avg_latency_ms', 0):.0f}ms")
            print(f"    Success Rate: {llm_stats.get('success_rate', 0)*100:.1f}%")

            # Cost breakdown by model
            model_usage = self.db.list_llm_usage_by_model(swarm_id if swarm_id != "default" else None, days=30)
            if model_usage:
                print(f"\n  {Colors.BOLD}Cost by Model:{Colors.RESET}")
                rows = []
                for m in model_usage:
                    rows.append([
                        m["model"][:25],
                        str(m["requests"]),
                        f"{m['tokens']:,}",
                        f"${m['cost_usd']:.4f}",
                    ])
                print_table(["Model", "Requests", "Tokens", "Cost"], rows, [27, 10, 12, 10])

            # Get wallet data (if available)
            try:
                with self.db.connection() as conn:
                    wallets = conn.execute("""
                        SELECT agent_id, balances
                        FROM wallets
                        LIMIT 20
                    """).fetchall()

                if wallets:
                    print(f"\n  {Colors.BOLD}Wallets:{Colors.RESET}")
                    rows = []
                    for w in wallets:
                        agent_id, balances = w
                        rows.append([agent_id[:20], str(balances)[:30] if balances else "-"])
                    print_table(["Agent", "Balances"], rows, [22, 32])
            except Exception:
                pass  # Wallet table may not have data

        except Exception as e:
            print(f"\n  {Colors.RED}Error fetching costs: {e}{Colors.RESET}")

    def cmd_tasks(self, args):
        """List tasks and their status."""
        swarm_id = args.swarm_id or "default"
        print_header(f"TASKS - Swarm: {swarm_id}")

        try:
            with self.db.connection() as conn:
                tasks = conn.execute("""
                    SELECT id, description, status, priority, assigned_to, bounty, created_at
                    FROM tasks
                    WHERE swarm_id = ? OR ? = 'all'
                    ORDER BY created_at DESC
                    LIMIT 20
                """, [swarm_id, swarm_id]).fetchall()

            if not tasks:
                print(f"\n  {Colors.YELLOW}No tasks found.{Colors.RESET}")
                return

            rows = []
            for t in tasks:
                task_id, desc, status, priority, assigned, bounty, created = t
                rows.append([
                    task_id[:10],
                    (desc or "")[:25],
                    fmt_status(status or "pending"),
                    str(priority or 1),
                    (assigned or "-")[:12],
                    fmt_decimal(bounty),
                ])

            print_table(
                ["ID", "Description", "Status", "Pri", "Assigned", "Bounty"],
                rows,
                [12, 27, 14, 5, 14, 10]
            )

            # Summary
            status_counts = {}
            for t in tasks:
                s = t[2] or "pending"
                status_counts[s] = status_counts.get(s, 0) + 1

            print(f"\n  {Colors.DIM}Summary: ", end="")
            print(" | ".join(f"{s}: {c}" for s, c in status_counts.items()))
            print(Colors.RESET)

        except Exception as e:
            print(f"\n  {Colors.RED}Error: {e}{Colors.RESET}")

    def cmd_mcp(self, args):
        """Show MCP oversight status."""
        swarm_id = args.swarm_id or "default"
        print_header(f"MASTER CONTROL PROGRAM - {swarm_id}")

        mcp = get_mcp(swarm_id)
        status = mcp.status()

        print(f"\n  {Colors.BOLD}MCP ID:{Colors.RESET} {status['mcp_id']}")
        print(f"  {Colors.BOLD}Active:{Colors.RESET} {Colors.GREEN if status['active'] else Colors.RED}{status['active']}{Colors.RESET}")
        print(f"  {Colors.BOLD}Agents:{Colors.RESET} {status['total_agents']}")
        print(f"  {Colors.BOLD}Violations:{Colors.RESET} {Colors.RED if status['total_violations'] > 0 else Colors.GREEN}{status['total_violations']}{Colors.RESET}")
        print(f"  {Colors.BOLD}Pending Requests:{Colors.RESET} {status['pending_requests']}")
        print(f"  {Colors.BOLD}Audit Entries:{Colors.RESET} {status['audit_entries']}")

        # Hierarchy
        print(f"\n  {Colors.BOLD}Agent Hierarchy:{Colors.RESET}")
        hierarchy = status.get('hierarchy', {}).get('levels', {})
        for level in ['MCP', 'OVERSEER', 'WARDEN', 'ARCHITECT', 'SUPERVISOR', 'WORKER', 'DRONE']:
            agents = hierarchy.get(level, [])
            if agents:
                print(f"    {fmt_clearance(ClearanceLevel[level].value)}: {', '.join(a[:15] for a in agents[:5])}")

    def cmd_violations(self, args):
        """List violations."""
        swarm_id = args.swarm_id or "default"
        print_header(f"VIOLATIONS - {swarm_id}")

        mcp = get_mcp(swarm_id)

        if not mcp.violations:
            print(f"\n  {Colors.GREEN}No violations recorded.{Colors.RESET}")
            return

        rows = []
        for vid, v in list(mcp.violations.items())[-20:]:
            severity_color = Colors.RED if v.severity >= 7 else Colors.YELLOW if v.severity >= 4 else Colors.WHITE
            rows.append([
                vid[:8],
                v.agent_id[:15],
                v.action.value[:12],
                f"{severity_color}{v.severity}{Colors.RESET}",
                v.reason[:30],
            ])

        print_table(
            ["ID", "Agent", "Action", "Sev", "Reason"],
            rows,
            [10, 17, 14, 5, 32]
        )

    def cmd_wallets(self, args):
        """Show wallet balances."""
        swarm_id = args.swarm_id or "default"
        print_header(f"WALLETS - {swarm_id}")

        try:
            with self.db.connection() as conn:
                wallets = conn.execute("""
                    SELECT agent_id, address, chain, balance, total_spent, total_received,
                           daily_limit, transaction_count
                    FROM wallets
                    ORDER BY balance DESC
                    LIMIT 30
                """).fetchall()

            if not wallets:
                print(f"\n  {Colors.YELLOW}No wallets found.{Colors.RESET}")
                return

            rows = []
            for w in wallets:
                agent, addr, chain, bal, spent, recv, limit, tx_count = w
                rows.append([
                    agent[:15],
                    (addr or "")[:12] + "..." if addr and len(addr) > 12 else (addr or "-"),
                    chain or "SOL",
                    fmt_decimal(bal),
                    fmt_decimal(limit),
                    str(tx_count or 0),
                ])

            print_table(
                ["Agent", "Address", "Chain", "Balance", "Daily Limit", "Txns"],
                rows,
                [17, 16, 6, 12, 12, 6]
            )

        except Exception as e:
            print(f"\n  {Colors.RED}Error: {e}{Colors.RESET}")

    def cmd_changes(self, args):
        """List pending/recent code changes."""
        swarm_id = args.swarm_id or "default"
        print_header(f"CODE CHANGES - {swarm_id}")

        try:
            from tool_management.swarm.self_improve import get_improvement_engine
            engine = get_improvement_engine(swarm_id, ".")

            status = engine.status()
            print(f"\n  {Colors.BOLD}Total Changes:{Colors.RESET} {status['total_changes']}")

            if status['total_changes'] > 0:
                print(f"\n  {Colors.BOLD}By Status:{Colors.RESET}")
                for s, count in status['by_status'].items():
                    if count > 0:
                        print(f"    {fmt_status(s)}: {count}")

                # List recent changes
                print(f"\n  {Colors.BOLD}Recent Changes:{Colors.RESET}")
                rows = []
                for cid, change in list(engine.changes.items())[-10:]:
                    rows.append([
                        cid[:8],
                        change.proposer_id[:12],
                        change.file_path[:20],
                        change.change_type.value[:10],
                        fmt_status(change.status.value),
                    ])

                print_table(
                    ["ID", "Proposer", "File", "Type", "Status"],
                    rows,
                    [10, 14, 22, 12, 14]
                )
        except Exception as e:
            print(f"\n  {Colors.YELLOW}No changes tracked: {e}{Colors.RESET}")

    def cmd_logs(self, args):
        """Show audit logs."""
        swarm_id = args.swarm_id or "default"
        n = args.n or 20
        print_header(f"AUDIT LOGS - {swarm_id} (last {n})")

        mcp = get_mcp(swarm_id)

        if not mcp.audit_log:
            print(f"\n  {Colors.DIM}No audit entries.{Colors.RESET}")
            return

        for entry in mcp.audit_log[-n:]:
            ts = str(entry.get('created_at', ''))[:19]
            event = entry.get('event_type', 'unknown')
            data = entry.get('data', {}) or {}

            # Color by event type
            if 'violation' in str(event):
                color = Colors.RED
            elif 'approved' in str(event) or 'registered' in str(event):
                color = Colors.GREEN
            elif 'vetoed' in str(event) or 'demoted' in str(event):
                color = Colors.YELLOW
            else:
                color = Colors.WHITE

            # Extract key details from data
            details_str = " ".join(f"{k}={v}" for k, v in list(data.items())[:3] if v)

            print(f"  {Colors.DIM}{ts}{Colors.RESET} {color}{str(event):20}{Colors.RESET} {details_str[:40]}")

    def cmd_hierarchy(self, args):
        """Show swarm hierarchy."""
        swarm_id = args.swarm_id or "default"
        print_header(f"SWARM HIERARCHY - {swarm_id}")

        try:
            from tool_management.swarm.swarm_hierarchy import get_swarm_controller

            controller = get_swarm_controller(swarm_id)
            status = controller.status()

            print(f"\n  {Colors.BOLD}Swarm:{Colors.RESET} {status['swarm_id']}")
            print(f"  {Colors.BOLD}Autonomy:{Colors.RESET} {status['autonomy_level']}")
            print(f"  {Colors.BOLD}Can Spawn Swarms:{Colors.RESET} {status['can_spawn_swarms']}")

            if status['parent_swarm']:
                print(f"\n  {Colors.BOLD}Parent:{Colors.RESET} {status['parent_swarm']}")
            else:
                print(f"\n  {Colors.BOLD}Parent:{Colors.RESET} None (root swarm)")

            if status['children']:
                print(f"\n  {Colors.BOLD}Children ({status['children_count']}):{Colors.RESET}")
                for child in status['children']:
                    print(f"    - {child}")
            else:
                print(f"\n  {Colors.DIM}No child swarms{Colors.RESET}")

        except Exception as e:
            print(f"\n  {Colors.RED}Error: {e}{Colors.RESET}")

    def cmd_spawn(self, args):
        """Spawn a child swarm."""
        print_header(f"SPAWNING CHILD SWARM")

        async def do_spawn():
            from tool_management.swarm.swarm_hierarchy import get_swarm_controller, AutonomyLevel
            from tool_management.swarm.master_control import ClearanceLevel

            controller = get_swarm_controller(args.parent_id)

            # Register parent as agent with spawn permission
            controller.mcp.register_agent(f"swarm-{args.parent_id}", ClearanceLevel.ARCHITECT)

            child_id = await controller.spawn_child_swarm(
                name=args.name,
                purpose=args.purpose,
                autonomy_level=AutonomyLevel(args.autonomy),
            )

            if child_id:
                print(f"\n  {Colors.GREEN}Spawned:{Colors.RESET} {child_id}")
                print(f"  {Colors.BOLD}Name:{Colors.RESET} {args.name}")
                print(f"  {Colors.BOLD}Parent:{Colors.RESET} {args.parent_id}")
                print(f"  {Colors.BOLD}Autonomy:{Colors.RESET} {AutonomyLevel(args.autonomy).name}")
            else:
                print(f"\n  {Colors.RED}Failed to spawn child swarm{Colors.RESET}")

        asyncio.run(do_spawn())

    def cmd_directive(self, args):
        """Send directive to child swarm."""
        print_header(f"SENDING DIRECTIVE")

        async def do_directive():
            from tool_management.swarm.swarm_hierarchy import get_swarm_controller

            controller = get_swarm_controller(args.from_swarm)

            directive = await controller.send_directive(
                to_swarm=args.to_swarm,
                directive_type=args.type,
                payload={"description": args.task} if args.task else {},
            )

            if directive:
                print(f"\n  {Colors.GREEN}Sent directive:{Colors.RESET} {directive.id[:12]}...")
                print(f"  {Colors.BOLD}Type:{Colors.RESET} {directive.directive_type}")
                print(f"  {Colors.BOLD}To:{Colors.RESET} {directive.to_swarm}")
                print(f"  {Colors.BOLD}Status:{Colors.RESET} {directive.status}")
            else:
                print(f"\n  {Colors.RED}Failed to send directive{Colors.RESET}")

        asyncio.run(do_directive())

    def cmd_health(self, args):
        """Check MCP backend health."""
        print_header("SYSTEM HEALTH CHECK")

        async def check_health():
            bridge = get_swarm_mcp_bridge()

            print(f"\n  {Colors.BOLD}LLM Backend:{Colors.RESET}")
            print(f"    Model: {DEFAULT_MODEL}")

            # Quick test
            try:
                result = await bridge.execute_llm_task(
                    task_description="Reply with 'OK'",
                    swarm_id="health-check"
                )
                if result.get("success"):
                    print(f"    Status: {Colors.GREEN}HEALTHY{Colors.RESET}")
                else:
                    print(f"    Status: {Colors.RED}ERROR - {result.get('error')}{Colors.RESET}")
            except Exception as e:
                print(f"    Status: {Colors.RED}FAILED - {e}{Colors.RESET}")

            print(f"\n  {Colors.BOLD}Database:{Colors.RESET}")
            try:
                db = get_swarm_db()
                with db.connection() as conn:
                    count = conn.execute("SELECT COUNT(*) FROM swarms").fetchone()[0]
                print(f"    Status: {Colors.GREEN}HEALTHY{Colors.RESET}")
                print(f"    Swarms: {count}")
            except Exception as e:
                print(f"    Status: {Colors.RED}FAILED - {e}{Colors.RESET}")

        asyncio.run(check_health())


def main():
    parser = argparse.ArgumentParser(
        description="Swarm CLI - Manage swarm state, costs, and agents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # status
    subparsers.add_parser("status", help="Show overall swarm status")

    # agents
    p = subparsers.add_parser("agents", help="List agents")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID (default: all)")

    # costs
    p = subparsers.add_parser("costs", help="Show cost management")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # tasks
    p = subparsers.add_parser("tasks", help="List tasks")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # mcp
    p = subparsers.add_parser("mcp", help="Show MCP status")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # violations
    p = subparsers.add_parser("violations", help="List violations")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # wallets
    p = subparsers.add_parser("wallets", help="Show wallet balances")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # changes
    p = subparsers.add_parser("changes", help="List code changes")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # logs
    p = subparsers.add_parser("logs", help="Show audit logs")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")
    p.add_argument("-n", type=int, default=20, help="Number of entries")

    # health
    subparsers.add_parser("health", help="Check system health")

    # hierarchy
    p = subparsers.add_parser("hierarchy", help="Show swarm hierarchy")
    p.add_argument("swarm_id", nargs="?", help="Swarm ID")

    # spawn
    p = subparsers.add_parser("spawn", help="Spawn a child swarm")
    p.add_argument("parent_id", help="Parent swarm ID")
    p.add_argument("--name", required=True, help="Child swarm name")
    p.add_argument("--purpose", default="delegated work", help="Purpose")
    p.add_argument("--autonomy", type=int, default=1, help="Autonomy level (0-4)")

    # directive
    p = subparsers.add_parser("directive", help="Send directive to child swarm")
    p.add_argument("from_swarm", help="Source swarm ID")
    p.add_argument("to_swarm", help="Target swarm ID")
    p.add_argument("--type", default="task", help="Directive type")
    p.add_argument("--task", help="Task description")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cli = SwarmCLI(no_color=args.no_color)

    cmd_method = getattr(cli, f"cmd_{args.command}", None)
    if cmd_method:
        cmd_method(args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()


if __name__ == "__main__":
    main()

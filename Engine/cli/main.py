"""
CLI for Agent Orchestration Platform

Command-line interface for task submission, agent management, and monitoring.
"""

import click
import json
from typing import Optional
from datetime import datetime
from tabulate import tabulate
import sys

from dsco_shared.config.service_registry import ServiceName, resolve

from api.client import AgentOrchestrationClient


@click.group()
@click.option("--api-url", default=resolve(ServiceName.ENGINE), help="API base URL")
@click.pass_context
def cli(ctx, api_url):
    """Agent Orchestration Platform CLI"""
    ctx.ensure_object(dict)
    ctx.obj["client"] = AgentOrchestrationClient(base_url=api_url)
    ctx.obj["api_url"] = api_url


@cli.command()
@click.pass_context
def health(ctx):
    """Check system health status."""
    try:
        client = ctx.obj["client"]
        status = client.health_check()
        click.secho("✓ System healthy", fg="green")
        click.echo(f"  Status: {status['status']}")
        click.echo(f"  Time: {status['timestamp']}")
    except Exception as e:
        click.secho(f"✗ Health check failed: {e}", fg="red")
        sys.exit(1)


@cli.group()
def task():
    """Manage tasks"""
    pass


@task.command("submit")
@click.option("--type", "task_type", required=True, help="Task type (code, search, llm, custom)")
@click.option("--data", "task_data", required=True, help="Task data as JSON")
@click.option("--capabilities", multiple=True, help="Required capabilities")
@click.option("--priority", type=int, default=5, help="Task priority (1-10)")
@click.option("--cost", type=float, default=1.0, help="Estimated cost")
@click.pass_context
def submit_task(ctx, task_type, task_data, capabilities, priority, cost):
    """Submit a new task."""
    try:
        client = ctx.obj["client"]
        data = json.loads(task_data)
        response = client.submit_task(
            task_type=task_type,
            task_data=data,
            required_capabilities=list(capabilities) if capabilities else None,
            priority=priority,
            estimated_cost=cost,
        )
        click.secho("✓ Task submitted", fg="green")
        click.echo(f"  Task ID: {response.task_id}")
        click.echo(f"  Status: {response.status}")
        click.echo(f"  Created: {response.created_at}")
        if response.estimated_completion:
            click.echo(f"  Est. Completion: {response.estimated_completion}")
    except json.JSONDecodeError:
        click.secho("✗ Invalid JSON in --data", fg="red")
        sys.exit(1)
    except Exception as e:
        click.secho(f"✗ Failed to submit task: {e}", fg="red")
        sys.exit(1)


@task.command("status")
@click.argument("task_id")
@click.pass_context
def task_status(ctx, task_id):
    """Get task status."""
    try:
        client = ctx.obj["client"]
        response = client.get_task_status(task_id)

        status_color = {
            "pending": "yellow",
            "executing": "blue",
            "completed": "green",
            "failed": "red",
        }.get(response.status, "white")

        click.echo(f"Task: {response.task_id}")
        click.secho(f"  Status: {response.status}", fg=status_color)
        click.echo(f"  Progress: {response.progress:.1%}")
        click.echo(f"  Created: {response.created_at}")
        if response.started_at:
            click.echo(f"  Started: {response.started_at}")
        if response.completed_at:
            click.echo(f"  Completed: {response.completed_at}")
        if response.result:
            click.echo(f"  Result: {json.dumps(response.result, indent=2)}")
        if response.error:
            click.secho(f"  Error: {response.error}", fg="red")
        click.echo(f"  Cost: ${response.cost_incurred:.2f}")
    except Exception as e:
        click.secho(f"✗ Failed to get task status: {e}", fg="red")
        sys.exit(1)


@task.command("list")
@click.option("--status", help="Filter by status")
@click.option("--limit", type=int, default=20, help="Max results")
@click.pass_context
def list_tasks(ctx, status, limit):
    """List all tasks."""
    try:
        client = ctx.obj["client"]
        tasks = client.list_tasks(status=status, limit=limit)

        if not tasks:
            click.echo("No tasks found")
            return

        table_data = []
        for t in tasks:
            status_emoji = {
                "pending": "⏳",
                "executing": "▶️",
                "completed": "✓",
                "failed": "✗",
            }.get(t.status, "?")

            table_data.append([
                t.task_id[:12],
                f"{status_emoji} {t.status}",
                f"{t.progress:.0%}",
                t.created_at.strftime("%H:%M:%S"),
                f"${t.cost_incurred:.2f}",
            ])

        headers = ["Task ID", "Status", "Progress", "Created", "Cost"]
        click.echo(tabulate(table_data, headers=headers, tablefmt="simple"))
    except Exception as e:
        click.secho(f"✗ Failed to list tasks: {e}", fg="red")
        sys.exit(1)


@cli.group()
def agent():
    """Manage agents"""
    pass


@agent.command("spawn")
@click.option("--model", default="claude-3.5-sonnet", help="Agent model")
@click.option("--budget", type=float, default=100.0, help="Budget in credits")
@click.option("--capabilities", multiple=True, help="Capabilities as key=value")
@click.option("--pool", default="default", help="Pool name")
@click.pass_context
def spawn_agent(ctx, model, budget, capabilities, pool):
    """Spawn a new agent."""
    try:
        client = ctx.obj["client"]

        cap_dict = {}
        for cap in capabilities:
            key, val = cap.split("=")
            cap_dict[key] = float(val)

        response = client.spawn_agent(
            model=model,
            budget=budget,
            capabilities=cap_dict if cap_dict else None,
            pool_name=pool,
        )

        click.secho("✓ Agent spawned", fg="green")
        click.echo(f"  Agent ID: {response.agent_id}")
        click.echo(f"  Model: {response.model}")
        click.echo(f"  Status: {response.status}")
        click.echo(f"  Budget: ${response.budget_remaining:.2f}")
        click.echo(f"  Created: {response.created_at}")
    except ValueError:
        click.secho("✗ Invalid capabilities format (use key=value)", fg="red")
        sys.exit(1)
    except Exception as e:
        click.secho(f"✗ Failed to spawn agent: {e}", fg="red")
        sys.exit(1)


@agent.command("list")
@click.option("--status", help="Filter by status")
@click.option("--pool", help="Filter by pool")
@click.pass_context
def list_agents(ctx, status, pool):
    """List all agents."""
    try:
        client = ctx.obj["client"]
        agents = client.list_agents(status=status, pool=pool)

        if not agents:
            click.echo("No agents found")
            return

        table_data = []
        for a in agents:
            status_emoji = {
                "healthy": "✓",
                "degraded": "⚠",
                "unhealthy": "✗",
                "dead": "💀",
            }.get(a.status, "?")

            table_data.append([
                a.agent_id[:12],
                f"{status_emoji} {a.status}",
                a.model.split("-")[0],
                f"${a.budget_remaining:.2f}",
                a.tasks_completed,
                a.tasks_failed,
                a.last_heartbeat.strftime("%H:%M:%S"),
            ])

        headers = ["Agent ID", "Status", "Model", "Budget", "Completed", "Failed", "Heartbeat"]
        click.echo(tabulate(table_data, headers=headers, tablefmt="simple"))
    except Exception as e:
        click.secho(f"✗ Failed to list agents: {e}", fg="red")
        sys.exit(1)


@agent.command("status")
@click.argument("agent_id")
@click.pass_context
def agent_status(ctx, agent_id):
    """Get agent details."""
    try:
        client = ctx.obj["client"]
        response = client.get_agent(agent_id)

        status_color = {
            "healthy": "green",
            "degraded": "yellow",
            "unhealthy": "red",
            "dead": "red",
        }.get(response.status, "white")

        click.echo(f"Agent: {response.agent_id}")
        click.secho(f"  Status: {response.status}", fg=status_color)
        click.echo(f"  Model: {response.model}")
        click.echo(f"  Budget: ${response.budget_remaining:.2f}")
        click.echo(f"  Tasks Completed: {response.tasks_completed}")
        click.echo(f"  Tasks Failed: {response.tasks_failed}")
        click.echo(f"  Created: {response.created_at}")
        click.echo(f"  Last Heartbeat: {response.last_heartbeat}")

        if response.capabilities:
            click.echo("  Capabilities:")
            for cap, level in response.capabilities.items():
                click.echo(f"    {cap}: {level:.2f}")
    except Exception as e:
        click.secho(f"✗ Failed to get agent status: {e}", fg="red")
        sys.exit(1)


@agent.command("health")
@click.argument("agent_id")
@click.pass_context
def agent_health(ctx, agent_id):
    """Check agent health."""
    try:
        client = ctx.obj["client"]
        result = client.check_agent_health(agent_id)

        if result.get("healthy"):
            click.secho(f"✓ {agent_id} is healthy", fg="green")
        else:
            click.secho(f"✗ {agent_id} is unhealthy", fg="red")

        click.echo(f"  Status: {result.get('status')}")
    except Exception as e:
        click.secho(f"✗ Failed to check agent health: {e}", fg="red")
        sys.exit(1)


@agent.command("prune")
@click.argument("agent_id")
@click.confirmation_option(prompt="Are you sure you want to remove this agent?")
@click.pass_context
def prune_agent(ctx, agent_id):
    """Remove an agent from the pool."""
    try:
        client = ctx.obj["client"]
        result = client.prune_agent(agent_id)
        click.secho(f"✓ Agent {agent_id} removed", fg="green")
    except Exception as e:
        click.secho(f"✗ Failed to prune agent: {e}", fg="red")
        sys.exit(1)


@cli.group()
def pool():
    """Manage agent pools"""
    pass


@pool.command("status")
@click.option("--name", default="default", help="Pool name")
@click.pass_context
def pool_status(ctx, name):
    """Get pool status and metrics."""
    try:
        client = ctx.obj["client"]
        response = client.get_pool_status(pool_name=name)

        click.echo(f"Pool: {response.pool_name}")
        click.echo(f"  Total Agents: {response.total_agents}")
        click.secho(f"    ✓ Healthy: {response.healthy_agents}", fg="green")
        click.secho(f"    ⚠ Degraded: {response.degraded_agents}", fg="yellow")
        click.secho(f"    ✗ Unhealthy: {response.unhealthy_agents}", fg="red")
        click.echo(f"  Capacity Utilization: {response.capacity_utilization:.1%}")
        click.echo(f"  Total Budget: ${response.total_budget_remaining:.2f}")
        click.echo(f"  Pending Tasks: {response.pending_tasks}")
        click.echo(f"  Executing Tasks: {response.executing_tasks}")
        click.echo(f"  Avg Capability Confidence: {response.avg_capability_confidence:.2f}")
    except Exception as e:
        click.secho(f"✗ Failed to get pool status: {e}", fg="red")
        sys.exit(1)


@cli.group()
def workflow():
    """Manage workflows"""
    pass


@workflow.command("run")
@click.option("--pattern", required=True, help="Pattern (fan_out, sequential, consensus, pipeline)")
@click.option("--tasks", required=True, help="Tasks as JSON array")
@click.option("--duration", type=float, default=60.0, help="Duration in seconds")
@click.pass_context
def run_workflow(ctx, pattern, tasks, duration):
    """Execute a multi-agent workflow."""
    try:
        client = ctx.obj["client"]
        tasks_list = json.loads(tasks)

        response = client.execute_workflow(
            pattern=pattern,
            tasks=tasks_list,
            duration_seconds=duration,
        )

        click.secho("✓ Workflow started", fg="green")
        click.echo(f"  Workflow ID: {response.workflow_id}")
        click.echo(f"  Pattern: {response.pattern}")
        click.echo(f"  Status: {response.status}")
        click.echo(f"  Subtasks: {response.total_subtasks}")
        click.echo(f"  Created: {response.created_at}")
    except json.JSONDecodeError:
        click.secho("✗ Invalid JSON in --tasks", fg="red")
        sys.exit(1)
    except Exception as e:
        click.secho(f"✗ Failed to run workflow: {e}", fg="red")
        sys.exit(1)


@workflow.command("status")
@click.argument("workflow_id")
@click.pass_context
def workflow_status(ctx, workflow_id):
    """Get workflow status."""
    try:
        client = ctx.obj["client"]
        response = client.get_workflow_status(workflow_id)

        status_color = {
            "running": "blue",
            "completed": "green",
            "failed": "red",
        }.get(response.status, "white")

        click.echo(f"Workflow: {response.workflow_id}")
        click.secho(f"  Status: {response.status}", fg=status_color)
        click.echo(f"  Pattern: {response.pattern}")
        click.echo(f"  Subtasks: {response.completed_subtasks}/{response.total_subtasks}")
        click.secho(f"  Failed: {response.failed_subtasks}", fg="red" if response.failed_subtasks > 0 else "white")
        click.echo(f"  Created: {response.created_at}")
        if response.completed_at:
            click.echo(f"  Completed: {response.completed_at}")

        if response.results:
            click.echo("  Results:")
            for key, val in response.results.items():
                click.echo(f"    {key}: {val}")

        if response.errors:
            click.echo("  Errors:")
            for key, err in response.errors.items():
                click.secho(f"    {key}: {err}", fg="red")
    except Exception as e:
        click.secho(f"✗ Failed to get workflow status: {e}", fg="red")
        sys.exit(1)


@cli.group()
def system():
    """Get system information"""
    pass


@system.command("status")
@click.pass_context
def system_status(ctx):
    """Get overall system status."""
    try:
        client = ctx.obj["client"]
        response = client.get_system_status()

        status_color = {
            "operational": "green",
            "degraded": "yellow",
            "warning": "yellow",
            "critical": "red",
        }.get(response.status, "white")

        click.secho(f"System Status: {response.status}", fg=status_color)
        click.echo(f"  Timestamp: {response.timestamp}")
        click.echo(f"  Uptime: {response.uptime_seconds/3600:.1f} hours")
        click.echo(f"  Requests: {response.requests_processed}")

        click.echo(f"\nAgent Pool:")
        click.echo(f"  Total: {response.agents.total_agents}")
        click.secho(f"  ✓ Healthy: {response.agents.healthy_agents}", fg="green")
        click.secho(f"  ⚠ Degraded: {response.agents.degraded_agents}", fg="yellow")
        click.secho(f"  ✗ Unhealthy: {response.agents.unhealthy_agents}", fg="red")
        click.echo(f"  Budget: ${response.agents.total_budget_remaining:.2f}")
        click.echo(f"  Capacity: {response.agents.capacity_utilization:.1%}")

        click.echo(f"\nGPU Cluster:")
        click.echo(f"  Total GPUs: {sum(response.gpu.total_gpus.values())}")
        click.echo(f"  Available: {sum(response.gpu.available_gpus.values())}")
        click.echo(f"  Pending Tasks: {response.gpu.pending_tasks}")
        click.echo(f"  Running Tasks: {response.gpu.running_tasks}")
        click.echo(f"  Utilization: {response.gpu.cluster_utilization:.1%}")

        click.echo(f"\nServices:")
        click.secho(f"  Temporal: {'✓' if response.temporal_healthy else '✗'}",
                   fg="green" if response.temporal_healthy else "red")
        click.secho(f"  Redis: {'✓' if response.redis_healthy else '✗'}",
                   fg="green" if response.redis_healthy else "red")
        click.secho(f"  GraphSub: {'✓' if response.graphsub_healthy else '✗'}",
                   fg="green" if response.graphsub_healthy else "red")
    except Exception as e:
        click.secho(f"✗ Failed to get system status: {e}", fg="red")
        sys.exit(1)


if __name__ == "__main__":
    cli()

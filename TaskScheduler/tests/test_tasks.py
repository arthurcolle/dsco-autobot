import pytest
import asyncio
from datetime import datetime, timedelta
from scheduling import FlexibleScheduler

@pytest.fixture
def scheduler():
    scheduler = FlexibleScheduler()
    scheduler.start()
    yield scheduler
    scheduler.stop()

def test_scheduler_initialization(scheduler):
    assert scheduler.objects == {}
    assert scheduler.scheduled_tasks == {}
    assert not scheduler.stopped

@pytest.mark.asyncio
async def test_schedule_and_run_task(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("test_task", "* * * * *", test_task)
    
    task = scheduler.get_job("test_task")
    assert task is not None
    assert task['cron_expression'] == "* * * * *"
    
    await asyncio.sleep(1)  # Wait for the task to be executed
    await scheduler.run_pending()
    
    # Check if the task was executed (you might need to add some mechanism to track executions)

def test_cancel_task(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("test_task", "* * * * *", test_task)
    assert "test_task" in scheduler.scheduled_tasks
    
    scheduler.cancel_task("test_task")
    assert "test_task" not in scheduler.scheduled_tasks

@pytest.mark.asyncio
async def test_modify_task(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("test_task", "* * * * *", test_task)
    scheduler.modify_task("test_task", cron_expression="0 * * * *", priority=1)
    
    task = scheduler.get_job("test_task")
    assert task['cron_expression'] == "0 * * * *"
    assert scheduler.task_priorities["test_task"] == 1

@pytest.mark.asyncio
async def test_add_one_time_task(scheduler):
    def test_task():
        return "One-time task executed"

    run_at = datetime.now() + timedelta(seconds=2)
    scheduler.add_one_time_task("one_time_task", test_task, run_at)
    
    await asyncio.sleep(3)  # Wait for the task to be executed
    # Check if the task was executed (you might need to add some mechanism to track executions)

@pytest.mark.asyncio
async def test_get_upcoming_tasks(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("task1", "* * * * *", test_task)
    scheduler.schedule_task("task2", "0 * * * *", test_task)
    
    upcoming_tasks = await scheduler.get_upcoming_tasks(limit=2)
    assert len(upcoming_tasks) == 2
    assert upcoming_tasks[0]['name'] in ["task1", "task2"]
    assert upcoming_tasks[1]['name'] in ["task1", "task2"]

@pytest.mark.asyncio
async def test_pause_and_resume_task(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("test_task", "* * * * *", test_task)
    
    scheduler.pause_task("test_task")
    assert scheduler.scheduled_tasks["test_task"].get('paused', False)
    
    scheduler.resume_task("test_task")
    assert not scheduler.scheduled_tasks["test_task"].get('paused', False)

@pytest.mark.asyncio
async def test_get_scheduler_stats(scheduler):
    def test_task():
        return "Task executed"

    scheduler.schedule_task("task1", "* * * * *", test_task)
    scheduler.schedule_task("task2", "0 * * * *", test_task)
    scheduler.pause_task("task2")
    
    stats = await scheduler.get_scheduler_stats()
    assert stats['total_tasks'] == 2
    assert stats['running_tasks'] == 1
    assert stats['paused_tasks'] == 1
    assert len(stats['upcoming_tasks']) == 2

def test_get_flexible_scheduler():
    from autonomous_agents.tasks.flexible_scheduler import get_flexible_scheduler
    
    scheduler1 = get_flexible_scheduler()
    scheduler2 = get_flexible_scheduler()
    
    assert scheduler1 is scheduler2  # Ensure it returns the same instance


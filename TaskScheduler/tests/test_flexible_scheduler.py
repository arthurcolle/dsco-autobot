import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from scheduling.flexible_scheduler import FlexibleScheduler

@pytest.fixture
def scheduler():
    scheduler = FlexibleScheduler(max_workers=2)
    scheduler.start()
    yield scheduler
    scheduler.stop()

@pytest.mark.asyncio
async def test_schedule_and_run_task(scheduler):
    mock_task = Mock()
    scheduler.schedule_task("test_task", "* * * * *", mock_task)
    
    # Force run pending tasks
    await scheduler.run_pending()
    
    mock_task.assert_called_once()

@pytest.mark.asyncio
async def test_cancel_task(scheduler):
    mock_task = Mock()
    scheduler.schedule_task("test_task", "* * * * *", mock_task)
    scheduler.cancel_task("test_task")
    
    await scheduler.run_pending()
    
    mock_task.assert_not_called()

@pytest.mark.asyncio
async def test_modify_task(scheduler):
    mock_task = Mock()
    scheduler.schedule_task("test_task", "0 0 * * *", mock_task)
    scheduler.modify_task("test_task", cron_expression="* * * * *", priority=1)
    
    task_info = scheduler.get_job("test_task")
    assert task_info['cron_expression'] == "* * * * *"
    assert scheduler.task_priorities["test_task"] == 1

@pytest.mark.asyncio
async def test_add_one_time_task(scheduler):
    mock_task = Mock()
    run_at = datetime.now() + timedelta(seconds=1)
    scheduler.add_one_time_task("one_time_task", mock_task, run_at)
    
    await asyncio.sleep(1.1)  # Wait for the task to run
    
    mock_task.assert_called_once()

@pytest.mark.asyncio
async def test_get_upcoming_tasks(scheduler):
    scheduler.schedule_task("task1", "0 0 * * *", Mock())
    scheduler.schedule_task("task2", "0 12 * * *", Mock())
    
    upcoming_tasks = await scheduler.get_upcoming_tasks(limit=2)
    assert len(upcoming_tasks) == 2
    assert upcoming_tasks[0]['name'] in ['task1', 'task2']
    assert upcoming_tasks[1]['name'] in ['task1', 'task2']

@pytest.mark.asyncio
async def test_pause_and_resume_task(scheduler):
    mock_task = Mock()
    scheduler.schedule_task("test_task", "* * * * *", mock_task)
    
    scheduler.pause_task("test_task")
    await scheduler.run_pending()
    mock_task.assert_not_called()
    
    scheduler.resume_task("test_task")
    await scheduler.run_pending()
    mock_task.assert_called_once()

@pytest.mark.asyncio
async def test_get_scheduler_stats(scheduler):
    scheduler.schedule_task("task1", "* * * * *", Mock())
    scheduler.schedule_task("task2", "0 0 * * *", Mock())
    scheduler.pause_task("task2")
    
    stats = await scheduler.get_scheduler_stats()
    assert stats['total_tasks'] == 2
    assert stats['running_tasks'] == 1
    assert stats['paused_tasks'] == 1
    assert len(stats['upcoming_tasks']) <= 5

@pytest.mark.asyncio
async def test_task_with_input_schema(scheduler):
    async def task_with_schema(x: int, y: str):
        return f"{x} - {y}"
    
    scheduler.schedule_task(
        "schema_task",
        "* * * * *",
        task_with_schema,
        input_schema={"x": (int, ...), "y": (str, ...)}
    )
    
    # Force run pending tasks
    await scheduler.run_pending()
    
    task_info = scheduler.get_job("schema_task")
    assert 'input_schema' in task_info
    assert task_info['input_schema'] == {"x": (int, ...), "y": (str, ...)}

@pytest.mark.asyncio
async def test_task_execution_with_priority(scheduler):
    results = []
    
    async def task(order):
        await asyncio.sleep(0.1)
        results.append(order)
    
    scheduler.schedule_task("task1", "* * * * *", task, 3, priority=2)
    scheduler.schedule_task("task2", "* * * * *", task, 1, priority=0)
    scheduler.schedule_task("task3", "* * * * *", task, 2, priority=1)
    
    await scheduler.run_pending()
    await asyncio.sleep(0.3)  # Wait for all tasks to complete
    
    assert results == [1, 2, 3]
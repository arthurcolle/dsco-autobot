import requests
import os
from typing import Dict, List, Optional
from datetime import datetime

class SchedulingAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def schedule_task(self, func_name: str, func_args: Dict, cron_string: str, max_duration: float,
                      job_id: Optional[str] = None, dependencies: List[str] = [],
                      priority: int = 0) -> Dict:
        """Schedule a new task."""
        endpoint = f"{self.base_url}/api/v1/tasks/schedule"
        payload = {
            "func_name": func_name,
            "func_args": func_args,
            "cron_string": cron_string,
            "max_duration": max_duration,
            "job_id": job_id,
            "dependencies": dependencies,
            "priority": priority
        }
        response = requests.post(endpoint, json=payload)
        response.raise_for_status()
        return response.json()

    def modify_task(self, job_id: str, new_cron_string: Optional[str] = None,
                    new_priority: Optional[int] = None) -> Dict:
        """Modify an existing task."""
        endpoint = f"{self.base_url}/api/v1/tasks/modify"
        payload = {
            "job_id": job_id,
            "new_cron_string": new_cron_string,
            "new_priority": new_priority
        }
        response = requests.put(endpoint, json=payload)
        response.raise_for_status()
        return response.json()

    def list_tasks(self) -> List[Dict]:
        """List all scheduled tasks."""
        endpoint = f"{self.base_url}/api/v1/tasks/list"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()["tasks"]

    def remove_task(self, job_id: str) -> Dict:
        """Remove a scheduled task."""
        endpoint = f"{self.base_url}/api/v1/tasks/remove"
        payload = {"job_id": job_id}
        response = requests.delete(endpoint, json=payload)
        response.raise_for_status()
        return response.json()

    def root(self) -> Dict:
        """Get root endpoint."""
        endpoint = f"{self.base_url}/"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()

# Example usage:
if __name__ == "__main__":
    port = os.environ.get('SCHEDULING_API_PORT', '2011')
    client = SchedulingAPIClient(f"http://localhost:{port}")
    
    # Schedule a task
    task = client.schedule_task(
        func_name="example_function",
        func_args={"arg1": "value1", "arg2": "value2"},
        cron_string="*/5 * * * *",
        max_duration=60.0,
        job_id="example_job",
        priority=1
    )
    print("Scheduled task:", task)

    # List tasks
    tasks = client.list_tasks()
    print("All tasks:", tasks)

    # Modify a task
    modified_task = client.modify_task("example_job", new_cron_string="*/10 * * * *", new_priority=2)
    print("Modified task:", modified_task)

    # Remove a task
    removed_task = client.remove_task("example_job")
    print("Removed task:", removed_task)

    # Get root
    root_info = client.root()
    print("Root info:", root_info)

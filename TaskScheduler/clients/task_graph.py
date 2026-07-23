from typing import Dict, Any, List, Optional
from .graph import GraphAPI
import json
import hashlib

class TaskGraph:
    def __init__(self):
        self.graph_api = GraphAPI()

    def create_task(self, task_data: Dict[str, Any]) -> Dict:
        # Generate a unique ID for the task
        task_id = self._generate_task_id(task_data)
        
        # Prepare task properties
        properties = {
            "task_id": task_id,
            "name": task_data.get("name", ""),
            "description": task_data.get("description", ""),
            "status": task_data.get("status", "pending"),
            "priority": task_data.get("priority", 0),
            "created_at": task_data.get("created_at", ""),
            "due_date": task_data.get("due_date", ""),
            "metadata": json.dumps(task_data.get("metadata", {})),
            "full_text": self._generate_full_text(task_data),
            "semantic_vector": json.dumps(task_data.get("semantic_vector", [])),
        }

        # Create task node with metadata
        result = self.graph_api.create_node_with_metadata("Task", properties)

        # Enable automatic indexing for full-text search
        self.graph_api.enable_automatic_indexing()

        return result

    def update_task(self, task_id: str, task_data: Dict[str, Any]) -> Dict:
        properties = {k: v for k, v in task_data.items() if k != "task_id"}
        properties["full_text"] = self._generate_full_text(task_data)
        if "metadata" in properties:
            properties["metadata"] = json.dumps(properties["metadata"])
        if "semantic_vector" in properties:
            properties["semantic_vector"] = json.dumps(properties["semantic_vector"])

        return self.graph_api.upsert_node("Task", "task_id", {"task_id": task_id, **properties})

    def get_task(self, task_id: str) -> Dict:
        result = self.graph_api.find_node("Task", {"task_id": task_id})
        if result.get("nodes"):
            return result["nodes"][0]
        return {}

    def delete_task(self, task_id: str) -> Dict:
        return self.graph_api.detach_delete_node(task_id)

    def find_tasks_by_text(self, text: str) -> List[Dict]:
        result = self.graph_api.find_nodes_by_text("Task", "", text)
        return result.get("nodes", [])

    def find_tasks_by_metadata(self, metadata: Dict[str, Any]) -> List[Dict]:
        query = {f"metadata->{k}": v for k, v in metadata.items()}
        result = self.graph_api.find_node("Task", query)
        return result.get("nodes", [])

    def create_task_relationship(self, task_id1: str, task_id2: str, relationship_type: str, properties: Dict[str, Any]) -> Dict:
        return self.graph_api.create_relationship_with_metadata(
            task_id1, task_id2, relationship_type, properties, "Task", "Task"
        )

    def _generate_task_id(self, task_data: Dict[str, Any]) -> str:
        # Generate a unique ID based on task data
        task_string = json.dumps(task_data, sort_keys=True)
        return hashlib.md5(task_string.encode()).hexdigest()

    def _generate_full_text(self, task_data: Dict[str, Any]) -> str:
        # Combine relevant fields for full-text search
        full_text = f"{task_data.get('name', '')} {task_data.get('description', '')}"
        if 'metadata' in task_data:
            full_text += f" {json.dumps(task_data['metadata'])}"
        return full_text

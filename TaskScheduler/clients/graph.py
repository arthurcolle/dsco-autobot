import requests
from typing import Dict, Any, Optional, List
import os

GRAPH_API_URL = os.getenv("GRAPH_API_URL", "http://localhost:2015")

class GraphAPI:
    def __init__(self):
        self.base_url = GRAPH_API_URL
    
    def add_node(self, label: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/node/create"
        payload = {
            "label": label,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def upsert_node(self, node_label: str, key_property: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/upsert/node"
        payload = {
            "node_label": node_label,
            "key_property": key_property,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def add_relationship(self, start_id: str, end_id: str, relationship_type: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/relationship/create"
        payload = {
            "start_id": start_id,
            "end_id": end_id,
            "relationship_type": relationship_type,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def find_nodes_by_text(self, node_label: str, relationship_label: str, text: str) -> Dict:
        url = f"{self.base_url}/api/v1/nodes/text"
        payload = {
            "node_label": node_label,
            "relationship_label": relationship_label,
            "text": text
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def find_relationships_by_text(self, node_label: str, relationship_label: str, text: str) -> Dict:
        url = f"{self.base_url}/api/v1/relationships/text"
        payload = {
            "node_label": node_label,
            "relationship_label": relationship_label,
            "text": text
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def find_path_between_nodes(self, start_id: str, end_id: str) -> Dict:
        url = f"{self.base_url}/api/v1/path"
        payload = {
            "start_id": start_id,
            "end_id": end_id
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def count_nodes(self, label_or_type: str) -> Dict:
        url = f"{self.base_url}/api/v1/count/nodes"
        payload = {
            "label_or_type": label_or_type
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def count_relationships(self, label_or_type: str) -> Dict:
        url = f"{self.base_url}/api/v1/count/relationships"
        payload = {
            "label_or_type": label_or_type
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def get_all_node_labels(self) -> Dict:
        url = f"{self.base_url}/api/v1/labels"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def get_all_relationship_types(self) -> Dict:
        url = f"{self.base_url}/api/v1/relationship-types"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def get_all_property_keys(self) -> Dict:
        url = f"{self.base_url}/api/v1/property-keys"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def merge_node(self, node_label: str, key_property: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/merge/node"
        payload = {
            "node_label": node_label,
            "key_property": key_property,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def merge_relationship(self, start_id: str, end_id: str, relationship_type: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/merge/relationship"
        payload = {
            "start_id": start_id,
            "end_id": end_id,
            "relationship_type": relationship_type,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def batch_update(self, operations: List[Dict[str, Any]]) -> Dict:
        url = f"{self.base_url}/api/v1/batch"
        payload = {
            "operations": operations
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def find_node(self, label: str, properties: Dict[str, Any], node_id: Optional[str] = None, limit: Optional[int] = 10, skip: Optional[int] = 0, order_by: Optional[str] = None, order_direction: Optional[str] = "ASC") -> Dict:
        url = f"{self.base_url}/api/v1/node/find"
        payload = {
            "label": label,
            "properties": properties,
            "node_id": node_id,
            "limit": limit,
            "skip": skip,
            "order_by": order_by,
            "order_direction": order_direction
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def list_indexes(self) -> Dict:
        url = f"{self.base_url}/api/v1/indexes"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def drop_index(self, index_name: str) -> Dict:
        url = f"{self.base_url}/api/v1/index/drop"
        payload = {
            "index_name": index_name
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def list_constraints(self) -> Dict:
        url = f"{self.base_url}/api/v1/constraints"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def drop_constraint(self, constraint_name: str) -> Dict:
        url = f"{self.base_url}/api/v1/constraint/drop"
        payload = {
            "constraint_name": constraint_name
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def enable_automatic_indexing(self) -> Dict:
        url = f"{self.base_url}/api/v1/indexing/enable"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def disable_automatic_indexing(self) -> Dict:
        url = f"{self.base_url}/api/v1/indexing/disable"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def get_deduped_node_labels_with_agent_id(self, agent_id: str) -> Dict:
        url = f"{self.base_url}/api/v1/node-labels"
        payload = {
            "agent_id": agent_id
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def upsert_node(self, node_label: str, key_property: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/upsert/node"
        payload = {
            "node_label": node_label,
            "key_property": key_property,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def upsert_relationship(self, start_id: str, end_id: str, relationship_type: str, key_property: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/upsert/relationship"
        payload = {
            "start_id": start_id,
            "end_id": end_id,
            "relationship_type": relationship_type,
            "key_property": key_property,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def compare_nodes(self, node_label: str, node1_id: str, node2_id: str) -> Dict:
        url = f"{self.base_url}/api/v1/compare-nodes"
        payload = {
            "node_label": node_label,
            "node1_id": node1_id,
            "node2_id": node2_id
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def get_node_connectivity_vector(self, start_label: str, relationship_type: str) -> Dict:
        url = f"{self.base_url}/api/v1/connectivity"
        payload = {
            "start_label": start_label,
            "relationship_type": relationship_type
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def node_relationship_adjacencies(self) -> Dict:
        url = f"{self.base_url}/api/v1/adjacencies"
        response = requests.post(url)
        response.raise_for_status()
        return response.json()

    def get_random_nodes_collection(self, node_label: str, count: int) -> Dict:
        url = f"{self.base_url}/api/v1/random-nodes"
        payload = {
            "node_label": node_label,
            "count": count
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def expand_node_set(self, node_label: str, relationship_type: str, count: int) -> Dict:
        url = f"{self.base_url}/api/v1/expand-node-set"
        payload = {
            "node_label": node_label,
            "relationship_type": relationship_type,
            "count": count
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def path_to_all_related_nodes(self, node_id: str) -> Dict:
        url = f"{self.base_url}/api/v1/path-to-related-nodes"
        payload = {
            "node_id": node_id
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def detach_delete_node(self, node_id: str) -> Dict:
        url = f"{self.base_url}/api/v1/node/delete"
        payload = {
            "node_id": node_id
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def delete_relationships_by_type(self, relationship_type: str) -> Dict:
        url = f"{self.base_url}/api/v1/relationships/delete"
        payload = {
            "relationship_type": relationship_type
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def create_node_with_metadata(self, label: str, properties: Dict[str, Any]) -> Dict:
        url = f"{self.base_url}/api/v1/node-with-metadata"
        payload = {
            "label": label,
            "properties": properties
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def create_relationship_with_metadata(self, start_id: str, end_id: str, relationship_type: str, properties: Dict[str, Any], start_label: str, end_label: str) -> Dict:
        url = f"{self.base_url}/api/v1/relationship-with-metadata"
        payload = {
            "start_id": start_id,
            "end_id": end_id,
            "relationship_type": relationship_type,
            "properties": properties,
            "start_label": start_label,
            "end_label": end_label
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()





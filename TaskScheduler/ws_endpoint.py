from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from scheduling.flexible_scheduler import get_flexible_scheduler
from models.models import Agent
from typing import Dict

router = APIRouter()
scheduler = get_flexible_scheduler()

# Store connected agents
connected_agents: Dict[str, WebSocket] = {}

@router.websocket("/ws/agent/{agent_id}")
async def agent_websocket_endpoint(websocket: WebSocket, agent_id: str):
    await websocket.accept()
    connected_agents[agent_id] = websocket
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get("type") == "register":
                # Handle agent registration
                agent = Agent(
                    agent_id=agent_id,
                    name=data.get("name", f"Agent-{agent_id}"),
                    description=data.get("description"),
                    capabilities=data.get("capabilities", []),
                    websocket_id=id(websocket)
                )
                # You might want to store agent info in your database/graph
                
            elif data.get("type") == "task_complete":
                # Handle task completion acknowledgment
                task_id = data.get("task_id")
                result = data.get("result")
                # Update task status in your system
                
            await websocket.send_json({"status": "acknowledged"})
            
    except WebSocketDisconnect:
        if agent_id in connected_agents:
            del connected_agents[agent_id]

async def notify_agent(agent_id: str, task_data: dict):
    if agent_id in connected_agents:
        websocket = connected_agents[agent_id]
        await websocket.send_json({
            "type": "execute_task",
            "task": task_data
        })

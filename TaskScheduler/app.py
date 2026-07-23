import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from ws_endpoint import router as websocket_router
from task_router import task_scheduling_and_execution

# Endpoints:
# 1. WebSocket: /ws
# 2. Root: /
# 
# Additional endpoints from routers:
# - websocket_router: Various WebSocket-related endpoints
# - task_scheduling_and_execution: Endpoints under /api/v1 prefix
#   (refer to task_router.py for specific endpoints)

ENGINE_API_HOST = os.getenv("ENGINE_API_HOST")
ENGINE_API_PORT = int(os.getenv("ENGINE_API_PORT", 8000))

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Receive and process messages from the client
            # Implement your WebSocket logic here
            # For example, you can send heartbeats every 5 seconds
            await websocket.send_text("heartbeat")
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        # Handle disconnection
        pass

# Include WebSocket routes
app.include_router(websocket_router)

# Include Task Scheduling and Execution routes
app.include_router(task_scheduling_and_execution, prefix="/api/v1")

@app.get("/health")
async def health_check():
    return {"status": "OK"}

@app.get("/")
async def root():
    return {"message": "Welcome to the Task Scheduling + Execution API"}

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("SCHEDULING_API_PORT", 2011))
    uvicorn.run(app, host="0.0.0.0", port=port)

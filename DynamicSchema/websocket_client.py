import asyncio
import websockets
import os

async def connect_websocket():
    uri = f"ws://{os.getenv('ENGINE_API_HOST', 'localhost')}:{os.getenv('ENGINE_API_PORT', '2010')}/ws"
    
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        # Send TRUST message
        await websocket.send("TRUST")
        response = await websocket.recv()
        print(f"Server response: {response}")

        # If trusted, you can send more messages
        if "Connection trusted" in response:
            while True:
                message = input("Enter message to send (or 'quit' to exit): ")
                if message.lower() == 'quit':
                    break
                await websocket.send(message)
                response = await websocket.recv()
                print(f"Server response: {response}")

        print("Disconnecting from WebSocket")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_websocket())

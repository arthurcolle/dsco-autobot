import asyncio
import os
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import json
from openai import AsyncOpenAI
from typing import Dict, Any, AsyncGenerator

app = FastAPI()
client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

async def llm(prompt: str, n: int = 1, temperature: float = 0.7, max_tokens: int = None, logprobs: bool = None, top_logprobs: int = None) -> AsyncGenerator[Dict[str, Any], None]:
    try:
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            n=n,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            logprobs=logprobs,
            top_logprobs=top_logprobs
        )
        async for chunk in response:
            if chunk.choices[0].delta.content is not None:
                yield {"type": "content", "data": chunk.choices[0].delta.content}
            if logprobs and chunk.choices[0].logprobs:
                logprobs_data = chunk.choices[0].logprobs.model_dump()
                if 'content' in logprobs_data and logprobs_data['content']:
                    for token_data in logprobs_data['content']:
                        yield {
                            "type": "logprobs",
                            "data": {
                                "token": token_data.token,
                                "logprob": token_data.logprob,
                                "top_logprobs": [
                                    {"token": tlp.token, "logprob": tlp.logprob}
                                    for tlp in token_data.top_logprobs
                                ] if token_data.top_logprobs else []
                            }
                        }
    except Exception as e:
        print(f"An error occurred: {e}")
        yield {"type": "error", "data": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            data = await websocket.receive_text()
            prompt = json.loads(data)["prompt"]
            async for item in llm(prompt=prompt, n=1, temperature=0.7, max_tokens=4096, logprobs=True, top_logprobs=5):
                await websocket.send_json(item)
        except json.JSONDecodeError:
            await websocket.send_json({"type": "error", "data": "Invalid JSON"})
        except KeyError:
            await websocket.send_json({"type": "error", "data": "Missing 'prompt' key in JSON"})
        except Exception as e:
            await websocket.send_json({"type": "error", "data": str(e)})

@app.get("/")
async def root():
    return FileResponse("llm_app.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9987)

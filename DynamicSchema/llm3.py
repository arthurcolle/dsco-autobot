import asyncio
import os
from openai import AsyncOpenAI

client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

async def llm(prompt, n=1, temperature=0.7, max_tokens=None, logprobs=None, top_logprobs=None):
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
                yield chunk.choices[0].delta.content
            if logprobs and chunk.choices[0].logprobs:
                yield chunk.choices[0].logprobs
    except Exception as e:
        print(f"An error occurred: {e}")
        yield None

async def get_user_input():
    return input("> ")

async def loop():
    while True:
        user_in = await get_user_input()
        if user_in.lower() in ['quit', 'exit', 'q']:
            break
        async for item in llm(prompt=user_in, n=1, temperature=0.7, max_tokens=4096, logprobs=True, top_logprobs=5):
            if isinstance(item, str):
                print(item, end='', flush=True)
            elif item:
                # Process logprobs here
                for token in item.content:
                    print(f"\nToken: {token.token}")
                    print(f"Log prob: {token.logprob}")
                    print(f"Top logprobs: {token.top_logprobs}")
            else:
                print("\nAn error occurred while generating the response.")
        print("\n")

if __name__ == "__main__":
    asyncio.run(loop())

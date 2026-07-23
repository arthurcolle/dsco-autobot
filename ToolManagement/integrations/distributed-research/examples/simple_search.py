"""Example: Simple search query."""

import asyncio
import httpx

from parallel_websearch_server.config import settings


async def main():
    """Run simple search example."""
    base_url = f"http://{settings.host}:{settings.port}"
    print(f"Connecting to {base_url}")

    async with httpx.AsyncClient(base_url=base_url) as client:
        # Simple search
        response = await client.post(
            "/v1beta/search",
            json={
                "objective": "Latest developments in quantum computing",
                "max_results": 5,
            }
        )

        print("Search Results:")
        print("=" * 80)
        data = response.json()

        for i, result in enumerate(data["results"], 1):
            print(f"\n{i}. {result['title']}")
            print(f"   URL: {result['url']}")
            if result.get("excerpts"):
                print(f"   Excerpt: {result['excerpts'][0][:200]}...")


if __name__ == "__main__":
    asyncio.run(main())

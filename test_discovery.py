# test_discovery.py
import asyncio
import aiohttp


async def test_api_endpoints():
    base_url = "http://localhost:1888"

    endpoints = [
        "/api/v2",
        "/api/v2/info",
        "/api/v2/status",
        "/v2",
        "/",
    ]

    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints:
            try:
                url = f"{base_url}{endpoint}"
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=3)
                ) as resp:
                    print(f"\n{url} - Status: {resp.status}")
                    if resp.status == 200:
                        data = await resp.json()
                        print(f"Response: {data}")
            except Exception as e:
                print(f"\n{url} - Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_api_endpoints())

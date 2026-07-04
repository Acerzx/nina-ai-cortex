"""
Продвинутый диагностический скрипт для WebSocket ninaAPI.
"""

import asyncio
import aiohttp
import websockets
import json


async def test_rest_api():
    """Проверяет доступность REST API."""
    print("\n🔍 Testing REST API endpoints...")

    urls = [
        "http://localhost:1888/v2/api/equipment",
        "http://localhost:1888/api/equipment",
        "http://localhost:1888/equipment",
    ]

    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=3)
                ) as resp:
                    if resp.status == 200:
                        print(f"✅ {url} - OK")
                        data = await resp.json()
                        print(f"   Response keys: {list(data.keys())[:5]}")
                    else:
                        print(f"❌ {url} - HTTP {resp.status}")
            except Exception as e:
                print(f"❌ {url} - {type(e).__name__}")


async def test_websocket_endpoints():
    """Тестирует различные WebSocket endpoints."""
    print("\n🔌 Testing WebSocket endpoints...")

    endpoints = [
        "ws://localhost:1888/v2",
        "ws://localhost:1888/v2/ws",
        "ws://localhost:1888/v2/events",
        "ws://localhost:1888/ws",
        "ws://localhost:1888/ws/events",
        "ws://localhost:1888/websocket",
        "ws://localhost:1888/api/v2/ws",
        "ws://localhost:1888",
    ]

    for url in endpoints:
        try:
            async with websockets.connect(url, open_timeout=3) as ws:
                print(f"✅ {url} - CONNECTED")

                # Пытаемся получить сообщение
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=2)
                    data = json.loads(msg)
                    print(f"   📨 Received: {json.dumps(data, indent=2)[:200]}")
                except asyncio.TimeoutError:
                    print(f"   ⏳ Connected, waiting for events...")
                except Exception as e:
                    print(f"   ⚠️ Connected but error receiving: {e}")

                return  # Нашли работающий endpoint

        except websockets.exceptions.InvalidStatus as e:
            print(f"❌ {url} - HTTP {e.status_code}")
        except ConnectionRefusedError:
            print(f"❌ {url} - Connection refused")
        except Exception as e:
            print(f"❌ {url} - {type(e).__name__}")

    print("\n⚠️ No WebSocket endpoint found!")


async def main():
    print("=" * 60)
    print("N.I.N.A. WebSocket Diagnostic Tool")
    print("=" * 60)

    await test_rest_api()
    await test_websocket_endpoints()

    print("\n" + "=" * 60)
    print("💡 Recommendations:")
    print("1. Ensure N.I.N.A. is running")
    print("2. Check Options → Plugins → Advanced API")
    print("3. Enable 'ServerEnabled' checkbox")
    print("4. Verify port 1888 is not blocked")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

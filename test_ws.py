import asyncio
import websockets

URLS_TO_TEST = [
    "ws://localhost:1888/ws/events",  # Стандартный путь ninaAPI v1/v2
    "ws://localhost:1888/v2/events",  # Возможный путь v2
    "ws://localhost:1888/v2",  # То, что мы пробовали
    "ws://localhost:1888/",  # Корневой путь
    "ws://localhost:8080/ws/events",  # Альтернативный порт
]


async def test_url(url):
    try:
        async with websockets.connect(url, open_timeout=3) as ws:
            print(f"✅ SUCCESS: {url}")
            # Попробуем прочитать одно сообщение
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
                print(f"   📨 Received: {msg[:100]}...")
            except asyncio.TimeoutError:
                print(f"   ⏳ Connected, waiting for events...")
            return True
    except Exception as e:
        print(f"❌ FAILED: {url} -> {type(e).__name__}: {e}")
        return False


async def main():
    print("🔍 Testing WebSocket endpoints...\n")
    for url in URLS_TO_TEST:
        await test_url(url)
        print("-" * 50)


if __name__ == "__main__":
    asyncio.run(main())

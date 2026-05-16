"""
Bybit Futures Price Alert Bot — Railway-версия
"""

import asyncio
import json
import time
import httpx
import websockets
from collections import defaultdict, deque
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# ──────────────────────────────────────────────
#  НАСТРОЙКИ
# ──────────────────────────────────────────────
import os
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "")
CHAT_ID    = os.environ.get("CHAT_ID", "")

THRESHOLD  = 1.5   # % изменения
WINDOW_MIN = 5     # за сколько минут
COOLDOWN   = 300   # секунд между алертами по одному токену
# ──────────────────────────────────────────────

BYBIT_WS_URL   = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_URL = "https://api.bybit.com/v5/market/instruments-info"
WINDOW_SEC     = WINDOW_MIN * 60

price_history: dict[str, deque] = defaultdict(lambda: deque())
last_alert: dict[str, float] = {}


# Railway требует открытый HTTP-порт — иначе считает деплой упавшим
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bybit Alert Bot is running")
    def log_message(self, *args):
        pass  # отключаем логи healthcheck

def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


async def get_all_usdt_symbols() -> list[str]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(BYBIT_REST_URL, params={
            "category": "linear",
            "status": "Trading",
            "limit": 1000,
        })
        data = r.json()
    symbols = [
        item["symbol"]
        for item in data["result"]["list"]
        if item["symbol"].endswith("USDT")
    ]
    print(f"[init] Найдено {len(symbols)} USDT-фьючерсов")
    return symbols


async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
            })
        except Exception as e:
            print(f"[telegram error] {e}")


def check_alert(symbol: str, current_price: float) -> None:
    now = time.time()
    history = price_history[symbol]

    while history and history[0][0] < now - WINDOW_SEC:
        history.popleft()

    history.append((now, current_price))

    if len(history) < 2:
        return

    old_price = history[0][1]
    if old_price == 0:
        return

    change_pct = (current_price - old_price) / old_price * 100

    if abs(change_pct) >= THRESHOLD:
        if now - last_alert.get(symbol, 0) < COOLDOWN:
            return
        last_alert[symbol] = now
        direction = "🚀 РОСТ" if change_pct > 0 else "🔻 ПАДЕНИЕ"
        ts = datetime.now().strftime("%H:%M:%S")
        text = (
            f"{direction} <b>{symbol}</b>\n"
            f"Изменение: <b>{change_pct:+.2f}%</b> за {WINDOW_MIN} мин\n"
            f"Цена сейчас: <b>{current_price}</b> USDT\n"
            f"Время: {ts}"
        )
        print(f"[alert] {symbol}  {change_pct:+.2f}%  @ {current_price}")
        asyncio.create_task(send_telegram(text))


async def subscribe_chunk(symbols: list[str]):
    topics = [f"tickers.{s}" for s in symbols]
    while True:
        try:
            async with websockets.connect(
                BYBIT_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": topics}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("topic", "").startswith("tickers."):
                        d = msg.get("data", {})
                        symbol = d.get("symbol")
                        last_price = d.get("lastPrice")
                        if symbol and last_price:
                            check_alert(symbol, float(last_price))
        except Exception as e:
            print(f"[ws error] {e} — переподключение через 5 сек…")
            await asyncio.sleep(5)


async def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] BOT_TOKEN и CHAT_ID не заданы! Добавь их в Variables на Railway.")
        return

    print(f"Порог: {THRESHOLD}%  |  Окно: {WINDOW_MIN} мин  |  Cooldown: {COOLDOWN}s")

    symbols = await get_all_usdt_symbols()
    chunks = [symbols[i:i+100] for i in range(0, len(symbols), 100)]
    print(f"[init] Запускаем {len(chunks)} WebSocket-соединений")

    await send_telegram(
        f"✅ <b>Bybit Alert Bot запущен на Railway</b>\n"
        f"Отслеживаю {len(symbols)} фьючерсов\n"
        f"Порог: {THRESHOLD}% за {WINDOW_MIN} мин"
    )

    await asyncio.gather(*(subscribe_chunk(chunk) for chunk in chunks))


if __name__ == "__main__":
    # Запускаем health-сервер в фоновом потоке
    t = threading.Thread(target=start_health_server, daemon=True)
    t.start()
    print("[health] HTTP сервер запущен")

    asyncio.run(main())

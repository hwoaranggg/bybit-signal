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
    """
    Получаем список символов через WebSocket (тот же домен что и стримы),
    минуя REST API который блокирует Railway.
    """
    symbols = []
    try:
        async with websockets.connect(
            "wss://stream.bybit.com/v5/public/linear",
            ping_interval=None,
            open_timeout=15,
        ) as ws:
            # Запрашиваем snapshot тикеров — в ответе придут все активные символы
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": ["tickers.BTCUSDT"]  # dummy подписка чтобы открыть соединение
            }))
            # Используем публичный WS endpoint для получения инструментов
            # Bybit поддерживает запрос через WS
            await ws.send(json.dumps({
                "op": "get_instruments",
                "req_id": "init"
            }))
            # Ждём ответы несколько секунд
            deadline = asyncio.get_event_loop().time() + 5
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    msg = json.loads(raw)
                    if isinstance(msg.get("result"), list):
                        for item in msg["result"]:
                            s = item.get("symbol", "")
                            if s.endswith("USDT"):
                                symbols.append(s)
                except asyncio.TimeoutError:
                    break
    except Exception as e:
        print(f"[init] WS symbols error: {e}")

    if not symbols:
        # Fallback — топ монеты которые точно торгуются
        symbols = [
            "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
            "DOGEUSDT","ADAUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
            "MATICUSDT","UNIUSDT","LTCUSDT","ATOMUSDT","NEARUSDT",
            "APTUSDT","ARBUSDT","OPUSDT","INJUSDT","SUIUSDT",
            "SEIUSDT","TIAUSDT","ORDIUSDT","WLDUSDT","FETUSDT",
            "RENDERUSDT","RNDRUSDT","ARUSDT","IMXUSDT","STXUSDT",
            "RUNEUSDT","FILUSDT","LDOUSDT","GRTUSDT","SANDUSDT",
            "MANAUSDT","AXSUSDT","APEUSDT","GMXUSDT","DYDXUSDT",
            "PEPEUSDT","SHIBUSDT","FLOKIUSDT","BONKUSDT","WIFUSDT",
            "JUPUSDT","PYTHUSDT","JITOUSDT","MEMEUSDT","BOMEUSDT",
            "TONUSDT","NOTUSDT","HMSTRUSDT","EIGENUSDT","NEIROUSDT",
        ]
        print(f"[init] Используем fallback список: {len(symbols)} символов")
    else:
        print(f"[init] Получено через WS: {len(symbols)} символов")

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
    t.start()
    print("[health] HTTP сервер запущен")

    asyncio.run(main())

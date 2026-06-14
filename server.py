import asyncio
import json
import random
import websockets
from datetime import datetime

# --- Başlangıç fiyatları ---
STOCKS = {
    "THYAO": 245.80,
    "GARAN": 112.40,
    "ASELS": 87.60,
    "BIMAS": 478.20,
    "SASA":  34.15,
}

# Her hisse için geçmiş fiyat listesi (grafik için)
price_history = {ticker: [price] for ticker, price in STOCKS.items()}

# Bağlı istemciler
connected_clients = set()


def generate_tick(ticker, current_price):
    """Gerçekçi rastgele fiyat değişimi üretir."""
    change_pct = random.gauss(0, 0.004)   # %0.4 standart sapma
    change_pct = max(-0.03, min(0.03, change_pct))  # max ±%3 tek seferde
    new_price = round(current_price * (1 + change_pct), 2)
    new_price = max(1.0, new_price)
    return new_price


async def broadcast_prices():
    """Her 0.8 saniyede bir tüm bağlı istemcilere fiyat gönderir."""
    global connected_clients
    while True:
        await asyncio.sleep(0.8)

        if not connected_clients:
            continue

        # Fiyatları güncelle
        updates = []
        for ticker in STOCKS:
            old_price = STOCKS[ticker]
            new_price = generate_tick(ticker, old_price)
            change = round(new_price - old_price, 2)
            change_pct = round((change / old_price) * 100, 3)

            STOCKS[ticker] = new_price
            price_history[ticker].append(new_price)
            if len(price_history[ticker]) > 60:   # son 60 tick'i tut
                price_history[ticker].pop(0)

            updates.append({
                "ticker": ticker,
                "price": new_price,
                "change": change,
                "change_pct": change_pct,
                "direction": "up" if change >= 0 else "down",
                "history": price_history[ticker][-30:],   # son 30 nokta
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })

        message = json.dumps({
            "type": "price_update",
            "data": updates,
        })

        # Bağlı tüm istemcilere gönder
        dead = set()
        for ws in connected_clients:
            try:
                await ws.send(message)
            except websockets.exceptions.ConnectionClosed:
                dead.add(ws)
        connected_clients -= dead


async def handler(websocket):
    """Yeni bağlantıyı karşılar ve yönetir."""
    connected_clients.add(websocket)
    client_ip = websocket.remote_address[0]
    print(f"[+] Yeni bağlantı: {client_ip}  |  Toplam: {len(connected_clients)}")

    # Bağlantı kurulunca anlık snapshot gönder
    snapshot = json.dumps({
        "type": "snapshot",
        "data": [
            {
                "ticker": t,
                "price": p,
                "change": 0.0,
                "change_pct": 0.0,
                "direction": "up",
                "history": price_history[t][-30:],
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            }
            for t, p in STOCKS.items()
        ],
    })
    await websocket.send(snapshot)

    try:
        # İstemciden gelen mesajları dinle (örn. "PING")
        async for message in websocket:
            data = json.loads(message)
            if data.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"[-] Bağlantı kapandı: {client_ip}  |  Kalan: {len(connected_clients)}")


async def main():
    print("=" * 50)
    print("  WebSocket Borsa Sunucusu başlatılıyor...")
    print("  ws://localhost:8765")
    print("=" * 50)

    async with websockets.serve(handler, "localhost", 8765):
        await broadcast_prices()   # sonsuza kadar çalışır


if __name__ == "__main__":
    asyncio.run(main())

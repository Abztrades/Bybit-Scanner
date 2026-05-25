import os
import asyncio
import aiohttp
from datetime import datetime, timezone

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

VOLUME_MIN = 35_000_000
OI_MIN = 5_000_000
OI_CHANGE_MAX = 20.0
FUNDING_MIN = -0.001
FUNDING_MAX = 0.001
SPREAD_MAX = 0.001
SCAN_INTERVAL = 600

BYBIT_BASE = "https://api.bybit.com"

async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        async with session.post(url, json=payload) as resp:
            await resp.json()
    except Exception as e:
        print(f"Telegram error: {e}")

async def get_all_symbols(session):
    url = f"{BYBIT_BASE}/v5/market/instruments-info"
    params = {"category": "linear", "limit": 1000}
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                raw = await resp.text()
                try:
                    import json
                    data = json.loads(raw)
                except Exception as parse_err:
                    print(f"Symbol JSON parse error (attempt {attempt+1}): {parse_err}")
                    print(f"Raw response preview: {raw[:200]}")
                    await asyncio.sleep(3)
                    continue
                symbols = []
                for item in data.get("result", {}).get("list", []):
                    if item.get("quoteCoin") == "USDT" and item.get("status") == "Trading":
                        symbols.append(item["symbol"])
                if symbols:
                    return symbols
        except Exception as e:
            print(f"Symbol fetch error (attempt {attempt+1}): {e}")
            await asyncio.sleep(3)
    print("All symbol fetch attempts failed.")
    return []

async def get_ticker(session, symbol):
    url = f"{BYBIT_BASE}/v5/market/tickers"
    params = {"category": "linear", "symbol": symbol}
    try:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            if data["result"]["list"]:
                return data["result"]["list"][0]
    except:
        pass
    return None

async def get_orderbook_spread(session, symbol):
    url = f"{BYBIT_BASE}/v5/market/orderbook"
    params = {"category": "linear", "symbol": symbol, "limit": 1}
    try:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            result = data["result"]
            best_ask = float(result["a"][0][0])
            best_bid = float(result["b"][0][0])
            spread_pct = (best_ask - best_bid) / best_ask
            return spread_pct
    except:
        return None

async def get_open_interest(session, symbol):
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": "1h",
        "limit": 25
    }
    try:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            oi_list = data["result"]["list"]
            if len(oi_list) >= 2:
                latest = float(oi_list[0]["openInterest"])
                older = float(oi_list[-1]["openInterest"])
                if older > 0:
                    oi_change_pct = abs((latest - older) / older) * 100
                else:
                    oi_change_pct = 0
                return latest, oi_change_pct
    except:
        pass
    return None, None

async def scan_symbol(session, symbol):
    ticker = await get_ticker(session, symbol)
    if not ticker:
        return None

    try:
        volume_24h = float(ticker.get("turnover24h", 0))
        funding_rate = float(ticker.get("fundingRate", 0))
        price = float(ticker.get("lastPrice", 0))
    except:
        return None

    if volume_24h < VOLUME_MIN:
        return None

    if not (FUNDING_MIN <= funding_rate <= FUNDING_MAX):
        return None

    oi_value, oi_change = await get_open_interest(session, symbol)
    if oi_value is None:
        return None

    if oi_value < OI_MIN:
        return None

    if oi_change > OI_CHANGE_MAX:
        return None

    spread = await get_orderbook_spread(session, symbol)
    if spread is None or spread > SPREAD_MAX:
        return None

    return {
        "symbol": symbol,
        "price": price,
        "volume_24h": volume_24h,
        "oi": oi_value,
        "oi_change": oi_change,
        "funding": funding_rate,
        "spread": spread
    }

async def run_scan(session):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Starting scan...")
    symbols = await get_all_symbols(session)
    print(f"Scanning {len(symbols)} symbols...")

    results = []
    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        tasks = [scan_symbol(session, s) for s in batch]
        batch_results = await asyncio.gather(*tasks)
        results.extend([r for r in batch_results if r is not None])
        await asyncio.sleep(0.5)

    if not results:
        print("No tokens passed filters this scan.")
        return

    results.sort(key=lambda x: x["volume_24h"], reverse=True)

    message = f"<b>Bybit Scanner Alert</b>\n"
    message += f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"
    message += f"Tokens passing all filters: {len(results)}\n\n"

    for r in results[:10]:
        message += f"<b>{r['symbol']}</b>\n"
        message += f"Price: ${r['price']:,.4f}\n"
        message += f"24H Volume: ${r['volume_24h']/1_000_000:.1f}M\n"
        message += f"OI: ${r['oi']/1_000_000:.1f}M\n"
        message += f"OI Change 24H: {r['oi_change']:.1f}%\n"
        message += f"Funding: {r['funding']*100:.4f}%\n"
        message += f"Spread: {r['spread']*100:.3f}%\n\n"

    if len(results) > 10:
        message += f"...and {len(results) - 10} more tokens passed filters.\n"

    message += "These tokens met your filters. Check charts before trading."

    await send_telegram(session, message)
    print(f"Alert sent. {len(results)} tokens passed filters.")

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("ERROR: BOT_TOKEN or CHAT_ID environment variable missing.")
        return

    async with aiohttp.ClientSession() as session:
        await send_telegram(session, "Bybit Scanner is live. First scan starting now.")
        while True:
            try:
                await run_scan(session)
            except Exception as e:
                print(f"Scan error: {e}")
            print(f"Next scan in {SCAN_INTERVAL // 60} minutes.")
            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())

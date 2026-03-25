import os
import asyncio
import logging
from datetime import datetime
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
import yfinance as yf
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
TZ = pytz.timezone("America/Argentina/Buenos_Aires")

# ── Assets ──────────────────────────────────────────────────────────────────

INDICES = {
    "S&P 500": "^GSPC",
    "Nasdaq":  "^IXIC",
    "Dow Jones": "^DJI",
    "DAX":     "^GDAXI",
    "Nikkei":  "^N225",
    "FTSE 100":"^FTSE",
}

COMMODITIES = {
    "Gold":    "GC=F",
    "Silver":  "SI=F",
    "Oil WTI": "CL=F",
    "Natural Gas": "NG=F",
}

FOREX = {
    "DXY":     "DX-Y.NYB",
    "EUR/USD": "EURUSD=X",
    "USD/ARS": "ARS=X",
}

US_STOCKS = {
    "Apple":   "AAPL",
    "NVIDIA":  "NVDA",
    "Tesla":   "TSLA",
    "Microsoft":"MSFT",
    "Meta":    "META",
    "Amazon":  "AMZN",
}

CRYPTO_IDS = {
    "Bitcoin":  "bitcoin",
    "Ethereum": "ethereum",
    "Solana":   "solana",
    "XRP":      "ripple",
    "BNB":      "binancecoin",
}

# ── Data fetchers ────────────────────────────────────────────────────────────

def fetch_yf(symbols: dict) -> dict:
    results = {}
    for name, ticker in symbols.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="2d")
            if len(hist) >= 2:
                prev  = hist["Close"].iloc[-2]
                curr  = hist["Close"].iloc[-1]
                chg   = ((curr - prev) / prev) * 100
                results[name] = {"price": curr, "change": chg}
            elif len(hist) == 1:
                curr  = hist["Close"].iloc[-1]
                results[name] = {"price": curr, "change": 0.0}
        except Exception as e:
            logger.warning(f"YF error for {ticker}: {e}")
    return results


def fetch_crypto() -> dict:
    try:
        ids = ",".join(CRYPTO_IDS.values())
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true"
        r = requests.get(url, timeout=10)
        data = r.json()
        results = {}
        for name, cid in CRYPTO_IDS.items():
            if cid in data:
                results[name] = {
                    "price":  data[cid]["usd"],
                    "change": data[cid].get("usd_24h_change", 0),
                }
        return results
    except Exception as e:
        logger.warning(f"Crypto error: {e}")
        return {}


def fetch_news(query: str = "markets finance economy", count: int = 4) -> list[str]:
    if not NEWS_API_KEY:
        return []
    try:
        url = (
            f"https://newsapi.org/v2/everything?q={query}"
            f"&language=en&sortBy=publishedAt&pageSize={count}"
            f"&apiKey={NEWS_API_KEY}"
        )
        r = requests.get(url, timeout=10)
        articles = r.json().get("articles", [])
        return [a["title"] for a in articles if a.get("title")]
    except Exception as e:
        logger.warning(f"News error: {e}")
        return []

# ── Formatters ───────────────────────────────────────────────────────────────

def arrow(chg: float) -> str:
    return "🟢 ▲" if chg >= 0 else "🔴 ▼"

def fmt_price(price: float, decimals: int = 2) -> str:
    if price >= 1000:
        return f"{price:,.0f}"
    elif price >= 1:
        return f"{price:,.{decimals}f}"
    else:
        return f"{price:.4f}"

def section(title: str, data: dict, decimals: int = 2) -> str:
    if not data:
        return ""
    lines = [f"\n{title}"]
    for name, v in data.items():
        p   = fmt_price(v["price"], decimals)
        chg = v["change"]
        lines.append(f"{arrow(chg)} *{name}*: ${p}  ({chg:+.2f}%)")
    return "\n".join(lines)

# ── Message builders ─────────────────────────────────────────────────────────

def build_opening() -> str:
    now   = datetime.now(TZ).strftime("%A %d %b %Y")
    indices    = fetch_yf(INDICES)
    commodities= fetch_yf(COMMODITIES)
    crypto     = fetch_crypto()
    news       = fetch_news("markets stocks economy", 4)

    msg  = f"🌅 *APERTURA DE MERCADOS — ST Capital*\n_{now}_\n"
    msg += section("📊 *ÍNDICES GLOBALES*", indices)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    msg += section("🛢 *COMMODITIES*", commodities)

    if news:
        msg += "\n\n📰 *NOTICIAS DE LA MAÑANA*"
        for n in news[:4]:
            msg += f"\n• {n}"

    msg += "\n\n_ST Capital · @capitalst\\_bot_"
    return msg


def build_midmorning() -> str:
    indices = fetch_yf(INDICES)
    stocks  = fetch_yf(US_STOCKS)
    news    = fetch_news("stock market Wall Street", 3)

    msg  = "📊 *MID-MORNING UPDATE — ST Capital*\n"
    msg += section("📈 *ÍNDICES*", indices)
    msg += section("🏢 *US STOCKS*", stocks)

    if news:
        msg += "\n\n📰 *ÚLTIMAS NOTICIAS*"
        for n in news[:3]:
            msg += f"\n• {n}"

    msg += "\n\n_ST Capital · @capitalst\\_bot_"
    return msg


def build_midday() -> str:
    commodities = fetch_yf(COMMODITIES)
    forex       = fetch_yf(FOREX)
    crypto      = fetch_crypto()
    news        = fetch_news("commodity oil gold forex", 3)

    msg  = "🔴 *MEDIODÍA — ST Capital*\n"
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("💱 *FOREX*", forex, decimals=4)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)

    if news:
        msg += "\n\n📰 *NOTICIAS MACRO*"
        for n in news[:3]:
            msg += f"\n• {n}"

    msg += "\n\n_ST Capital · @capitalst\\_bot_"
    return msg


def build_preclose() -> str:
    stocks  = fetch_yf(US_STOCKS)
    indices = fetch_yf({"S&P 500": "^GSPC", "Nasdaq": "^IXIC", "Dow Jones": "^DJI"})
    news    = fetch_news("earnings stocks Wall Street close", 3)

    msg  = "📈 *PRE-CIERRE USA — ST Capital*\n"
    msg += section("🏢 *US STOCKS*", stocks)
    msg += section("📊 *ÍNDICES USA*", indices)

    if news:
        msg += "\n\n📰 *NOTICIAS PRE-CIERRE*"
        for n in news[:3]:
            msg += f"\n• {n}"

    msg += "\n\n_ST Capital · @capitalst\\_bot_"
    return msg


def build_close() -> str:
    indices     = fetch_yf(INDICES)
    stocks      = fetch_yf(US_STOCKS)
    commodities = fetch_yf(COMMODITIES)
    crypto      = fetch_crypto()

    msg  = "🌙 *CIERRE DEL DÍA — ST Capital*\n"
    msg += section("📊 *ÍNDICES GLOBALES*", indices)
    msg += section("🏢 *US STOCKS*", stocks)
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    msg += "\n\n_Eso fue todo por hoy. Hasta mañana 💼_"
    msg += "\n_ST Capital · @capitalst\\_bot_"
    return msg

# ── Sender ───────────────────────────────────────────────────────────────────

async def send(bot: Bot, text: str):
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        logger.info("Message sent successfully.")
    except Exception as e:
        logger.error(f"Send error: {e}")

# ── Scheduled jobs ───────────────────────────────────────────────────────────

async def job_opening(bot):    await send(bot, build_opening())
async def job_midmorning(bot): await send(bot, build_midmorning())
async def job_midday(bot):     await send(bot, build_midday())
async def job_preclose(bot):   await send(bot, build_preclose())
async def job_close(bot):      await send(bot, build_close())

# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    scheduler = AsyncIOScheduler(timezone=TZ)

    scheduler.add_job(job_opening,    "cron", hour=9,  minute=0,  args=[bot])
    scheduler.add_job(job_midmorning, "cron", hour=11, minute=0,  args=[bot])
    scheduler.add_job(job_midday,     "cron", hour=13, minute=0,  args=[bot])
    scheduler.add_job(job_preclose,   "cron", hour=15, minute=0,  args=[bot])
    scheduler.add_job(job_close,      "cron", hour=17, minute=0,  args=[bot])

    scheduler.start()
    logger.info("✅ ST Capital Bot running — 5 daily messages scheduled.")

    # Keep alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())

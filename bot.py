import os
import asyncio
import logging
from datetime import datetime
import pytz
import numpy as np
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot, Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import yfinance as yf
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")
NEWS_API_KEY   = os.environ.get("NEWS_API_KEY", "")
TZ             = pytz.timezone("America/Argentina/Buenos_Aires")

# ── Assets ───────────────────────────────────────────────────────────────────

INDICES = {
    "S&P 500":   "^GSPC",
    "Nasdaq":    "^IXIC",
    "Dow Jones": "^DJI",
    "DAX":       "^GDAXI",
    "Nikkei":    "^N225",
    "FTSE 100":  "^FTSE",
}
COMMODITIES = {
    "Gold":        "GC=F",
    "Silver":      "SI=F",
    "Oil WTI":     "CL=F",
    "Natural Gas": "NG=F",
}
FOREX = {
    "DXY":     "DX-Y.NYB",
    "EUR/USD": "EURUSD=X",
    "USD/ARS": "ARS=X",
}
US_STOCKS = {
    "Apple":     "AAPL",
    "NVIDIA":    "NVDA",
    "Tesla":     "TSLA",
    "Microsoft": "MSFT",
    "Meta":      "META",
    "Amazon":    "AMZN",
}
CRYPTO_IDS = {
    "Bitcoin":  "bitcoin",
    "Ethereum": "ethereum",
    "Solana":   "solana",
    "XRP":      "ripple",
    "BNB":      "binancecoin",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def arrow(chg: float) -> str:
    return "🟢 ▲" if chg >= 0 else "🔴 ▼"

def fmt_price(price: float, decimals: int = 2) -> str:
    if price >= 1000:
        return f"{price:,.0f}"
    elif price >= 1:
        return f"{price:,.{decimals}f}"
    else:
        return f"{price:.4f}"

def fmt_large(val: float) -> str:
    if val >= 1e12:
        return f"${val/1e12:.2f}T"
    elif val >= 1e9:
        return f"${val/1e9:.2f}B"
    elif val >= 1e6:
        return f"${val/1e6:.2f}M"
    return f"${val:,.0f}"

# ── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(closes: np.ndarray, period: int = 14) -> float:
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def compute_rs_score(ticker_closes: np.ndarray, spy_closes: np.ndarray) -> float:
    n = min(63, len(ticker_closes), len(spy_closes))
    if n < 2:
        return 0.0
    ticker_ret = (ticker_closes[-1] / ticker_closes[-n]) - 1
    spy_ret    = (spy_closes[-1]    / spy_closes[-n])    - 1
    return round((ticker_ret - spy_ret) * 100, 2)

def pct_from_ma(current: float, ma: float) -> float:
    return round(((current - ma) / ma) * 100, 2)

# ── On-demand ticker analysis ─────────────────────────────────────────────────

def analyze_ticker(symbol: str) -> str:
    symbol = symbol.upper().strip()
    try:
        t    = yf.Ticker(symbol)
        hist = t.history(period="1y")

        if hist.empty or len(hist) < 20:
            return f"❌ No encontré datos para *{symbol}*. Verificá el ticker e intentá de nuevo."

        closes  = hist["Close"].values
        volumes = hist["Volume"].values
        current = closes[-1]

        prev_close  = closes[-2] if len(closes) >= 2 else current
        day_chg     = ((current - prev_close) / prev_close) * 100

        rsi = compute_rsi(closes) if len(closes) >= 16 else None

        try:
            spy_hist   = yf.Ticker("SPY").history(period="1y")
            spy_closes = spy_hist["Close"].values
            rs_score   = compute_rs_score(closes, spy_closes)
        except Exception:
            rs_score = None

        if len(closes) >= 200:
            ema = closes[0]
            k   = 2 / (200 + 1)
            for c in closes:
                ema = c * k + ema * (1 - k)
            ema200      = round(ema, 2)
            dist_ema200 = pct_from_ma(current, ema200)
        else:
            ema200 = dist_ema200 = None

        if len(closes) >= 50:
            sma50      = round(float(np.mean(closes[-50:])), 2)
            dist_sma50 = pct_from_ma(current, sma50)
        else:
            sma50 = dist_sma50 = None

        high_52w      = round(float(np.max(closes)), 2)
        low_52w       = round(float(np.min(closes)), 2)
        dist_52w_high = pct_from_ma(current, high_52w)
        dist_52w_low  = pct_from_ma(current, low_52w)

        vol_last  = int(volumes[-1]) if len(volumes) > 0 else 0
        vol_avg20 = int(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_last
        vol_ratio = round(vol_last / vol_avg20, 2) if vol_avg20 > 0 else 1.0

        info     = t.info
        mktcap   = info.get("marketCap")
        name     = info.get("longName") or info.get("shortName") or symbol
        sector   = info.get("sector", "")
        industry = info.get("industry", "")

        news_headlines = []
        if NEWS_API_KEY:
            try:
                url = (
                    f"https://newsapi.org/v2/everything?q={symbol}&language=en"
                    f"&sortBy=publishedAt&pageSize=3&apiKey={NEWS_API_KEY}"
                )
                r              = requests.get(url, timeout=8)
                articles       = r.json().get("articles", [])
                news_headlines = [a["title"] for a in articles if a.get("title")][:3]
            except Exception:
                pass

        def rsi_label(r):
            if r is None: return ""
            if r >= 70:   return " ⚠️ Sobrecomprado"
            if r <= 30:   return " ⚠️ Sobrevendido"
            return ""

        def rs_label(s):
            if s is None: return ""
            if s > 10:    return " 💪 Fuerte vs mercado"
            if s < -10:   return " 🥶 Débil vs mercado"
            return " ➡️ Neutral"

        msg = f"📊 *{name}* \(`{symbol}`\)\n"
        if sector:
            msg += f"_{sector} · {industry}_\n"
        msg += "\n"
        msg += f"💵 *Price:* ${fmt_price(current)}  {arrow(day_chg)} {day_chg:+.2f}%\n"
        if mktcap:
            msg += f"🏦 *Market Cap:* {fmt_large(mktcap)}\n"

        msg += "\n*— Indicadores técnicos —*\n"
        if rsi is not None:
            msg += f"📉 *RSI \(14\):* {rsi}{rsi_label(rsi)}\n"
        if rs_score is not None:
            msg += f"⚡ *RS Score vs SPY:* {rs_score:+.2f}%{rs_label(rs_score)}\n"
        if ema200 and dist_ema200 is not None:
            emoji = "🟢" if dist_ema200 >= 0 else "🔴"
            msg += f"{emoji} *Dist\. EMA 200:* {dist_ema200:+.2f}%  \(EMA: ${fmt_price(ema200)}\)\n"
        if sma50 and dist_sma50 is not None:
            emoji = "🟢" if dist_sma50 >= 0 else "🔴"
            msg += f"{emoji} *Dist\. SMA 50:* {dist_sma50:+.2f}%  \(SMA: ${fmt_price(sma50)}\)\n"

        msg += "\n*— 52W Range —*\n"
        msg += f"📈 *MAX 52W:* ${fmt_price(high_52w)}  \({dist_52w_high:+.2f}% desde ATH anual\)\n"
        msg += f"📉 *MIN 52W:* ${fmt_price(low_52w)}  \({dist_52w_low:+.2f}% desde mínimo\)\n"

        msg += "\n*— Volumen —*\n"
        vol_emoji = "🔥" if vol_ratio > 1.5 else ("📊" if vol_ratio >= 0.8 else "😴")
        msg += f"{vol_emoji} *Vol\. último día:* {vol_last:,}\n"
        msg += f"📊 *Vol\. promedio 20d:* {vol_avg20:,}  \(ratio: {vol_ratio}x\)\n"

        if news_headlines:
            msg += "\n*— Últimas noticias —*\n"
            for h in news_headlines:
                msg += f"• {h}\n"

        msg += "\n_ST Capital · @capitalst\_bot_\n"
        msg += "⚠️ _No es asesoramiento financiero\._"

        return msg

    except Exception as e:
        logger.error(f"analyze_ticker error for {symbol}: {e}")
        return f"❌ Error analizando *{symbol}*\. Intentá de nuevo en unos segundos\."

# ── Scheduled data fetchers ───────────────────────────────────────────────────

def fetch_yf(symbols: dict) -> dict:
    results = {}
    for name, ticker in symbols.items():
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(period="2d")
            if len(hist) >= 2:
                prev = hist["Close"].iloc[-2]
                curr = hist["Close"].iloc[-1]
                chg  = ((curr - prev) / prev) * 100
                results[name] = {"price": curr, "change": chg}
            elif len(hist) == 1:
                results[name] = {"price": hist["Close"].iloc[-1], "change": 0.0}
        except Exception as e:
            logger.warning(f"YF error {ticker}: {e}")
    return results

def fetch_crypto() -> dict:
    try:
        ids  = ",".join(CRYPTO_IDS.values())
        url  = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true"
        r    = requests.get(url, timeout=10)
        data = r.json()
        return {
            name: {"price": data[cid]["usd"], "change": data[cid].get("usd_24h_change", 0)}
            for name, cid in CRYPTO_IDS.items() if cid in data
        }
    except Exception as e:
        logger.warning(f"Crypto error: {e}")
        return {}

def fetch_news(query: str, count: int = 4) -> list:
    if not NEWS_API_KEY:
        return []
    try:
        url = (
            f"https://newsapi.org/v2/everything?q={query}"
            f"&language=en&sortBy=publishedAt&pageSize={count}&apiKey={NEWS_API_KEY}"
        )
        r = requests.get(url, timeout=10)
        return [a["title"] for a in r.json().get("articles", []) if a.get("title")]
    except Exception as e:
        logger.warning(f"News error: {e}")
        return []

def section(title: str, data: dict, decimals: int = 2) -> str:
    if not data:
        return ""
    lines = [f"\n{title}"]
    for name, v in data.items():
        p   = fmt_price(v["price"], decimals)
        chg = v["change"]
        lines.append(f"{arrow(chg)} *{name}*: ${p}  ({chg:+.2f}%)")
    return "\n".join(lines)

# ── Scheduled message builders ────────────────────────────────────────────────

def build_opening() -> str:
    now  = datetime.now(TZ).strftime("%A %d %b %Y")
    msg  = f"🌅 *APERTURA DE MERCADOS — ST Capital*\n_{now}_\n"
    msg += section("📊 *ÍNDICES GLOBALES*", fetch_yf(INDICES))
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    news = fetch_news("markets stocks economy", 4)
    if news:
        msg += "\n\n📰 *NOTICIAS DE LA MAÑANA*"
        for n in news[:4]: msg += f"\n• {n}"
    msg += "\n\n_ST Capital · @capitalst\_bot_"
    return msg

def build_midmorning() -> str:
    msg  = "📊 *MID-MORNING UPDATE — ST Capital*\n"
    msg += section("📈 *ÍNDICES*", fetch_yf(INDICES))
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    news = fetch_news("stock market Wall Street", 3)
    if news:
        msg += "\n\n📰 *ÚLTIMAS NOTICIAS*"
        for n in news[:3]: msg += f"\n• {n}"
    msg += "\n\n_ST Capital · @capitalst\_bot_"
    return msg

def build_midday() -> str:
    msg  = "🔴 *MEDIODÍA — ST Capital*\n"
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    msg += section("💱 *FOREX*", fetch_yf(FOREX), decimals=4)
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    news = fetch_news("commodity oil gold forex", 3)
    if news:
        msg += "\n\n📰 *NOTICIAS MACRO*"
        for n in news[:3]: msg += f"\n• {n}"
    msg += "\n\n_ST Capital · @capitalst\_bot_"
    return msg

def build_preclose() -> str:
    msg  = "📈 *PRE-CIERRE USA — ST Capital*\n"
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    msg += section("📊 *ÍNDICES USA*", fetch_yf({"S&P 500": "^GSPC", "Nasdaq": "^IXIC", "Dow Jones": "^DJI"}))
    news = fetch_news("earnings stocks Wall Street close", 3)
    if news:
        msg += "\n\n📰 *NOTICIAS PRE-CIERRE*"
        for n in news[:3]: msg += f"\n• {n}"
    msg += "\n\n_ST Capital · @capitalst\_bot_"
    return msg

def build_close() -> str:
    msg  = "🌙 *CIERRE DEL DÍA — ST Capital*\n"
    msg += section("📊 *ÍNDICES GLOBALES*", fetch_yf(INDICES))
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    msg += "\n\n_Eso fue todo por hoy. Hasta mañana 💼_"
    msg += "\n_ST Capital · @capitalst\_bot_"
    return msg

# ── Telegram handlers ─────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_id = str(update.effective_user.id)
    if user_id != str(CHAT_ID):
        return

    text = update.message.text.strip()

    if text.lower() in ["/start", "/help", "help", "ayuda"]:
        await update.message.reply_text(
            "👋 *ST Capital Bot*\n\n"
            "Escribime el ticker de cualquier activo y te mando el análisis completo\\.\n\n"
            "*Ejemplos:*\n"
            "• `AAPL` → Apple\n"
            "• `BTC-USD` → Bitcoin\n"
            "• `GC=F` → Gold\n"
            "• `SPY` → S&P 500 ETF\n\n"
            "_ST Capital · @capitalst\\_bot_",
            parse_mode="MarkdownV2"
        )
        return

    symbol = text.upper().replace(" ", "").replace("/", "")
    await update.message.reply_text(f"🔍 Analizando `{symbol}`\\.\\.\\.", parse_mode="MarkdownV2")

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, analyze_ticker, symbol)
    await update.message.reply_text(result, parse_mode="MarkdownV2", disable_web_page_preview=True)

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle_message))

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(lambda: asyncio.create_task(
        app.bot.send_message(chat_id=CHAT_ID, text=build_opening(), parse_mode="Markdown", disable_web_page_preview=True)
    ), "cron", hour=9,  minute=0)
    scheduler.add_job(lambda: asyncio.create_task(
        app.bot.send_message(chat_id=CHAT_ID, text=build_midmorning(), parse_mode="Markdown", disable_web_page_preview=True)
    ), "cron", hour=11, minute=0)
    scheduler.add_job(lambda: asyncio.create_task(
        app.bot.send_message(chat_id=CHAT_ID, text=build_midday(), parse_mode="Markdown", disable_web_page_preview=True)
    ), "cron", hour=13, minute=0)
    scheduler.add_job(lambda: asyncio.create_task(
        app.bot.send_message(chat_id=CHAT_ID, text=build_preclose(), parse_mode="Markdown", disable_web_page_preview=True)
    ), "cron", hour=15, minute=0)
    scheduler.add_job(lambda: asyncio.create_task(
        app.bot.send_message(chat_id=CHAT_ID, text=build_close(), parse_mode="Markdown", disable_web_page_preview=True)
    ), "cron", hour=17, minute=0)

    scheduler.start()
    logger.info("✅ ST Capital Bot running — scheduled + on-demand active.")

    await app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())

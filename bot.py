import os
import logging
from datetime import datetime
import pytz
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import yfinance as yf
import requests
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
 
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")
NEWS_API_KEY   = os.environ.get("NEWS_API_KEY", "")
TZ             = pytz.timezone("America/Argentina/Buenos_Aires")
 
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
 
def arrow_emoji(chg):
    return "🟢" if chg >= 0 else "🔴"
 
def fmt_price(price, decimals=2):
    if price >= 1000:
        return "{:,.0f}".format(price)
    elif price >= 1:
        return "{:,.{}f}".format(price, decimals)
    else:
        return "{:.4f}".format(price)
 
def fmt_large(val):
    if val >= 1e12:
        return "${:.2f}T".format(val / 1e12)
    elif val >= 1e9:
        return "${:.2f}B".format(val / 1e9)
    elif val >= 1e6:
        return "${:.2f}M".format(val / 1e6)
    return "${:,.0f}".format(val)
 
def compute_rsi(closes, period=14):
    deltas   = np.diff(closes)
    gains    = np.where(deltas > 0, deltas, 0)
    losses   = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)
 
def compute_rs_score(ticker_closes, spy_closes):
    n = min(63, len(ticker_closes), len(spy_closes))
    if n < 2:
        return 0.0
    ticker_ret = (ticker_closes[-1] / ticker_closes[-n]) - 1
    spy_ret    = (spy_closes[-1]    / spy_closes[-n])    - 1
    return round((ticker_ret - spy_ret) * 100, 2)
 
def pct_from_ma(current, ma):
    return round(((current - ma) / ma) * 100, 2)
 
# ── Ticker analysis ───────────────────────────────────────────────────────────
 
def analyze_ticker(symbol):
    symbol = symbol.upper().strip()
    try:
        t    = yf.Ticker(symbol)
        hist = t.history(period="1y")
 
        if hist.empty or len(hist) < 20:
            return "No encontre datos para " + symbol + ". Verifica el ticker."
 
        closes  = hist["Close"].values
        volumes = hist["Volume"].values
        current = closes[-1]
 
        prev_close = closes[-2] if len(closes) >= 2 else current
        day_chg    = ((current - prev_close) / prev_close) * 100
 
        rsi = compute_rsi(closes) if len(closes) >= 16 else None
 
        rs_score = None
        try:
            spy_hist   = yf.Ticker("SPY").history(period="1y")
            spy_closes = spy_hist["Close"].values
            rs_score   = compute_rs_score(closes, spy_closes)
        except Exception:
            pass
 
        ema200 = dist_ema200 = None
        if len(closes) >= 200:
            ema = closes[0]
            k   = 2 / (200 + 1)
            for c in closes:
                ema = c * k + ema * (1 - k)
            ema200      = round(float(ema), 2)
            dist_ema200 = pct_from_ma(current, ema200)
 
        sma50 = dist_sma50 = None
        if len(closes) >= 50:
            sma50      = round(float(np.mean(closes[-50:])), 2)
            dist_sma50 = pct_from_ma(current, sma50)
 
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
                url      = "https://newsapi.org/v2/everything?q=" + symbol + "&language=en&sortBy=publishedAt&pageSize=3&apiKey=" + NEWS_API_KEY
                r        = requests.get(url, timeout=8)
                articles = r.json().get("articles", [])
                news_headlines = [a["title"] for a in articles if a.get("title")][:3]
            except Exception:
                pass
 
        lines = []
        lines.append("*" + name + "* (" + symbol + ")")
        if sector:
            lines.append("_" + sector + " - " + industry + "_")
        lines.append("")
        lines.append(arrow_emoji(day_chg) + " *Price:* $" + fmt_price(current) + "  " + "{:+.2f}".format(day_chg) + "%")
        if mktcap:
            lines.append("🏦 *Market Cap:* " + fmt_large(mktcap))
        lines.append("")
        lines.append("*Indicadores tecnicos*")
        if rsi is not None:
            tag = " - Sobrecomprado" if rsi >= 70 else (" - Sobrevendido" if rsi <= 30 else "")
            lines.append("📉 *RSI 14:* " + str(rsi) + tag)
        if rs_score is not None:
            tag = " - Fuerte vs mercado" if rs_score > 10 else (" - Debil vs mercado" if rs_score < -10 else " - Neutral")
            lines.append("⚡ *RS Score vs SPY:* " + "{:+.2f}".format(rs_score) + "%" + tag)
        if ema200 is not None:
            lines.append(arrow_emoji(dist_ema200) + " *Dist EMA 200:* " + "{:+.2f}".format(dist_ema200) + "% (EMA: $" + fmt_price(ema200) + ")")
        if sma50 is not None:
            lines.append(arrow_emoji(dist_sma50) + " *Dist SMA 50:* " + "{:+.2f}".format(dist_sma50) + "% (SMA: $" + fmt_price(sma50) + ")")
        lines.append("")
        lines.append("*52W Range*")
        lines.append("📈 *MAX 52W:* $" + fmt_price(high_52w) + "  (" + "{:+.2f}".format(dist_52w_high) + "% del maximo)")
        lines.append("📉 *MIN 52W:* $" + fmt_price(low_52w) + "  (" + "{:+.2f}".format(dist_52w_low) + "% del minimo)")
        lines.append("")
        lines.append("*Volumen*")
        vol_emoji = "🔥" if vol_ratio > 1.5 else ("📊" if vol_ratio >= 0.8 else "😴")
        lines.append(vol_emoji + " *Vol ultimo dia:* " + "{:,}".format(vol_last))
        lines.append("📊 *Vol promedio 20d:* " + "{:,}".format(vol_avg20) + " (ratio: " + str(vol_ratio) + "x)")
        if news_headlines:
            lines.append("")
            lines.append("*Ultimas noticias*")
            for h in news_headlines:
                lines.append("- " + h)
        lines.append("")
        lines.append("_ST Capital - No es asesoramiento financiero._")
 
        return "\n".join(lines)
 
    except Exception as e:
        logger.error("analyze_ticker error: " + str(e))
        return "Error analizando " + symbol + ". Intenta de nuevo."
 
# ── Market data ───────────────────────────────────────────────────────────────
 
def fetch_yf(symbols):
    results = {}
    for name, ticker in symbols.items():
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(period="2d")
            if len(hist) >= 2:
                prev = hist["Close"].iloc[-2]
                curr = hist["Close"].iloc[-1]
                results[name] = {"price": float(curr), "change": float(((curr - prev) / prev) * 100)}
            elif len(hist) == 1:
                results[name] = {"price": float(hist["Close"].iloc[-1]), "change": 0.0}
        except Exception as e:
            logger.warning("YF error " + ticker + ": " + str(e))
    return results
 
def fetch_crypto():
    try:
        ids  = ",".join(CRYPTO_IDS.values())
        r    = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=" + ids + "&vs_currencies=usd&include_24hr_change=true", timeout=10)
        data = r.json()
        return {name: {"price": data[cid]["usd"], "change": data[cid].get("usd_24h_change", 0)} for name, cid in CRYPTO_IDS.items() if cid in data}
    except Exception as e:
        logger.warning("Crypto error: " + str(e))
        return {}
 
def fetch_news(query, count=4):
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get("https://newsapi.org/v2/everything?q=" + query + "&language=en&sortBy=publishedAt&pageSize=" + str(count) + "&apiKey=" + NEWS_API_KEY, timeout=10)
        return [a["title"] for a in r.json().get("articles", []) if a.get("title")]
    except Exception:
        return []
 
def section(title, data, decimals=2):
    if not data:
        return ""
    lines = ["\n" + title]
    for name, v in data.items():
        lines.append(arrow_emoji(v["change"]) + " *" + name + "*: $" + fmt_price(v["price"], decimals) + "  (" + "{:+.2f}".format(v["change"]) + "%)")
    return "\n".join(lines)
 
# ── Message builders ──────────────────────────────────────────────────────────
 
def build_opening():
    now = datetime.now(TZ).strftime("%A %d %b %Y")
    msg = "🌅 *APERTURA - ST Capital*\n_" + now + "_\n"
    msg += section("📊 *INDICES GLOBALES*", fetch_yf(INDICES))
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    news = fetch_news("markets stocks economy", 4)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:4]: msg += "\n- " + n
    msg += "\n\n_ST Capital_"
    return msg
 
def build_midmorning():
    msg = "📊 *MID-MORNING - ST Capital*\n"
    msg += section("📈 *INDICES*", fetch_yf(INDICES))
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    news = fetch_news("stock market Wall Street", 3)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    msg += "\n\n_ST Capital_"
    return msg
 
def build_midday():
    msg = "🔴 *MEDIODIA - ST Capital*\n"
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    msg += section("💱 *FOREX*", fetch_yf(FOREX), decimals=4)
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    news = fetch_news("commodity oil gold forex", 3)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    msg += "\n\n_ST Capital_"
    return msg
 
def build_preclose():
    msg = "📈 *PRE-CIERRE USA - ST Capital*\n"
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    msg += section("📊 *INDICES USA*", fetch_yf({"S&P 500": "^GSPC", "Nasdaq": "^IXIC", "Dow Jones": "^DJI"}))
    news = fetch_news("earnings stocks Wall Street", 3)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    msg += "\n\n_ST Capital_"
    return msg
 
def build_close():
    msg = "🌙 *CIERRE DEL DIA - ST Capital*\n"
    msg += section("📊 *INDICES*", fetch_yf(INDICES))
    msg += section("🏢 *US STOCKS*", fetch_yf(US_STOCKS))
    msg += section("🛢 *COMMODITIES*", fetch_yf(COMMODITIES))
    msg += section("🪙 *CRYPTO*", fetch_crypto(), decimals=0)
    msg += "\n\n_ST Capital - Hasta manana_"
    return msg
 
# ── Global app reference ──────────────────────────────────────────────────────
 
APP = None
 
def scheduled_send(builder_fn):
    if APP is None:
        return
    try:
        text = builder_fn()
        import asyncio
        loop = APP.bot._application.update_queue._loop if hasattr(APP, '_loop') else None
        asyncio.run_coroutine_threadsafe(
            APP.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown", disable_web_page_preview=True),
            APP.bot._httpx_client._transport._pool._ssl_context._loop if False else asyncio.get_event_loop()
        )
    except Exception as e:
        logger.error("Scheduled send error: " + str(e))
 
# ── Handlers ──────────────────────────────────────────────────────────────────
 
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if str(update.effective_user.id) != str(CHAT_ID):
        return
 
    text   = update.message.text.strip()
    symbol = text.upper().replace(" ", "").lstrip("/")
 
    if symbol in ["START", "HELP", "AYUDA"]:
        await update.message.reply_text("ST Capital Bot\n\nEscribime cualquier ticker:\nAAPL, BTC-USD, GC=F, SPY, TSLA...")
        return
 
    await update.message.reply_text("Analizando " + symbol + "...")
 
    import asyncio
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, analyze_ticker, symbol)
    await update.message.reply_text(result, parse_mode="Markdown", disable_web_page_preview=True)
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def main():
    global APP
    APP = Application.builder().token(TELEGRAM_TOKEN).build()
    APP.add_handler(MessageHandler(filters.TEXT, handle_message))
 
    scheduler = BackgroundScheduler(timezone=TZ)
 
    def make_job(fn):
        def job():
            try:
                text = fn()
                import asyncio
                future = asyncio.run_coroutine_threadsafe(
                    APP.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown", disable_web_page_preview=True),
                    APP.bot._httpx_client._transport._pool._loop if False else APP.update_queue._loop
                )
                future.result(timeout=30)
            except Exception as e:
                logger.error("Job error: " + str(e))
        return job
 
    scheduler.add_job(make_job(build_opening),    "cron", hour=9,  minute=0)
    scheduler.add_job(make_job(build_midmorning), "cron", hour=11, minute=0)
    scheduler.add_job(make_job(build_midday),     "cron", hour=13, minute=0)
    scheduler.add_job(make_job(build_preclose),   "cron", hour=15, minute=0)
    scheduler.add_job(make_job(build_close),      "cron", hour=17, minute=0)
 
    scheduler.start()
    logger.info("ST Capital Bot running.")
 
    APP.run_polling(drop_pending_updates=True)
 
if __name__ == "__main__":
    main()

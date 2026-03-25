import os
import logging
from datetime import datetime
import pytz
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import requests
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
 
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID          = os.environ.get("CHAT_ID")
NEWS_API_KEY     = os.environ.get("NEWS_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
TZ               = pytz.timezone("America/Argentina/Buenos_Aires")
 
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
 
# ── Yahoo Finance direct API ──────────────────────────────────────────────────
 
def get_yahoo_data(symbol, period="1y"):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    range_map = {"2d": "5d", "1y": "1y"}
    yf_range = range_map.get(period, period)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?interval=1d&range=" + yf_range
    try:
        r      = requests.get(url, headers=headers, timeout=15)
        data   = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        closes  = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        volumes = result[0].get("indicators", {}).get("quote", [{}])[0].get("volume", [])
        closes  = [c for c in closes if c is not None]
        volumes = [v if v is not None else 0 for v in volumes]
        if not closes:
            return None
        return {"closes": closes, "volumes": volumes}
    except Exception as e:
        logger.warning("YF API error for " + symbol + ": " + str(e))
        return None
 
def get_ticker_info(symbol):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/" + symbol + "?modules=assetProfile,price"
    try:
        r      = requests.get(url, headers=headers, timeout=10)
        data   = r.json()
        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return {}
        price_data = result[0].get("price", {})
        asset_data = result[0].get("assetProfile", {})
        return {
            "longName":  price_data.get("longName") or price_data.get("shortName", symbol),
            "sector":    asset_data.get("sector", ""),
            "industry":  asset_data.get("industry", ""),
            "marketCap": price_data.get("marketCap", {}).get("raw"),
        }
    except Exception as e:
        logger.warning("Ticker info error: " + str(e))
        return {}
 
# ── Claude AI ─────────────────────────────────────────────────────────────────
 
def ask_claude(prompt):
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 600,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        data = r.json()
        return data.get("content", [{}])[0].get("text", "").strip()
    except Exception as e:
        logger.error("Claude API error: " + str(e))
        return ""
 
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
    arr      = np.array(closes, dtype=float)
    deltas   = np.diff(arr)
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
    logger.info("Analyzing: " + symbol)
 
    data = get_yahoo_data(symbol, "1y")
    if not data or len(data["closes"]) < 20:
        return "No encontre datos para " + symbol + ". Verifica que sea un ticker valido (ej: AAPL, BTC-USD, GC=F, SPY)"
 
    closes  = data["closes"]
    volumes = data["volumes"]
    current = closes[-1]
 
    prev_close = closes[-2] if len(closes) >= 2 else current
    day_chg    = ((current - prev_close) / prev_close) * 100
 
    rsi = compute_rsi(closes) if len(closes) >= 16 else None
 
    rs_score = None
    try:
        spy_data = get_yahoo_data("SPY", "1y")
        if spy_data and len(spy_data["closes"]) >= 10:
            rs_score = compute_rs_score(closes, spy_data["closes"])
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
 
    closes_arr    = np.array(closes)
    high_52w      = round(float(np.max(closes_arr)), 2)
    low_52w       = round(float(np.min(closes_arr)), 2)
    dist_52w_high = pct_from_ma(current, high_52w)
    dist_52w_low  = pct_from_ma(current, low_52w)
 
    vol_last  = int(volumes[-1]) if volumes else 0
    vol_avg20 = int(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_last
    vol_ratio = round(vol_last / vol_avg20, 2) if vol_avg20 > 0 else 1.0
 
    info     = get_ticker_info(symbol)
    name     = info.get("longName") or symbol
    sector   = info.get("sector", "")
    industry = info.get("industry", "")
    mktcap   = info.get("marketCap")
 
    # Build technical summary for Claude
    tech_summary = (
        "Activo: " + name + " (" + symbol + ")\n"
        "Precio actual: $" + fmt_price(current) + " (" + "{:+.2f}".format(day_chg) + "% hoy)\n"
        + ("RSI 14: " + str(rsi) + "\n" if rsi else "")
        + ("RS Score vs SPY: " + "{:+.2f}".format(rs_score) + "%\n" if rs_score is not None else "")
        + ("Distancia EMA 200: " + "{:+.2f}".format(dist_ema200) + "%\n" if dist_ema200 is not None else "")
        + ("Distancia SMA 50: " + "{:+.2f}".format(dist_sma50) + "%\n" if dist_sma50 is not None else "")
        + "MAX 52W: $" + fmt_price(high_52w) + " (" + "{:+.2f}".format(dist_52w_high) + "% del maximo)\n"
        + "MIN 52W: $" + fmt_price(low_52w) + "\n"
        + "Volumen ratio vs 20d: " + str(vol_ratio) + "x\n"
    )
 
    claude_analysis = ask_claude(
        "Sos un analista financiero experto. Con los siguientes datos tecnicos de " + name + ", "
        "escribi un analisis breve (3-4 oraciones) en español sobre el estado actual del activo, "
        "destacando lo mas relevante para un trader. Se directo, concreto y profesional. "
        "No uses markdown, no uses asteriscos, no uses guiones al inicio.\n\n"
        + tech_summary
    )
 
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
 
    if claude_analysis:
        lines.append("")
        lines.append("*Analisis*")
        lines.append(claude_analysis)
 
    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)
 
# ── Market data ───────────────────────────────────────────────────────────────
 
def fetch_market(symbols, decimals=2):
    results = {}
    for name, ticker in symbols.items():
        data = get_yahoo_data(ticker, "2d")
        if data and len(data["closes"]) >= 2:
            curr = data["closes"][-1]
            prev = data["closes"][-2]
            results[name] = {"price": float(curr), "change": float(((curr - prev) / prev) * 100)}
        elif data and len(data["closes"]) == 1:
            results[name] = {"price": float(data["closes"][0]), "change": 0.0}
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
 
def build_market_snapshot():
    indices     = fetch_market(INDICES)
    crypto      = fetch_crypto()
    commodities = fetch_market(COMMODITIES)
    forex       = fetch_market(FOREX)
    stocks      = fetch_market(US_STOCKS)
 
    snapshot = "INDICES GLOBALES:\n"
    for name, v in indices.items():
        snapshot += name + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snapshot += "\nCRYPTO:\n"
    for name, v in crypto.items():
        snapshot += name + ": $" + fmt_price(v["price"], 0) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snapshot += "\nCOMMODITIES:\n"
    for name, v in commodities.items():
        snapshot += name + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snapshot += "\nFOREX:\n"
    for name, v in forex.items():
        snapshot += name + ": " + fmt_price(v["price"], 4) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snapshot += "\nUS STOCKS:\n"
    for name, v in stocks.items():
        snapshot += name + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    return snapshot, indices, crypto, commodities, forex, stocks
 
# ── Scheduled message builders ────────────────────────────────────────────────
 
def build_opening():
    now = datetime.now(TZ).strftime("%A %d %b %Y")
    snapshot, indices, crypto, commodities, _, _ = build_market_snapshot()
    news = fetch_news("markets stocks economy", 4)
 
    news_text = ""
    if news:
        news_text = "\nNoticias destacadas del dia:\n" + "\n".join(["- " + n for n in news[:4]])
 
    analysis = ask_claude(
        "Sos un analista financiero experto de ST Capital. Hoy es " + now + ". "
        "Con los siguientes datos de mercado al inicio del dia, escribi un analisis de apertura "
        "de 4-5 oraciones en español. Destaca las tendencias mas importantes, que activos lideran "
        "o rezagan, y que deberia monitorear un trader hoy. Se directo y profesional. Sin markdown.\n\n"
        + snapshot + news_text
    )
 
    msg = "🌅 *APERTURA - ST Capital*\n_" + now + "_\n"
    msg += section("📊 *INDICES GLOBALES*", indices)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    msg += section("🛢 *COMMODITIES*", commodities)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Analisis de apertura*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg
 
def build_midmorning():
    snapshot, indices, _, _, _, stocks = build_market_snapshot()
    news = fetch_news("stock market Wall Street", 3)
 
    analysis = ask_claude(
        "Sos un analista de ST Capital. Con estos datos de mercado a media manana, "
        "escribi un update breve de 3 oraciones en español sobre como esta yendo la sesion "
        "y que sectores o activos merecen atencion. Sin markdown.\n\n" + snapshot
    )
 
    msg = "📊 *MID-MORNING - ST Capital*\n"
    msg += section("📈 *INDICES*", indices)
    msg += section("🏢 *US STOCKS*", stocks)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Update*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg
 
def build_midday():
    snapshot, _, crypto, commodities, forex, _ = build_market_snapshot()
    news = fetch_news("commodity oil gold forex macro", 3)
 
    analysis = ask_claude(
        "Sos un analista de ST Capital. Con estos datos de mercado al mediodia, "
        "escribi un analisis de 3 oraciones en español enfocado en commodities, forex y crypto. "
        "Que narrativa macro domina? Sin markdown.\n\n" + snapshot
    )
 
    msg = "🔴 *MEDIODIA - ST Capital*\n"
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("💱 *FOREX*", forex, decimals=4)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Analisis macro*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg
 
def build_preclose():
    snapshot, indices, _, _, _, stocks = build_market_snapshot()
    news = fetch_news("earnings stocks Wall Street close", 3)
 
    analysis = ask_claude(
        "Sos un analista de ST Capital. Faltan 2 horas para el cierre de Wall Street. "
        "Con estos datos, escribi un analisis de 3 oraciones en español sobre como se perfila "
        "el cierre y que niveles o activos son clave monitorear. Sin markdown.\n\n" + snapshot
    )
 
    msg = "📈 *PRE-CIERRE USA - ST Capital*\n"
    msg += section("🏢 *US STOCKS*", stocks)
    msg += section("📊 *INDICES USA*", {k: v for k, v in indices.items() if k in ["S&P 500", "Nasdaq", "Dow Jones"]})
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Analisis pre-cierre*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg
 
def build_close():
    snapshot, indices, crypto, commodities, _, stocks = build_market_snapshot()
 
    analysis = ask_claude(
        "Sos un analista de ST Capital. El mercado acaba de cerrar. "
        "Con estos datos del cierre, escribi un resumen del dia de 4-5 oraciones en español. "
        "Que movimientos fueron mas relevantes? Que contexto deja para manana? Sin markdown.\n\n"
        + snapshot
    )
 
    msg = "🌙 *CIERRE DEL DIA - ST Capital*\n"
    msg += section("📊 *INDICES*", indices)
    msg += section("🏢 *US STOCKS*", stocks)
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    if analysis:
        msg += "\n\n*Resumen del dia*\n" + analysis
    msg += "\n\n_ST Capital - Hasta manana_"
    return msg
 
# ── Telegram handlers ─────────────────────────────────────────────────────────
 
APP = None
 
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if str(update.effective_user.id) != str(CHAT_ID):
        return
 
    text   = update.message.text.strip()
    symbol = text.upper().replace(" ", "").lstrip("/")
 
    if symbol in ["START", "HELP", "AYUDA"]:
        await update.message.reply_text(
            "ST Capital Bot\n\nEscribime cualquier ticker:\nAAPL, BTC-USD, GC=F, SPY, TSLA, ETH-USD, ^GSPC..."
        )
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
            import asyncio
            try:
                text   = fn()
                future = asyncio.run_coroutine_threadsafe(
                    APP.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown", disable_web_page_preview=True),
                    APP.update_queue._loop
                )
                future.result(timeout=60)
            except Exception as e:
                logger.error("Job error: " + str(e))
        return job
 
    scheduler.add_job(make_job(build_opening),    "cron", hour=9,  minute=0)
    scheduler.add_job(make_job(build_midmorning), "cron", hour=11, minute=0)
    scheduler.add_job(make_job(build_midday),     "cron", hour=13, minute=0)
    scheduler.add_job(make_job(build_preclose),   "cron", hour=15, minute=0)
    scheduler.add_job(make_job(build_close),      "cron", hour=17, minute=0)
 
    scheduler.start()
    logger.info("ST Capital Bot running - Claude AI integrated.")
    APP.run_polling(drop_pending_updates=True)
 
if __name__ == "__main__":
    main()

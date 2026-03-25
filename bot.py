import os
import re
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

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID           = os.environ.get("CHAT_ID")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
TZ                = pytz.timezone("America/Argentina/Buenos_Aires")

# ── Asset lists ───────────────────────────────────────────────────────────────

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

# Argentine tickers on Yahoo Finance (.BA suffix)
AR_STOCKS_YF = {
    "GGAL": "GGAL.BA", "BMA": "BMA.BA", "TXAR": "TXAR.BA",
    "YPFD": "YPFD.BA", "PAMP": "PAMP.BA", "TECO2": "TECO2.BA",
    "SUPV": "SUPV.BA", "CEPU": "CEPU.BA", "MIRG": "MIRG.BA",
    "LOMA": "LOMA.BA", "CRES": "CRES.BA", "ALUA": "ALUA.BA",
    "COME": "COME.BA", "EDN": "EDN.BA",  "TRAN": "TRAN.BA",
    "VALO": "VALO.BA", "BYMA": "BYMA.BA","HARG": "HARG.BA",
}

# Bonos soberanos con tickers Yahoo Finance .BA
BONOS_AR = [
    "AL29", "AL30", "AL35", "AL41",
    "GD29", "GD30", "GD35", "GD38", "GD41", "GD46",
    "AE38",
]

# Mapeo bono -> ticker Yahoo Finance
BONOS_YF = {
    "AL29": "AL29.BA", "AL30": "AL30.BA", "AL35": "AL35.BA", "AL41": "AL41.BA",
    "GD29": "GD29.BA", "GD30": "GD30.BA", "GD35": "GD35.BA",
    "GD38": "GD38.BA", "GD41": "GD41.BA", "GD46": "GD46.BA",
    "AE38": "AE38.BA",
    # versiones dolar (D al final)
    "AL29D": "AL29D.BA", "AL30D": "AL30D.BA", "AL35D": "AL35D.BA",
    "GD29D": "GD29D.BA", "GD30D": "GD30D.BA", "GD35D": "GD35D.BA",
    "GD38D": "GD38D.BA", "GD41D": "GD41D.BA", "GD46D": "GD46D.BA",
}

# ONs conocidas
ONS_AR = [
    "YPF", "PAMPAR", "TLC1O", "AUSA", "IRCP",
    "TECPETROL", "GENNEIA", "CGNC",
]

# CEDEARs (.BA on Yahoo)
CEDEARS_YF = {
    "AAPL": "AAPL.BA", "MSFT": "MSFT.BA", "GOOGL": "GOOGL.BA",
    "AMZN": "AMZN.BA", "TSLA": "TSLA.BA", "NVDA": "NVDA.BA",
    "META": "META.BA", "BABA": "BABA.BA", "MELI": "MELI.BA",
}

AR_TICKER_SET = (
    set(AR_STOCKS_YF.keys()) |
    set(BONOS_AR) |
    set(ONS_AR) |
    set(CEDEARS_YF.keys()) |
    {"AL29D","AL30D","AL35D","GD29D","GD30D","GD35D","GD38D","GD41D","GD46D"}
)

# ── Commodity / alias map ────────────────────────────────────────────────────
TICKER_ALIASES = {
    "BRENT":   "BZ=F",
    "WTI":     "CL=F",
    "OIL":     "CL=F",
    "PETROLEO":"CL=F",
    "PETRÓLEO":"CL=F",
    "GOLD":    "GC=F",
    "ORO":     "GC=F",
    "SILVER":  "SI=F",
    "PLATA":   "SI=F",
    "GAS":     "NG=F",
    "NATGAS":  "NG=F",
    "CORN":    "ZC=F",
    "WHEAT":   "ZW=F",
    "SOJA":    "ZS=F",
    "SOY":     "ZS=F",
    "COBRE":   "HG=F",
    "COPPER":  "HG=F",
}

# ADRs argentinos que cotizan en NYSE/Nasdaq en USD
AR_ADRS_USD = {
    "GGAL", "BMA", "YPF", "PAM", "TGS", "CEPU",
    "SUPV", "LOMA", "IRS", "MELI", "GLOB", "DESP",
    "BIOX", "CAAP", "CRESY", "EDN", "IRCP", "NMT",
    "PBRA", "PKX", "TS", "TX",
}

# ── Yahoo Finance ─────────────────────────────────────────────────────────────

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
        quotes  = result[0].get("indicators", {}).get("quote", [{}])[0]
        closes  = [c for c in quotes.get("close", []) if c is not None]
        volumes = [v if v is not None else 0 for v in quotes.get("volume", [])]
        if not closes:
            return None
        return {"closes": closes, "volumes": volumes}
    except Exception as e:
        logger.warning("YF error " + symbol + ": " + str(e))
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
        logger.warning("Ticker info error " + symbol + ": " + str(e))
        return {}

# ── Argentine data sources ────────────────────────────────────────────────────

def get_dolar_ar():
    """Fetch USD types from dolarito.ar public API."""
    try:
        r    = requests.get("https://dolarito.ar/api/frontend/history/1", timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        result = {}
        for item in data:
            name = item.get("nombre", "").lower()
            buy  = item.get("compra")
            sell = item.get("venta")
            if buy and sell:
                if "blue" in name:
                    result["Blue"] = {"compra": buy, "venta": sell}
                elif "mep" in name or "bolsa" in name:
                    result["MEP"] = {"compra": buy, "venta": sell}
                elif "contado" in name or "ccl" in name:
                    result["CCL"] = {"compra": buy, "venta": sell}
                elif "oficial" in name:
                    result["Oficial"] = {"compra": buy, "venta": sell}
        return result
    except Exception as e:
        logger.warning("Dolar AR error: " + str(e))
        return {}

def get_dolar_ar_v2():
    """Fallback: dolarapi.com"""
    try:
        endpoints = {
            "Blue":    "https://dolarapi.com/v1/dolares/blue",
            "MEP":     "https://dolarapi.com/v1/dolares/bolsa",
            "CCL":     "https://dolarapi.com/v1/dolares/contadoconliqui",
            "Oficial": "https://dolarapi.com/v1/dolares/oficial",
        }
        result = {}
        for name, url in endpoints.items():
            r    = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            data = r.json()
            if data.get("compra") and data.get("venta"):
                result[name] = {"compra": data["compra"], "venta": data["venta"]}
        return result
    except Exception as e:
        logger.warning("Dolar AR v2 error: " + str(e))
        return {}

def get_dolar():
    d = get_dolar_ar_v2()
    if not d:
        d = get_dolar_ar()
    return d

def get_bono_price(ticker):
    """Fetch Argentine bond price — multiple sources with robust parsing."""
    ticker_up = ticker.upper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-AR,es;q=0.9",
        "Referer": "https://www.rava.com/",
    }

    # Source 1: Rava API (most reliable public endpoint for AR bonds)
    try:
        url = "https://www.rava.com/api/cotizacion?especie=" + ticker_up
        r   = requests.get(url, timeout=10, headers=headers)
        if r.status_code == 200:
            data  = r.json()
            price = data.get("ultimo") or data.get("cierre") or data.get("ultimoPrecio")
            prev  = data.get("cierreAnterior") or data.get("apertura")
            if price:
                price  = float(str(price).replace(",", "."))
                change = 0.0
                if prev:
                    prev   = float(str(prev).replace(",", "."))
                    change = ((price - prev) / prev) * 100 if prev else 0
                return {"price": price, "change": round(change, 2), "source": "Rava"}
    except Exception as e:
        logger.warning("Rava bono " + ticker_up + ": " + str(e))

    # Source 2: Ambito mercados
    for endpoint in [
        "https://mercados.ambito.com/titulo/" + ticker_up + "/info",
        "https://mercados.ambito.com/bonos/" + ticker_up + "/info",
        "https://mercados.ambito.com//titulo//" + ticker_up + "//variacion",
    ]:
        try:
            r    = requests.get(endpoint, timeout=8, headers=headers)
            if r.status_code != 200:
                continue
            data  = r.json()
            price = (data.get("ultimoPrecio") or data.get("ultimo") or
                     data.get("cotizacion") or data.get("precio") or
                     data.get("venta"))
            change = (data.get("variacion") or data.get("variacionPorcentual") or
                      data.get("diferencia") or 0)
            if price:
                price  = float(str(price).replace(",", ".").replace("%","").replace("$","").strip())
                change = float(str(change).replace(",", ".").replace("%","").strip())
                return {"price": price, "change": change, "source": "Ambito"}
        except Exception:
            continue

    # Source 3: Cohen (ByMA data, 20min delay)
    try:
        url = "https://www.cohen.com.ar/api/Bursatil/Especie/" + ticker_up + "/Cotizacion"
        r   = requests.get(url, timeout=8, headers=headers)
        if r.status_code == 200:
            data  = r.json()
            price = data.get("ultimoPrecio") or data.get("ultimo") or data.get("precio")
            change = data.get("variacion") or data.get("variacionPorcentual") or 0
            if price:
                return {"price": float(price), "change": float(change), "source": "Cohen"}
    except Exception:
        pass

    # Source 4: Scrape Rava page for price
    try:
        url  = "https://www.rava.com/perfil/" + ticker_up.lower()
        r    = requests.get(url, timeout=10, headers=headers)
        text = r.text
        import re as _re
        patterns = [
            r'"ultimo"\s*:\s*([\d.,]+)',
            r'"ultimoPrecio"\s*:\s*([\d.,]+)',
            r'"cierre"\s*:\s*([\d.,]+)',
            r'class="cotizacion[^"]*"[^>]*>\s*\$?\s*([\d.,]+)',
        ]
        for pattern in patterns:
            m = _re.search(pattern, text)
            if m:
                price = float(m.group(1).replace(",", "."))
                if price > 0:
                    return {"price": price, "change": 0.0, "source": "Rava (web)"}
    except Exception:
        pass

    return None

def get_ar_stock_price(ticker):
    """Try Yahoo Finance .BA first, then Ambito."""
    yf_ticker = AR_STOCKS_YF.get(ticker, ticker + ".BA")
    data = get_yahoo_data(yf_ticker, "2d")
    if data and len(data["closes"]) >= 2:
        curr = data["closes"][-1]
        prev = data["closes"][-2]
        return {"price": float(curr), "change": float(((curr - prev) / prev) * 100), "source": "yahoo"}
    # Fallback Ambito
    try:
        url = "https://mercados.ambito.com/acciones/" + ticker + "/info"
        r   = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        price  = data.get("ultimoPrecio") or data.get("ultimo")
        change = data.get("variacion") or 0
        if price:
            price  = float(str(price).replace(",", "."))
            change = float(str(change).replace(",", ".").replace("%", ""))
            return {"price": price, "change": change, "source": "ambito"}
    except Exception:
        pass
    return None

def get_cedear_price(ticker):
    """CEDEARs via Yahoo Finance .BA"""
    yf_ticker = CEDEARS_YF.get(ticker, ticker + ".BA")
    data = get_yahoo_data(yf_ticker, "2d")
    if data and len(data["closes"]) >= 2:
        curr = data["closes"][-1]
        prev = data["closes"][-2]
        return {"price": float(curr), "change": float(((curr - prev) / prev) * 100), "source": "yahoo"}
    return None

def detect_ar_asset_type(symbol):
    """
    Returns: 'bono', 'on', 'cedear', 'accion_ar', 'dolar_ar', 'adr_usd', or None
    Explicit suffix rules:
      GGAL     -> adr_usd  (NYSE, USD)
      GGAL.BA  -> accion_ar (BCBA, ARS)
      GGAL.AR  -> accion_ar (BCBA, ARS)
    """
    s = symbol.upper()

    # Explicit suffix overrides
    if s.endswith(".BA") or s.endswith(".AR"):
        return "accion_ar"

    if s in ["MEP", "CCL", "BLUE", "DOLAR", "USD"]:
        return "dolar_ar"

    # Bond heuristic first (AL30, GD35, AE38, etc.)
    if s in BONOS_AR or s in BONOS_YF or s.rstrip("D") in BONOS_AR:
        return "bono"
    if re.match(r'^(AL|GD|AE|TV|PBA|BDC)\d', s):
        return "bono"

    if s in ONS_AR:
        return "on"

    # CEDEARs only if explicitly in list AND not an ADR
    if s in CEDEARS_YF and s not in AR_ADRS_USD:
        return "cedear"

    # ADRs in USD — route as global ticker (USD)
    if s in AR_ADRS_USD:
        return "adr_usd"

    # .BA stocks
    if s in AR_STOCKS_YF:
        return "accion_ar"

    return None

# ── Claude AI ─────────────────────────────────────────────────────────────────

def ask_claude(prompt, max_tokens=800):
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        data = r.json()
        return data.get("content", [{}])[0].get("text", "").strip()
    except Exception as e:
        logger.error("Claude error: " + str(e))
        return ""

def is_question(text):
    """Detect if user sent a free-form question rather than a ticker."""
    words = text.strip().split()
    if len(words) >= 4:
        return True
    question_words = ["que", "qué", "como", "cómo", "cuando", "cuándo", "por", "cual",
                      "cuál", "explica", "analiza", "contame", "hablame", "describe",
                      "situacion", "situación", "contexto", "opinion", "opinión"]
    low = text.lower()
    for w in question_words:
        if w in low:
            return True
    return False

def answer_question(text):
    """Claude answers a free-form macro/micro financial question."""
    dolar = get_dolar()
    dolar_str = ""
    for name, v in dolar.items():
        dolar_str += name + ": $" + str(v.get("venta", "")) + " | "

    prompt = (
        "Sos un analista financiero y economico experto en Argentina y mercados globales de ST Capital. "
        "El usuario te hace la siguiente consulta:\n\n"
        "\"" + text + "\"\n\n"
        "Respondele con un analisis claro, concreto y educativo. "
        "Incluye contexto macro o micro segun corresponda, menciona variables relevantes "
        "(tasas, inflacion, tipo de cambio, commodities, politica, etc). "
        "Si es sobre Argentina, considera el contexto economico actual del pais. "
        "Maximo 6 oraciones. Tono profesional, en español. Sin markdown ni asteriscos.\n\n"
        "Datos de referencia actuales - Dolar Argentina: " + dolar_str
    )
    return ask_claude(prompt, max_tokens=700)

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

# ── Argentine ticker analysis ─────────────────────────────────────────────────

def analyze_ar_dolar():
    dolar = get_dolar()
    if not dolar:
        return "No pude obtener los datos del dolar ahora. Intenta en unos minutos."

    lines = ["*Dolar Argentina* 🇦🇷", ""]
    for name, v in dolar.items():
        compra = v.get("compra", "-")
        venta  = v.get("venta", "-")
        lines.append("💵 *" + name + "*: Compra $" + str(compra) + " | Venta $" + str(venta))

    # Brecha
    oficial = dolar.get("Oficial", {}).get("venta")
    blue    = dolar.get("Blue", {}).get("venta")
    mep     = dolar.get("MEP", {}).get("venta")
    ccl     = dolar.get("CCL", {}).get("venta")
    if oficial and blue:
        try:
            brecha = round(((float(blue) - float(oficial)) / float(oficial)) * 100, 1)
            lines.append("")
            lines.append("📊 *Brecha Blue/Oficial:* " + str(brecha) + "%")
        except Exception:
            pass

    analysis = ask_claude(
        "Sos analista financiero de Argentina de ST Capital. "
        "Con estos datos del dolar, escribi un analisis de 3-4 oraciones en español sobre "
        "el contexto cambiario argentino actual: que implica la brecha, como esta el mercado "
        "cambiario, y que factores macro explican estos valores. Sin markdown.\n\n"
        "Dolar Oficial: " + str(oficial) + " | Blue: " + str(blue) +
        " | MEP: " + str(mep) + " | CCL: " + str(ccl),
        max_tokens=400
    )

    if analysis:
        lines.append("")
        lines.append("*Analisis cambiario*")
        lines.append(analysis)

    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

def analyze_ar_bono(symbol):
    data = get_bono_price(symbol)
    if not data:
        return "No encontre datos para el bono *" + symbol + "*. Verifica el ticker (ej: AL30, GD35, GD30)."

    price  = data["price"]
    change = data["change"]

    analysis = ask_claude(
        "Sos analista de renta fija argentina de ST Capital. "
        "El bono " + symbol + " cotiza a $" + str(price) + " con una variacion de " + str(change) + "% hoy. "
        "Escribi un analisis de 4 oraciones en español que explique: "
        "1) Que es este bono y sus caracteristicas principales (ley, vencimiento, moneda). "
        "2) Que factores macro argentinos e internacionales afectan su precio. "
        "3) Como esta el contexto actual de la deuda soberana argentina. "
        "Sin markdown ni asteriscos.",
        max_tokens=500
    )

    lines = ["*Bono " + symbol + "* 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio:* $" + fmt_price(price) + "  " + "{:+.2f}".format(change) + "%")
    lines.append("📡 _Fuente: Ambito Financiero_")
    if analysis:
        lines.append("")
        lines.append("*Analisis*")
        lines.append(analysis)
    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

def analyze_ar_stock(symbol):
    data = get_ar_stock_price(symbol)
    if not data:
        return "No encontre datos para *" + symbol + "*. Verifica el ticker (ej: GGAL, BMA, YPFD, PAMP)."

    price  = data["price"]
    change = data["change"]
    source = data["source"]

    # Try to get historical for indicators
    yf_ticker = AR_STOCKS_YF.get(symbol, symbol + ".BA")
    hist = get_yahoo_data(yf_ticker, "1y")
    rsi = sma50 = dist_sma50 = high_52w = low_52w = None
    if hist and len(hist["closes"]) >= 16:
        closes = hist["closes"]
        rsi    = compute_rsi(closes)
        if len(closes) >= 50:
            sma50      = round(float(np.mean(closes[-50:])), 2)
            dist_sma50 = pct_from_ma(price, sma50)
        high_52w = round(float(np.max(closes)), 2)
        low_52w  = round(float(np.min(closes)), 2)

    analysis = ask_claude(
        "Sos analista de acciones argentinas de ST Capital. "
        "La accion " + symbol + " cotiza a $" + fmt_price(price) + " ARS con " + "{:+.2f}".format(change) + "% hoy. "
        + ("RSI: " + str(rsi) + ". " if rsi else "")
        + ("SMA50: $" + fmt_price(sma50) + " (" + "{:+.2f}".format(dist_sma50) + "%). " if sma50 else "")
        + ("MAX 52W: $" + fmt_price(high_52w) + " | MIN 52W: $" + fmt_price(low_52w) + ". " if high_52w else "")
        + "Escribi un analisis de 4-5 oraciones en español que incluya: "
        "1) Que hace esta empresa y su rol en la economia argentina. "
        "2) Que variables macro (tipo de cambio, inflacion, tarifas, politica) afectan su precio. "
        "3) Una lectura del momento tecnico actual. "
        "Sin markdown ni asteriscos.",
        max_tokens=550
    )

    lines = ["*" + symbol + "* (Accion ARG) 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio:* $" + fmt_price(price) + " ARS  " + "{:+.2f}".format(change) + "%")
    if rsi is not None:
        tag = " - Sobrecomprado" if rsi >= 70 else (" - Sobrevendido" if rsi <= 30 else "")
        lines.append("📉 *RSI 14:* " + str(rsi) + tag)
    if sma50 is not None:
        lines.append(arrow_emoji(dist_sma50) + " *Dist SMA 50:* " + "{:+.2f}".format(dist_sma50) + "% (SMA: $" + fmt_price(sma50) + ")")
    if high_52w:
        lines.append("📈 *MAX 52W:* $" + fmt_price(high_52w))
        lines.append("📉 *MIN 52W:* $" + fmt_price(low_52w))
    lines.append("📡 _Fuente: " + source + "_")
    if analysis:
        lines.append("")
        lines.append("*Analisis*")
        lines.append(analysis)
    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

def analyze_cedear(symbol):
    data = get_cedear_price(symbol)
    if not data:
        return "No encontre datos para el CEDEAR *" + symbol + "*. Verifica (ej: AAPL, TSLA, GOOGL, MELI)."

    price  = data["price"]
    change = data["change"]

    # Also get the underlying US stock for comparison
    us_data = get_yahoo_data(symbol, "2d")
    us_price = us_change = None
    if us_data and len(us_data["closes"]) >= 2:
        us_price  = us_data["closes"][-1]
        us_change = ((us_data["closes"][-1] - us_data["closes"][-2]) / us_data["closes"][-2]) * 100

    analysis = ask_claude(
        "Sos analista financiero de ST Capital especializado en CEDEARs argentinos. "
        "El CEDEAR de " + symbol + " cotiza a $" + fmt_price(price) + " ARS (" + "{:+.2f}".format(change) + "% hoy). "
        + ("El subyacente en USA cotiza a $" + fmt_price(us_price) + " USD (" + "{:+.2f}".format(us_change) + "%). " if us_price else "")
        + "Escribi un analisis de 4 oraciones en español que explique: "
        "1) Que es un CEDEAR y como funciona este instrumento en Argentina. "
        "2) Como el tipo de cambio (CCL/MEP) afecta el precio en pesos. "
        "3) El contexto actual del subyacente en mercados internacionales. "
        "Sin markdown ni asteriscos.",
        max_tokens=500
    )

    lines = ["*CEDEAR " + symbol + "* 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio ARS:* $" + fmt_price(price) + "  " + "{:+.2f}".format(change) + "%")
    if us_price:
        lines.append(arrow_emoji(us_change) + " *Subyacente USD:* $" + fmt_price(us_price) + "  " + "{:+.2f}".format(us_change) + "%")
    lines.append("📡 _Fuente: Yahoo Finance .BA_")
    if analysis:
        lines.append("")
        lines.append("*Analisis*")
        lines.append(analysis)
    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

# ── Global ticker analysis ────────────────────────────────────────────────────

def analyze_ticker(symbol):
    symbol = symbol.upper().strip()

    # Resolve commodity/common aliases
    symbol = TICKER_ALIASES.get(symbol, symbol)

    logger.info("Analyzing: " + symbol)

    # Route Argentine assets
    ar_type = detect_ar_asset_type(symbol)
    if ar_type == "dolar_ar" or symbol in ["DOLAR", "MEP", "CCL", "BLUE"]:
        return analyze_ar_dolar()
    if ar_type == "bono":
        return analyze_ar_bono(symbol)
    if ar_type == "on":
        return analyze_ar_bono(symbol)
    if ar_type == "cedear":
        return analyze_cedear(symbol)
    if ar_type == "accion_ar":
        return analyze_ar_stock(symbol)
    # adr_usd falls through to global analysis below (USD price from NYSE)

    # Try as .BA if not found globally
    data = get_yahoo_data(symbol, "1y")
    if not data or len(data["closes"]) < 5:
        # Try with .BA suffix
        data_ba = get_yahoo_data(symbol + ".BA", "1y")
        if data_ba and len(data_ba["closes"]) >= 5:
            return analyze_ar_stock(symbol)
        return "No encontre datos para *" + symbol + "*.\n\nSi es un activo argentino, proba con:\n- Acciones: GGAL, BMA, YPFD, PAMP\n- Bonos: AL30, GD35, GD30\n- CEDEARs: AAPL, TSLA, GOOGL\n- Dolar: BLUE, MEP, CCL"

    closes  = data["closes"]
    volumes = data["volumes"]
    current = closes[-1]

    prev_close = closes[-2] if len(closes) >= 2 else current
    day_chg    = ((current - prev_close) / prev_close) * 100
    rsi        = compute_rsi(closes) if len(closes) >= 16 else None

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

    ticker_news      = fetch_news(name + " " + symbol, 4)
    ticker_news_text = "\n".join(ticker_news[:4]) if ticker_news else "Sin noticias recientes."

    tech_summary = (
        "Activo: " + name + " (" + symbol + ")\n"
        "Precio: $" + fmt_price(current) + " (" + "{:+.2f}".format(day_chg) + "% hoy)\n"
        + ("RSI 14: " + str(rsi) + "\n" if rsi else "")
        + ("RS Score vs SPY: " + "{:+.2f}".format(rs_score) + "%\n" if rs_score is not None else "")
        + ("Dist EMA 200: " + "{:+.2f}".format(dist_ema200) + "%\n" if dist_ema200 is not None else "")
        + ("Dist SMA 50: " + "{:+.2f}".format(dist_sma50) + "%\n" if dist_sma50 is not None else "")
        + "MAX 52W: $" + fmt_price(high_52w) + " (" + "{:+.2f}".format(dist_52w_high) + "% del maximo)\n"
        + "MIN 52W: $" + fmt_price(low_52w) + "\n"
        + "Volumen ratio 20d: " + str(vol_ratio) + "x\n"
    )

    claude_analysis = ask_claude(
        "Sos analista financiero senior de ST Capital. Analiza " + name + " (" + symbol + ").\n\n"
        "Escribi DOS parrafos separados por linea en blanco:\n\n"
        "PARRAFO 1 - CONTEXTO GLOBAL (3-4 oraciones): "
        "Que es este activo, que rol cumple a nivel mundial, y cuales son las principales "
        "variables macro, geopoliticas o sectoriales que afectan su precio hoy.\n\n"
        "PARRAFO 2 - ANALISIS TECNICO (3-4 oraciones): "
        "Con los datos tecnicos, describe la tendencia, zonas de soporte/resistencia, "
        "que dicen RSI y medias moviles, y una conclusion del momento del activo.\n\n"
        "Sin markdown, sin asteriscos, sin titulos.\n\n"
        "Datos tecnicos:\n" + tech_summary +
        "\nNoticias:\n" + ticker_news_text,
        max_tokens=650
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
    vol_emoji_str = "🔥" if vol_ratio > 1.5 else ("📊" if vol_ratio >= 0.8 else "😴")
    lines.append(vol_emoji_str + " *Vol ultimo dia:* " + "{:,}".format(vol_last))
    lines.append("📊 *Vol promedio 20d:* " + "{:,}".format(vol_avg20) + " (ratio: " + str(vol_ratio) + "x)")

    if claude_analysis:
        parts = claude_analysis.strip().split("\n\n", 1)
        lines.append("")
        lines.append("*Contexto global*")
        lines.append(parts[0].strip())
        if len(parts) > 1:
            lines.append("")
            lines.append("*Analisis tecnico*")
            lines.append(parts[1].strip())

    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

# ── Market data helpers ───────────────────────────────────────────────────────

def fetch_market(symbols):
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
    snapshot = "INDICES:\n"
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
    news_text = "\n".join(["- " + n for n in news[:4]]) if news else ""
    analysis = ask_claude(
        "Sos analista de ST Capital. Hoy es " + now + ". Con estos datos de apertura, "
        "escribi un analisis de 4-5 oraciones en español sobre las tendencias del dia, "
        "que monitorear y el contexto macro. Directo, sin markdown.\n\n" + snapshot + "\nNoticias:\n" + news_text,
        max_tokens=450
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
        "Sos analista de ST Capital. Update mid-morning: 3 oraciones en español sobre la sesion. Sin markdown.\n\n" + snapshot,
        max_tokens=300
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
        "Sos analista de ST Capital. Mediodia: 3 oraciones en español sobre commodities, forex y crypto. Sin markdown.\n\n" + snapshot,
        max_tokens=300
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
        "Sos analista de ST Capital. Faltan 2hs para el cierre de Wall Street. "
        "3 oraciones en español sobre como se perfila el cierre. Sin markdown.\n\n" + snapshot,
        max_tokens=300
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
        "Sos analista de ST Capital. El mercado cerro. 4-5 oraciones en español resumiendo el dia. Sin markdown.\n\n" + snapshot,
        max_tokens=450
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

def build_hourly():
    now  = datetime.now(TZ).strftime("%H:%M hs")
    hour = datetime.now(TZ).hour

    snapshot, indices, crypto, commodities, forex, stocks = build_market_snapshot()

    # Fetch broad news from multiple angles
    news_economy  = fetch_news("economy markets Fed interest rates inflation", 5)
    news_geo      = fetch_news("geopolitics war conflict military sanctions energy", 5)
    news_usa      = fetch_news("Trump United States economy trade tariffs", 4)
    news_energy   = fetch_news("oil gas energy OPEC Middle East", 4)
    news_finance  = fetch_news("stocks earnings bonds yield Federal Reserve", 4)

    all_headlines = (
        "ECONOMIA/MERCADOS: " + " | ".join(news_economy[:4]) + "\n"
        "GEOPOLITICA/GUERRA: " + " | ".join(news_geo[:4]) + "\n"
        "ESTADOS UNIDOS: " + " | ".join(news_usa[:3]) + "\n"
        "ENERGIA: " + " | ".join(news_energy[:3]) + "\n"
        "FINANZAS: " + " | ".join(news_finance[:3])
    )

    prompt = (
        "Sos un analista financiero senior de ST Capital. Son las " + now + " hora Argentina.\n\n"
        "Con las noticias y datos de mercado provistos, redacta un ALERTA DE MERCADO en español "
        "con este formato EXACTO (sin markdown, sin asteriscos, texto plano):\n\n"
        "🚨 ALERTA DE MERCADO - " + now + "\n\n"
        "Que esta pasando:\n"
        "[2-3 oraciones explicando el evento o dinamica mas relevante del momento. "
        "Enfocate en lo que realmente mueve mercados: geopolitica, macro, Fed, datos economicos, "
        "tensiones comerciales, crisis energetica, o cualquier evento de alto impacto. "
        "Se especifico, nombra paises, funcionarios, datos concretos.]\n\n"
        "Impacto en mercados:\n"
        "[2-3 oraciones describiendo como esto afecta indices, commodities, cripto, bonos o acciones especificas. "
        "Nombra activos concretos que se ven beneficiados y perjudicados. "
        "Menciona CEDEARs relevantes si aplica.]\n\n"
        "Que hacer:\n"
        "[1-2 oraciones con una perspectiva practica para el inversor: "
        "donde poner atencion, que sectores monitorear, sin dar senales directas de compra/venta.]\n\n"
        "⚠️ Analisis informativo. No es asesoramiento financiero.\n\n"
        "---\n"
        "Datos de mercado actuales:\n" + snapshot + "\n"
        "Noticias recientes (usa estas como fuente principal):\n" + all_headlines
    )

    alert = ask_claude(prompt, max_tokens=700)

    # Build message
    msg = "🚨 *ALERTA DE MERCADO - " + now + "*\n_ST Capital_\n"
    msg += section("📊 *INDICES*", indices)
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)

    if alert:
        msg += "\n\n" + alert
    else:
        # Fallback if Claude fails
        top = news_economy[:2] + news_geo[:1] + news_usa[:1]
        if top:
            msg += "\n\n📰 *Titulares*"
            for n in top:
                msg += "\n- " + n

    msg += "\n\n_ST Capital_"
    return msg

# ── Telegram handler ──────────────────────────────────────────────────────────

APP = None

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()

    if text.upper().lstrip("/") in ["START", "HELP", "AYUDA"]:
        await update.message.reply_text(
            "ST Capital Bot\n\n"
            "Escribime cualquier ticker o pregunta:\n\n"
            "GLOBALES: AAPL, BTC-USD, GC=F, SPY, TSLA\n"
            "ARGENTINOS: GGAL, BMA, YPFD, AL30, GD35\n"
            "CEDEARs: AAPL, TSLA, GOOGL (detecta automatico)\n"
            "DOLAR: BLUE, MEP, CCL\n\n"
            "O preguntame algo: 'Como esta la economia argentina?'"
        )
        return

    # Detect if it's a question or a ticker
    if is_question(text):
        await update.message.reply_text("Analizando tu consulta...")
        import asyncio
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, answer_question, text)
        if result:
            await update.message.reply_text(result, disable_web_page_preview=True)
        else:
            await update.message.reply_text("No pude generar un analisis. Intenta de nuevo.")
        return

    symbol = text.upper().replace(" ", "").lstrip("/")
    symbol = TICKER_ALIASES.get(symbol, symbol)
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

    scheduled_hours = {9, 11, 13, 15, 17}
    for h in range(8, 23):
        if h not in scheduled_hours:
            scheduler.add_job(make_job(build_hourly), "cron", hour=h, minute=0)

    scheduler.start()
    logger.info("ST Capital Bot v10 - Argentina + Global + Claude AI.")
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

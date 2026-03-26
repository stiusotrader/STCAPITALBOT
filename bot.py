import os
import re
import logging
import numpy as np
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import requests
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID           = os.environ.get("CHAT_ID")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TZ                = pytz.timezone("America/Argentina/Buenos_Aires")

# ── Asset maps ────────────────────────────────────────────────────────────────

TICKER_ALIASES = {
    # Commodities
    "BRENT": "BZ=F", "WTI": "CL=F", "OIL": "CL=F",
    "PETROLEO": "CL=F", "PETRÓLEO": "CL=F",
    "GOLD": "GC=F", "ORO": "GC=F",
    "SILVER": "SI=F", "PLATA": "SI=F",
    "GAS": "NG=F", "NATGAS": "NG=F",
    "SOJA": "ZS=F", "SOY": "ZS=F",
    "COBRE": "HG=F", "COPPER": "HG=F",
    "CORN": "ZC=F", "WHEAT": "ZW=F",
    # Forex — both formats
    "EURUSD":  "EURUSD=X", "EUR/USD": "EURUSD=X", "EUR":     "EURUSD=X",
    "GBPUSD":  "GBPUSD=X", "GBP/USD": "GBPUSD=X", "GBP":     "GBPUSD=X",
    "USDJPY":  "USDJPY=X", "USD/JPY": "USDJPY=X", "JPY":     "USDJPY=X",
    "AUDUSD":  "AUDUSD=X", "AUD/USD": "AUDUSD=X", "AUD":     "AUDUSD=X",
    "USDCHF":  "USDCHF=X", "USD/CHF": "USDCHF=X", "CHF":     "USDCHF=X",
    "USDCAD":  "USDCAD=X", "USD/CAD": "USDCAD=X", "CAD":     "USDCAD=X",
    "NZDUSD":  "NZDUSD=X", "NZD/USD": "NZDUSD=X", "NZD":     "NZDUSD=X",
    # Latam
    "USDBRL":  "USDBRL=X", "USD/BRL": "USDBRL=X", "BRL":     "USDBRL=X", "REAL": "USDBRL=X",
    "USDMXN":  "USDMXN=X", "USD/MXN": "USDMXN=X", "MXN":     "USDMXN=X", "PESO": "USDMXN=X",
    "USDCLP":  "USDCLP=X", "USD/CLP": "USDCLP=X", "CLP":     "USDCLP=X",
    "USDCOP":  "USDCOP=X", "USD/COP": "USDCOP=X", "COP":     "USDCOP=X",
    "USDARS":  "ARS=X",    "USD/ARS": "ARS=X",    "ARS":     "ARS=X",
    # DXY
    "DXY":     "DX-Y.NYB", "DOLAR":   "DX-Y.NYB", "DOLLAR":  "DX-Y.NYB",
}

# Forex tickers set for special analysis routing
FOREX_TICKERS = {
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCHF=X",
    "USDCAD=X", "NZDUSD=X", "USDBRL=X", "USDMXN=X", "USDCLP=X",
    "USDCOP=X", "ARS=X", "DX-Y.NYB",
}

FOREX_NAMES = {
    "EURUSD=X": "EUR/USD", "GBPUSD=X": "GBP/USD", "USDJPY=X": "USD/JPY",
    "AUDUSD=X": "AUD/USD", "USDCHF=X": "USD/CHF", "USDCAD=X": "USD/CAD",
    "NZDUSD=X": "NZD/USD", "USDBRL=X": "USD/BRL", "USDMXN=X": "USD/MXN",
    "USDCLP=X": "USD/CLP", "USDCOP=X": "USD/COP", "ARS=X":    "USD/ARS",
    "DX-Y.NYB": "DXY (Indice Dolar)",
}

INDICES = {
    "S&P 500": "^GSPC", "Nasdaq": "^IXIC", "Dow Jones": "^DJI",
    "DAX": "^GDAXI", "Nikkei": "^N225", "FTSE 100": "^FTSE",
}
COMMODITIES = {
    "Gold": "GC=F", "Silver": "SI=F", "Oil WTI": "CL=F", "Natural Gas": "NG=F",
}
FOREX = {
    "DXY": "DX-Y.NYB", "EUR/USD": "EURUSD=X", "USD/ARS": "ARS=X",
}
US_STOCKS = {
    "Apple": "AAPL", "NVIDIA": "NVDA", "Tesla": "TSLA",
    "Microsoft": "MSFT", "Meta": "META", "Amazon": "AMZN",
}
CRYPTO_IDS = {
    "Bitcoin": "bitcoin", "Ethereum": "ethereum", "Solana": "solana",
    "XRP": "ripple", "BNB": "binancecoin",
}

AR_STOCKS_YF = {
    "GGAL": "GGAL.BA", "BMA": "BMA.BA", "TXAR": "TXAR.BA",
    "YPFD": "YPFD.BA", "PAMP": "PAMP.BA", "TECO2": "TECO2.BA",
    "SUPV": "SUPV.BA", "CEPU": "CEPU.BA", "MIRG": "MIRG.BA",
    "LOMA": "LOMA.BA", "CRES": "CRES.BA", "ALUA": "ALUA.BA",
    "COME": "COME.BA", "EDN": "EDN.BA", "TRAN": "TRAN.BA",
    "VALO": "VALO.BA", "BYMA": "BYMA.BA", "HARG": "HARG.BA",
}
AR_ADRS_USD = {
    "GGAL", "BMA", "YPF", "PAM", "TGS", "CEPU", "SUPV",
    "LOMA", "IRS", "MELI", "GLOB", "DESP", "BIOX", "CAAP",
}
BONOS_AR = [
    "AL29", "AL30", "AL35", "AL41",
    "GD29", "GD30", "GD35", "GD38", "GD41", "GD46", "AE38",
]
ONS_AR = ["YPF", "PAMPAR", "TLC1O", "AUSA", "IRCP", "TECPETROL", "GENNEIA"]
CEDEARS_YF = {
    "AAPL": "AAPL.BA", "MSFT": "MSFT.BA", "GOOGL": "GOOGL.BA",
    "AMZN": "AMZN.BA", "TSLA": "TSLA.BA", "NVDA": "NVDA.BA",
    "META": "META.BA", "BABA": "BABA.BA", "MELI": "MELI.BA",
}

# ── Formatters ────────────────────────────────────────────────────────────────

def arrow_emoji(chg):
    return "🟢" if chg >= 0 else "🔴"

def fmt_price(price, decimals=2):
    try:
        price = float(price)
        if price >= 1000:   return "{:,.0f}".format(price)
        elif price >= 1:    return "{:,.{}f}".format(price, decimals)
        else:               return "{:.4f}".format(price)
    except Exception:
        return str(price)

def fmt_large(val):
    try:
        val = float(val)
        if val >= 1e12: return "${:.2f}T".format(val / 1e12)
        elif val >= 1e9: return "${:.2f}B".format(val / 1e9)
        elif val >= 1e6: return "${:.2f}M".format(val / 1e6)
        return "${:,.0f}".format(val)
    except Exception:
        return str(val)

def pct_from_ma(current, ma):
    try:
        return round(((float(current) - float(ma)) / float(ma)) * 100, 2)
    except Exception:
        return 0.0

# ── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(closes, period=14):
    try:
        arr     = np.array(closes, dtype=float)
        deltas  = np.diff(arr)
        gains   = np.where(deltas > 0, deltas, 0.0)
        losses  = np.where(deltas < 0, -deltas, 0.0)
        ag      = np.mean(gains[-period:])
        al      = np.mean(losses[-period:])
        if al == 0: return 100.0
        return round(100 - (100 / (1 + ag / al)), 2)
    except Exception:
        return None

def compute_rs_score(ticker_closes, spy_closes):
    try:
        n = min(63, len(ticker_closes), len(spy_closes))
        if n < 2: return 0.0
        tr = (ticker_closes[-1] / ticker_closes[-n]) - 1
        sr = (spy_closes[-1]    / spy_closes[-n])    - 1
        return round((tr - sr) * 100, 2)
    except Exception:
        return None

# ── Yahoo Finance ─────────────────────────────────────────────────────────────

def get_yahoo_data(symbol, period="1y"):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    yf_range = {"2d": "5d"}.get(period, period)
    try:
        url    = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?interval=1d&range=" + yf_range
        r      = requests.get(url, headers=headers, timeout=8)
        data   = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result: return None
        quotes  = result[0].get("indicators", {}).get("quote", [{}])[0]
        closes  = [c for c in quotes.get("close", []) if c is not None]
        volumes = [v if v is not None else 0 for v in quotes.get("volume", [])]
        if not closes: return None
        return {"closes": closes, "volumes": volumes}
    except Exception as e:
        logger.warning("YF " + symbol + ": " + str(e))
        return None

def get_ticker_info(symbol):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        url    = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/" + symbol + "?modules=assetProfile,price"
        r      = requests.get(url, headers=headers, timeout=8)
        result = r.json().get("quoteSummary", {}).get("result", [])
        if not result: return {}
        pd = result[0].get("price", {})
        ad = result[0].get("assetProfile", {})
        return {
            "longName":  pd.get("longName") or pd.get("shortName", symbol),
            "sector":    ad.get("sector", ""),
            "industry":  ad.get("industry", ""),
            "marketCap": pd.get("marketCap", {}).get("raw"),
        }
    except Exception:
        return {}

# ── Argentine data ────────────────────────────────────────────────────────────

def get_dolar():
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        endpoints = {
            "Blue":    "https://dolarapi.com/v1/dolares/blue",
            "MEP":     "https://dolarapi.com/v1/dolares/bolsa",
            "CCL":     "https://dolarapi.com/v1/dolares/contadoconliqui",
            "Oficial": "https://dolarapi.com/v1/dolares/oficial",
        }
        result = {}
        for name, url in endpoints.items():
            try:
                r = requests.get(url, timeout=6, headers=headers)
                d = r.json()
                if d.get("compra") and d.get("venta"):
                    result[name] = {"compra": d["compra"], "venta": d["venta"]}
            except Exception:
                pass
        return result
    except Exception:
        return {}

def get_bono_price(ticker):
    ticker_up = ticker.upper().strip()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "es-AR,es;q=0.9",
    }

    # Source 1: PPI — public page with all bonds
    try:
        r = requests.get("https://www.portfoliopersonal.com/Cotizaciones/Bonos",
                         headers=headers, timeout=10)
        if r.status_code == 200:
            text = r.text
            idx  = text.find("[" + ticker_up + "]")
            if idx > 0:
                snippet = text[idx:idx+600]
                m_price = re.search(r"(?:AR\$|US\$)\s*([\d.,]+)", snippet)
                m_var   = re.search(r"(-?[\d.,]+)%", snippet)
                if m_price:
                    raw    = m_price.group(1).replace(".", "").replace(",", ".")
                    price  = float(raw)
                    change = 0.0
                    if m_var:
                        change = float(m_var.group(1).replace(",", "."))
                    if price > 0:
                        return {"price": price, "change": change, "source": "PPI"}
    except Exception as e:
        logger.warning("PPI bono: " + str(e))

    # Source 2: Ambito
    h2 = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for url in [
        "https://mercados.ambito.com//titulo//" + ticker_up + "//variacion",
        "https://mercados.ambito.com/titulo/" + ticker_up + "/info",
    ]:
        try:
            r    = requests.get(url, headers=h2, timeout=6)
            if r.status_code == 200:
                data  = r.json()
                price = (data.get("ultimo") or data.get("ultimoPrecio") or
                         data.get("cotizacion") or data.get("venta"))
                chg   = data.get("variacion") or data.get("variacionPorcentual") or 0
                if price:
                    price = float(str(price).replace(",", ".").replace("$", "").strip())
                    if price > 0:
                        return {"price": price,
                                "change": float(str(chg).replace(",", ".").replace("%", "").strip()),
                                "source": "Ambito"}
        except Exception:
            continue

    # Source 3: Rava
    try:
        r = requests.get("https://www.rava.com/api/cotizacion?especie=" + ticker_up,
                         headers=h2, timeout=6)
        if r.status_code == 200:
            data  = r.json()
            price = data.get("ultimo") or data.get("cierre") or data.get("ultimoPrecio")
            prev  = data.get("cierreAnterior") or data.get("apertura")
            if price:
                price = float(str(price).replace(",", "."))
                if price > 0:
                    change = 0.0
                    if prev:
                        prev   = float(str(prev).replace(",", "."))
                        change = ((price - prev) / prev) * 100 if prev else 0
                    return {"price": price, "change": round(change, 2), "source": "Rava"}
    except Exception:
        pass

    return None

def get_ar_stock_price(ticker):
    yf_ticker = AR_STOCKS_YF.get(ticker, ticker + ".BA")
    data = get_yahoo_data(yf_ticker, "2d")
    if data and len(data["closes"]) >= 2:
        curr = data["closes"][-1]
        prev = data["closes"][-2]
        return {"price": float(curr), "change": float(((curr - prev) / prev) * 100), "source": "Yahoo"}
    return None

def get_cedear_price(ticker):
    yf_ticker = CEDEARS_YF.get(ticker, ticker + ".BA")
    data = get_yahoo_data(yf_ticker, "2d")
    if data and len(data["closes"]) >= 2:
        curr = data["closes"][-1]
        prev = data["closes"][-2]
        return {"price": float(curr), "change": float(((curr - prev) / prev) * 100), "source": "Yahoo"}
    return None

def detect_ar_asset_type(symbol):
    s = symbol.upper()
    if s.endswith(".BA") or s.endswith(".AR"):
        # Check if it's a known CEDEAR base ticker
        base = s.replace(".BA", "").replace(".AR", "")
        if base in CEDEARS_YF:
            return "cedear"
        return "accion_ar"
    if s in ["MEP", "CCL", "BLUE", "DOLAR", "USD"]:
        return "dolar_ar"
    if s in BONOS_AR or s.rstrip("D") in BONOS_AR:
        return "bono"
    if re.match(r"^(AL|GD|AE|TV|PBA|BDC)\d", s):
        return "bono"
    if s in ONS_AR:
        return "on"
    # CEDEARs only detected with explicit .BA suffix (already handled above)
    # AAPL, TSLA etc without suffix = global stock analysis
    if s in AR_ADRS_USD:
        return "adr_usd"
    if s in AR_STOCKS_YF:
        return "accion_ar"
    return None

# ── Claude AI ─────────────────────────────────────────────────────────────────

def ask_claude(prompt, max_tokens=600):
    if not ANTHROPIC_API_KEY:
        return ""
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
            timeout=20,
        )
        return r.json().get("content", [{}])[0].get("text", "").strip()
    except Exception as e:
        logger.error("Claude: " + str(e))
        return ""

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
        r    = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=" + ids + "&vs_currencies=usd&include_24hr_change=true",
            timeout=7)
        data = r.json()
        return {
            name: {"price": data[cid]["usd"], "change": data[cid].get("usd_24h_change", 0)}
            for name, cid in CRYPTO_IDS.items() if cid in data
        }
    except Exception as e:
        logger.warning("Crypto: " + str(e))
        return {}

def fetch_news(query, count=4):
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything?q=" + query +
            "&language=en&sortBy=publishedAt&pageSize=" + str(count) +
            "&apiKey=" + NEWS_API_KEY,
            timeout=7)
        return [a["title"] for a in r.json().get("articles", []) if a.get("title")]
    except Exception:
        return []

def section(title, data, decimals=2):
    if not data: return ""
    lines = ["\n" + title]
    for name, v in data.items():
        lines.append(arrow_emoji(v["change"]) + " *" + name + "*: $" +
                     fmt_price(v["price"], decimals) + "  (" +
                     "{:+.2f}".format(v["change"]) + "%)")
    return "\n".join(lines)

def build_market_snapshot():
    indices     = fetch_market(INDICES)
    crypto      = fetch_crypto()
    commodities = fetch_market(COMMODITIES)
    forex       = fetch_market(FOREX)
    stocks      = fetch_market(US_STOCKS)
    snap = "INDICES:\n"
    for n, v in indices.items():
        snap += n + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snap += "\nCRYPTO:\n"
    for n, v in crypto.items():
        snap += n + ": $" + fmt_price(v["price"], 0) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snap += "\nCOMMODITIES:\n"
    for n, v in commodities.items():
        snap += n + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snap += "\nFOREX:\n"
    for n, v in forex.items():
        snap += n + ": " + fmt_price(v["price"], 4) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    snap += "\nUS STOCKS:\n"
    for n, v in stocks.items():
        snap += n + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)\n"
    return snap, indices, crypto, commodities, forex, stocks

# ── Question detection ────────────────────────────────────────────────────────

def is_question(text):
    words = text.strip().split()
    if len(words) >= 4:
        return True
    question_words = ["que", "qué", "como", "cómo", "cuando", "cuándo", "por",
                      "cual", "cuál", "explica", "analiza", "contame", "hablame",
                      "describe", "situacion", "situación", "contexto", "opinion",
                      "opinión", "cuanto", "cuánto", "precio", "cotiza", "vale"]
    low = text.lower()
    return any(w in low for w in question_words)

def answer_question(text):
    dolar       = get_dolar()
    crypto      = fetch_crypto()
    indices     = fetch_market({"S&P 500": "^GSPC", "Nasdaq": "^IXIC"})
    commodities = fetch_market({"Gold": "GC=F", "Oil WTI": "CL=F"})

    dolar_str = " | ".join(
        n + ": $" + str(v.get("venta", "")) for n, v in dolar.items()
    )
    crypto_str = " | ".join(
        n + ": $" + fmt_price(v["price"], 0) + " (" + "{:+.2f}".format(v["change"]) + "%)"
        for n, v in crypto.items()
    )
    indices_str = " | ".join(
        n + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)"
        for n, v in indices.items()
    )
    commodities_str = " | ".join(
        n + ": $" + fmt_price(v["price"]) + " (" + "{:+.2f}".format(v["change"]) + "%)"
        for n, v in commodities.items()
    )

    prompt = (
        "Sos analista financiero de ST Capital. El usuario pregunta:\n\n"
        "\"" + text + "\"\n\n"
        "REGLA CRITICA: Usa SOLO los precios de los datos en tiempo real provistos. "
        "NUNCA uses precios de tu entrenamiento. Si el precio no esta, decilo.\n\n"
        "Responde de forma clara mencionando valores reales exactos. "
        "Maximo 4 oraciones. En español. Sin markdown.\n\n"
        "=== DATOS EN TIEMPO REAL ===\n"
        "Dolar AR: " + dolar_str + "\n"
        "Crypto: " + crypto_str + "\n"
        "Indices: " + indices_str + "\n"
        "Commodities: " + commodities_str
    )
    return ask_claude(prompt, max_tokens=500)

# ── Argentine asset analyzers ─────────────────────────────────────────────────

def analyze_ar_dolar():
    dolar = get_dolar()
    if not dolar:
        return "No pude obtener los datos del dolar ahora. Intenta en unos minutos."
    lines = ["*Dolar Argentina* 🇦🇷", ""]
    for name, v in dolar.items():
        lines.append("💵 *" + name + "*: Compra $" + str(v.get("compra", "-")) +
                     " | Venta $" + str(v.get("venta", "-")))
    oficial = dolar.get("Oficial", {}).get("venta")
    blue    = dolar.get("Blue", {}).get("venta")
    if oficial and blue:
        try:
            brecha = round(((float(blue) - float(oficial)) / float(oficial)) * 100, 1)
            lines.append("")
            lines.append("📊 *Brecha Blue/Oficial:* " + str(brecha) + "%")
        except Exception:
            pass
    analysis = ask_claude(
        "Analisis cambiario argentino breve (3 oraciones, sin markdown): "
        "Oficial=" + str(oficial) + " Blue=" + str(blue) +
        " MEP=" + str(dolar.get("MEP", {}).get("venta", "")) +
        " CCL=" + str(dolar.get("CCL", {}).get("venta", "")),
        max_tokens=300
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
        return ("No encontre datos para el bono *" + symbol + "*.\n"
                "Verifica el ticker (ej: AL30, GD30, GD35, AE38, AL35).")
    price  = data["price"]
    change = data["change"]
    source = data["source"]
    analysis = ask_claude(
        "Analisis del bono argentino " + symbol + " (4 oraciones, sin markdown): "
        "precio $" + fmt_price(price) + ", variacion " + "{:+.2f}".format(change) + "% hoy. "
        "Explica que es, su ley, vencimiento, y contexto de deuda soberana argentina.",
        max_tokens=400
    )
    lines = ["*Bono " + symbol + "* 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio:* $" + fmt_price(price) +
                 "  " + "{:+.2f}".format(change) + "%")
    lines.append("📡 _Fuente: " + source + "_")
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
        return "No encontre datos para *" + symbol + "*. Verifica el ticker (ej: GGAL, BMA, YPFD)."
    price  = data["price"]
    change = data["change"]
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
        "Analisis accion argentina " + symbol + " (4 oraciones, sin markdown): "
        "precio $" + fmt_price(price) + " ARS, variacion " + "{:+.2f}".format(change) + "% hoy. "
        + ("RSI=" + str(rsi) + " " if rsi else "")
        + "Explica que hace la empresa y variables macro que afectan su precio.",
        max_tokens=400
    )
    lines = ["*" + symbol + "* (Accion ARG) 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio:* $" + fmt_price(price) +
                 " ARS  " + "{:+.2f}".format(change) + "%")
    if rsi is not None:
        tag = " - Sobrecomprado" if rsi >= 70 else (" - Sobrevendido" if rsi <= 30 else "")
        lines.append("📉 *RSI 14:* " + str(rsi) + tag)
    if sma50 is not None:
        lines.append(arrow_emoji(dist_sma50) + " *Dist SMA 50:* " +
                     "{:+.2f}".format(dist_sma50) + "% (SMA: $" + fmt_price(sma50) + ")")
    if high_52w:
        lines.append("📈 *MAX 52W:* $" + fmt_price(high_52w))
        lines.append("📉 *MIN 52W:* $" + fmt_price(low_52w))
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
        return "No encontre datos para el CEDEAR *" + symbol + "*."
    price  = data["price"]
    change = data["change"]
    us_data = get_yahoo_data(symbol, "2d")
    us_price = us_change = None
    if us_data and len(us_data["closes"]) >= 2:
        us_price  = us_data["closes"][-1]
        us_change = ((us_data["closes"][-1] - us_data["closes"][-2]) / us_data["closes"][-2]) * 100
    analysis = ask_claude(
        "Analisis CEDEAR " + symbol + " en Argentina (3 oraciones, sin markdown): "
        "precio ARS $" + fmt_price(price) + " (" + "{:+.2f}".format(change) + "%). "
        + ("Subyacente USD $" + fmt_price(us_price) + ". " if us_price else "")
        + "Explica que es un CEDEAR y como el tipo de cambio afecta el precio.",
        max_tokens=350
    )
    lines = ["*CEDEAR " + symbol + "* 🇦🇷", ""]
    lines.append(arrow_emoji(change) + " *Precio ARS:* $" + fmt_price(price) +
                 "  " + "{:+.2f}".format(change) + "%")
    if us_price:
        lines.append(arrow_emoji(us_change) + " *Subyacente USD:* $" + fmt_price(us_price) +
                     "  " + "{:+.2f}".format(us_change) + "%")
    if analysis:
        lines.append("")
        lines.append("*Analisis*")
        lines.append(analysis)
    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

# ── Forex analyzer ───────────────────────────────────────────────────────────

def analyze_forex(symbol):
    """Full forex analysis with context and technicals."""
    pair_name = FOREX_NAMES.get(symbol, symbol)
    data = get_yahoo_data(symbol, "1y")
    if not data or len(data["closes"]) < 5:
        return "No encontre datos para *" + pair_name + "*. Intenta de nuevo."

    closes  = data["closes"]
    current = closes[-1]
    prev    = closes[-2] if len(closes) >= 2 else current
    day_chg = ((current - prev) / prev) * 100

    rsi = compute_rsi(closes) if len(closes) >= 16 else None

    sma50 = dist_sma50 = None
    if len(closes) >= 50:
        sma50      = round(float(np.mean(closes[-50:])), 2)
        dist_sma50 = pct_from_ma(current, sma50)

    ema200 = dist_ema200 = None
    if len(closes) >= 200:
        ema = closes[0]
        k   = 2 / 201
        for c in closes:
            ema = c * k + ema * (1 - k)
        ema200      = round(float(ema), 4)
        dist_ema200 = pct_from_ma(current, ema200)

    arr      = np.array(closes)
    high_52w = round(float(np.max(arr)), 4)
    low_52w  = round(float(np.min(arr)), 4)
    dist_52h = pct_from_ma(current, high_52w)
    dist_52l = pct_from_ma(current, low_52w)

    # Determine decimals based on pair
    dec = 2 if "JPY" in symbol or "CLP" in symbol or "COP" in symbol or "ARS" in symbol else 4

    tech = (
        "Par: " + pair_name + "\n"
        "Precio actual: " + fmt_price(current, dec) + " (" + "{:+.2f}".format(day_chg) + "% hoy)\n"
        + ("RSI 14: " + str(rsi) + "\n" if rsi else "")
        + ("Dist SMA50: " + "{:+.2f}".format(dist_sma50) + "%\n" if dist_sma50 is not None else "")
        + ("Dist EMA200: " + "{:+.2f}".format(dist_ema200) + "%\n" if dist_ema200 is not None else "")
        + "MAX 52W: " + fmt_price(high_52w, dec) + " (" + "{:+.2f}".format(dist_52h) + "%)\n"
        + "MIN 52W: " + fmt_price(low_52w, dec) + "\n"
    )

    analysis = ask_claude(
        "Sos analista de forex de ST Capital. Analiza el par " + pair_name + ".\n\n"
        "Dos parrafos separados por linea en blanco:\n\n"
        "PARRAFO 1 - CONTEXTO MACRO (3 oraciones): que bancos centrales influyen, "
        "que variables macro (tasas, inflacion, crecimiento, geopolitica) mueven este par hoy.\n\n"
        "PARRAFO 2 - ANALISIS TECNICO (3 oraciones): tendencia actual, niveles clave, "
        "que dicen RSI y medias moviles, conclusion del momento.\n\n"
        "Sin markdown, sin asteriscos, sin titulos.\n\n"
        "Datos:\n" + tech,
        max_tokens=600
    )

    lines = ["*" + pair_name + "* (Forex) 💱", ""]
    lines.append(arrow_emoji(day_chg) + " *Precio:* " + fmt_price(current, dec) +
                 "  " + "{:+.2f}".format(day_chg) + "%")
    lines.append("")
    lines.append("*Indicadores tecnicos*")
    if rsi is not None:
        tag = " - Sobrecomprado" if rsi >= 70 else (" - Sobrevendido" if rsi <= 30 else "")
        lines.append("📉 *RSI 14:* " + str(rsi) + tag)
    if sma50 is not None:
        lines.append(arrow_emoji(dist_sma50) + " *Dist SMA 50:* " +
                     "{:+.2f}".format(dist_sma50) + "% (SMA: " + fmt_price(sma50, dec) + ")")
    if ema200 is not None:
        lines.append(arrow_emoji(dist_ema200) + " *Dist EMA 200:* " +
                     "{:+.2f}".format(dist_ema200) + "% (EMA: " + fmt_price(ema200, dec) + ")")
    lines.append("")
    lines.append("*52W Range*")
    lines.append("📈 *MAX 52W:* " + fmt_price(high_52w, dec) +
                 "  (" + "{:+.2f}".format(dist_52h) + "% del maximo)")
    lines.append("📉 *MIN 52W:* " + fmt_price(low_52w, dec) +
                 "  (" + "{:+.2f}".format(dist_52l) + "% del minimo)")

    if analysis:
        parts = analysis.strip().split("\n\n", 1)
        lines.append("")
        lines.append("*Contexto macro*")
        lines.append(parts[0].strip())
        if len(parts) > 1:
            lines.append("")
            lines.append("*Analisis tecnico*")
            lines.append(parts[1].strip())

    lines.append("")
    lines.append("_ST Capital - No es asesoramiento financiero._")
    return "\n".join(lines)

# ── Global ticker analyzer ────────────────────────────────────────────────────

def analyze_ticker(symbol):
    symbol = symbol.upper().strip()
    symbol = TICKER_ALIASES.get(symbol, symbol)
    logger.info("Analyzing: " + symbol)

    # Route forex pairs
    if symbol in FOREX_TICKERS:
        return analyze_forex(symbol)

    ar_type = detect_ar_asset_type(symbol)
    if ar_type == "dolar_ar":   return analyze_ar_dolar()
    if ar_type == "bono":       return analyze_ar_bono(symbol)
    if ar_type == "on":         return analyze_ar_bono(symbol)
    if ar_type == "cedear":     return analyze_cedear(symbol)
    if ar_type == "accion_ar":  return analyze_ar_stock(symbol)
    # adr_usd falls through to global analysis

    data = get_yahoo_data(symbol, "1y")
    if not data or len(data["closes"]) < 5:
        data_ba = get_yahoo_data(symbol + ".BA", "1y")
        if data_ba and len(data_ba["closes"]) >= 5:
            return analyze_ar_stock(symbol)
        return ("No encontre datos para *" + symbol + "*.\n\n"
                "Activos argentinos:\n"
                "- Acciones: GGAL, BMA, YPFD, PAMP\n"
                "- Bonos: AL30, GD30, GD35, AE38\n"
                "- CEDEARs: AAPL, TSLA, GOOGL\n"
                "- Dolar: BLUE, MEP, CCL")

    closes  = data["closes"]
    volumes = data["volumes"]
    current = closes[-1]
    prev    = closes[-2] if len(closes) >= 2 else current
    day_chg = ((current - prev) / prev) * 100

    rsi      = compute_rsi(closes) if len(closes) >= 16 else None
    rs_score = None
    try:
        spy = get_yahoo_data("SPY", "1y")
        if spy and len(spy["closes"]) >= 10:
            rs_score = compute_rs_score(closes, spy["closes"])
    except Exception:
        pass

    ema200 = dist_ema200 = None
    if len(closes) >= 200:
        ema = closes[0]
        k   = 2 / 201
        for c in closes:
            ema = c * k + ema * (1 - k)
        ema200      = round(float(ema), 2)
        dist_ema200 = pct_from_ma(current, ema200)

    sma50 = dist_sma50 = None
    if len(closes) >= 50:
        sma50      = round(float(np.mean(closes[-50:])), 2)
        dist_sma50 = pct_from_ma(current, sma50)

    arr          = np.array(closes)
    high_52w     = round(float(np.max(arr)), 2)
    low_52w      = round(float(np.min(arr)), 2)
    dist_52h     = pct_from_ma(current, high_52w)
    dist_52l     = pct_from_ma(current, low_52w)

    vol_last  = int(volumes[-1]) if volumes else 0
    vol_avg20 = int(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_last
    vol_ratio = round(vol_last / vol_avg20, 2) if vol_avg20 > 0 else 1.0

    info     = get_ticker_info(symbol)
    name     = info.get("longName") or symbol
    sector   = info.get("sector", "")
    industry = info.get("industry", "")
    mktcap   = info.get("marketCap")

    ticker_news = fetch_news(name + " " + symbol, 3)
    news_text   = "\n".join(ticker_news) if ticker_news else "Sin noticias recientes."

    tech = (
        "Precio: $" + fmt_price(current) + " (" + "{:+.2f}".format(day_chg) + "% hoy)\n"
        + ("RSI 14: " + str(rsi) + "\n" if rsi else "")
        + ("RS vs SPY: " + "{:+.2f}".format(rs_score) + "%\n" if rs_score is not None else "")
        + ("Dist EMA200: " + "{:+.2f}".format(dist_ema200) + "%\n" if dist_ema200 is not None else "")
        + ("Dist SMA50: " + "{:+.2f}".format(dist_sma50) + "%\n" if dist_sma50 is not None else "")
        + "MAX 52W: $" + fmt_price(high_52w) + " (" + "{:+.2f}".format(dist_52h) + "%)\n"
        + "MIN 52W: $" + fmt_price(low_52w) + "\n"
        + "Vol ratio 20d: " + str(vol_ratio) + "x\n"
    )

    analysis = ask_claude(
        "Analisis de " + name + " (" + symbol + "). Dos parrafos separados por linea en blanco:\n\n"
        "PARRAFO 1 - CONTEXTO GLOBAL (3 oraciones): que es este activo, su rol mundial, "
        "variables macro que afectan su precio hoy.\n\n"
        "PARRAFO 2 - ANALISIS TECNICO (3 oraciones): tendencia, soportes/resistencias, "
        "RSI y medias moviles, conclusion del momento.\n\n"
        "Sin markdown, sin asteriscos, sin titulos.\n\n"
        "Datos:\n" + tech + "\nNoticias:\n" + news_text,
        max_tokens=600
    )

    lines = ["*" + name + "* (" + symbol + ")"]
    if sector:
        lines.append("_" + sector + " - " + industry + "_")
    lines.append("")
    lines.append(arrow_emoji(day_chg) + " *Price:* $" + fmt_price(current) +
                 "  " + "{:+.2f}".format(day_chg) + "%")
    if mktcap:
        lines.append("🏦 *Market Cap:* " + fmt_large(mktcap))
    lines.append("")
    lines.append("*Indicadores tecnicos*")
    if rsi is not None:
        tag = " - Sobrecomprado" if rsi >= 70 else (" - Sobrevendido" if rsi <= 30 else "")
        lines.append("📉 *RSI 14:* " + str(rsi) + tag)
    if rs_score is not None:
        tag = (" - Fuerte" if rs_score > 10 else (" - Debil" if rs_score < -10 else " - Neutral"))
        lines.append("⚡ *RS vs SPY:* " + "{:+.2f}".format(rs_score) + "%" + tag)
    if ema200 is not None:
        lines.append(arrow_emoji(dist_ema200) + " *Dist EMA 200:* " +
                     "{:+.2f}".format(dist_ema200) + "% (EMA: $" + fmt_price(ema200) + ")")
    if sma50 is not None:
        lines.append(arrow_emoji(dist_sma50) + " *Dist SMA 50:* " +
                     "{:+.2f}".format(dist_sma50) + "% (SMA: $" + fmt_price(sma50) + ")")
    lines.append("")
    lines.append("*52W Range*")
    lines.append("📈 *MAX 52W:* $" + fmt_price(high_52w) + "  (" + "{:+.2f}".format(dist_52h) + "% del maximo)")
    lines.append("📉 *MIN 52W:* $" + fmt_price(low_52w) + "  (" + "{:+.2f}".format(dist_52l) + "% del minimo)")
    lines.append("")
    lines.append("*Volumen*")
    ve = "🔥" if vol_ratio > 1.5 else ("📊" if vol_ratio >= 0.8 else "😴")
    lines.append(ve + " *Vol ultimo dia:* " + "{:,}".format(vol_last))
    lines.append("📊 *Vol promedio 20d:* " + "{:,}".format(vol_avg20) + " (ratio: " + str(vol_ratio) + "x)")

    if analysis:
        parts = analysis.strip().split("\n\n", 1)
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

# ── Scheduled messages ────────────────────────────────────────────────────────

def build_opening():
    now  = datetime.now(TZ).strftime("%A %d %b %Y")
    snap, indices, crypto, commodities, _, _ = build_market_snapshot()
    news = fetch_news("markets stocks economy", 4)
    analysis = ask_claude(
        "Analisis de apertura de mercados para ST Capital. Hoy " + now + ". "
        "4-5 oraciones en español sobre tendencias del dia y que monitorear. Sin markdown.\n\n"
        + snap + "\nNoticias:\n" + "\n".join(news[:4]),
        max_tokens=400
    )
    msg  = "🌅 *APERTURA - ST Capital*\n_" + now + "_\n"
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
    snap, indices, _, _, _, stocks = build_market_snapshot()
    news = fetch_news("stock market Wall Street", 3)
    analysis = ask_claude(
        "Update mid-morning ST Capital. 3 oraciones sobre la sesion. Sin markdown.\n\n" + snap,
        max_tokens=250)
    msg  = "📊 *MID-MORNING - ST Capital*\n"
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
    snap, _, crypto, commodities, forex, _ = build_market_snapshot()
    news = fetch_news("commodity oil gold forex macro", 3)
    analysis = ask_claude(
        "Mediodia ST Capital. 3 oraciones sobre commodities, forex y crypto. Sin markdown.\n\n" + snap,
        max_tokens=250)
    msg  = "🔴 *MEDIODIA - ST Capital*\n"
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("💱 *FOREX*", forex, decimals=4)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Analisis*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg

def build_preclose():
    snap, indices, _, _, _, stocks = build_market_snapshot()
    news = fetch_news("earnings stocks Wall Street close", 3)
    analysis = ask_claude(
        "Pre-cierre Wall Street ST Capital. 3 oraciones sobre como se perfila el cierre. Sin markdown.\n\n" + snap,
        max_tokens=250)
    msg  = "📈 *PRE-CIERRE USA - ST Capital*\n"
    msg += section("🏢 *US STOCKS*", stocks)
    msg += section("📊 *INDICES USA*", {k: v for k, v in indices.items() if k in ["S&P 500", "Nasdaq", "Dow Jones"]})
    if news:
        msg += "\n\n📰 *NOTICIAS*"
        for n in news[:3]: msg += "\n- " + n
    if analysis:
        msg += "\n\n*Analisis*\n" + analysis
    msg += "\n\n_ST Capital_"
    return msg

def build_close():
    snap, indices, crypto, commodities, _, stocks = build_market_snapshot()
    analysis = ask_claude(
        "Resumen cierre del dia ST Capital. 4-5 oraciones. Sin markdown.\n\n" + snap,
        max_tokens=400)
    msg  = "🌙 *CIERRE DEL DIA - ST Capital*\n"
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
    snap, indices, crypto, commodities, _, _ = build_market_snapshot()
    news_eco = fetch_news("economy markets Fed interest rates inflation", 4)
    news_geo = fetch_news("geopolitics war conflict military sanctions", 4)
    news_usa = fetch_news("Trump United States economy trade", 3)
    news_egy = fetch_news("oil gas energy OPEC Middle East", 3)

    all_news = (
        "MERCADOS: " + " | ".join(news_eco[:3]) + "\n"
        "GEOPOLITICA: " + " | ".join(news_geo[:3]) + "\n"
        "USA: " + " | ".join(news_usa[:2]) + "\n"
        "ENERGIA: " + " | ".join(news_egy[:2])
    )

    alert = ask_claude(
        "Sos analista de ST Capital. Son las " + now + " hora Argentina.\n\n"
        "Con las noticias y datos de mercado, redacta una ALERTA DE MERCADO en español "
        "con este formato exacto (texto plano, sin markdown):\n\n"
        "🚨 ALERTA DE MERCADO - " + now + "\n\n"
        "Que esta pasando:\n"
        "[2-3 oraciones sobre el evento mas relevante del momento con datos concretos]\n\n"
        "Impacto en mercados:\n"
        "[2-3 oraciones sobre como afecta indices, commodities, cripto, acciones especificas]\n\n"
        "Que hacer:\n"
        "[1-2 oraciones de perspectiva practica para el inversor]\n\n"
        "⚠️ Analisis informativo. No es asesoramiento financiero.\n\n"
        "Datos de mercado:\n" + snap + "\n"
        "Noticias:\n" + all_news,
        max_tokens=650
    )

    msg  = "🚨 *ALERTA DE MERCADO - " + now + "*\n_ST Capital_\n"
    msg += section("📊 *INDICES*", indices)
    msg += section("🛢 *COMMODITIES*", commodities)
    msg += section("🪙 *CRYPTO*", crypto, decimals=0)
    if alert:
        msg += "\n\n" + alert
    else:
        top = news_eco[:1] + news_geo[:1] + news_usa[:1]
        if top:
            msg += "\n\n📰 *Titulares*"
            for n in top: msg += "\n- " + n
    msg += "\n\n_ST Capital_"
    return msg

# ── Telegram handler ──────────────────────────────────────────────────────────

APP = None

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    import asyncio
    text = update.message.text.strip()

    if text.upper().lstrip("/") in ["START", "HELP", "AYUDA"]:
        await update.message.reply_text(
            "ST Capital Bot\n\n"
            "Escribime un ticker o pregunta:\n\n"
            "GLOBALES: AAPL, BTC-USD, GC=F, SPY, TSLA, NU, MU\n"
            "BONOS AR: AL30, GD30, GD35, AE38, AL35\n"
            "ACCIONES AR: GGAL, BMA, YPFD, PAMP\n"
            "CEDEARs: AAPL, TSLA, GOOGL\n"
            "DOLAR: BLUE, MEP, CCL\n"
            "COMMODITIES: BRENT, WTI, ORO, SOJA\n\n"
            "O preguntame: 'cuanto cotiza el BTC?'"
        )
        return

    if is_question(text):
        await update.message.reply_text("Analizando tu consulta...")
        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, answer_question, text),
                timeout=40.0
            )
            await update.message.reply_text(result or "No pude generar un analisis. Intenta de nuevo.")
        except asyncio.TimeoutError:
            await update.message.reply_text("La consulta tardo demasiado. Intenta de nuevo.")
        return

    symbol = text.upper().replace(" ", "").lstrip("/")
    symbol = TICKER_ALIASES.get(symbol, symbol)
    await update.message.reply_text("Analizando " + symbol + "...")
    try:
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, analyze_ticker, symbol),
            timeout=45.0
        )
        await update.message.reply_text(result, parse_mode="Markdown", disable_web_page_preview=True)
    except asyncio.TimeoutError:
        await update.message.reply_text("El analisis de " + symbol + " tardo demasiado. Intenta de nuevo.")
    except Exception as e:
        logger.error("handle_message error: " + str(e))
        await update.message.reply_text("Error analizando " + symbol + ". Intenta de nuevo.")

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
                    APP.bot.send_message(
                        chat_id=CHAT_ID, text=text,
                        parse_mode="Markdown", disable_web_page_preview=True
                    ),
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
    logger.info("ST Capital Bot v18 - running.")
    APP.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

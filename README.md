# ST Capital Bot 🤖

Bot de Telegram que envía 5 mensajes diarios con datos de mercado.

## Horarios (Argentina UTC-3)
- 🌅 9:00 AM — Apertura: índices globales + crypto + noticias
- 📊 11:00 AM — Mid-morning: índices + US stocks
- 🔴 1:00 PM — Mediodía: commodities + forex + crypto
- 📈 3:00 PM — Pre-cierre: US stocks + alertas
- 🌙 5:00 PM — Cierre: resumen completo del día

## Datos que incluye
- Índices: S&P 500, Nasdaq, Dow Jones, DAX, Nikkei, FTSE 100
- Crypto: BTC, ETH, SOL, XRP, BNB
- Commodities: Gold, Silver, Oil WTI, Natural Gas
- US Stocks: AAPL, NVDA, TSLA, MSFT, META, AMZN
- Forex: DXY, EUR/USD, USD/ARS
- Noticias del día (requiere NewsAPI key opcional)

---

## Deploy en Railway (paso a paso)

### 1. Subir a GitHub
1. Creá una cuenta en github.com si no tenés
2. Creá un repositorio nuevo (privado) llamado `stcapital-bot`
3. Subí los 3 archivos: `bot.py`, `requirements.txt`, `railway.toml`

### 2. Deploy en Railway
1. Entrá a railway.app y creá cuenta (gratis)
2. Click en "New Project" → "Deploy from GitHub repo"
3. Seleccioná tu repo `stcapital-bot`
4. Railway detecta automáticamente que es Python

### 3. Variables de entorno (IMPORTANTE)
En Railway, ir a tu proyecto → Variables → agregar:

| Variable | Valor |
|---|---|
| `TELEGRAM_TOKEN` | El token que te dio BotFather |
| `CHAT_ID` | 1181186154 |
| `NEWS_API_KEY` | (opcional) tu key de newsapi.org |

### 4. Deploy
Click en "Deploy" — Railway instala las dependencias y arranca el bot.
Vas a ver en los logs: `✅ ST Capital Bot running — 5 daily messages scheduled.`

---

## NewsAPI (opcional pero recomendado)
1. Registrate gratis en newsapi.org
2. Copiá tu API key
3. Agregala como variable `NEWS_API_KEY` en Railway
- Plan gratuito: 100 requests/día (suficiente para el bot)

---

## Fuentes de datos
- **Yahoo Finance** — índices, stocks, commodities, forex (gratis, sin key)
- **CoinGecko** — crypto (gratis, sin key)
- **NewsAPI** — noticias (gratis con registro)

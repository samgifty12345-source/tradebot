<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Icon's Trading Terminal</title>
<link rel="stylesheet" href="/static/style.css" />
</head>
<body>

<div id="login-screen" class="centered-screen">
  <div class="login-card">
    <h1>Trading Terminal</h1>
    <p class="subtitle">Log in with any MT4/MT5 account — demo, broker, or prop firm</p>
    <input id="login" placeholder="Login (account number)" />
    <input id="password" type="password" placeholder="Password" />
    <input id="server" placeholder="Server (e.g. FundingPips-Live)" />
    <select id="platform">
      <option value="mt5">MT5</option>
      <option value="mt4">MT4</option>
    </select>
    <button class="primary-btn" onclick="connectAccount()">Connect</button>
    <p id="login-status" class="status-text"></p>

    <div class="divider"><span>or</span></div>

    <button class="secondary-btn" onclick="useDemoAccount()">Use Built-in Demo Account</button>
    <p class="subtitle" style="margin-top:6px;">No MetaApi needed — $5,000 fake balance, real live prices, works exactly like the real thing</p>
  </div>
</div>

<div id="dashboard" style="display:none;">
  <header class="topbar">
    <span class="brand">Trading Terminal</span>
    <div class="topbar-actions">
      <button id="settings-btn" onclick="openSettings()">Settings</button>
      <button id="logout-btn" onclick="logout()">&larr; Log out</button>
    </div>
  </header>

  <div id="settings-overlay" class="settings-overlay" style="display:none;" onclick="if(event.target===this) closeSettings()">
    <div class="settings-panel">
      <div class="card-header-row">
        <h2>Settings</h2>
        <button class="icon-btn" onclick="closeSettings()">&times;</button>
      </div>
      <p class="subtitle">Preferred lot size per asset class. The Auto Trader sizes trades from these instead of one tiny fixed lot — set them to match your prop firm's leverage.</p>
      <label class="settings-label">Gold (XAUUSD) lot size<input id="settings-lot-gold" type="number" step="0.01" min="0.01" /></label>
      <label class="settings-label">BTC / Crypto lot size <span class="min-note">(min 0.10)</span><input id="settings-lot-btc" type="number" step="0.01" min="0.10" /></label>
      <label class="settings-label">Forex lot size<input id="settings-lot-forex" type="number" step="0.01" min="0.01" /></label>
      <label class="settings-label">Risk management notes (used by both AIs)<input id="settings-risk-notes" placeholder="e.g. never risk more than 1% per trade" /></label>
      <button class="primary-btn" onclick="saveSettings()">Save Settings</button>
      <p id="settings-status" class="status-text"></p>
    </div>
  </div>

  <div class="grid">
    <section class="card account-card">
      <div class="card-header-row">
        <h2>Account</h2>
        <button class="ghost-btn" onclick="resetSimAccount()">Reset Demo Balance</button>
      </div>
      <div id="account-info" class="account-stats">—</div>
    </section>

    <section class="card chart-card">
      <div class="card-header-row">
        <h2>Live Chart</h2>
        <div class="combo inline-combo" id="chart-symbol-combo">
          <input id="chart-symbol" class="inline-input" placeholder="Symbol" value="XAUUSD" autocomplete="off" />
          <div class="combo-list" id="chart-symbol-list"></div>
        </div>
      </div>
      <div class="timeframe-row">
        <button class="tf-btn active" data-tf="1min" onclick="changeTimeframe('1min', this)">M1</button>
        <button class="tf-btn" data-tf="5min" onclick="changeTimeframe('5min', this)">M5</button>
        <button class="tf-btn" data-tf="15min" onclick="changeTimeframe('15min', this)">M15</button>
        <button class="tf-btn" data-tf="1h" onclick="changeTimeframe('1h', this)">H1</button>
        <button class="tf-btn" data-tf="4h" onclick="changeTimeframe('4h', this)">H4</button>
        <button class="tf-btn" data-tf="1day" onclick="changeTimeframe('1day', this)">D1</button>
      </div>
      <div id="chart-container"></div>
      <p id="chart-status" class="status-text error-text"></p>
    </section>

    <section class="card trade-card">
      <h2>Place Trade</h2>
      <div class="trade-form">
        <div class="combo" id="symbol-combo">
          <input id="symbol" placeholder="Symbol" value="XAUUSD" autocomplete="off" oninput="updateTradePreview()" />
          <div class="combo-list" id="symbol-list"></div>
        </div>
        <input id="volume" placeholder="Volume (lots)" value="0.01" />
        <input id="sl" placeholder="Stop Loss (price)" />
        <input id="tp" placeholder="Take Profit (price)" />
      </div>
      <div id="trade-preview" class="trade-preview"></div>
      <div class="trade-buttons">
        <button class="buy-btn" onclick="trade('buy')">Buy</button>
        <button class="sell-btn" onclick="trade('sell')">Sell</button>
      </div>
      <p id="trade-status" class="status-text"></p>
    </section>

    <section class="card positions-card">
      <h2>Open Positions</h2>
      <div id="positions-total" class="positions-total"></div>
      <div id="positions" class="positions-list"></div>
    </section>

    <section class="card autotrade-card">
      <div class="card-header-row">
        <h2>AI Auto-Trade Log</h2>
        <div class="header-btn-group">
          <button id="run-scan-btn" class="primary-btn" onclick="runScanNow()">Run Scan Now</button>
          <button class="ghost-btn" onclick="clearAutotradeLog()">Clear</button>
        </div>
      </div>
      <p class="subtitle">Runs automatically in the background every 15 min — this just shows what it decided</p>
      <div id="autotrade-status" class="autotrade-status-badge" style="display:none;"></div>
      <div id="autotrade-log" class="autotrade-list"></div>
    </section>

    <section class="card tradelog-card">
      <div class="card-header-row">
        <div class="tradelog-toggle" onclick="toggleTradeLog()">
          <h2>Trade Log</h2>
          <span id="tradelog-chevron" class="chevron">&#9656;</span>
        </div>
        <button class="ghost-btn" onclick="clearTradeLog()">Clear</button>
      </div>
      <p class="subtitle">Every trade — manual, chat, or auto, real or demo — with entry, SL/TP, risk:reward, expected & actual P/L. Click a trade to open it on the chart. Click the title to expand/collapse.</p>
      <div id="trade-log" class="tradelog-list" style="display:none;"></div>
    </section>

    <section class="card chat-card">
      <h2>Talk to your AI</h2>
      <p class="subtitle">Tell it to watch a different pair, switch timeframe, or set your risk rules — it updates the auto-trader live</p>
      <div id="chat-messages" class="chat-messages"></div>
      <div class="chat-input-row">
        <select id="chat-ai-select" class="chat-ai-select">
          <option value="">Team (auto)</option>
          <option value="gemini">Gemini</option>
          <option value="groq">Groq</option>
          <option value="deepseek">DeepSeek</option>
        </select>
        <input id="chat-input" placeholder="e.g. Watch EURUSD on H1, risk max 1% per trade" onkeydown="if(event.key==='Enter') sendChat()" />
        <button class="primary-btn chat-send-btn" onclick="sendChat()">Send</button>
      </div>
    </section>
  </div>
</div>

<div id="ai-conversation-bar" class="ai-conversation-bar" style="display:none;" onclick="openCouncilModal()">
  <span class="ai-convo-label">Council chat:</span>
  <span id="ai-convo-text" class="ai-convo-text"></span>
  <span class="ai-convo-expand">Tap for full discussion &rsaquo;</span>
</div>

<div id="council-overlay" class="settings-overlay" style="display:none;" onclick="if(event.target===this) closeCouncilModal()">
  <div class="settings-panel council-panel">
    <div class="card-header-row">
      <h2>Council Discussion</h2>
      <button class="icon-btn" onclick="closeCouncilModal()">&times;</button>
    </div>
    <p class="subtitle">Gemini and Groq scan independently every cycle; DeepSeek reviews before a trade is allowed to fire. Newest first, live while open.</p>
    <div id="council-feed" class="council-feed"></div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script src="/static/app.js"></script>
</body>
</html>

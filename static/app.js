const API_BASE = ""; // same origin on Render

let chart = null;
let candleSeries = null;
let currentInterval = "1min";
let chartPollInterval = null;
let lastFormattedCandles = [];
let lastBoundaryMarkers = [];
let lastPositionMarkers = [];
let lastPositionsForMarkers = [];

// ---------- Searchable pair dropdown ----------

const ALL_PAIRS = ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD", "NZDUSD", "USDCHF", "EURJPY", "GBPJPY", "BTCUSD", "ETHUSD"];

function getPairUsage() {
  try {
    return JSON.parse(localStorage.getItem("pairUsage") || "{}");
  } catch (err) {
    return {};
  }
}

function recordPairUsage(symbol) {
  if (!symbol) return;
  symbol = symbol.toUpperCase().replace("/", "");
  const usage = getPairUsage();
  usage[symbol] = (usage[symbol] || 0) + 1;
  localStorage.setItem("pairUsage", JSON.stringify(usage));
}

function topPairs(limit = 6) {
  const usage = getPairUsage();
  return Object.keys(usage)
    .sort((a, b) => usage[b] - usage[a])
    .slice(0, limit);
}

function orderedPairList() {
  const top = topPairs(6);
  const rest = ALL_PAIRS.filter((p) => !top.includes(p));
  return [...top.filter((p) => ALL_PAIRS.includes(p) || true), ...rest];
}

function initCombo(inputId, listId, onSelect) {
  const input = document.getElementById(inputId);
  const list = document.getElementById(listId);
  if (!input || !list) return;

  function render(filterText) {
    const term = (filterText || "").toUpperCase().trim();
    const top = topPairs(6);
    const pairs = orderedPairList().filter((p) => !term || p.includes(term));
    list.innerHTML = "";

    if (top.length && !term) {
      const label = document.createElement("div");
      label.className = "combo-section-label";
      label.innerText = "Most used";
      list.appendChild(label);
    }

    if (pairs.length === 0) {
      list.innerHTML += `<div class="combo-empty">No matching pairs</div>`;
    }

    pairs.forEach((p, i) => {
      if (!term && i === top.length && top.length > 0) {
        const label = document.createElement("div");
        label.className = "combo-section-label";
        label.innerText = "All pairs";
        list.appendChild(label);
      }
      const row = document.createElement("div");
      row.className = "combo-item";
      row.innerText = p;
      row.onmousedown = (e) => {
        e.preventDefault();
        input.value = p;
        list.style.display = "none";
        if (onSelect) onSelect(p);
      };
      list.appendChild(row);
    });

    list.style.display = "block";
  }

  input.addEventListener("focus", () => render(input.value === input.dataset.defaultValue ? "" : input.value));
  input.addEventListener("input", () => render(input.value));
  input.addEventListener("blur", () => setTimeout(() => (list.style.display = "none"), 120));
}

let accountId = localStorage.getItem("accountId");
if (accountId) showDashboard();

async function connectAccount() {
  const login = document.getElementById("login").value;
  const password = document.getElementById("password").value;
  const server = document.getElementById("server").value;
  const platform = document.getElementById("platform").value;

  document.getElementById("login-status").innerText = "Connecting... (can take 30-60s first time)";

  try {
    const res = await fetch(`${API_BASE}/api/connect`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ login, password, server, platform }),
    });
    const data = await res.json();
    if (!res.ok) {
      document.getElementById("login-status").innerText = data.detail || "Connect failed";
      return;
    }
    accountId = data.accountId;
    localStorage.setItem("accountId", accountId);
    showDashboard();
  } catch (err) {
    document.getElementById("login-status").innerText = "Failed: " + err.message;
  }
}

function useDemoAccount() {
  accountId = "SIM";
  localStorage.setItem("accountId", "SIM");
  showDashboard();
}

function isSim() {
  return accountId === "SIM";
}

function logout(reason) {
  // pure local action — clears session and resets the UI even if
  // network calls elsewhere on the page are frozen/hanging
  localStorage.removeItem("accountId");
  accountId = null;
  document.getElementById("dashboard").style.display = "none";
  document.getElementById("login-screen").style.display = "flex";
  document.getElementById("login-status").innerText = reason || "";
}

function showDashboard() {
  document.getElementById("login-screen").style.display = "none";
  document.getElementById("dashboard").style.display = "block";
  refreshAccount();
  refreshPositions();
  refreshAutotradeLog();
  refreshTradeLog();
  loadChatHistory();
  loadSettingsIntoForm();
  startChart();
  initCombo("chart-symbol", "chart-symbol-list", (p) => { recordPairUsage(p); changeSymbol(); });
  initCombo("symbol", "symbol-list", (p) => { recordPairUsage(p); updateTradePreview(); });
  ["volume", "sl", "tp"].forEach((id) => {
    document.getElementById(id).addEventListener("input", debounce(updateTradePreview, 400));
  });
  updateTradePreview();
  setInterval(refreshAccount, 20000);
  setInterval(refreshPositions, 20000);
  setInterval(refreshAutotradeLog, 30000);
  setInterval(refreshTradeLog, 30000);
  refreshAutotradeStatus();
  setInterval(refreshAutotradeStatus, 5000);
  refreshAiConversation();
  setInterval(refreshAiConversation, 6000);
}

let councilModalOpen = false;
let councilPollTimer = null;
let lastCouncilItems = [];

async function refreshAiConversation() {
  try {
    const res = await fetch(`${API_BASE}/api/ai-conversation`);
    if (!res.ok) return;
    const items = await res.json();
    lastCouncilItems = items;
    const bar = document.getElementById("ai-conversation-bar");
    const textEl = document.getElementById("ai-convo-text");
    if (!items.length) { bar.style.display = "none"; return; }
    bar.style.display = "flex";
    textEl.innerText = items.map((i) => `${i.speaker}: ${i.text}`).join("   •   ");
    if (councilModalOpen) renderCouncilFeed(items);
  } catch (err) {
    // silent
  }
}

function openCouncilModal() {
  councilModalOpen = true;
  document.getElementById("council-overlay").style.display = "flex";
  renderCouncilFeed(lastCouncilItems);
  refreshAiConversation();
  if (councilPollTimer) clearInterval(councilPollTimer);
  councilPollTimer = setInterval(refreshAiConversation, 4000);
}

function closeCouncilModal() {
  councilModalOpen = false;
  document.getElementById("council-overlay").style.display = "none";
  if (councilPollTimer) { clearInterval(councilPollTimer); councilPollTimer = null; }
}

function renderCouncilFeed(items) {
  const feed = document.getElementById("council-feed");
  if (!feed) return;
  if (!items || !items.length) {
    feed.innerHTML = '<p class="subtitle">No discussion yet — run a scan to see the council talk it through.</p>';
    return;
  }
  feed.innerHTML = items.map((i) => {
    const speakerClass = "council-" + (i.speaker || "").toLowerCase().replace(/[^a-z]/g, "");
    const time = new Date(i.time).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    const safeText = (i.text || "").replace(/</g, "&lt;");
    return `<div class="council-msg ${speakerClass}">
      <div class="council-msg-top"><span class="council-speaker">${i.speaker}</span><span class="council-time">${time}</span></div>
      <div class="council-text">${safeText}</div>
    </div>`;
  }).join("");
}

async function runScanNow() {
  const btn = document.getElementById("run-scan-btn");
  btn.disabled = true;
  try {
    const res = await fetch(`${API_BASE}/api/autotrade-run`, { method: "POST" });
    await res.json();
    refreshAutotradeStatus();
  } catch (err) {
    btn.disabled = false;
  }
}

let lastAutotradeNote = null;

async function refreshAutotradeStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/autotrade-status`);
    if (!res.ok) return;
    const status = await res.json();
    const el = document.getElementById("autotrade-status");
    const btn = document.getElementById("run-scan-btn");
    btn.disabled = status.state === "analyzing";
    if (status.state === "analyzing") {
      lastAutotradeNote = null;
      el.style.display = "flex";
      el.classList.remove("done");
      el.innerHTML = `<span class="spinner"></span> ${status.note || "Analyzing..."}`;
    } else if (status.note && status.note !== lastAutotradeNote) {
      lastAutotradeNote = status.note;
      el.style.display = "flex";
      el.classList.add("done");
      el.innerHTML = `✅ ${status.note}`;
      setTimeout(() => { el.style.display = "none"; el.classList.remove("done"); }, 6000);
    }
  } catch (err) {
    // silent
  }
}

function debounce(fn, wait) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), wait);
  };
}

async function loadChatHistory() {
  try {
    const res = await fetch(`${API_BASE}/api/chat/history`);
    if (!res.ok) return;
    const history = await res.json();
    const el = document.getElementById("chat-messages");
    el.innerHTML = "";
    history.forEach((h) => appendChatBubble(h.role === "user" ? "user" : "ai", h.text));
  } catch (err) {
    // silent
  }
}

async function refreshAutotradeLog() {
  try {
    const res = await fetch(`${API_BASE}/api/autotrade/log`);
    if (!res.ok) return;
    const log = await res.json();
    const el = document.getElementById("autotrade-log");
    el.innerHTML = "";
    if (log.length === 0) {
      el.innerHTML = `<p class="empty-note">No AI checks yet — will appear here once the scheduler starts running</p>`;
      return;
    }
    log.forEach((entry, idx) => {
      const row = document.createElement("div");
      row.className = "autotrade-row autotrade-row-clickable";
      const time = new Date(entry.time).toLocaleString();
      const decision = entry.decision || {};
      const marketNote = entry.market ? (entry.market.is_open ? "Market: open" : "Market: CLOSED (not enforced yet)") : "";
      const newsNote = entry.news_check || "";
      let statusClass = "at-hold";
      let statusLabel = "HOLD";
      if (entry.status === "trade_placed") { statusClass = "at-trade"; statusLabel = decision.action ? decision.action.toUpperCase() : "TRADE"; }
      else if (entry.status === "error") { statusClass = "at-error"; statusLabel = "ERROR"; }
      else if (entry.status === "skipped") { statusClass = "at-skip"; statusLabel = "SKIPPED"; }

      const detailId = `at-detail-${idx}`;
      const scanned = decision.scanned || {};
      const scannedRows = Object.keys(scanned).length
        ? Object.entries(scanned).map(([sym, d]) => `<div class="at-detail-row"><strong>${sym}</strong>: ${JSON.stringify(d)}</div>`).join("")
        : "";
      const council = entry.council ? `<div class="at-detail-row"><strong>Council:</strong> lead ${entry.council.lead_action || ""} ${entry.council.lead_symbol || ""}, DeepSeek agree: ${entry.council.deepseek_agree === null ? "n/a" : entry.council.deepseek_agree}${entry.council.deepseek_note ? " — " + entry.council.deepseek_note : ""}</div>` : "";

      row.innerHTML = `
        <div class="at-top">
          <span class="at-badge ${statusClass}">${statusLabel}</span>
          <span class="at-time">${time}</span>
        </div>
        <div class="at-reason">${entry.reason || decision.reason || ""}</div>
        ${marketNote || newsNote ? `<div class="at-meta">${[marketNote, newsNote].filter(Boolean).join(" · ")}</div>` : ""}
        <div id="${detailId}" class="at-detail" style="display:none;">
          ${decision.action ? `<div class="at-detail-row"><strong>Decision:</strong> ${decision.action.toUpperCase()} ${decision.best_symbol || ""} — confidence ${decision.confidence ?? "?"}%</div>` : ""}
          ${decision.stop_loss ? `<div class="at-detail-row"><strong>SL:</strong> ${decision.stop_loss} &nbsp; <strong>TP:</strong> ${decision.take_profit ?? "-"}</div>` : ""}
          ${decision.reason ? `<div class="at-detail-row"><strong>Full reasoning:</strong> ${decision.reason}</div>` : ""}
          ${council}
          ${entry.hold_reason ? `<div class="at-detail-row"><strong>Hold reason:</strong> ${entry.hold_reason}</div>` : ""}
          ${scannedRows ? `<div class="at-detail-row"><strong>Per-pair scan:</strong></div>${scannedRows}` : ""}
        </div>
      `;
      row.onclick = () => {
        const d = document.getElementById(detailId);
        d.style.display = d.style.display === "none" ? "block" : "none";
      };
      el.appendChild(row);
    });
  } catch (err) {
    // silent — non-critical panel
  }
}

async function refreshAccount() {
  const url = isSim() ? `${API_BASE}/api/sim/account` : `${API_BASE}/api/account/${accountId}`;
  const res = await fetch(url);
  if (res.status === 404) { logout("Session expired — please log in again."); return; }
  if (!res.ok) return;
  const info = await res.json();
  document.getElementById("account-info").innerHTML = `
    <div><span class="stat-label">Balance${isSim() ? " (demo)" : ""}</span>${info.balance} ${info.currency}</div>
    <div><span class="stat-label">Equity</span>${info.equity} ${info.currency}</div>
  `;
}

function findNearestCandleTime(entryTimeSec) {
  if (!lastFormattedCandles.length) return null;
  let chosen = lastFormattedCandles[0].time;
  for (const c of lastFormattedCandles) {
    if (c.time <= entryTimeSec) chosen = c.time;
    else break;
  }
  return chosen;
}

function computeEntryMarkers(positions, chartSymbol) {
  if (!lastFormattedCandles.length) return [];
  const markers = [];
  positions.forEach((p) => {
    if (p.symbol !== chartSymbol) return;
    const openIso = p.open_time || p.time || p.openTime;
    if (!openIso) return;
    const entrySec = Math.floor(new Date(openIso).getTime() / 1000);
    if (isNaN(entrySec)) return;
    const snapped = findNearestCandleTime(entrySec);
    if (snapped === null) return;
    const side = (p.side || p.type || "").toLowerCase();
    const isBuy = side.includes("buy");
    markers.push({
      time: snapped,
      position: "inBar",
      color: isBuy ? "#2f6dff" : "#f5a623",
      shape: "circle",
      size: 1,
    });
  });
  return markers;
}

function applyChartMarkers() {
  if (!candleSeries) return;
  const merged = [...lastBoundaryMarkers, ...lastPositionMarkers].sort((a, b) => a.time - b.time);
  candleSeries.setMarkers(merged);
}

let activePriceLines = [];

function clearPositionLines() {
  if (!candleSeries) return;
  activePriceLines.forEach((line) => candleSeries.removePriceLine(line));
  activePriceLines = [];
}

function drawPositionLines(positions) {
  if (!candleSeries) return;
  clearPositionLines();
  const chartSymbol = (document.getElementById("chart-symbol").value || "").toUpperCase().replace("/", "");

  lastPositionsForMarkers = positions;
  lastPositionMarkers = computeEntryMarkers(positions, chartSymbol);
  applyChartMarkers();

  positions.forEach((p) => {
    if (p.symbol !== chartSymbol) return; // only draw lines for the symbol currently on screen
    const entry = p.entry_price ?? p.openPrice;
    const sl = p.sl ?? p.stopLoss;
    const tp = p.tp ?? p.takeProfit;

    if (entry) {
      activePriceLines.push(candleSeries.createPriceLine({
        price: entry, color: "#2f6dff", lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: "Entry",
      }));
    }
    if (sl) {
      activePriceLines.push(candleSeries.createPriceLine({
        price: sl, color: "#ef4655", lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: "SL",
      }));
    }
    if (tp) {
      activePriceLines.push(candleSeries.createPriceLine({
        price: tp, color: "#1fae6b", lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: "TP",
      }));
    }
  });
}

async function refreshPositions() {
  const url = isSim() ? `${API_BASE}/api/sim/positions` : `${API_BASE}/api/positions/${accountId}`;
  const res = await fetch(url);
  if (res.status === 404) { logout("Session expired — please log in again."); return; }
  if (!res.ok) return;
  const positions = await res.json();
  drawPositionLines(positions);
  const el = document.getElementById("positions");
  const totalEl = document.getElementById("positions-total");
  el.innerHTML = "";

  if (positions.length === 0) {
    totalEl.innerHTML = "";
    el.innerHTML = `<p class="empty-note">No open positions</p>`;
    return;
  }

  const totalPnl = positions.reduce((sum, p) => sum + (p.profit || 0), 0);
  const totalUp = totalPnl >= 0;
  totalEl.innerHTML = `
    <span class="stat-label">Total Floating P/L (${positions.length} open)</span>
    <span class="total-pnl ${totalUp ? "profit" : "loss"}">${totalUp ? "+" : ""}${totalPnl.toFixed(2)}</span>
  `;

  positions.forEach((p) => {
    const isProfit = p.profit >= 0;
    const row = document.createElement("div");
    row.className = "position-row position-row-clickable";
    row.title = `Click to view ${p.symbol} on the chart`;
    row.onclick = (e) => { if (e.target.tagName !== "BUTTON") openTradeInChart(p.symbol); };
    row.innerHTML = `
      <div>
        <strong>${p.symbol}</strong>
        <span class="pos-meta"> ${p.type} · ${p.volume} lots</span>
      </div>
      <div class="pos-profit ${isProfit ? "profit" : "loss"}">${isProfit ? "+" : ""}${p.profit.toFixed(2)}</div>
    `;
    const closeBtn = document.createElement("button");
    closeBtn.innerText = "Close";
    closeBtn.onclick = () => closePosition(p.id);
    row.appendChild(closeBtn);
    el.appendChild(row);
  });
}

async function trade(side) {
  const symbol = document.getElementById("symbol").value;
  const volume = document.getElementById("volume").value;
  const sl = document.getElementById("sl").value;
  const tp = document.getElementById("tp").value;

  document.getElementById("trade-status").innerText = "Placing order...";
  try {
    const url = isSim() ? `${API_BASE}/api/sim/trade` : `${API_BASE}/api/trade/${accountId}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol, side, volume, sl, tp }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Order failed");
    document.getElementById("trade-status").innerText = "Order placed.";
    recordPairUsage(symbol);
    refreshPositions();
    refreshAccount();
    refreshTradeLog();
  } catch (err) {
    document.getElementById("trade-status").innerText = "Failed: " + err.message;
  }
}

// ---------- Trade preview: real $ amounts before placing a trade ----------

async function updateTradePreview() {
  const previewEl = document.getElementById("trade-preview");
  const symbol = document.getElementById("symbol").value;
  const volume = document.getElementById("volume").value;
  const sl = document.getElementById("sl").value;
  const tp = document.getElementById("tp").value;
  if (!symbol || !volume) { previewEl.innerHTML = ""; return; }

  try {
    const params = new URLSearchParams({ symbol, side: "buy", volume });
    if (sl) params.set("sl", sl);
    if (tp) params.set("tp", tp);
    const res = await fetch(`${API_BASE}/api/trade-preview?${params.toString()}`);
    if (!res.ok) { previewEl.innerHTML = ""; return; }
    const data = await res.json();

    const parts = [];
    if (data.expected_profit !== null && data.expected_profit !== undefined) {
      parts.push(`<span class="preview-profit">Potential profit: +$${Math.abs(data.expected_profit).toFixed(2)}</span>`);
    }
    if (data.expected_loss !== null && data.expected_loss !== undefined) {
      parts.push(`<span class="preview-loss">Potential loss: -$${Math.abs(data.expected_loss).toFixed(2)}</span>`);
    }
    if (data.risk_reward !== null && data.risk_reward !== undefined) {
      parts.push(`<span class="preview-rr">Risk:Reward 1:${data.risk_reward}</span>`);
    }
    previewEl.innerHTML = parts.length
      ? parts.join(" &nbsp;·&nbsp; ")
      : `<span class="preview-hint">Add a Stop Loss / Take Profit to see potential $ profit and loss</span>`;
  } catch (err) {
    previewEl.innerHTML = "";
  }
}

// ---------- Settings page ----------

function openSettings() {
  document.getElementById("settings-overlay").style.display = "flex";
  loadSettingsIntoForm();
}

function closeSettings() {
  document.getElementById("settings-overlay").style.display = "none";
}

async function loadSettingsIntoForm() {
  try {
    const res = await fetch(`${API_BASE}/api/settings`);
    if (!res.ok) return;
    const s = await res.json();
    const lots = s.lot_sizes || {};
    document.getElementById("settings-lot-gold").value = lots.gold ?? 0.02;
    document.getElementById("settings-lot-btc").value = lots.btc ?? 0.10;
    document.getElementById("settings-lot-forex").value = lots.forex ?? 0.01;
    document.getElementById("settings-risk-notes").value = s.risk_notes || "";
  } catch (err) {
    // silent
  }
}

async function saveSettings() {
  const statusEl = document.getElementById("settings-status");
  statusEl.innerText = "Saving...";
  try {
    const payload = {
      lot_sizes: {
        gold: parseFloat(document.getElementById("settings-lot-gold").value) || 0.02,
        btc: parseFloat(document.getElementById("settings-lot-btc").value) || 0.10,
        forex: parseFloat(document.getElementById("settings-lot-forex").value) || 0.01,
      },
      risk_notes: document.getElementById("settings-risk-notes").value,
    };
    const res = await fetch(`${API_BASE}/api/settings`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error("Save failed");
    statusEl.innerText = "Saved — the Auto Trader will use these lot sizes.";
    setTimeout(() => (statusEl.innerText = ""), 2500);
  } catch (err) {
    statusEl.innerText = "Failed: " + err.message;
  }
}

async function resetSimAccount() {
  if (!confirm("Reset demo balance to $5,000 and clear all open demo positions?")) return;
  await fetch(`${API_BASE}/api/sim/reset`, { method: "POST" });
  refreshAccount();
  refreshPositions();
}

async function clearTradeLog() {
  if (!confirm("Clear the entire trade log? This can't be undone.")) return;
  await fetch(`${API_BASE}/api/trades`, { method: "DELETE" });
  refreshTradeLog();
}

async function clearAutotradeLog() {
  if (!confirm("Clear the entire auto-trade log? This can't be undone.")) return;
  await fetch(`${API_BASE}/api/autotrade/log`, { method: "DELETE" });
  refreshAutotradeLog();
}

function openTradeInChart(symbol) {
  document.getElementById("chart-symbol").value = symbol;
  loadChartHistory();
  document.getElementById("chart-container").scrollIntoView({ behavior: "smooth", block: "center" });
}

function toggleTradeLog() {
  const el = document.getElementById("trade-log");
  const chevron = document.getElementById("tradelog-chevron");
  const open = el.style.display !== "none";
  el.style.display = open ? "none" : "block";
  chevron.innerHTML = open ? "&#9656;" : "&#9662;";
}

async function refreshTradeLog() {
  try {
    const res = await fetch(`${API_BASE}/api/trades`);
    if (!res.ok) return;
    const trades = await res.json();
    const el = document.getElementById("trade-log");
    el.innerHTML = "";
    if (trades.length === 0) {
      el.innerHTML = `<p class="empty-note">No trades logged yet</p>`;
      return;
    }
    trades.slice(0, 30).forEach((t) => {
      const row = document.createElement("div");
      row.className = "tradelog-row tradelog-row-clickable";
      row.title = `Click to open ${t.symbol} on the chart`;
      row.onclick = () => openTradeInChart(t.symbol);
      const time = new Date(t.time).toLocaleString();
      const statusClass = t.status === "open" ? "tl-open" : "tl-closed";
      const pnlHtml = t.actual_pnl !== null && t.actual_pnl !== undefined
        ? `<span class="${t.actual_pnl >= 0 ? "profit" : "loss"}">${t.actual_pnl >= 0 ? "+" : ""}${t.actual_pnl.toFixed(2)} actual</span>`
        : `<span class="tl-pending">open</span>`;
      const expected = (t.expected_profit !== null || t.expected_loss !== null)
        ? `expected +$${t.expected_profit ?? "—"} / -$${Math.abs(t.expected_loss ?? 0)}`
        : "";
      row.innerHTML = `
        <div class="tl-top">
          <span class="tl-badge ${statusClass}">${t.side.toUpperCase()} ${t.symbol}</span>
          <span class="at-time">${time}</span>
        </div>
        <div class="tl-detail">${t.volume} lots @ ${t.entry} · SL ${t.sl ?? "—"} · TP ${t.tp ?? "—"}${t.risk_reward ? ` · R:R 1:${t.risk_reward}` : ""}</div>
        <div class="tl-detail">${expected} ${t.confidence !== null && t.confidence !== undefined ? `· ${t.confidence}% confidence` : ""} · via ${t.source}/${t.account}</div>
        <div class="tl-bottom">
          <span class="at-reason">${t.reason || ""}</span>
          ${pnlHtml}
        </div>
      `;
      el.appendChild(row);
    });
  } catch (err) {
    // silent — non-critical panel
  }
}

async function closePosition(positionId) {
  const url = isSim() ? `${API_BASE}/api/sim/close/${positionId}` : `${API_BASE}/api/close/${accountId}/${positionId}`;
  await fetch(url, { method: "POST" });
  refreshPositions();
  refreshAccount();
}

// ---------- Chat with AI ----------

function appendChatBubble(role, text) {
  const el = document.getElementById("chat-messages");
  const bubble = document.createElement("div");
  bubble.className = `chat-bubble ${role === "user" ? "chat-user" : "chat-ai"}`;
  bubble.innerText = text;
  el.appendChild(bubble);
  el.scrollTop = el.scrollHeight;
}

async function sendChat() {
  const input = document.getElementById("chat-input");
  const message = input.value.trim();
  if (!message) return;
  const ai = document.getElementById("chat-ai-select").value; // "" = team/auto, or "gemini"/"groq"/"deepseek"
  input.value = "";
  appendChatBubble("user", ai ? `[${ai}] ${message}` : message);

  const thinking = document.createElement("div");
  thinking.className = "chat-bubble chat-ai chat-thinking";
  thinking.innerText = "...";
  document.getElementById("chat-messages").appendChild(thinking);

  try {
    const res = await fetch(`${API_BASE}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, ai: ai || null }),
    });
    const data = await res.json();
    thinking.remove();
    if (!res.ok) {
      appendChatBubble("ai", data.detail || "Something went wrong");
      return;
    }
    appendChatBubble("ai", data.reply);
  } catch (err) {
    thinking.remove();
    appendChatBubble("ai", "Failed: " + err.message);
  }
}

function currentSymbol() {
  const raw = (document.getElementById("chart-symbol").value || "XAUUSD").toUpperCase().replace("/", "");
  return raw.length === 6 ? `${raw.slice(0, 3)}/${raw.slice(3)}` : raw; // XAUUSD -> XAU/USD
}

function initChartIfNeeded() {
  if (chart) return;
  const container = document.getElementById("chart-container");
  chart = LightweightCharts.createChart(container, {
    layout: { background: { color: "#0d1117" }, textColor: "#7a8494" },
    grid: {
      vertLines: { color: "#1a2029" },
      horzLines: { color: "#1a2029" },
    },
    timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#232a35" },
    rightPriceScale: { borderColor: "#232a35" },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    autoSize: true,
  });
  candleSeries = chart.addCandlestickSeries({
    upColor: "#1fae6b",
    downColor: "#ef4655",
    borderVisible: false,
    wickUpColor: "#1fae6b",
    wickDownColor: "#ef4655",
  });
}

function computeBoundaryMarkers(formatted, interval) {
  // The free chart library doesn't support true vertical divider lines,
  // so this places a small labeled marker at each day/week/month boundary instead.
  const markers = [];
  let lastKey = null;
  formatted.forEach((c) => {
    const d = new Date(c.time * 1000);
    let key, text;
    if (interval === "4h") {
      const day = d.getUTCDay();
      const diffToMonday = (day === 0 ? -6 : 1) - day;
      const monday = new Date(d);
      monday.setUTCDate(d.getUTCDate() + diffToMonday);
      key = monday.toISOString().slice(0, 10);
      text = "Week";
    } else if (interval === "1day") {
      key = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
      text = d.toLocaleDateString(undefined, { month: "short" });
    } else {
      key = d.toISOString().slice(0, 10);
      text = d.toLocaleDateString(undefined, { weekday: "short" });
    }
    if (key !== lastKey) {
      markers.push({ time: c.time, position: "aboveBar", color: "#7a8494", shape: "circle", text });
      lastKey = key;
    }
  });
  return markers;
}

function toChartTime(datetimeStr) {
  // Twelve Data returns "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" for daily
  return Math.floor(new Date(datetimeStr.replace(" ", "T") + "Z").getTime() / 1000);
}

async function loadChartHistory() {
  const statusEl = document.getElementById("chart-status");

  if (typeof LightweightCharts === "undefined") {
    statusEl.style.color = "#ef4655";
    statusEl.innerText = "Chart library failed to load (network/ad-blocker may be blocking the CDN script). Try disabling ad blockers for this site, or a different network.";
    return;
  }

  initChartIfNeeded();
  statusEl.style.color = "#7a8494";
  statusEl.innerText = "Loading chart...";
  try {
    const res = await fetch(
      `${API_BASE}/api/chart?symbol=${encodeURIComponent(currentSymbol())}&interval=${currentInterval}&outputsize=300`
    );
    const data = await res.json();
    if (!res.ok) {
      statusEl.style.color = "#ef4655";
      statusEl.innerText = data.detail || "Chart failed to load";
      return;
    }
    if (!data.candles || data.candles.length === 0) {
      statusEl.style.color = "#ef4655";
      statusEl.innerText = "No data returned for this symbol/timeframe";
      return;
    }
    statusEl.innerText = "";
    const formatted = data.candles.map((c) => ({
      time: toChartTime(c.time),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));
    candleSeries.priceScale().applyOptions({ autoScale: true });
    candleSeries.setData(formatted);
    lastFormattedCandles = formatted;
    lastBoundaryMarkers = computeBoundaryMarkers(formatted, currentInterval);
    lastPositionMarkers = computeEntryMarkers(lastPositionsForMarkers, (document.getElementById("chart-symbol").value || "").toUpperCase().replace("/", ""));
    applyChartMarkers();
    chart.timeScale().fitContent();
  } catch (err) {
    statusEl.style.color = "#ef4655";
    statusEl.innerText = "Chart failed to load: " + err.message;
  }
}

async function pollLatestCandle() {
  if (!candleSeries) return;
  try {
    const res = await fetch(
      `${API_BASE}/api/chart?symbol=${encodeURIComponent(currentSymbol())}&interval=${currentInterval}&outputsize=2`
    );
    if (!res.ok) return;
    const data = await res.json();
    const latest = data.candles[data.candles.length - 1];
    if (!latest) return;
    // update() moves/replaces the last bar or appends a new one without
    // resetting the user's current zoom/scroll position
    candleSeries.update({
      time: toChartTime(latest.time),
      open: latest.open,
      high: latest.high,
      low: latest.low,
      close: latest.close,
    });
  } catch (err) {
    // silent — next poll will retry
  }
}

function changeTimeframe(interval, btnEl) {
  currentInterval = interval;
  document.querySelectorAll(".tf-btn").forEach((b) => b.classList.remove("active"));
  btnEl.classList.add("active");
  loadChartHistory();
}

function changeSymbol() {
  loadChartHistory();
}

function changeTradeSymbol() {
  updateTradePreview();
}

function startChart() {
  loadChartHistory();
  if (chartPollInterval) clearInterval(chartPollInterval);
  chartPollInterval = setInterval(pollLatestCandle, 60000); // stay under free-tier rate limits
}

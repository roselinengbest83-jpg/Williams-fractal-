async function loadStatus() {
  try {
    const response = await fetch("/api/status");

    if (!response.ok) {
      throw new Error("Server response failed");
    }

    const data = await response.json();

    updateSystemStatus(data);
    updateDashboard(data);

  } catch (error) {
    console.error("Dashboard error:", error);

    const status = document.getElementById("systemStatus");
    const dot = document.getElementById("systemDot");

    if (status) {
      status.textContent = "Offline";
    }

    if (dot) {
      dot.style.background = "#ff4d67";
    }
  }
}


function updateSystemStatus(data) {
  const status = document.getElementById("systemStatus");
  const dot = document.getElementById("systemDot");

  if (!status || !dot) return;

  if (data && data.running !== false) {
    status.textContent = "Scanner Online";
    dot.style.background = "#45e08a";
  } else {
    status.textContent = "Scanner Offline";
    dot.style.background = "#ff4d67";
  }
}


function updateDashboard(data) {
  const pairsGrid = document.getElementById("pairsGrid");
  const lastScan = document.getElementById("lastScan");
  const signalCount = document.getElementById("signalCount");

  if (!pairsGrid) return;

  const pairs = data.pairs || {};
  const pairNames = Object.keys(pairs);

  if (pairNames.length === 0) {
    pairsGrid.innerHTML = `
      <div class="loading">
        Waiting for scanner data...
      </div>
    `;
    return;
  }

  let signals = 0;

  pairsGrid.innerHTML = pairNames.map(pair => {

    const item = pairs[pair] || {};

    const signal = String(item.signal || "WAIT").toUpperCase();

    if (signal === "BUY" || signal === "SELL") {
      signals++;
    }

    const signalClass =
      signal === "BUY"
        ? "buy"
        : signal === "SELL"
        ? "sell"
        : "wait";

    return `
      <div class="pair-card">

        <div class="pair-header">
          <div class="pair-name">${pair}</div>

          <div class="signal ${signalClass}">
            ${signal}
          </div>
        </div>

        <div class="metrics">

          <div class="metric">
            <span>12H Direction</span>
            <strong>${formatDirection(item.direction12H)}</strong>
          </div>

          <div class="metric">
            <span>1H Structure</span>
            <strong>${formatDirection(item.structure1H)}</strong>
          </div>

          <div class="metric">
            <span>15M Trend</span>
            <strong>${formatDirection(item.trend15M)}</strong>
          </div>

          <div class="metric">
            <span>15M Line Break</span>
            <strong>${formatBoolean(item.lineBreak)}</strong>
          </div>

          <div class="metric">
            <span>Retest / Rejection</span>
            <strong>${formatBoolean(item.retest)}</strong>
          </div>

          <div class="metric">
            <span>5M Confirmation</span>
            <strong>${formatBoolean(item.confirm5M)}</strong>
          </div>

        </div>

        <div class="price-area">

          <div>
            <span>Entry</span>
            <strong>${formatPrice(item.entry)}</strong>
          </div>

          <div>
            <span>Stop Loss</span>
            <strong>${formatPrice(item.sl)}</strong>
          </div>

          <div>
            <span>Take Profit</span>
            <strong>${formatPrice(item.tp)}</strong>
          </div>

          <div>
            <span>R:R</span>
            <strong>1 : 2</strong>
          </div>

        </div>

      </div>
    `;

  }).join("");

  if (lastScan && data.lastScan) {
    lastScan.textContent = new Date(data.lastScan)
      .toLocaleTimeString();
  }

  if (signalCount) {
    signalCount.textContent =
      `${signals} signal${signals === 1 ? "" : "s"}`;
  }
}


function formatDirection(value) {

  if (value === 1 || value === "1") {
    return "BULLISH";
  }

  if (value === -1 || value === "-1") {
    return "BEARISH";
  }

  return "WAIT";
}


function formatBoolean(value) {

  if (value === true) {
    return "YES";
  }

  if (value === false) {
    return "NO";
  }

  return "--";
}


function formatPrice(value) {

  if (value === undefined || value === null) {
    return "--";
  }

  const number = Number(value);

  if (Number.isNaN(number)) {
    return "--";
  }

  return number.toFixed(3);
}


// Load immediately
loadStatus();

// Refresh dashboard every 10 seconds
setInterval(loadStatus, 10000);

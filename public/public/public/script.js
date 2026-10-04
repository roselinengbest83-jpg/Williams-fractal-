async function loadDashboard() {
  try {
    const response = await fetch("/api/status");
    const data = await response.json();

    updateSystem(data);
    updatePairs(data);
  } catch (error) {
    console.error("Dashboard error:", error);
    document.getElementById("systemStatus").textContent =
      "Connection Error";
  }
}

function updateSystem(data) {
  const dot = document.getElementById("systemDot");
  const status = document.getElementById("systemStatus");
  const lastScan = document.getElementById("lastScan");
  const pairCount = document.getElementById("pairCount");

  if (data.system?.derivConnected) {
    dot.className = "dot online";
    status.textContent = data.system.marketClosed
      ? "Market Closed"
      : "Deriv Connected";
  } else {
    dot.className = "dot offline";
    status.textContent = "Connecting...";
  }

  pairCount.textContent =
    data.system?.pairCount || 0;

  if (data.system?.lastScan) {
    lastScan.textContent =
      new Date(
        data.system.lastScan
      ).toLocaleTimeString();
  } else {
    lastScan.textContent = "--";
  }
}

function updatePairs(data) {
  const grid =
    document.getElementById("pairsGrid");

  const signalCount =
    document.getElementById("signalCount");

  if (!grid) return;

  const pairs = data.pairs || {};

  const pairNames = Object.keys(pairs);

  let signals = 0;

  grid.innerHTML = "";

  if (pairNames.length === 0) {
    grid.innerHTML = `
      <div class="loading">
        Waiting for forex pairs...
      </div>
    `;

    return;
  }

  pairNames.forEach(pair => {
    const item = pairs[pair];

    if (
      item.status === "BUY SIGNAL" ||
      item.status === "SELL SIGNAL"
    ) {
      signals++;
    }

    const directionClass =
      item.direction12H === "BULLISH"
        ? "bullish"
        : item.direction12H === "BEARISH"
        ? "bearish"
        : "";

    const statusClass =
      item.status === "BUY SIGNAL"
        ? "buy"
        : item.status === "SELL SIGNAL"
        ? "sell"
        : item.status === "MARKET CLOSED"
        ? "closed"
        : "";

    const card =
      document.createElement("div");

    card.className = "pair-card";

    card.innerHTML = `
      <div class="pair-header">
        <div>
          <h3>${pair}</h3>
          <span class="pair-status ${statusClass}">
            ${item.status || "WAIT"}
          </span>
        </div>

        <div class="direction ${directionClass}">
          ${item.direction12H || "WAIT"}
        </div>
      </div>

      <div class="strategy-grid">

        <div class="strategy-item">
          <span>12H</span>
          <strong>
            ${item.direction12H || "WAIT"}
          </strong>
        </div>

        <div class="strategy-item">
          <span>1H</span>
          <strong>
            ${item.structure1H || "WAIT"}
          </strong>
        </div>

        <div class="strategy-item">
          <span>15M Trend</span>
          <strong>
            ${item.trend15M || "WAIT"}
          </strong>
        </div>

        <div class="strategy-item">
          <span>15M Break</span>
          <strong>
            ${item.break15M || "WAIT"}
          </strong>
        </div>

        <div class="strategy-item">
          <span>Retest</span>
          <strong>
            ${item.retest15M || "WAIT"}
          </strong>
        </div>

        <div class="strategy-item">
          <span>5M</span>
          <strong>
            ${item.confirm5M || "WAIT"}
          </strong>
        </div>

      </div>

      ${
        item.signal
          ? `
            <div class="signal-box">
              <div>
                <span>Entry</span>
                <strong>${item.entry}</strong>
              </div>

              <div>
                <span>Stop Loss</span>
                <strong>${item.stopLoss}</strong>
              </div>

              <div>
                <span>Take Profit</span>
                <strong>${item.takeProfit}</strong>
              </div>

              <div>
                <span>R:R</span>
                <strong>1:${item.riskReward}</strong>
              </div>
            </div>
          `
          : ""
      }

      ${
        item.error
          ? `
            <div class="error-message">
              ${item.error}
            </div>
          `
          : ""
      }

      <div class="pair-footer">
        Last scan:
        ${
          item.lastScan
            ? new Date(
                item.lastScan
              ).toLocaleTimeString()
            : "--"
        }
      </div>
    `;

    grid.appendChild(card);
  });

  signalCount.textContent =
    `${signals} signal${signals === 1 ? "" : "s"}`;
}

/*
========================================================
AUTO REFRESH
========================================================
*/

loadDashboard();

setInterval(
  loadDashboard,
  5000
);

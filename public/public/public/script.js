/* =========================================
   FRACTAL FOREX SCANNER
   DASHBOARD SCRIPT
========================================= */

let lastData = null;


/* =========================================
   LOAD DASHBOARD
========================================= */

async function loadDashboard() {

  try {

    const response =
      await fetch(
        "/api/status",
        {
          cache: "no-store"
        }
      );

    if (!response.ok) {
      throw new Error(
        `HTTP ${response.status}`
      );
    }

    const data =
      await response.json();

    lastData = data;

    updateSystem(data);

    updatePairs(data);

  } catch (error) {

    console.error(
      "[DASHBOARD]",
      error
    );

    showConnectionError();

  }

}


/* =========================================
   SYSTEM STATUS
========================================= */

function updateSystem(data) {

  const dot =
    document.getElementById(
      "systemDot"
    );

  const status =
    document.getElementById(
      "systemStatus"
    );

  const pairCount =
    document.getElementById(
      "pairCount"
    );

  const lastScan =
    document.getElementById(
      "lastScan"
    );

  const scannerBadge =
    document.getElementById(
      "scannerBadge"
    );


  const system =
    data.system || {};


  /* PAIR COUNT */

  const pairs =
    data.pairs || {};

  pairCount.textContent =
    Object.keys(pairs).length ||
    system.pairCount ||
    0;


  /* CONNECTION */

  if (
    system.derivConnected
  ) {

    dot.className =
      "status-dot online";

    if (
      system.marketClosed
    ) {

      status.textContent =
        "Deriv Connected • Market Closed";

    } else {

      status.textContent =
        "Deriv Connected";

    }

  } else {

    dot.className =
      "status-dot offline";

    status.textContent =
      "Connecting to Deriv...";

  }


  /* LAST SCAN */

  if (
    system.lastScan
  ) {

    lastScan.textContent =
      formatTime(
        system.lastScan
      );

  } else {

    lastScan.textContent =
      "--";

  }


  /* SCANNER BADGE */

  if (
    system.marketClosed
  ) {

    scannerBadge.textContent =
      "MARKET CLOSED";

    scannerBadge.style.color =
      "#aab2bf";

    scannerBadge.style.background =
      "#202631";

    scannerBadge.style.borderColor =
      "#333b49";

  } else if (
    system.scannerRunning
  ) {

    scannerBadge.textContent =
      "SCANNING";

    scannerBadge.style.color =
      "#36e39b";

    scannerBadge.style.background =
      "#0d211c";

    scannerBadge.style.borderColor =
      "#1a4b3e";

  } else {

    scannerBadge.textContent =
      "READY";

    scannerBadge.style.color =
      "#8fa0b8";

    scannerBadge.style.background =
      "#121a28";

    scannerBadge.style.borderColor =
      "#26334a";

  }

}


/* =========================================
   UPDATE PAIRS
========================================= */

function updatePairs(data) {

  const grid =
    document.getElementById(
      "pairsGrid"
    );

  const signalCount =
    document.getElementById(
      "signalCount"
    );


  if (!grid) {
    return;
  }


  const pairs =
    data.pairs || {};

  const pairNames =
    Object.keys(pairs);


  let signals = 0;


  /* NO PAIRS */

  if (
    pairNames.length === 0
  ) {

    grid.innerHTML = `

      <div class="loading-card">

        <div class="loading-spinner"></div>

        <strong>
          Waiting for forex pairs...
        </strong>

        <span>
          Deriv symbols are being loaded
        </span>

      </div>

    `;

    signalCount.textContent =
      "0";

    return;

  }


  /* BUILD CARDS */

  grid.innerHTML = "";


  pairNames.forEach(
    pair => {

      const item =
        pairs[pair] || {};


      if (
        item.status ===
          "BUY SIGNAL" ||

        item.status ===
          "SELL SIGNAL"
      ) {

        signals++;

      }


      const card =
        document.createElement(
          "div"
        );


      card.className =
        "pair-card";


      /* =========================
         DIRECTION CLASS
      ========================== */

      let directionClass = "";

      if (
        item.direction12H ===
        "BULLISH"
      ) {

        directionClass =
          "bullish";

      } else if (
        item.direction12H ===
        "BEARISH"
      ) {

        directionClass =
          "bearish";

      }


      /* =========================
         STATUS CLASS
      ========================== */

      let statusClass = "";

      if (
        item.status ===
        "BUY SIGNAL"
      ) {

        statusClass =
          "buy";

      } else if (
        item.status ===
        "SELL SIGNAL"
      ) {

        statusClass =
          "sell";

      } else if (
        item.status ===
        "MARKET CLOSED"
      ) {

        statusClass =
          "closed";

      } else if (
        item.status ===
        "ERROR"
      ) {

        statusClass =
          "error";

      }


      const directionText =
        item.direction12H ||
        "WAIT";


      /* =========================
         CARD HTML
      ========================== */

      card.innerHTML = `

        <div class="pair-header">

          <div>

            <div class="pair-name">
              ${escapeHtml(pair)}
            </div>

            <div class="pair-subtitle">
              Deriv Forex
            </div>

            <span
              class="pair-status ${statusClass}"
            >
              ${escapeHtml(
                item.status ||
                "WAIT"
              )}
            </span>

          </div>


          <div
            class="direction ${directionClass}"
          >
            ${escapeHtml(
              directionText
            )}
          </div>

        </div>


        <!-- STRATEGY CHECKS -->

        <div class="strategy-grid">


          <div class="strategy-item">

            <span>
              12H Direction
            </span>

            <strong
              class="${getValueClass(
                item.direction12H
              )}"
            >
              ${escapeHtml(
                item.direction12H ||
                "WAIT"
              )}
            </strong>

          </div>


          <div class="strategy-item">

            <span>
              1H Structure
            </span>

            <strong
              class="${getValueClass(
                item.structure1H
              )}"
            >
              ${escapeHtml(
                item.structure1H ||
                "WAIT"
              )}
            </strong>

          </div>


          <div class="strategy-item">

            <span>
              15M Trend
            </span>

            <strong
              class="${getValueClass(
                item.trend15M
              )}"
            >
              ${escapeHtml(
                item.trend15M ||
                "WAIT"
              )}
            </strong>

          </div>


          <div class="strategy-item">

            <span>
              15M Break
            </span>

            <strong
              class="${getValueClass(
                item.break15M
              )}"
            >
              ${escapeHtml(
                item.break15M ||
                "WAIT"
              )}
            </strong>

          </div>


          <div class="strategy-item">

            <span>
              Retest
            </span>

            <strong
              class="${getValueClass(
                item.retest15M
              )}"
            >
              ${escapeHtml(
                item.retest15M ||
                "WAIT"
              )}
            </strong>

          </div>


          <div class="strategy-item">

            <span>
              5M Confirmation
            </span>

            <strong
              class="${getValueClass(
                item.confirm5M
              )}"
            >
              ${escapeHtml(
                item.confirm5M ||
                "WAIT"
              )}
            </strong>

          </div>


        </div>


        ${
          item.signal
            ? `

              <div class="signal-box">

                <div class="signal-value">

                  <span>
                    ENTRY
                  </span>

                  <strong>
                    ${formatPrice(
                      item.entry
                    )}
                  </strong>

                </div>


                <div class="signal-value">

                  <span>
                    STOP LOSS
                  </span>

                  <strong>
                    ${formatPrice(
                      item.stopLoss
                    )}
                  </strong>

                </div>


                <div class="signal-value">

                  <span>
                    TAKE PROFIT
                  </span>

                  <strong>
                    ${formatPrice(
                      item.takeProfit
                    )}
                  </strong>

                </div>


                <div class="signal-value">

                  <span>
                    R:R
                  </span>

                  <strong>
                    1:${escapeHtml(
                      item.riskReward ||
                      2
                    )}
                  </strong>

                </div>

              </div>

            `
            : ""
        }


        ${
          item.error
            ? `

              <div class="error-message">

                ${escapeHtml(
                  item.error
                )}

              </div>

            `
            : ""
        }


        <div class="pair-footer">

          <span>
            Last scan
          </span>

          <span>
            ${
              item.lastScan
                ? formatTime(
                    item.lastScan
                  )
                : "--"
            }
          </span>

        </div>

      `;


      grid.appendChild(
        card
      );

    }
  );


  signalCount.textContent =
    signals;


}


/* =========================================
   VALUE COLORS
========================================= */

function getValueClass(
  value
) {

  if (
    value ===
    "BULLISH"
  ) {

    return "value-bullish";

  }

  if (
    value ===
    "BEARISH"
  ) {

    return "value-bearish";

  }

  if (
    value ===
    "CONFIRMED"
  ) {

    return "value-confirmed";

  }

  return "";

}


/* =========================================
   FORMAT TIME
========================================= */

function formatTime(
  value
) {

  try {

    return new Date(
      value
    ).toLocaleTimeString(
      [],
      {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      }
    );

  } catch {

    return "--";

  }

}


/* =========================================
   FORMAT PRICE
========================================= */

function formatPrice(
  value
) {

  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {

    return "--";

  }

  const number =
    Number(value);


  if (
    Number.isNaN(number)
  ) {

    return String(value);

  }


  /*
    Forex prices normally need
    3–5 decimal places.

    This keeps the dashboard
    readable without changing
    the actual backend value.
  */

  return number.toFixed(
    number >= 100
      ? 3
      : 5
  );

}


/* =========================================
   ESCAPE HTML
========================================= */

function escapeHtml(
  value
) {

  return String(
    value ?? ""
  )

    .replace(
      /&/g,
      "&amp;"
    )

    .replace(
      /</g,
      "&lt;"
    )

    .replace(
      />/g,
      "&gt;"
    )

    .replace(
      /"/g,
      "&quot;"
    )

    .replace(
      /'/g,
      "&#039;"
    );

}


/* =========================================
   CONNECTION ERROR
========================================= */

function showConnectionError() {

  const dot =
    document.getElementById(
      "systemDot"
    );

  const status =
    document.getElementById(
      "systemStatus"
    );

  const badge =
    document.getElementById(
      "scannerBadge"
    );


  if (dot) {

    dot.className =
      "status-dot offline";

  }


  if (status) {

    status.textContent =
      "Dashboard Connection Error";

  }


  if (badge) {

    badge.textContent =
      "OFFLINE";

    badge.style.color =
      "#ff6877";

    badge.style.background =
      "#2b1218";

    badge.style.borderColor =
      "#55202a";

  }

}


/* =========================================
   START
========================================= */

loadDashboard();


/*
  Refresh dashboard every 5 seconds.
*/

setInterval(
  loadDashboard,
  5000
);

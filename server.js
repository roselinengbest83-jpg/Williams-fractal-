const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

/*
========================================================
DERIV CONNECTION
========================================================
*/

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
========================================================
TELEGRAM
========================================================
*/

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

/*
========================================================
FOREX PAIRS
========================================================
*/

const PAIRS = [
  "EUR/USD",
  "GBP/USD",
  "USD/JPY",
  "GBP/JPY",
  "EUR/JPY",
  "AUD/USD",
  "USD/CAD",
  "USD/CHF",
  "AUD/JPY",
  "NZD/USD"
];

/*
========================================================
TIMEFRAMES
========================================================
*/

const M5 = 300;
const M15 = 900;
const H1 = 3600;

/*
========================================================
STRATEGY SETTINGS
========================================================
*/

const FRACTAL_PERIODS = 2;

const RISK_REWARD = 2;

const SCAN_INTERVAL = 60 * 1000;

const MAX_CANDLES = 200;

/*
========================================================
STATE
========================================================
*/

const state = {};

for (const pair of PAIRS) {
  state[pair] = {
    pair,
    status: "WAIT",
    signal: null,
    direction12H: "WAIT",
    structure1H: "WAIT",
    trend15M: "WAIT",
    break15M: "WAIT",
    retest15M: "WAIT",
    confirm5M: "WAIT",
    entry: null,
    stopLoss: null,
    takeProfit: null,
    riskReward: RISK_REWARD,
    error: null,
    lastScan: null
  };
}

let lastScan = null;
let scannerRunning = false;
let derivConnected = false;
let marketClosed = false;

let derivWS = null;
let requestId = 1;

const pendingRequests = new Map();

let symbolMap = {};

/*
========================================================
UTILITY
========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeName(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z]/g, "");
}

/*
========================================================
DERIV SYMBOL MAPPING
========================================================
*/

function normalizeDerivPair(name, symbol) {
  const aliases = {
    EURUSD: "EUR/USD",
    GBPUSD: "GBP/USD",
    USDJPY: "USD/JPY",
    GBPJPY: "GBP/JPY",
    EURJPY: "EUR/JPY",
    AUDUSD: "AUD/USD",
    USDCAD: "USD/CAD",
    USDCHF: "USD/CHF",
    AUDJPY: "AUD/JPY",
    NZDUSD: "NZD/USD"
  };

  const values = [name, symbol]
    .filter(Boolean)
    .map(value => normalizeName(value));

  for (const value of values) {
    if (aliases[value]) {
      return aliases[value];
    }

    if (value.startsWith("FRX")) {
      const forexCode = value.substring(3);

      if (aliases[forexCode]) {
        return aliases[forexCode];
      }
    }
  }

  return null;
}

/*
========================================================
DERIV CONNECTION
========================================================
*/

function connectDeriv() {
  console.log("[DERIV] Connecting...");

  derivWS = new WebSocket(DERIV_WS_URL);

  derivWS.on("open", async () => {
    derivConnected = true;

    console.log("[DERIV] WebSocket connected");

    try {
      await loadActiveSymbols();
    } catch (error) {
      console.error(
        "[DERIV] Active symbols error:",
        error.message
      );
    }
  });

  derivWS.on("message", message => {
    try {
      const data = JSON.parse(message.toString());

      if (data.req_id && pendingRequests.has(data.req_id)) {
        const request = pendingRequests.get(data.req_id);

        clearTimeout(request.timeout);

        pendingRequests.delete(data.req_id);

        if (data.error) {
          request.reject(
            new Error(
              data.error.message ||
              data.error.code ||
              "Deriv API error"
            )
          );
        } else {
          request.resolve(data);
        }
      }
    } catch (error) {
      console.error(
        "[DERIV] Message parsing error:",
        error.message
      );
    }
  });

  derivWS.on("error", error => {
    derivConnected = false;

    console.error(
      "[DERIV] WebSocket error:",
      error.message
    );
  });

  derivWS.on("close", (code, reason) => {
    derivConnected = false;

    console.log(
      `[DERIV] WebSocket closed. Code: ${code} Reason: ${reason || ""}`
    );

    setTimeout(connectDeriv, 5000);
  });
}

/*
========================================================
SEND DERIV REQUEST
========================================================
*/

function sendDerivRequest(payload, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    if (!derivWS || derivWS.readyState !== WebSocket.OPEN) {
      reject(new Error("Deriv WebSocket is not connected"));
      return;
    }

    const req_id = requestId++;

    const timeout = setTimeout(() => {
      pendingRequests.delete(req_id);

      reject(
        new Error("Deriv request timed out")
      );
    }, timeoutMs);

    pendingRequests.set(req_id, {
      resolve,
      reject,
      timeout
    });

    derivWS.send(
      JSON.stringify({
        ...payload,
        req_id
      })
    );
  });
}

/*
========================================================
ACTIVE SYMBOLS
========================================================
*/

async function loadActiveSymbols() {
  const response = await sendDerivRequest({
    active_symbols: "brief"
  });

  const symbols = response.active_symbols || [];

  console.log(
    `[DERIV] Received ${symbols.length} active symbols`
  );

  symbolMap = {};

  for (const item of symbols) {
    const pair = normalizeDerivPair(
      item.underlying_symbol_name ||
      item.display_name ||
      item.name,
      item.underlying_symbol ||
      item.symbol
    );

    if (pair && PAIRS.includes(pair)) {
      symbolMap[pair] =
        item.underlying_symbol ||
        item.symbol;
    }
  }

  const mappedCount = PAIRS.filter(
    pair => symbolMap[pair]
  ).length;

  console.log(
    `[DERIV] Successfully mapped ${mappedCount}/${PAIRS.length} forex pairs`
  );

  for (const pair of PAIRS) {
    if (!symbolMap[pair]) {
      console.warn(
        `[DERIV] WARNING: No symbol found for ${pair}`
      );
    }
  }
}

/*
========================================================
GET CANDLES
========================================================
*/

async function getCandles(pair, granularity, count = MAX_CANDLES) {
  const symbol = symbolMap[pair];

  if (!symbol) {
    throw new Error(
      `No Deriv symbol found for ${pair}`
    );
  }

  /*
    IMPORTANT:
    No subscribe field here.
    The new Deriv endpoint rejects subscribe
    on this ticks_history request.
  */

  const response = await sendDerivRequest({
    ticks_history: symbol,
    end: "latest",
    count,
    style: "candles",
    granularity
  });

  if (
    response.error &&
    response.error.message
  ) {
    throw new Error(response.error.message);
  }

  if (!response.candles) {
    throw new Error(
      "No candle data returned by Deriv"
    );
  }

  return response.candles.map(candle => ({
    epoch: Number(candle.epoch),
    open: Number(candle.open),
    high: Number(candle.high),
    low: Number(candle.low),
    close: Number(candle.close)
  }));
}

/*
========================================================
MARKET CLOSED DETECTION
========================================================
*/

function isMarketClosedError(message) {
  const text = String(message || "").toLowerCase();

  return (
    text.includes("market is presently closed") ||
    text.includes("market will open") ||
    text.includes("market closed")
  );
}

/*
========================================================
12H CANDLES
========================================================
*/

function build12HCandlesFrom1H(candles) {
  const groups = new Map();

  for (const candle of candles) {
    const date = new Date(
      candle.epoch * 1000
    );

    const hour = date.getUTCHours();

    /*
      12H blocks:
      00:00 - 11:59
      12:00 - 23:59
    */

    const blockHour =
      hour < 12 ? 0 : 12;

    const blockDate = new Date(
      Date.UTC(
        date.getUTCFullYear(),
        date.getUTCMonth(),
        date.getUTCDate(),
        blockHour
      )
    );

    const key = blockDate.getTime();

    if (!groups.has(key)) {
      groups.set(key, []);
    }

    groups.get(key).push(candle);
  }

  const result = [];

  for (const [, group] of groups) {
    group.sort(
      (a, b) => a.epoch - b.epoch
    );

    if (group.length < 12) {
      continue;
    }

    result.push({
      epoch: group[0].epoch,
      open: group[0].open,
      high: Math.max(
        ...group.map(c => c.high)
      ),
      low: Math.min(
        ...group.map(c => c.low)
      ),
      close:
        group[group.length - 1].close
    });
  }

  return result.sort(
    (a, b) => a.epoch - b.epoch
  );
}

/*
========================================================
12H DIRECTION
========================================================
*/

function get12HDirection(candles) {
  if (!candles || candles.length < 3) {
    return "WAIT";
  }

  const completed =
    candles.slice(0, -1);

  const last =
    completed[completed.length - 1];

  const previous =
    completed[completed.length - 2];

  if (
    last.close > last.open &&
    last.close > previous.close
  ) {
    return "BULLISH";
  }

  if (
    last.close < last.open &&
    last.close < previous.close
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/*
========================================================
1H STRUCTURE
========================================================
*/

function get1HStructure(candles) {
  if (!candles || candles.length < 10) {
    return "WAIT";
  }

  const completed =
    candles.slice(0, -1);

  const recent =
    completed.slice(-6);

  const highs =
    recent.map(c => c.high);

  const lows =
    recent.map(c => c.low);

  const highest =
    Math.max(...highs);

  const lowest =
    Math.min(...lows);

  const last =
    completed[completed.length - 1];

  if (
    last.close > highest
  ) {
    return "BULLISH";
  }

  if (
    last.close < lowest
  ) {
    return "BEARISH";
  }

  /*
    Simpler fallback using recent movement.
  */

  const first =
    recent[0];

  if (
    last.close > first.close
  ) {
    return "BULLISH";
  }

  if (
    last.close < first.close
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/*
========================================================
WILLIAMS FRACTALS
========================================================
*/

function getFractals(
  candles,
  n = FRACTAL_PERIODS
) {
  const upFractals = [];
  const downFractals = [];

  for (
    let i = n;
    i < candles.length - n;
    i++
  ) {
    const current = candles[i];

    let downFractal = true;
    let upFractal = true;

    for (
      let j = 1;
      j <= n;
      j++
    ) {
      if (
        candles[i - j].high >= current.high ||
        candles[i + j].high >= current.high
      ) {
        downFractal = false;
      }

      if (
        candles[i - j].low <= current.low ||
        candles[i + j].low <= current.low
      ) {
        upFractal = false;
      }
    }

    if (downFractal) {
      downFractals.push({
        index: i,
        epoch: current.epoch,
        price: current.high
      });
    }

    if (upFractal) {
      upFractals.push({
        index: i,
        epoch: current.epoch,
        price: current.low
      });
    }
  }

  return {
    upFractals,
    downFractals
  };
}

/*
========================================================
TREND LINE
========================================================
*/

function calculateTrendLine(
  candles,
  direction
) {
  const {
    upFractals,
    downFractals
  } = getFractals(candles);

  if (direction === "BULLISH") {
    if (upFractals.length < 2) {
      return null;
    }

    const first =
      upFractals[
        upFractals.length - 2
      ];

    const second =
      upFractals[
        upFractals.length - 1
      ];

    return {
      type: "UP",
      first,
      second
    };
  }

  if (direction === "BEARISH") {
    if (downFractals.length < 2) {
      return null;
    }

    const first =
      downFractals[
        downFractals.length - 2
      ];

    const second =
      downFractals[
        downFractals.length - 1
      ];

    return {
      type: "DOWN",
      first,
      second
    };
  }

  return null;
}

/*
========================================================
TREND LINE PRICE
========================================================
*/

function trendLinePrice(
  line,
  epoch
) {
  if (!line) {
    return null;
  }

  const x1 =
    line.first.epoch;

  const x2 =
    line.second.epoch;

  const y1 =
    line.first.price;

  const y2 =
    line.second.price;

  if (x2 === x1) {
    return y2;
  }

  const slope =
    (y2 - y1) /
    (x2 - x1);

  return (
    y1 +
    slope *
      (epoch - x1)
  );
}

/*
========================================================
15M TREND
========================================================
*/

function get15MTrend(candles) {
  if (!candles || candles.length < 20) {
    return "WAIT";
  }

  const completed =
    candles.slice(0, -1);

  const lineBull =
    calculateTrendLine(
      completed,
      "BULLISH"
    );

  const lineBear =
    calculateTrendLine(
      completed,
      "BEARISH"
    );

  const last =
    completed[completed.length - 1];

  if (lineBull && last.close > lineBull.second.price) {
    return "BULLISH";
  }

  if (lineBear && last.close < lineBear.second.price) {
    return "BEARISH";
  }

  if (
    last.close >
    completed[
      completed.length - 5
    ].close
  ) {
    return "BULLISH";
  }

  if (
    last.close <
    completed[
      completed.length - 5
    ].close
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/*
========================================================
15M BREAK
========================================================
*/

function check15MBreak(
  candles,
  direction
) {
  if (!candles || candles.length < 30) {
    return false;
  }

  const completed =
    candles.slice(0, -1);

  const line =
    calculateTrendLine(
      completed,
      direction
    );

  if (!line) {
    return false;
  }

  const last =
    completed[completed.length - 1];

  const linePrice =
    trendLinePrice(
      line,
      last.epoch
    );

  if (direction === "BULLISH") {
    return last.close > linePrice;
  }

  if (direction === "BEARISH") {
    return last.close < linePrice;
  }

  return false;
}

/*
========================================================
RETEST / REJECTION
========================================================
*/

function checkRetest(
  candles,
  direction
) {
  if (!candles || candles.length < 10) {
    return false;
  }

  const completed =
    candles.slice(0, -1);

  const last =
    completed[completed.length - 1];

  const previous =
    completed[completed.length - 2];

  if (direction === "BULLISH") {
    return (
      previous.low <=
        previous.high &&
      last.close > last.open &&
      last.close > previous.high
    );
  }

  if (direction === "BEARISH") {
    return (
      previous.high >=
        previous.low &&
      last.close < last.open &&
      last.close < previous.low
    );
  }

  return false;
}

/*
========================================================
5M CONFIRMATION
========================================================
*/

function check5MConfirmation(
  candles,
  direction
) {
  if (!candles || candles.length < 10) {
    return false;
  }

  const completed =
    candles.slice(0, -1);

  const last =
    completed[completed.length - 1];

  const previous =
    completed[completed.length - 2];

  if (direction === "BULLISH") {
    return (
      last.close > last.open &&
      last.close > previous.high
    );
  }

  if (direction === "BEARISH") {
    return (
      last.close < last.open &&
      last.close < previous.low
    );
  }

  return false;
}

/*
========================================================
ATR
========================================================
*/

function calculateATR(
  candles,
  period = 14
) {
  if (
    !candles ||
    candles.length < period + 1
  ) {
    return null;
  }

  const trs = [];

  for (
    let i = 1;
    i < candles.length;
    i++
  ) {
    const current =
      candles[i];

    const previous =
      candles[i - 1];

    const tr = Math.max(
      current.high - current.low,
      Math.abs(
        current.high -
          previous.close
      ),
      Math.abs(
        current.low -
          previous.close
      )
    );

    trs.push(tr);
  }

  const recent =
    trs.slice(-period);

  if (!recent.length) {
    return null;
  }

  return (
    recent.reduce(
      (sum, value) =>
        sum + value,
      0
    ) / recent.length
  );
}

/*
========================================================
BUILD SIGNAL
========================================================
*/

function buildSignal(
  pair,
  direction,
  candles5M,
  candles15M
) {
  const completed5M =
    candles5M.slice(0, -1);

  const last =
    completed5M[
      completed5M.length - 1
    ];

  const atr =
    calculateATR(candles5M);

  if (!atr) {
    return null;
  }

  const entry =
    last.close;

  let stopLoss;
  let takeProfit;

  if (direction === "BULLISH") {
    stopLoss =
      last.low - atr * 0.2;

    const risk =
      entry - stopLoss;

    takeProfit =
      entry +
      risk * RISK_REWARD;
  } else {
    stopLoss =
      last.high + atr * 0.2;

    const risk =
      stopLoss - entry;

    takeProfit =
      entry -
      risk * RISK_REWARD;
  }

  return {
    pair,
    direction,
    entry,
    stopLoss,
    takeProfit,
    riskReward: RISK_REWARD,
    timeframe: "5M",
    strategy:
      "12H → 1H → 15M Fractal Trend Line → Retest → 5M Confirmation"
  };
}

/*
========================================================
TELEGRAM
========================================================
*/

async function sendTelegram(message) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }

  try {
    const url =
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

    await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type":
          "application/json"
      },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message
      })
    });
  } catch (error) {
    console.error(
      "[TELEGRAM] Error:",
      error.message
    );
  }
}

/*
========================================================
SIGNAL MESSAGE
========================================================
*/

function formatSignal(signal) {
  const direction =
    signal.direction === "BULLISH"
      ? "BUY"
      : "SELL";

  return `
🚨 FOREX SIGNAL

Pair: ${signal.pair}
Direction: ${direction}

Entry: ${signal.entry}
SL: ${signal.stopLoss}
TP: ${signal.takeProfit}

Risk/Reward: 1:${signal.riskReward}

Timeframe: 5M

Strategy:
12H Direction
↓
1H Structure
↓
15M Williams Fractal Trend Line
↓
Candle Close Break
↓
Retest/Rejection
↓
5M Confirmation

Signal only — no automatic trade.
`;
}

/*
========================================================
SCAN ONE PAIR
========================================================
*/

async function scanPair(pair) {
  const currentState =
    state[pair];

  currentState.lastScan =
    new Date().toISOString();

  currentState.error = null;
  currentState.signal = null;
  currentState.status = "SCANNING";

  try {
    /*
    ================================================
    1H DATA
    ================================================
    */

    const candles1H =
      await getCandles(
        pair,
        H1,
        200
      );

    /*
    ================================================
    12H DIRECTION
    ================================================
    */

    const candles12H =
      build12HCandlesFrom1H(
        candles1H
      );

    const direction12H =
      get12HDirection(
        candles12H
      );

    currentState.direction12H =
      direction12H;

    /*
    ================================================
    1H STRUCTURE
    ================================================
    */

    const structure1H =
      get1HStructure(
        candles1H
      );

    currentState.structure1H =
      structure1H;

    /*
    ================================================
    DIRECTION ALIGNMENT
    ================================================
    */

    let workingDirection =
      direction12H;

    if (
      direction12H === "WAIT"
    ) {
      currentState.status =
        "WAIT";

      return;
    }

    if (
      structure1H !==
        direction12H
    ) {
      currentState.status =
        "WAIT";

      return;
    }

    /*
    ================================================
    15M
    ================================================
    */

    const candles15M =
      await getCandles(
        pair,
        M15,
        200
      );

    const trend15M =
      get15MTrend(
        candles15M
      );

    currentState.trend15M =
      trend15M;

    if (
      trend15M !==
      workingDirection
    ) {
      currentState.status =
        "WAIT";

      return;
    }

    /*
    ================================================
    15M BREAK
    ================================================
    */

    const break15M =
      check15MBreak(
        candles15M,
        workingDirection
      );

    currentState.break15M =
      break15M
        ? "CONFIRMED"
        : "WAIT";

    if (!break15M) {
      currentState.status =
        "WAIT";

      return;
    }

    /*
    ================================================
    RETEST
    ================================================
    */

    const retest15M =
      checkRetest(
        candles15M,
        workingDirection
      );

    currentState.retest15M =
      retest15M
        ? "CONFIRMED"
        : "WAIT";

    if (!retest15M) {
      currentState.status =
        "WAIT";

      return;
    }

    /*
    ================================================
    5M
    ================================================
    */

    const candles5M =
      await getCandles(
        pair,
        M5,
        200
      );

    const confirmation5M =
      check5MConfirmation(
        candles5M,
        workingDirection
      );

    currentState.confirm5M =
      confirmation5M
        ? "CONFIRMED"
        : "WAIT";

    if (!confirmation5M) {
      currentState.status =
        "WAIT";

      return;
    }

    /*
    ================================================
    BUILD SIGNAL
    ================================================
    */

    const signal =
      buildSignal(
        pair,
        workingDirection,
        candles5M,
        candles15M
      );

    if (!signal) {
      currentState.status =
        "WAIT";

      return;
    }

    currentState.signal =
      signal;

    currentState.entry =
      signal.entry;

    currentState.stopLoss =
      signal.stopLoss;

    currentState.takeProfit =
      signal.takeProfit;

    currentState.riskReward =
      signal.riskReward;

    currentState.status =
      workingDirection === "BULLISH"
        ? "BUY SIGNAL"
        : "SELL SIGNAL";

    /*
    ================================================
    TELEGRAM
    ================================================
    */

    await sendTelegram(
      formatSignal(signal)
    );

    console.log(
      `[SIGNAL] ${pair} ${workingDirection}`
    );

  } catch (error) {
    const message =
      error.message || "Unknown error";

    /*
      Weekend / market closed is NOT
      treated as a bot failure.
    */

    if (
      isMarketClosedError(
        message
      )
    ) {
      marketClosed = true;

      currentState.status =
        "MARKET CLOSED";

      currentState.error =
        "MARKET CLOSED";

      console.log(
        `[SCAN] ${pair}: Market closed`
      );

      return;
    }

    currentState.status =
      "ERROR";

    currentState.error =
      message;

    console.error(
      `[SCAN] ${pair} ERROR: ${message}`
    );
  }
}

/*
========================================================
SCAN ALL PAIRS
========================================================
*/

async function scanAllPairs() {
  if (scannerRunning) {
    return;
  }

  scannerRunning = true;
  marketClosed = false;

  lastScan =
    new Date().toISOString();

  console.log(
    `[SCAN] Starting scan of ${PAIRS.length} pairs`
  );

  try {
    for (const pair of PAIRS) {
      await scanPair(pair);

      /*
        Small delay between pairs so the
        Deriv connection is not hammered.
      */

      await sleep(300);
    }
  } catch (error) {
    console.error(
      "[SCAN] Global error:",
      error.message
    );
  } finally {
    scannerRunning = false;

    console.log(
      "[SCAN] Scan complete"
    );
  }
}

/*
========================================================
API STATUS
========================================================
*/

app.get("/api/status", (req, res) => {
  res.json({
    success: true,

    system: {
      derivConnected,
      marketClosed,
      scannerRunning,
      lastScan,
      pairCount: PAIRS.length
    },

    pairs: state
  });
});

/*
========================================================
HEALTH CHECK
========================================================
*/

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    derivConnected,
    marketClosed,
    time: new Date().toISOString()
  });
});

/*
========================================================
MANUAL SCAN
========================================================
*/

app.post("/api/scan", async (req, res) => {
  if (scannerRunning) {
    return res.json({
      success: false,
      message: "Scanner already running"
    });
  }

  scanAllPairs();

  res.json({
    success: true,
    message: "Scan started"
  });
});

/*
========================================================
DASHBOARD
========================================================
*/

app.get("/", (req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

/*
========================================================
START SERVER
========================================================
*/

app.listen(PORT, () => {
  console.log(
    `Server running on port ${PORT}`
  );

  console.log(
    `Watching ${PAIRS.length} forex pairs`
  );

  connectDeriv();

  /*
    First scan after Deriv connection
    has had time to initialize.
  */

  setTimeout(() => {
    scanAllPairs();
  }, 10000);

  /*
    Continue scanning every minute.
  */

  setInterval(() => {
    scanAllPairs();
  }, SCAN_INTERVAL);
});

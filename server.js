const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

// Deriv public WebSocket
const DERIV_WS_URL =
  "wss://ws.binaryws.com/websockets/v3";

// Strategy settings
const FRACTAL_PERIODS = 2;
const SCAN_INTERVAL = 60 * 1000;
const RETEST_LOOKBACK_15M = 8;

// 10 forex pairs
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

const symbolMap = {};
const pairState = {};

for (const pair of PAIRS) {
  pairState[pair] = {
    pair,
    symbol: null,
    price: null,

    direction12H: "WAIT",
    structure1H: "WAIT",

    trend15M: "WAIT",
    fractalHigh: null,
    fractalLow: null,
    trendLine: null,

    lineBreak: "WAIT",
    retest: "WAIT",
    confirmation5M: "WAIT",

    signal: "WAIT",
    entry: null,
    stopLoss: null,
    takeProfit: null,
    riskReward: "1:2",

    lastSignalTime: null,
    lastError: null
  };
}

let ws = null;
let connected = false;
let connecting = false;
let symbolsLoaded = false;
let scannerStarted = false;

let requestId = 1;

const pendingRequests = new Map();


// ======================================================
// DERIV WEBSOCKET
// ======================================================

function sendRequest(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return null;
  }

  const reqId = requestId++;

  payload.req_id = reqId;

  ws.send(JSON.stringify(payload));

  return reqId;
}


function connectDeriv() {
  if (connecting) return;

  if (ws && ws.readyState === WebSocket.OPEN) {
    return;
  }

  connecting = true;

  console.log("[DERIV] Connecting...");
  console.log("[DERIV] Endpoint:", DERIV_WS_URL);

  ws = new WebSocket(DERIV_WS_URL);

  ws.on("open", () => {
    connecting = false;
    connected = true;

    console.log("[DERIV] WebSocket connected");

    sendRequest({
      active_symbols: "brief",
      product_type: "basic"
    });
  });


  ws.on("message", raw => {
    try {
      const data = JSON.parse(raw.toString());

      handleDerivMessage(data);

    } catch (error) {
      console.error(
        "[DERIV] Message parsing error:",
        error.message
      );
    }
  });


  ws.on("error", error => {
    connected = false;

    console.error(
      "[DERIV] WebSocket error:",
      error.message
    );
  });


  ws.on("close", (code, reason) => {
    connected = false;
    connecting = false;
    symbolsLoaded = false;

    console.log(
      `[DERIV] WebSocket closed. Code: ${code} Reason: ${reason || "none"}`
    );

    // Reject pending requests
    for (const [id, request] of pendingRequests.entries()) {
      clearTimeout(request.timeout);

      request.reject(
        new Error("Deriv WebSocket disconnected")
      );

      pendingRequests.delete(id);
    }

    // Reconnect after 5 seconds
    setTimeout(() => {
      connectDeriv();
    }, 5000);
  });
}


// ======================================================
// DERIV MESSAGE HANDLER
// ======================================================

function handleDerivMessage(data) {

  if (data.error) {

    console.error(
      "[DERIV ERROR]",
      data.error.code || "",
      data.error.message || ""
    );

    if (data.req_id && pendingRequests.has(data.req_id)) {

      const request = pendingRequests.get(data.req_id);

      clearTimeout(request.timeout);

      pendingRequests.delete(data.req_id);

      request.reject(
        new Error(data.error.message || "Deriv API error")
      );
    }

    return;
  }


  // Handle responses to our requests
  if (data.req_id && pendingRequests.has(data.req_id)) {

    const request = pendingRequests.get(data.req_id);

    clearTimeout(request.timeout);

    pendingRequests.delete(data.req_id);

    request.resolve(data);

    return;
  }


  // Active symbols response
  if (data.msg_type === "active_symbols") {

    console.log(
      `[DERIV] Received ${data.active_symbols?.length || 0} active symbols`
    );

    resolveForexSymbols(
      data.active_symbols || []
    );
  }
}


// ======================================================
// SYMBOL MATCHING
// ======================================================

function normalizeName(value) {

  return String(value || "")
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/-/g, "/")
    .replace(/_/g, "/");
}


function compactPair(pair) {

  return pair
    .toUpperCase()
    .replace("/", "");
}


function resolveForexSymbols(symbols) {

  for (const pair of PAIRS) {

    const wanted = normalizeName(pair);
    const compactWanted = compactPair(pair);

    let found = symbols.find(symbol => {

      const names = [
        symbol.display_name,
        symbol.name,
        symbol.underlying_symbol_name,
        symbol.symbol
      ];

      return names.some(name => {

        const normalized = normalizeName(name);

        return (
          normalized === wanted ||
          normalized === pair.replace("/", "") ||
          compactPair(normalized) === compactWanted
        );
      });
    });


    // Extra fallback for common Deriv forex symbols
    if (!found) {

      const forexCode =
        "frx" + compactWanted;

      found = symbols.find(symbol =>
        String(symbol.symbol || "").toUpperCase() ===
        forexCode.toUpperCase()
      );
    }


    if (found) {

      const code =
        found.underlying_symbol ||
        found.symbol;

      symbolMap[pair] = code;

      pairState[pair].symbol = code;

      pairState[pair].lastError = null;

      console.log(
        `[SYMBOL] ${pair} -> ${code}`
      );

    } else {

      pairState[pair].lastError =
        "Deriv symbol not found";

      console.log(
        `[SYMBOL] ${pair} NOT FOUND`
      );
    }
  }


  symbolsLoaded = true;

  startScanner();
}


// ======================================================
// REQUEST HISTORICAL CANDLES
// ======================================================

function getCandles(symbol, granularity, count) {

  return new Promise((resolve, reject) => {

    const reqId = sendRequest({
      ticks_history: symbol,
      end: "latest",
      count,
      style: "candles",
      granularity,
      subscribe: 0
    });


    if (!reqId) {

      reject(
        new Error(
          "Deriv WebSocket not connected"
        )
      );

      return;
    }


    const timeout = setTimeout(() => {

      pendingRequests.delete(reqId);

      reject(
        new Error(
          "Deriv candle request timeout"
        )
      );

    }, 20000);


    pendingRequests.set(reqId, {
      resolve,
      reject,
      timeout
    });
  });
}


// ======================================================
// CANDLE CONVERSION
// ======================================================

function convertCandles(response) {

  return (response.candles || [])
    .map(c => ({
      time: Number(c.epoch),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close)
    }))
    .filter(c =>
      [
        c.open,
        c.high,
        c.low,
        c.close
      ].every(Number.isFinite)
    );
}


// ======================================================
// BUILD 12H CANDLES FROM 1H
// ======================================================

function build12HCandles(oneHour) {

  const groups = new Map();


  for (const candle of oneHour) {

    const date =
      new Date(candle.time * 1000);

    const hour =
      date.getUTCHours();

    const startHour =
      hour < 12 ? 0 : 12;


    const start =
      Date.UTC(
        date.getUTCFullYear(),
        date.getUTCMonth(),
        date.getUTCDate(),
        startHour
      ) / 1000;


    if (!groups.has(start)) {
      groups.set(start, []);
    }

    groups
      .get(start)
      .push(candle);
  }


  const result = [];


  for (const [time, group] of groups) {

    group.sort(
      (a, b) => a.time - b.time
    );


    // Only use complete 12H blocks
    if (group.length < 12) {
      continue;
    }


    result.push({
      time,

      open:
        group[0].open,

      high:
        Math.max(
          ...group.map(c => c.high)
        ),

      low:
        Math.min(
          ...group.map(c => c.low)
        ),

      close:
        group[group.length - 1].close
    });
  }


  return result;
}


// ======================================================
// MARKET DIRECTION
// ======================================================

function determineDirection(candles) {

  if (candles.length < 4) {
    return "WAIT";
  }


  const a =
    candles[candles.length - 4];

  const b =
    candles[candles.length - 3];

  const c =
    candles[candles.length - 2];

  const d =
    candles[candles.length - 1];


  // Higher highs + higher lows
  if (
    d.high > c.high &&
    c.high >= b.high &&
    d.low > c.low &&
    c.low >= b.low
  ) {
    return "BULLISH";
  }


  // Lower highs + lower lows
  if (
    d.high < c.high &&
    c.high <= b.high &&
    d.low < c.low &&
    c.low <= b.low
  ) {
    return "BEARISH";
  }


  // Fallback
  if (d.close > d.open) {
    return "BULLISH";
  }


  if (d.close < d.open) {
    return "BEARISH";
  }


  return "WAIT";
}


// ======================================================
// WILLIAMS FRACTALS
// ======================================================

function findFractals(
  candles,
  n = 2
) {

  const highs = [];
  const lows = [];


  for (
    let i = n;
    i < candles.length - n;
    i++
  ) {

    let down = true;
    let up = true;


    for (
      let j = 1;
      j <= n;
      j++
    ) {

      if (
        candles[i - j].high >=
          candles[i].high ||

        candles[i + j].high >=
          candles[i].high
      ) {
        down = false;
      }


      if (
        candles[i - j].low <=
          candles[i].low ||

        candles[i + j].low <=
          candles[i].low
      ) {
        up = false;
      }
    }


    if (down) {

      highs.push({
        index: i,
        time: candles[i].time,
        price: candles[i].high
      });
    }


    if (up) {

      lows.push({
        index: i,
        time: candles[i].time,
        price: candles[i].low
      });
    }
  }


  return {
    highs,
    lows
  };
}


// ======================================================
// FRACTAL TREND LINE
// ======================================================

function calculateTrendLine(
  fractals,
  direction
) {

  if (
    direction === "BULLISH" &&
    fractals.lows.length >= 2
  ) {

    return {
      type: "BULLISH",

      a:
        fractals.lows[
          fractals.lows.length - 2
        ],

      b:
        fractals.lows[
          fractals.lows.length - 1
        ]
    };
  }


  if (
    direction === "BEARISH" &&
    fractals.highs.length >= 2
  ) {

    return {
      type: "BEARISH",

      a:
        fractals.highs[
          fractals.highs.length - 2
        ],

      b:
        fractals.highs[
          fractals.highs.length - 1
        ]
    };
  }


  return null;
}


// ======================================================
// TREND LINE PRICE
// ======================================================

function trendLinePrice(
  line,
  time
) {

  if (!line) {
    return null;
  }


  if (
    line.b.time ===
    line.a.time
  ) {
    return line.b.price;
  }


  return (
    line.a.price +
    (
      (line.b.price - line.a.price) /
      (line.b.time - line.a.time)
    ) *
    (time - line.a.time)
  );
}


// ======================================================
// 1H STRUCTURE
// ======================================================

function determine1HStructure(candles) {

  if (candles.length < 6) {
    return "WAIT";
  }


  const last =
    candles.slice(-6);


  const highs =
    last.map(c => c.high);

  const lows =
    last.map(c => c.low);


  const hh =
    highs[5] > highs[3] &&
    highs[3] >= highs[1];


  const hl =
    lows[5] > lows[3] &&
    lows[3] >= lows[1];


  const lh =
    highs[5] < highs[3] &&
    highs[3] <= highs[1];


  const ll =
    lows[5] < lows[3] &&
    lows[3] <= lows[1];


  if (hh && hl) {
    return "BULLISH";
  }


  if (lh && ll) {
    return "BEARISH";
  }


  return determineDirection(candles);
}


// ======================================================
// 15M TREND LINE BREAK
// ======================================================

function detectLineBreak(
  candles,
  line,
  direction
) {

  if (
    !line ||
    candles.length < 2
  ) {
    return null;
  }


  const previous =
    candles[candles.length - 2];

  const current =
    candles[candles.length - 1];


  const previousLine =
    trendLinePrice(
      line,
      previous.time
    );


  const currentLine =
    trendLinePrice(
      line,
      current.time
    );


  if (
    direction === "BULLISH" &&
    previous.close <= previousLine &&
    current.close > currentLine
  ) {

    return {
      type: "BULLISH",
      candle: current
    };
  }


  if (
    direction === "BEARISH" &&
    previous.close >= previousLine &&
    current.close < currentLine
  ) {

    return {
      type: "BEARISH",
      candle: current
    };
  }


  return null;
}


// ======================================================
// 15M RETEST / REJECTION
// ======================================================

function detectRetestRejection(
  candles,
  line,
  direction,
  breakInfo
) {

  if (
    !breakInfo ||
    !line
  ) {
    return null;
  }


  const breakIndex =
    candles.findIndex(
      c =>
        c.time ===
        breakInfo.candle.time
    );


  if (breakIndex < 0) {
    return null;
  }


  const start =
    breakIndex + 1;


  const end =
    candles.length;


  for (
    let i = start;
    i < end;
    i++
  ) {

    const candle =
      candles[i];


    const linePrice =
      trendLinePrice(
        line,
        candle.time
      );


    if (linePrice == null) {
      continue;
    }


    const tolerance =
      Math.max(
        Math.abs(linePrice) * 0.00015,
        0.00001
      );


    const touched =
      candle.low <=
        linePrice + tolerance &&

      candle.high >=
        linePrice - tolerance;


    // Bullish retest
    if (
      direction === "BULLISH" &&
      touched &&
      candle.close > linePrice &&
      candle.close > candle.open
    ) {

      return {
        type: "BULLISH",
        candle
      };
    }


    // Bearish retest
    if (
      direction === "BEARISH" &&
      touched &&
      candle.close < linePrice &&
      candle.close < candle.open
    ) {

      return {
        type: "BEARISH",
        candle
      };
    }
  }


  return null;
}


// ======================================================
// 5M CONFIRMATION
// ======================================================

function confirm5M(
  candles,
  direction
) {

  if (candles.length < 3) {
    return false;
  }


  const current =
    candles[candles.length - 1];

  const previous =
    candles[candles.length - 2];


  if (direction === "BULLISH") {

    return (
      current.close >
        current.open &&

      current.close >
        previous.high
    );
  }


  if (direction === "BEARISH") {

    return (
      current.close <
        current.open &&

      current.close <
        previous.low
    );
  }


  return false;
}


// ======================================================
// ATR
// ======================================================

function calculateATR(
  candles,
  period = 14
) {

  if (
    candles.length <
    period + 1
  ) {
    return null;
  }


  const trueRanges = [];


  for (
    let i = 1;
    i < candles.length;
    i++
  ) {

    const current =
      candles[i];

    const previous =
      candles[i - 1];


    trueRanges.push(
      Math.max(
        current.high -
          current.low,

        Math.abs(
          current.high -
          previous.close
        ),

        Math.abs(
          current.low -
          previous.close
        )
      )
    );
  }


  const recent =
    trueRanges.slice(-period);


  return (
    recent.reduce(
      (sum, value) =>
        sum + value,
      0
    ) / recent.length
  );
}


// ======================================================
// PRICE DECIMALS
// ======================================================

function decimalsForPair(pair) {

  if (
    pair.includes("JPY")
  ) {
    return 3;
  }


  return 5;
}


function roundPrice(
  pair,
  value
) {

  return Number(
    value.toFixed(
      decimalsForPair(pair)
    )
  );
}


// ======================================================
// CREATE SIGNAL
// ======================================================

function createSignal(
  pair,
  direction,
  candles5M,
  atr
) {

  if (
    !atr ||
    atr <= 0
  ) {
    return null;
  }


  const price =
    candles5M[
      candles5M.length - 1
    ].close;


  const entry =
    price;


  const stopLoss =
    direction === "BULLISH"
      ? price - atr
      : price + atr;


  const takeProfit =
    direction === "BULLISH"
      ? price + atr * 2
      : price - atr * 2;


  return {

    pair,

    signal:
      direction === "BULLISH"
        ? "BUY"
        : "SELL",

    entry:
      roundPrice(
        pair,
        entry
      ),

    stopLoss:
      roundPrice(
        pair,
        stopLoss
      ),

    takeProfit:
      roundPrice(
        pair,
        takeProfit
      ),

    riskReward: "1:2",

    time:
      new Date().toISOString()
  };
}


// ======================================================
// TELEGRAM
// ======================================================

async function sendTelegram(
  message
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }


  try {

    const url =
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;


    await fetch(
      url,
      {
        method: "POST",

        headers: {
          "Content-Type":
            "application/json"
        },

        body: JSON.stringify({
          chat_id:
            TELEGRAM_CHAT_ID,

          text:
            message
        })
      }
    );

  } catch (error) {

    console.error(
      "[TELEGRAM]",
      error.message
    );
  }
}


// ======================================================
// TELEGRAM SIGNAL MESSAGE
// ======================================================

function formatSignal(signal) {

  return `🚨 DERIV FOREX SIGNAL

Pair: ${signal.pair}
Signal: ${signal.signal}

Entry: ${signal.entry}
SL: ${signal.stopLoss}
TP: ${signal.takeProfit}
Risk/Reward: 1:2

12H Direction
↓
1H Structure
↓
15M Williams Fractal Trend Line
↓
15M Candle Close Break
↓
15M Retest / Rejection
↓
5M Confirmation

⚠️ Signal only — verify before trading.`;
}


// ======================================================
// SCAN ONE PAIR
// ======================================================

async function scanPair(pair) {

  const state =
    pairState[pair];

  const symbol =
    symbolMap[pair];


  if (!symbol) {
    return;
  }


  try {

    // ----------------------------------------------
    // 1H DATA
    // ----------------------------------------------

    const h1 =
      convertCandles(
        await getCandles(
          symbol,
          3600,
          120
        )
      );


    if (h1.length < 30) {
      return;
    }


    // Remove current incomplete candle
    const completedH1 =
      h1.slice(0, -1);


    // ----------------------------------------------
    // 12H DIRECTION
    // ----------------------------------------------

    const h12 =
      build12HCandles(
        completedH1
      );


    const direction12H =
      determineDirection(h12);


    // ----------------------------------------------
    // 1H STRUCTURE
    // ----------------------------------------------

    const structure1H =
      determine1HStructure(
        completedH1
      );


    state.direction12H =
      direction12H;

    state.structure1H =
      structure1H;


    // ----------------------------------------------
    // 15M DATA
    // ----------------------------------------------

    const m15 =
      convertCandles(
        await getCandles(
          symbol,
          900,
          180
        )
      );


    if (m15.length < 30) {
      return;
    }


    const completed15M =
      m15.slice(0, -1);


    // ----------------------------------------------
    // WILLIAMS FRACTALS
    // ----------------------------------------------

    const fractals =
      findFractals(
        completed15M,
        FRACTAL_PERIODS
      );


    // Only trade when 12H and 1H agree
    const alignedDirection =
      direction12H === structure1H
        ? direction12H
        : "WAIT";


    state.trend15M =
      alignedDirection;


    state.fractalHigh =
      fractals.highs[
        fractals.highs.length - 1
      ] || null;


    state.fractalLow =
      fractals.lows[
        fractals.lows.length - 1
      ] || null;


    // ----------------------------------------------
    // FRACTAL TREND LINE
    // ----------------------------------------------

    const line =
      calculateTrendLine(
        fractals,
        alignedDirection
      );


    state.trendLine =
      line;


    // ----------------------------------------------
    // 15M BREAK
    // ----------------------------------------------

    const breakInfo =
      detectLineBreak(
        completed15M,
        line,
        alignedDirection
      );


    state.lineBreak =
      breakInfo
        ? "CONFIRMED"
        : "WAIT";


    // ----------------------------------------------
    // RETEST
    // ----------------------------------------------

    const retest =
      detectRetestRejection(
        completed15M,
        line,
        alignedDirection,
        breakInfo
      );


    state.retest =
      retest
        ? "CONFIRMED"
        : "WAIT";


    // ----------------------------------------------
    // 5M DATA
    // ----------------------------------------------

    const m5 =
      convertCandles(
        await getCandles(
          symbol,
          300,
          120
        )
      );


    if (m5.length < 25) {
      return;
    }


    const completed5M =
      m5.slice(0, -1);


    state.price =
      completed5M[
        completed5M.length - 1
      ].close;


    // ----------------------------------------------
    // 5M CONFIRMATION
    // ----------------------------------------------

    const confirmation =
      retest
        ? confirm5M(
            completed5M,
            alignedDirection
          )
        : false;


    state.confirmation5M =
      confirmation
        ? "CONFIRMED"
        : "WAIT";


    // ----------------------------------------------
    // FINAL SIGNAL
    // ----------------------------------------------

    if (
      alignedDirection !== "WAIT" &&
      breakInfo &&
      retest &&
      confirmation
    ) {

      const atr =
        calculateATR(
          completed5M,
          14
        );


      const signal =
        createSignal(
          pair,
          alignedDirection,
          completed5M,
          atr
        );


      if (!signal) {
        return;
      }


      const signalTime =
        completed5M[
          completed5M.length - 1
        ].time;


      // Prevent duplicate alerts
      if (
        state.lastSignalTime ===
        signalTime
      ) {
        return;
      }


      state.signal =
        signal.signal;


      state.entry =
        signal.entry;


      state.stopLoss =
        signal.stopLoss;


      state.takeProfit =
        signal.takeProfit;


      state.lastSignalTime =
        signalTime;


      console.log(
        `[SIGNAL] ${pair} ${signal.signal}`
      );


      await sendTelegram(
        formatSignal(signal)
      );

    } else {

      state.signal =
        "WAIT";

      state.entry =
        null;

      state.stopLoss =
        null;

      state.takeProfit =
        null;
    }


    state.lastError =
      null;


  } catch (error) {

    state.lastError =
      error.message;


    console.error(
      `[${pair}]`,
      error.message
    );
  }
}


// ======================================================
// SCAN ALL PAIRS
// ======================================================

let scanning = false;


async function scanAllPairs() {

  if (scanning) {
    return;
  }


  if (!connected) {
    console.log(
      "[SCAN] Waiting for Deriv connection..."
    );

    return;
  }


  if (!symbolsLoaded) {
    console.log(
      "[SCAN] Waiting for Deriv symbols..."
    );

    return;
  }


  scanning = true;


  try {

    for (const pair of PAIRS) {

      await scanPair(pair);

      // Small delay between requests
      await new Promise(
        resolve =>
          setTimeout(resolve, 700)
      );
    }

  } finally {

    scanning = false;
  }
}


// ======================================================
// START SCANNER
// ======================================================

function startScanner() {

  if (scannerStarted) {
    return;
  }


  scannerStarted = true;


  console.log(
    "[SCAN] Scanner started"
  );


  // First scan
  scanAllPairs();


  // Continue every 60 seconds
  setInterval(
    scanAllPairs,
    SCAN_INTERVAL
  );
}


// ======================================================
// DASHBOARD API
// ======================================================

app.get(
  "/api/status",
  (req, res) => {

    res.json({

      connected,

      scannerRunning:
        scannerStarted,

      derivSymbolsLoaded:
        symbolsLoaded,

      strategy: [

        "12H direction",

        "1H structure",

        "15M Williams Fractal trend line",

        "15M candle close beyond line",

        "15M retest/rejection",

        "5M confirmation",

        "signal"

      ],

      pairs:
        pairState
    });
  }
);


// ======================================================
// HEALTH CHECK
// ======================================================

app.get(
  "/health",
  (req, res) => {

    res.json({

      status: "online",

      derivConnected:
        connected,

      symbolsLoaded:
        symbolsLoaded,

      scannerRunning:
        scannerStarted
    });
  }
);


// ======================================================
// START EXPRESS
// ======================================================

app.listen(
  PORT,
  () => {

    console.log(
      `Server running on port ${PORT}`
    );

    connectDeriv();
  }
);

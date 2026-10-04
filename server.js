const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

/*
========================================================
DERIV FOREX WILLIAMS FRACTAL SIGNAL BOT
========================================================

STRATEGY

12H DIRECTION
      ↓
1H STRUCTURE
      ↓
15M WILLIAMS FRACTAL TREND LINE
      ↓
15M CANDLE CLOSE BEYOND LINE
      ↓
RETEST + REJECTION
      ↓
5M CONFIRMATION
      ↓
BUY / SELL SIGNAL

Signal only.
NO automatic trading.

Market data:
Deriv Public WebSocket API.
========================================================
*/

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ======================================================
// DERIV
// ======================================================

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

// ======================================================
// TELEGRAM
// ======================================================

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN || "";

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID || "";

// ======================================================
// 10 FOREX PAIRS
// ======================================================

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

// ======================================================
// TIMEFRAMES
// ======================================================

const TIMEFRAMES = {
  M5: 300,
  M15: 900,
  H1: 3600
};

// ======================================================
// SETTINGS
// ======================================================

const SCAN_INTERVAL = 60000;

const FRACTAL_PERIODS = 2;

const RR = 2;

// ======================================================
// WEBSOCKET STATE
// ======================================================

let ws = null;

let connected = false;

let connecting = false;

let requestId = 1000;

let pendingRequests = new Map();

let symbolMap = {};

let lastScan = null;

let scannerRunning = false;

// ======================================================
// PAIR STATES
// ======================================================

const pairStates = {};

for (const pair of PAIRS) {

  pairStates[pair] = {

    pair,

    symbol: null,

    connected: false,

    price: null,

    direction12H: "WAIT",

    structure1H: "WAIT",

    trend15M: "WAIT",

    lineBreak15M: false,

    retest15M: false,

    confirmation5M: false,

    fractalHigh: null,

    fractalLow: null,

    trendLine: null,

    entry: null,

    sl: null,

    tp: null,

    signal: "WAIT",

    score: 0,

    lastUpdate: null,

    error: null

  };

}

// ======================================================
// HELPERS
// ======================================================

function nextRequestId() {

  requestId += 1;

  return requestId;

}

function sleep(ms) {

  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );

}

function nowISO() {

  return new Date().toISOString();

}

function roundPrice(price, digits = 5) {

  if (
    price === null ||
    price === undefined ||
    !Number.isFinite(Number(price))
  ) {

    return null;

  }

  return Number(
    Number(price).toFixed(digits)
  );

}

function getDigits(pair) {

  if (pair.includes("JPY")) {

    return 3;

  }

  return 5;

}

// ======================================================
// DERIV CONNECTION
// ======================================================

function connectDeriv() {

  if (
    connecting ||
    connected
  ) {

    return;

  }

  connecting = true;

  console.log(
    "[DERIV] Connecting to:",
    DERIV_WS_URL
  );

  try {

    ws =
      new WebSocket(
        DERIV_WS_URL
      );

  } catch (error) {

    connecting = false;

    console.log(
      "[DERIV] Connection creation error:",
      error.message
    );

    setTimeout(
      connectDeriv,
      5000
    );

    return;

  }

  ws.on("open", () => {

    connecting = false;

    connected = true;

    console.log(
      "[DERIV] WebSocket CONNECTED"
    );

    requestActiveSymbols();

  });

  ws.on("message", message => {

    try {

      const data =
        JSON.parse(
          message.toString()
        );

      handleDerivMessage(data);

    } catch (error) {

      console.log(
        "[DERIV] Message parse error:",
        error.message
      );

    }

  });

  ws.on("error", error => {

    console.log(
      "[DERIV] WebSocket ERROR:",
      error.message
    );

  });

  ws.on("close", (code, reason) => {

    connected = false;

    connecting = false;

    console.log(
      "[DERIV] WebSocket CLOSED. Code:",
      code,
      "Reason:",
      reason.toString()
    );

    for (const pair of PAIRS) {

      pairStates[pair].connected =
        false;

    }

    setTimeout(
      connectDeriv,
      5000
    );

  });

}

// ======================================================
// SEND DERIV REQUEST
// ======================================================

function sendDerivRequest(payload) {

  return new Promise(
    (resolve, reject) => {

      if (
        !ws ||
        !connected
      ) {

        reject(
          new Error(
            "Deriv WebSocket is not connected"
          )
        );

        return;

      }

      const reqId =
        nextRequestId();

      const request = {

        ...payload,

        req_id: reqId

      };

      const timeout =
        setTimeout(() => {

          pendingRequests.delete(
            reqId
          );

          reject(
            new Error(
              `Deriv request timeout: ${
                payload.msg_type ||
                Object.keys(payload)[0]
              }`
            )
          );

        }, 20000);

      pendingRequests.set(
        reqId,
        {
          resolve,
          reject,
          timeout
        }
      );

      try {

        ws.send(
          JSON.stringify(request)
        );

      } catch (error) {

        clearTimeout(timeout);

        pendingRequests.delete(
          reqId
        );

        reject(error);

      }

    }
  );

}

// ======================================================
// HANDLE DERIV MESSAGE
// ======================================================

function handleDerivMessage(data) {

  if (data.req_id) {

    const pending =
      pendingRequests.get(
        data.req_id
      );

    if (pending) {

      clearTimeout(
        pending.timeout
      );

      pendingRequests.delete(
        data.req_id
      );

      if (data.error) {

        pending.reject(
          new Error(
            data.error.message ||
            "Deriv API error"
          )
        );

      } else {

        pending.resolve(data);

      }

      return;

    }

  }

  if (data.error) {

    console.log(
      "[DERIV] API ERROR:",
      data.error.message
    );

    return;

  }

  if (
    data.msg_type === "tick"
  ) {

    const tick =
      data.tick;

    if (!tick) {

      return;

    }

    const symbol =
      tick.symbol;

    const pair =
      Object.keys(symbolMap)
        .find(
          key =>
            symbolMap[key] === symbol
        );

    if (!pair) {

      return;

    }

    pairStates[pair].price =
      Number(tick.quote);

    pairStates[pair].lastUpdate =
      nowISO();

  }

}

// ======================================================
// SMART DERIV SYMBOL MAPPER
// ======================================================

function normalizeDerivPair(
  name,
  symbol
) {

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

  const values = [

    name,

    symbol

  ]

    .filter(Boolean)

    .map(value =>

      String(value)
        .toUpperCase()
        .replace(
          /[^A-Z]/g,
          ""
        )

    );

  for (
    const value of values
  ) {

    // Direct match
    if (
      aliases[value]
    ) {

      return aliases[value];

    }

    // Deriv Forex symbols
    // Example:
    // frxEURUSD
    // frxUSDCHF

    if (
      value.startsWith("FRX")
    ) {

      const forexCode =
        value.substring(3);

      if (
        aliases[forexCode]
      ) {

        return aliases[
          forexCode
        ];

      }

    }

  }

  return null;

}

// ======================================================
// ACTIVE SYMBOLS
// ======================================================

async function requestActiveSymbols() {

  try {

    console.log(
      "[DERIV] Requesting active forex symbols..."
    );

    const response =
      await sendDerivRequest({

        active_symbols:
          "brief"

      });

    const symbols =
      response.active_symbols ||
      [];

    console.log(
      `[DERIV] Received ${symbols.length} active symbols`
    );

    symbolMap = {};

    for (
      const item of symbols
    ) {

      const symbol =
        item.underlying_symbol ||
        item.symbol;

      const display =
        item.underlying_symbol_name ||
        item.display_name;

      const normalized =
        normalizeDerivPair(
          display,
          symbol
        );

      if (
        normalized &&
        PAIRS.includes(
          normalized
        )
      ) {

        symbolMap[
          normalized
        ] = symbol;

        pairStates[
          normalized
        ].symbol =
          symbol;

        pairStates[
          normalized
        ].connected =
          true;

        console.log(
          `[DERIV] ${normalized} → ${symbol}`
        );

      }

    }

    // ==================================================
    // SHOW MISSING PAIRS
    // ==================================================

    for (
      const pair of PAIRS
    ) {

      if (
        !symbolMap[pair]
      ) {

        console.log(
          `[DERIV] WARNING: No symbol mapping found for ${pair}`
        );

      }

    }

    console.log(
      `[DERIV] Successfully mapped ${Object.keys(symbolMap).length}/${PAIRS.length} forex pairs`
    );

    subscribeToPrices();

    startScanner();

  } catch (error) {

    console.log(
      "[DERIV] Active symbols error:",
      error.message
    );

    setTimeout(
      requestActiveSymbols,
      10000
    );

  }

}

// ======================================================
// PRICE SUBSCRIPTIONS
// ======================================================

async function subscribeToPrices() {

  for (
    const pair of PAIRS
  ) {

    const symbol =
      symbolMap[pair];

    if (!symbol) {

      continue;

    }

    try {

      ws.send(
        JSON.stringify({

          ticks:
            symbol,

          subscribe:
            1,

          req_id:
            nextRequestId()

        })
      );

      console.log(
        `[DERIV] Price stream started: ${pair} (${symbol})`
      );

    } catch (error) {

      console.log(
        `[DERIV] Tick subscription failed for ${pair}:`,
        error.message
      );

    }

    await sleep(250);

  }

}

// ======================================================
// GET CANDLES
// ======================================================

async function getCandles(
  pair,
  granularity,
  count = 150
) {

  const symbol =
    symbolMap[pair];

  if (!symbol) {

    throw new Error(
      `No Deriv symbol found for ${pair}`
    );

  }

  const response =
    await sendDerivRequest({

      ticks_history:
        symbol,

      end:
        "latest",

      count,

      style:
        "candles",

      granularity,

      subscribe:
        0

    });

  if (
    response.msg_type !==
      "candles" ||
    !Array.isArray(
      response.candles
    )
  ) {

    throw new Error(
      `No candle data returned for ${pair}`
    );

  }

  return response.candles.map(
    candle => ({

      epoch:
        Number(candle.epoch),

      open:
        Number(candle.open),

      high:
        Number(candle.high),

      low:
        Number(candle.low),

      close:
        Number(candle.close)

    })
  );

}

// ======================================================
// BUILD 12H FROM COMPLETED 1H CANDLES
// ======================================================

function build12HCandles(
  hourlyCandles
) {

  const groups = {};

  for (
    const candle of hourlyCandles
  ) {

    const epoch =
      Number(candle.epoch);

    const date =
      new Date(
        epoch * 1000
      );

    const hour =
      date.getUTCHours();

    const day =
      date.toISOString()
        .slice(0, 10);

    const block =
      hour < 12
        ? `${day}-00`
        : `${day}-12`;

    if (
      !groups[block]
    ) {

      groups[block] = [];

    }

    groups[block].push(
      candle
    );

  }

  const result = [];

  for (
    const key of Object.keys(
      groups
    )
  ) {

    const group =
      groups[key].sort(
        (a, b) =>
          a.epoch -
          b.epoch
      );

    // Only use complete 12H blocks
    if (
      group.length !== 12
    ) {

      continue;

    }

    result.push({

      epoch:
        group[0].epoch,

      open:
        group[0].open,

      high:
        Math.max(
          ...group.map(
            x => x.high
          )
        ),

      low:
        Math.min(
          ...group.map(
            x => x.low
          )
        ),

      close:
        group[
          group.length - 1
        ].close

    });

  }

  return result.sort(
    (a, b) =>
      a.epoch -
      b.epoch
  );

}

// ======================================================
// DETERMINE DIRECTION
// ======================================================

function determineDirection(
  candles
) {

  if (
    !candles ||
    candles.length < 4
  ) {

    return 0;

  }

  const recent =
    candles.slice(-5);

  let bullish = 0;

  let bearish = 0;

  for (
    let i = 1;
    i < recent.length;
    i++
  ) {

    if (
      recent[i].high >
        recent[i - 1].high &&
      recent[i].low >
        recent[i - 1].low
    ) {

      bullish++;

    }

    if (
      recent[i].high <
        recent[i - 1].high &&
      recent[i].low <
        recent[i - 1].low
    ) {

      bearish++;

    }

  }

  if (
    bullish >= 2
  ) {

    return 1;

  }

  if (
    bearish >= 2
  ) {

    return -1;

  }

  const last =
    candles[
      candles.length - 1
    ];

  if (
    last.close >
    last.open
  ) {

    return 1;

  }

  if (
    last.close <
    last.open
  ) {

    return -1;

  }

  return 0;

}

// ======================================================
// 1H STRUCTURE
// ======================================================

function determine1HStructure(
  candles
) {

  if (
    !candles ||
    candles.length < 6
  ) {

    return 0;

  }

  const recent =
    candles.slice(-6);

  let bullish = 0;

  let bearish = 0;

  for (
    let i = 1;
    i < recent.length;
    i++
  ) {

    if (
      recent[i].high >
        recent[i - 1].high &&
      recent[i].low >
        recent[i - 1].low
    ) {

      bullish++;

    }

    if (
      recent[i].high <
        recent[i - 1].high &&
      recent[i].low <
        recent[i - 1].low
    ) {

      bearish++;

    }

  }

  if (
    bullish >= 2
  ) {

    return 1;

  }

  if (
    bearish >= 2
  ) {

    return -1;

  }

  return determineDirection(
    candles
  );

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

  if (
    !candles ||
    candles.length <
      n * 2 + 1
  ) {

    return {
      highs,
      lows
    };

  }

  for (
    let i = n;
    i <
      candles.length - n;
    i++
  ) {

    let downFractal =
      true;

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

        downFractal =
          false;

        break;

      }

    }

    if (
      downFractal
    ) {

      highs.push({

        index:
          i,

        epoch:
          candles[i].epoch,

        price:
          candles[i].high

      });

    }

    let upFractal =
      true;

    for (
      let j = 1;
      j <= n;
      j++
    ) {

      if (
        candles[i - j].low <=
          candles[i].low ||
        candles[i + j].low <=
          candles[i].low
      ) {

        upFractal =
          false;

        break;

      }

    }

    if (
      upFractal
    ) {

      lows.push({

        index:
          i,

        epoch:
          candles[i].epoch,

        price:
          candles[i].low

      });

    }

  }

  return {
    highs,
    lows
  };

}

// ======================================================
// TREND LINE
// ======================================================

function calculateTrendLine(
  candles,
  direction
) {

  const fractals =
    findFractals(
      candles,
      FRACTAL_PERIODS
    );

  if (
    direction === 1
  ) {

    if (
      fractals.lows.length < 2
    ) {

      return {
        line: null,
        fractals
      };

    }

    const first =
      fractals.lows[
        fractals.lows.length - 2
      ];

    const second =
      fractals.lows[
        fractals.lows.length - 1
      ];

    if (
      second.epoch ===
      first.epoch
    ) {

      return {
        line: null,
        fractals
      };

    }

    const slope =
      (
        second.price -
        first.price
      ) /
      (
        second.epoch -
        first.epoch
      );

    return {

      line: {

        type:
          "support",

        first,

        second,

        slope

      },

      fractals

    };

  }

  if (
    direction === -1
  ) {

    if (
      fractals.highs.length < 2
    ) {

      return {
        line: null,
        fractals
      };

    }

    const first =
      fractals.highs[
        fractals.highs.length - 2
      ];

    const second =
      fractals.highs[
        fractals.highs.length - 1
      ];

    if (
      second.epoch ===
      first.epoch
    ) {

      return {
        line: null,
        fractals
      };

    }

    const slope =
      (
        second.price -
        first.price
      ) /
      (
        second.epoch -
        first.epoch
      );

    return {

      line: {

        type:
          "resistance",

        first,

        second,

        slope

      },

      fractals

    };

  }

  return {

    line:
      null,

    fractals

  };

}

// ======================================================
// TREND LINE PRICE
// ======================================================

function trendLinePrice(
  line,
  epoch
) {

  if (!line) {

    return null;

  }

  return (
    line.first.price +
    line.slope *
      (
        epoch -
        line.first.epoch
      )
  );

}

// ======================================================
// 15M TREND
// ======================================================

function detectTrend15M(
  candles
) {

  const direction =
    determineDirection(
      candles
    );

  if (
    direction === 1
  ) {

    return "BULLISH";

  }

  if (
    direction === -1
  ) {

    return "BEARISH";

  }

  return "WAIT";

}

// ======================================================
// 15M TRENDLINE BREAK
// ======================================================

function detectLineBreak(
  candles,
  line,
  direction
) {

  if (
    !line ||
    candles.length < 3
  ) {

    return false;

  }

  // Ignore currently forming candle
  const last =
    candles[
      candles.length - 2
    ];

  const lineValue =
    trendLinePrice(
      line,
      last.epoch
    );

  if (
    lineValue === null
  ) {

    return false;

  }

  if (
    direction === 1
  ) {

    return (
      last.close >
      lineValue
    );

  }

  if (
    direction === -1
  ) {

    return (
      last.close <
      lineValue
    );

  }

  return false;

}

// ======================================================
// RETEST + REJECTION
// ======================================================

function detectRetestRejection(
  candles,
  line,
  direction
) {

  if (
    !line ||
    candles.length < 5
  ) {

    return false;

  }

  const recent =
    candles.slice(-8);

  for (
    const candle of recent
  ) {

    const lineValue =
      trendLinePrice(
        line,
        candle.epoch
      );

    if (
      lineValue === null
    ) {

      continue;

    }

    const tolerance =
      Math.abs(
        candle.close *
        0.0005
      );

    const touched =
      candle.low <=
        lineValue +
          tolerance &&
      candle.high >=
        lineValue -
          tolerance;

    if (
      !touched
    ) {

      continue;

    }

    if (
      direction === 1
    ) {

      if (
        candle.close >
          lineValue &&
        candle.close >
          candle.open
      ) {

        return true;

      }

    }

    if (
      direction === -1
    ) {

      if (
        candle.close <
          lineValue &&
        candle.close <
          candle.open
      ) {

        return true;

      }

    }

  }

  return false;

}

// ======================================================
// 5M CONFIRMATION
// ======================================================

function confirm5M(
  candles,
  direction
) {

  if (
    !candles ||
    candles.length < 4
  ) {

    return false;

  }

  // Ignore current forming candle
  const last =
    candles[
      candles.length - 2
    ];

  const previous =
    candles[
      candles.length - 3
    ];

  if (
    direction === 1
  ) {

    return (
      last.close >
        last.open &&
      last.close >
        previous.high
    );

  }

  if (
    direction === -1
  ) {

    return (
      last.close <
        last.open &&
      last.close <
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
    !candles ||
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

    const tr =
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

      );

    trueRanges.push(
      tr
    );

  }

  const recent =
    trueRanges.slice(
      -period
    );

  if (
    !recent.length
  ) {

    return null;

  }

  return (
    recent.reduce(
      (
        sum,
        value
      ) =>
        sum + value,
      0
    ) /
    recent.length
  );

}

// ======================================================
// BUILD SIGNAL
// ======================================================

function buildSignal(
  pair,
  direction12H,
  structure1H,
  candles15M,
  candles5M
) {

  const state =
    pairStates[pair];

  state.signal =
    "WAIT";

  state.score =
    0;

  state.entry =
    null;

  state.sl =
    null;

  state.tp =
    null;

  state.lineBreak15M =
    false;

  state.retest15M =
    false;

  state.confirmation5M =
    false;

  // 12H and 1H required
  if (
    direction12H === 0 ||
    structure1H === 0
  ) {

    return;

  }

  // Must agree
  if (
    direction12H !==
    structure1H
  ) {

    return;

  }

  const direction =
    direction12H;

  const trend =
    detectTrend15M(
      candles15M
    );

  state.trend15M =
    trend;

  const trendLineData =
    calculateTrendLine(
      candles15M,
      direction
    );

  state.trendLine =
    trendLineData.line;

  if (
    trendLineData.fractals
      .highs.length
  ) {

    state.fractalHigh =
      trendLineData.fractals.highs[
        trendLineData.fractals.highs.length - 1
      ].price;

  }

  if (
    trendLineData.fractals
      .lows.length
  ) {

    state.fractalLow =
      trendLineData.fractals.lows[
        trendLineData.fractals.lows.length - 1
      ].price;

  }

  if (
    !trendLineData.line
  ) {

    return;

  }

  // 15M trend must agree
  if (
    (
      direction === 1 &&
      trend !== "BULLISH"
    ) ||
    (
      direction === -1 &&
      trend !== "BEARISH"
    )
  ) {

    return;

  }

  // 15M close beyond trendline
  const lineBreak =
    detectLineBreak(
      candles15M,
      trendLineData.line,
      direction
    );

  state.lineBreak15M =
    lineBreak;

  if (
    !lineBreak
  ) {

    return;

  }

  // Retest + rejection
  const retest =
    detectRetestRejection(
      candles15M,
      trendLineData.line,
      direction
    );

  state.retest15M =
    retest;

  if (
    !retest
  ) {

    return;

  }

  // 5M confirmation
  const confirmation =
    confirm5M(
      candles5M,
      direction
    );

  state.confirmation5M =
    confirmation;

  if (
    !confirmation
  ) {

    return;

  }

  const entry =
    Number(
      candles5M[
        candles5M.length - 2
      ].close
    );

  const atr =
    calculateATR(
      candles5M,
      14
    );

  if (
    !atr ||
    atr <= 0
  ) {

    return;

  }

  const digits =
    getDigits(pair);

  let sl;

  let tp;

  if (
    direction === 1
  ) {

    sl =
      entry - atr;

    tp =
      entry +
      atr * RR;

    state.signal =
      "BUY";

  } else {

    sl =
      entry + atr;

    tp =
      entry -
      atr * RR;

    state.signal =
      "SELL";

  }

  state.entry =
    roundPrice(
      entry,
      digits
    );

  state.sl =
    roundPrice(
      sl,
      digits
    );

  state.tp =
    roundPrice(
      tp,
      digits
    );

  state.score =
    5;

}

// ======================================================
// SCAN ONE PAIR
// ======================================================

async function scanPair(
  pair
) {

  const state =
    pairStates[pair];

  if (
    !symbolMap[pair]
  ) {

    state.error =
      "No Deriv symbol mapping";

    console.log(
      `[SCAN] ${pair} skipped: no Deriv symbol mapping`
    );

    return;

  }

  try {

    state.error =
      null;

    const candles1H =
      await getCandles(
        pair,
        TIMEFRAMES.H1,
        150
      );

    await sleep(200);

    const candles15M =
      await getCandles(
        pair,
        TIMEFRAMES.M15,
        200
      );

    await sleep(200);

    const candles5M =
      await getCandles(
        pair,
        TIMEFRAMES.M5,
        200
      );

    const candles12H =
      build12HCandles(
        candles1H
      );

    const direction12H =
      determineDirection(
        candles12H
      );

    const structure1H =
      determine1HStructure(
        candles1H
      );

    state.direction12H =
      direction12H === 1
        ? "BULLISH"
        : direction12H === -1
          ? "BEARISH"
          : "WAIT";

    state.structure1H =
      structure1H === 1
        ? "BULLISH"
        : structure1H === -1
          ? "BEARISH"
          : "WAIT";

    buildSignal(
      pair,
      direction12H,
      structure1H,
      candles15M,
      candles5M
    );

    state.lastUpdate =
      nowISO();

    console.log(

      `[SCAN] ${pair} | ` +
      `12H: ${state.direction12H} | ` +
      `1H: ${state.structure1H} | ` +
      `15M: ${state.trend15M} | ` +
      `Break: ${state.lineBreak15M} | ` +
      `Retest: ${state.retest15M} | ` +
      `5M: ${state.confirmation5M} | ` +
      `SIGNAL: ${state.signal}`

    );

    if (
      state.signal === "BUY" ||
      state.signal === "SELL"
    ) {

      await sendTelegramSignal(
        pair,
        state
      );

    }

  } catch (error) {

    state.error =
      error.message;

    console.log(
      `[SCAN] ${pair} ERROR: ${error.message}`
    );

  }

}

// ======================================================
// SCAN ALL PAIRS
// ======================================================

async function scanAllPairs() {

  if (
    scannerRunning ||
    !connected
  ) {

    return;

  }

  scannerRunning =
    true;

  console.log(
    "=========================================="
  );

  console.log(
    "[SCAN] Starting 10-pair scan"
  );

  for (
    const pair of PAIRS
  ) {

    if (
      !connected
    ) {

      break;

    }

    await scanPair(
      pair
    );

    await sleep(700);

  }

  lastScan =
    nowISO();

  scannerRunning =
    false;

  console.log(
    "[SCAN] Scan completed"
  );

  console.log(
    "=========================================="
  );

}

// ======================================================
// START SCANNER
// ======================================================

function startScanner() {

  if (
    global.scanInterval
  ) {

    return;

  }

  console.log(
    "[SCAN] Scanner started"
  );

  scanAllPairs();

  global.scanInterval =
    setInterval(
      scanAllPairs,
      SCAN_INTERVAL
    );

}

// ======================================================
// TELEGRAM
// ======================================================

async function sendTelegramSignal(
  pair,
  state
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {

    console.log(
      "[TELEGRAM] Telegram variables not configured"
    );

    return;

  }

  const emoji =
    state.signal === "BUY"
      ? "🟢"
      : "🔴";

  const message =

`${emoji} ${state.signal} SIGNAL

📊 Pair: ${pair}

🧭 12H Direction: ${state.direction12H}
📈 1H Structure: ${state.structure1H}
📐 15M Trend: ${state.trend15M}

✅ 15M Trendline Break
✅ Retest / Rejection
✅ 5M Confirmation

💰 Entry: ${state.entry}
🛑 SL: ${state.sl}
🎯 TP: ${state.tp}

⚖️ Risk/Reward: 1:${RR}

📡 Source: Deriv
📐 Strategy: Williams Fractal

Signal only — no automatic trade.`;

  try {

    const response =
      await fetch(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        {

          method:
            "POST",

          headers: {

            "Content-Type":
              "application/json"

          },

          body:
            JSON.stringify({

              chat_id:
                TELEGRAM_CHAT_ID,

              text:
                message

            })

        }
      );

    const result =
      await response.json();

    if (
      !result.ok
    ) {

      console.log(
        "[TELEGRAM] Error:",
        result.description
      );

    } else {

      console.log(
        `[TELEGRAM] ${pair} ${state.signal} sent`
      );

    }

  } catch (error) {

    console.log(
      "[TELEGRAM] Request error:",
      error.message
    );

  }

}

// ======================================================
// API STATUS
// ======================================================

app.get(
  "/api/status",
  (req, res) => {

    res.json({

      bot:
        "Deriv Forex Williams Fractal Bot",

      version:
        "2.1.0",

      source:
        "Deriv Public WebSocket",

      connected,

      scannerRunning,

      lastScan,

      pairCount:
        PAIRS.length,

      mappedPairs:
        Object.keys(
          symbolMap
        ).length,

      pairs:
        pairStates

    });

  }
);

// ======================================================
// HEALTH
// ======================================================

app.get(
  "/health",
  (req, res) => {

    res.json({

      status:
        connected
          ? "online"
          : "waiting",

      derivConnected:
        connected,

      mappedPairs:
        Object.keys(
          symbolMap
        ).length,

      totalPairs:
        PAIRS.length,

      lastScan

    });

  }
);

// ======================================================
// DASHBOARD
// ======================================================

app.get(
  "*",
  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        "public",
        "index.html"
      )
    );

  }
);

// ======================================================
// START SERVER
// ======================================================

app.listen(
  PORT,
  () => {

    console.log(
      "=========================================="
    );

    console.log(
      "DERIV FOREX WILLIAMS FRACTAL BOT"
    );

    console.log(
      "=========================================="
    );

    console.log(
      `[SERVER] Running on port ${PORT}`
    );

    console.log(
      `[SERVER] Total pairs: ${PAIRS.length}`
    );

    console.log(
      `[SERVER] Deriv endpoint: ${DERIV_WS_URL}`
    );

    console.log(
      "[SERVER] Strategy: 12H → 1H → 15M → Retest → 5M"
    );

    console.log(
      "=========================================="
    );

    connectDeriv();

  }
);

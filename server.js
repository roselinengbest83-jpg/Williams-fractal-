const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

/* =========================================================
   TELEGRAM
========================================================= */

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN || "";

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID || "";

/* =========================================================
   DERIV
========================================================= */

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/* =========================================================
   MARKETS
========================================================= */

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
  "NZD/USD",

  "EUR/GBP",
  "EUR/AUD",
  "EUR/CAD",
  "EUR/CHF",
  "GBP/AUD",
  "GBP/CAD",
  "GBP/CHF",
  "AUD/CAD",
  "AUD/CHF",
  "NZD/JPY",

  "USD/SGD",
  "USD/HKD",
  "USD/NOK",
  "USD/SEK",
  "USD/PLN",
  "EUR/NZD",
  "GBP/NZD",
  "CAD/JPY",
  "CHF/JPY",
  "NZD/CAD",

  "XAU/USD"
];

/* =========================================================
   STRATEGY SETTINGS
========================================================= */

const FRACTAL_PERIODS = 2;

const RISK_REWARD = 2;

const MAX_CANDLES = 150;

const SCAN_INTERVAL = 5 * 60 * 1000;

const MAX_BREAK_AGE_15M =
  60 * 60;

const MAX_RETEST_AGE_15M =
  60 * 60;

const RETEST_ATR_MULTIPLIER =
  0.35;

/* =========================================================
   DERIV STATE
========================================================= */

let derivWs = null;

let derivConnected = false;

let activeSymbolsLoaded = false;

let marketClosed = false;

let scannerRunning = false;

let lastScan = null;

let activeSymbols = [];

const symbolMap = {};

/* =========================================================
   PAIR STATE
========================================================= */

const state = {};

for (const pair of PAIRS) {
  state[pair] = {
    pair,

    status: "WAIT",

    error: null,

    signal: null,

    entry: null,

    stopLoss: null,

    takeProfit: null,

    direction12H: "WAIT",

    structure1H: "WAIT",

    trend15M: "WAIT",

    break15M: "WAIT",

    retest15M: "WAIT",

    confirm5M: "WAIT",

    setup: {
      active: false,
      direction: null,
      breakEpoch: null,
      line: null,
      linePrice: null,
      retestEpoch: null
    },

    lastAlertKey: null,

    lastUpdate: null
  };
}

/* =========================================================
   DERIV REQUEST SYSTEM
========================================================= */

let requestId = 1;

const pendingRequests = new Map();

function sendDerivRequest(payload) {
  return new Promise((resolve, reject) => {
    if (!derivWs || derivWs.readyState !== WebSocket.OPEN) {
      reject(
        new Error("Deriv WebSocket is not connected")
      );
      return;
    }

    const id = requestId++;

    const request = {
      ...payload,
      req_id: id
    };

    const timeout = setTimeout(() => {
      pendingRequests.delete(id);

      reject(
        new Error(
          "Deriv request timed out"
        )
      );
    }, 20000);

    pendingRequests.set(id, {
      resolve,
      reject,
      timeout
    });

    try {
      derivWs.send(
        JSON.stringify(request)
      );
    } catch (error) {
      clearTimeout(timeout);
      pendingRequests.delete(id);
      reject(error);
    }
  });
}

/* =========================================================
   SYMBOL NORMALIZATION
========================================================= */

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
    NZDUSD: "NZD/USD",

    EURGBP: "EUR/GBP",
    EURAUD: "EUR/AUD",
    EURCAD: "EUR/CAD",
    EURCHF: "EUR/CHF",

    GBPAUD: "GBP/AUD",
    GBPCAD: "GBP/CAD",
    GBPCHF: "GBP/CHF",

    AUDCAD: "AUD/CAD",
    AUDCHF: "AUD/CHF",

    NZDJPY: "NZD/JPY",

    USDSGD: "USD/SGD",
    USDHKD: "USD/HKD",
    USDNOK: "USD/NOK",
    USDSEK: "USD/SEK",
    USDPLN: "USD/PLN",

    EURNZD: "EUR/NZD",
    GBPNZD: "GBP/NZD",

    CADJPY: "CAD/JPY",
    CHFJPY: "CHF/JPY",

    NZDCAD: "NZD/CAD"
  };

  const values = [
    name,
    symbol
  ]
    .filter(Boolean)
    .map(value =>
      String(value)
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, "")
    );

  /*
   * Direct matching
   */

  for (const value of values) {
    if (aliases[value]) {
      return aliases[value];
    }

    if (value.startsWith("FRX")) {
      const forexCode =
        value.substring(3);

      if (aliases[forexCode]) {
        return aliases[forexCode];
      }
    }
  }

  /*
   * Flexible matching
   */

  for (const value of values) {
    for (const code of Object.keys(aliases)) {
      if (
        value === code ||
        value === `FRX${code}` ||
        value.includes(code)
      ) {
        return aliases[code];
      }
    }
  }

  /*
   * Gold
   */

  for (const value of values) {
    if (
      value.includes("XAUUSD") ||
      value.includes("GOLD") ||
      value.includes("XAU")
    ) {
      return "XAU/USD";
    }
  }

  return null;
}

/* =========================================================
   LOAD ACTIVE SYMBOLS
========================================================= */

async function loadActiveSymbols() {
  try {
    const response =
      await sendDerivRequest({
        active_symbols: "brief",
        product_type: "basic"
      });

    if (
      response.error &&
      response.error.message
    ) {
      throw new Error(
        response.error.message
      );
    }

    activeSymbols =
      response.active_symbols || [];

    activeSymbolsLoaded = true;

    console.log(
      `[DERIV] Received ${activeSymbols.length} active symbols`
    );

    /*
     * DEBUG:
     * Show symbols containing the currencies
     * that have been difficult to map.
     */

    console.log(
      "[DERIV] Searching for requested forex symbols..."
    );

    for (const item of activeSymbols) {
      const name = String(
        item.underlying_symbol_name ||
        item.display_name ||
        item.name ||
        ""
      ).toUpperCase();

      const symbol = String(
        item.underlying_symbol ||
        item.symbol ||
        ""
      ).toUpperCase();

      if (
        name.includes("NZD") ||
        name.includes("CAD") ||
        name.includes("NOK") ||
        name.includes("HKD")
      ) {
        console.log(
          `[DERIV DEBUG] name="${name}" symbol="${symbol}"`
        );
      }
    }

    /*
     * Clear previous mapping
     */

    for (const key of Object.keys(symbolMap)) {
      delete symbolMap[key];
    }

    /*
     * Build mapping
     */

    for (const item of activeSymbols) {
      const name =
        item.underlying_symbol_name ||
        item.display_name ||
        item.name ||
        "";

      const symbol =
        item.underlying_symbol ||
        item.symbol ||
        "";

      const pair =
        normalizeDerivPair(
          name,
          symbol
        );

      if (
        pair &&
        PAIRS.includes(pair) &&
        !symbolMap[pair]
      ) {
        symbolMap[pair] = symbol;

        console.log(
          `[DERIV] Mapped ${pair} → ${symbol}`
        );
      }
    }

    /*
     * Print missing markets
     */

    for (const pair of PAIRS) {
      if (!symbolMap[pair]) {
        console.log(
          `[DERIV] Not mapped: ${pair}`
        );
      }
    }

    console.log(
      `[DERIV] Successfully mapped ${Object.keys(symbolMap).length}/${PAIRS.length} markets`
    );

    return true;
  } catch (error) {
    console.error(
      "[DERIV] Active symbols error:",
      error.message
    );

    activeSymbolsLoaded = false;

    return false;
  }
}

/* =========================================================
   DERIV CONNECTION
========================================================= */

function connectDeriv() {
  console.log(
    "[DERIV] Connecting..."
  );

  try {
    derivWs =
      new WebSocket(
        DERIV_WS_URL
      );

    derivWs.on(
      "open",
      async () => {
        derivConnected = true;

        console.log(
          "[DERIV] WebSocket connected"
        );

        await loadActiveSymbols();
      }
    );

    derivWs.on(
      "message",
      rawMessage => {
        try {
          const response =
            JSON.parse(
              rawMessage.toString()
            );

          if (
            response.req_id &&
            pendingRequests.has(
              response.req_id
            )
          ) {
            const pending =
              pendingRequests.get(
                response.req_id
              );

            clearTimeout(
              pending.timeout
            );

            pendingRequests.delete(
              response.req_id
            );

            pending.resolve(
              response
            );
          }
        } catch (error) {
          console.error(
            "[DERIV] Message parse error:",
            error.message
          );
        }
      }
    );

    derivWs.on(
      "error",
      error => {
        derivConnected = false;

        console.error(
          "[DERIV] WebSocket error:",
          error.message
        );
      }
    );

    derivWs.on(
      "close",
      () => {
        derivConnected = false;

        console.log(
          "[DERIV] WebSocket closed"
        );

        setTimeout(
          connectDeriv,
          5000
        );
      }
    );
  } catch (error) {
    derivConnected = false;

    console.error(
      "[DERIV] Connection error:",
      error.message
    );

    setTimeout(
      connectDeriv,
      5000
    );
  }
}

/* =========================================================
   MARKET CLOSED
========================================================= */

function isMarketClosedError(message) {
  const text =
    String(message || "")
      .toLowerCase();

  return (
    text.includes(
      "market is presently closed"
    ) ||
    text.includes(
      "market will open"
    ) ||
    text.includes(
      "market closed"
    )
  );
}

/* =========================================================
   GET CANDLES
========================================================= */

async function getCandles(
  pair,
  granularity,
  count = MAX_CANDLES
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
    throw new Error(
      response.error.message
    );
  }

  if (!response.candles) {
    throw new Error(
      "No candle data returned by Deriv"
    );
  }

  return response.candles.map(
    candle => ({
      epoch: Number(
        candle.epoch
      ),

      open: Number(
        candle.open
      ),

      high: Number(
        candle.high
      ),

      low: Number(
        candle.low
      ),

      close: Number(
        candle.close
      )
    })
  );
}

/* =========================================================
   12H DIRECTION
========================================================= */

function get12HDirection(candles) {
  if (
    !candles ||
    candles.length < 3
  ) {
    return "WAIT";
  }

  const last =
    candles[candles.length - 1];

  const previous =
    candles[candles.length - 2];

  const older =
    candles[candles.length - 3];

  if (
    last.close > last.open &&
    last.close > previous.close &&
    previous.close >= older.close
  ) {
    return "BULLISH";
  }

  if (
    last.close < last.open &&
    last.close < previous.close &&
    previous.close <= older.close
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/* =========================================================
   1H STRUCTURE
========================================================= */

function get1HStructure(candles) {
  if (
    !candles ||
    candles.length < 20
  ) {
    return "WAIT";
  }

  const completed =
    candles.slice(0, -1);

  const recent =
    completed.slice(-20);

  const swingHighs = [];
  const swingLows = [];

  for (
    let i = 1;
    i < recent.length - 1;
    i++
  ) {
    const previous =
      recent[i - 1];

    const current =
      recent[i];

    const next =
      recent[i + 1];

    if (
      current.high >
        previous.high &&
      current.high >=
        next.high
    ) {
      swingHighs.push(
        current
      );
    }

    if (
      current.low <
        previous.low &&
      current.low <=
        next.low
    ) {
      swingLows.push(
        current
      );
    }
  }

  if (
    swingHighs.length >= 2 &&
    swingLows.length >= 2
  ) {
    const previousHigh =
      swingHighs[
        swingHighs.length - 2
      ];

    const latestHigh =
      swingHighs[
        swingHighs.length - 1
      ];

    const previousLow =
      swingLows[
        swingLows.length - 2
      ];

    const latestLow =
      swingLows[
        swingLows.length - 1
      ];

    const bullish =
      latestHigh.high >
        previousHigh.high &&
      latestLow.low >
        previousLow.low;

    const bearish =
      latestHigh.high <
        previousHigh.high &&
      latestLow.low <
        previousLow.low;

    if (bullish) {
      return "BULLISH";
    }

    if (bearish) {
      return "BEARISH";
    }
  }

  if (completed.length >= 7) {
    const last =
      completed[
        completed.length - 1
      ];

    const reference =
      completed[
        completed.length - 6
      ];

    if (
      last.close >
      reference.close
    ) {
      return "BULLISH";
    }

    if (
      last.close <
      reference.close
    ) {
      return "BEARISH";
    }
  }

  return "WAIT";
}

/* =========================================================
   WILLIAMS FRACTALS
========================================================= */

function getFractals(
  candles,
  n = FRACTAL_PERIODS
) {
  const upFractals = [];
  const downFractals = [];

  if (
    !candles ||
    candles.length <
      n * 2 + 1
  ) {
    return {
      upFractals,
      downFractals
    };
  }

  for (
    let i = n;
    i < candles.length - n;
    i++
  ) {
    const current =
      candles[i];

    let downFractal = true;

    let upFractal = true;

    for (
      let j = 1;
      j <= n;
      j++
    ) {
      if (
        candles[i - j].high >=
          current.high ||
        candles[i + j].high >=
          current.high
      ) {
        downFractal = false;
      }

      if (
        candles[i - j].low <=
          current.low ||
        candles[i + j].low <=
          current.low
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

/* =========================================================
   TREND LINE
========================================================= */

function calculateTrendLine(
  candles,
  direction
) {
  const {
    upFractals,
    downFractals
  } =
    getFractals(candles);

  /*
   * Bullish:
   * latest two DOWN fractals
   * form resistance line
   */

  if (
    direction === "BULLISH"
  ) {
    if (
      downFractals.length < 2
    ) {
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
      type: "RESISTANCE",
      first,
      second
    };
  }

  /*
   * Bearish:
   * latest two UP fractals
   * form support line
   */

  if (
    direction === "BEARISH"
  ) {
    if (
      upFractals.length < 2
    ) {
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
      type: "SUPPORT",
      first,
      second
    };
  }

  return null;
}

/* =========================================================
   TREND LINE PRICE
========================================================= */

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

/* =========================================================
   15M TREND
========================================================= */

function get15MTrend(
  candles,
  direction
) {
  if (
    !candles ||
    candles.length < 30
  ) {
    return {
      direction: "WAIT",
      line: null,
      linePrice: null
    };
  }

  const completed =
    candles.slice(0, -1);

  const line =
    calculateTrendLine(
      completed,
      direction
    );

  if (!line) {
    return {
      direction: "WAIT",
      line: null,
      linePrice: null
    };
  }

  const last =
    completed[
      completed.length - 1
    ];

  const linePrice =
    trendLinePrice(
      line,
      last.epoch
    );

  if (
    !Number.isFinite(
      linePrice
    )
  ) {
    return {
      direction: "WAIT",
      line,
      linePrice: null
    };
  }

  if (
    direction === "BULLISH"
  ) {
    return {
      direction:
        last.close >
        linePrice
          ? "BULLISH"
          : "WAIT",

      line,
      linePrice
    };
  }

  if (
    direction === "BEARISH"
  ) {
    return {
      direction:
        last.close <
        linePrice
          ? "BEARISH"
          : "WAIT",

      line,
      linePrice
    };
  }

  return {
    direction: "WAIT",
    line,
    linePrice
  };
}

/* =========================================================
   15M BREAK
========================================================= */

function check15MBreak(
  candles,
  direction
) {
  if (
    !candles ||
    candles.length < 30
  ) {
    return null;
  }

  const completed =
    candles.slice(0, -1);

  const line =
    calculateTrendLine(
      completed,
      direction
    );

  if (!line) {
    return null;
  }

  const last =
    completed[
      completed.length - 1
    ];

  const previous =
    completed[
      completed.length - 2
    ];

  const linePrice =
    trendLinePrice(
      line,
      last.epoch
    );

  if (
    !Number.isFinite(
      linePrice
    )
  ) {
    return null;
  }

  const previousLinePrice =
    trendLinePrice(
      line,
      previous.epoch
    );

  /*
   * Require an actual candle CLOSE
   * crossing the line.
   */

  if (
    direction === "BULLISH" &&
    previous.close <=
      previousLinePrice &&
    last.close >
      linePrice
  ) {
    return {
      confirmed: true,
      epoch: last.epoch,
      line,
      linePrice
    };
  }

  if (
    direction === "BEARISH" &&
    previous.close >=
      previousLinePrice &&
    last.close <
      linePrice
  ) {
    return {
      confirmed: true,
      epoch: last.epoch,
      line,
      linePrice
    };
  }

  return {
    confirmed: false,
    epoch: last.epoch,
    line,
    linePrice
  };
}

/* =========================================================
   ATR
========================================================= */

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
    ) /
    recent.length
  );
}

/* =========================================================
   RETEST / REJECTION
========================================================= */

function checkRetest(
  candles,
  direction,
  setup
) {
  if (
    !setup ||
    !setup.active ||
    !setup.breakEpoch ||
    !setup.line
  ) {
    return null;
  }

  const completed =
    candles.slice(0, -1);

  const afterBreak =
    completed.filter(
      candle =>
        candle.epoch >
        setup.breakEpoch
    );

  if (
    afterBreak.length === 0
  ) {
    return {
      confirmed: false,
      expired: false
    };
  }

  const last =
    afterBreak[
      afterBreak.length - 1
    ];

  const age =
    last.epoch -
    setup.breakEpoch;

  if (
    age >
    MAX_BREAK_AGE_15M
  ) {
    return {
      confirmed: false,
      expired: true,
      reason: "BREAK_TOO_OLD"
    };
  }

  const linePrice =
    trendLinePrice(
      setup.line,
      last.epoch
    );

  if (
    !Number.isFinite(
      linePrice
    )
  ) {
    return {
      confirmed: false,
      expired: false
    };
  }

  const atr =
    calculateATR(
      completed
    );

  const candleRange =
    Math.max(
      last.high -
        last.low,
      0.00001
    );

  const tolerance =
    atr
      ? atr *
        RETEST_ATR_MULTIPLIER
      : candleRange *
        0.35;

  if (
    direction ===
    "BULLISH"
  ) {
    const touchedLine =
      last.low <=
      linePrice +
        tolerance;

    const rejectedUp =
      last.close >
        last.open &&
      last.close >
        linePrice;

    if (
      touchedLine &&
      rejectedUp
    ) {
      return {
        confirmed: true,
        expired: false,
        epoch: last.epoch,
        linePrice
      };
    }
  }

  if (
    direction ===
    "BEARISH"
  ) {
    const touchedLine =
      last.high >=
      linePrice -
        tolerance;

    const rejectedDown =
      last.close <
        last.open &&
      last.close <
        linePrice;

    if (
      touchedLine &&
      rejectedDown
    ) {
      return {
        confirmed: true,
        expired: false,
        epoch: last.epoch,
        linePrice
      };
    }
  }

  return {
    confirmed: false,
    expired: false,
    epoch: last.epoch,
    linePrice
  };
}

/* =========================================================
   5M CONFIRMATION
========================================================= */

function check5MConfirmation(
  candles,
  direction,
  retestEpoch
) {
  if (
    !candles ||
    candles.length < 10 ||
    !retestEpoch
  ) {
    return {
      confirmed: false,
      epoch: null
    };
  }

  const completed =
    candles.slice(0, -1);

  const afterRetest =
    completed.filter(
      candle =>
        candle.epoch >
        retestEpoch
    );

  if (
    afterRetest.length === 0
  ) {
    return {
      confirmed: false,
      epoch: null
    };
  }

  const last =
    afterRetest[
      afterRetest.length - 1
    ];

  const lastIndex =
    completed.indexOf(
      last
    );

  if (
    lastIndex <= 0
  ) {
    return {
      confirmed: false,
      epoch: last.epoch
    };
  }

  const previous =
    completed[
      lastIndex - 1
    ];

  if (
    direction ===
    "BULLISH"
  ) {
    return {
      confirmed:
        last.close >
          last.open &&
        last.close >
          previous.high,

      epoch:
        last.epoch
    };
  }

  if (
    direction ===
    "BEARISH"
  ) {
    return {
      confirmed:
        last.close <
          last.open &&
        last.close <
          previous.low,

      epoch:
        last.epoch
    };
  }

  return {
    confirmed: false,
    epoch: last.epoch
  };
}

/* =========================================================
   PRICE PRECISION
========================================================= */

function getDecimals(pair) {
  if (
    pair === "XAU/USD"
  ) {
    return 2;
  }

  if (
    pair.includes("JPY")
  ) {
    return 3;
  }

  return 5;
}

function roundPrice(
  value,
  pair
) {
  return Number(
    Number(value).toFixed(
      getDecimals(pair)
    )
  );
}

/* =========================================================
   SIGNAL BUILDER
========================================================= */

function buildSignal(
  pair,
  direction,
  candles5M
) {
  const completed5M =
    candles5M.slice(0, -1);

  if (
    completed5M.length < 2
  ) {
    return null;
  }

  const last =
    completed5M[
      completed5M.length - 1
    ];

  const atr =
    calculateATR(
      candles5M
    );

  if (!atr) {
    return null;
  }

  const entry =
    last.close;

  let stopLoss;
  let takeProfit;

  if (
    direction ===
    "BULLISH"
  ) {
    stopLoss =
      last.low -
      atr * 0.20;

    const risk =
      entry -
      stopLoss;

    if (
      risk <= 0
    ) {
      return null;
    }

    takeProfit =
      entry +
      risk *
        RISK_REWARD;
  } else if (
    direction ===
    "BEARISH"
  ) {
    stopLoss =
      last.high +
      atr * 0.20;

    const risk =
      stopLoss -
      entry;

    if (
      risk <= 0
    ) {
      return null;
    }

    takeProfit =
      entry -
      risk *
        RISK_REWARD;
  } else {
    return null;
  }

  return {
    pair,

    direction,

    entry:
      roundPrice(
        entry,
        pair
      ),

    stopLoss:
      roundPrice(
        stopLoss,
        pair
      ),

    takeProfit:
      roundPrice(
        takeProfit,
        pair
      ),

    riskReward:
      RISK_REWARD,

    timeframe:
      "5M",

    strategy:
      "12H → 1H → 15M Williams Fractal → Break → Retest → 5M"
  };
}

/* =========================================================
   TELEGRAM
========================================================= */

async function sendTelegram(
  message
) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    console.log(
      "[TELEGRAM] Credentials not configured"
    );

    return;
  }

  try {
    const url =
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

    const response =
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

            text: message
          })
        }
      );

    if (!response.ok) {
      console.error(
        "[TELEGRAM] HTTP error:",
        response.status
      );
    } else {
      console.log(
        "[TELEGRAM] Signal sent"
      );
    }
  } catch (error) {
    console.error(
      "[TELEGRAM] Error:",
      error.message
    );
  }
}

/* =========================================================
   SIGNAL MESSAGE
========================================================= */

function formatSignal(
  signal
) {
  const direction =
    signal.direction ===
    "BULLISH"
      ? "BUY"
      : "SELL";

  const isGold =
    signal.pair ===
    "XAU/USD";

  return `
🚨 ${isGold ? "GOLD" : "FOREX"} SIGNAL

Pair: ${signal.pair}

Direction: ${direction}

Entry: ${signal.entry}

SL: ${signal.stopLoss}

TP: ${signal.takeProfit}

Risk/Reward: 1:${signal.riskReward}

Timeframe: 5M

Strategy:
12H Direction ↓
1H Structure ↓
15M Williams Fractal Trend Line ↓
15M Candle Close Break ↓
15M Retest / Rejection ↓
5M Confirmation

Signal only — no automatic trade.
`;
}

/* =========================================================
   CLEAR SETUP
========================================================= */

function clearSetup(
  currentState
) {
  currentState.setup = {
    active: false,
    direction: null,
    breakEpoch: null,
    line: null,
    linePrice: null,
    retestEpoch: null
  };
}

/* =========================================================
   RESET PAIR FOR SCAN
========================================================= */

function resetPairForScan(
  currentState
) {
  currentState.status =
    "SCANNING";

  currentState.signal =
    null;

  currentState.entry =
    null;

  currentState.stopLoss =
    null;

  currentState.takeProfit =
    null;

  currentState.error =
    null;

  currentState.direction12H =
    "WAIT";

  currentState.structure1H =
    "WAIT";

  currentState.trend15M =
    "WAIT";

  currentState.break15M =
    "WAIT";

  currentState.retest15M =
    "WAIT";

  currentState.confirm5M =
    "WAIT";

  currentState.lastUpdate =
    new Date().toISOString();
}

/* =========================================================
   SCAN ONE PAIR
========================================================= */

async function scanPair(
  pair
) {
  const currentState =
    state[pair];

  resetPairForScan(
    currentState
  );

  /*
   * SYMBOL CHECK
   */

  if (!symbolMap[pair]) {
    currentState.status =
      "WAIT";

    currentState.error =
      "SYMBOL NOT MAPPED";

    console.log(
      `[SCAN] ${pair} | STATUS: WAIT — SYMBOL NOT MAPPED`
    );

    return;
  }

  try {
    /*
     * 1H DATA
     */

    const candles1H =
      await getCandles(
        pair,
        3600
      );

    /*
     * 12H DATA
     *
     * Build 12H candles from
     * completed 1H candles.
     */

    const completed1H =
      candles1H.slice(0, -1);

    const candles12H = [];

    for (
      let i = 0;
      i + 12 <=
        completed1H.length;
      i += 12
    ) {
      const group =
        completed1H.slice(
          i,
          i + 12
        );

      if (
        group.length !== 12
      ) {
        continue;
      }

      candles12H.push({
        epoch:
          group[0].epoch,

        open:
          group[0].open,

        high:
          Math.max(
            ...group.map(
              candle =>
                candle.high
            )
          ),

        low:
          Math.min(
            ...group.map(
              candle =>
                candle.low
            )
          ),

        close:
          group[
            group.length - 1
          ].close
      });
    }

    /*
     * 12H DIRECTION
     */

    const direction12H =
      get12HDirection(
        candles12H
      );

    currentState.direction12H =
      direction12H;

    if (
      direction12H ===
      "WAIT"
    ) {
      currentState.status =
        "WAIT";

      console.log(
        `[SCAN] ${pair} | STATUS: WAIT — 12H DIRECTION`
      );

      return;
    }

    /*
     * 1H STRUCTURE
     */

    const structure1H =
      get1HStructure(
        candles1H
      );

    currentState.structure1H =
      structure1H;

    if (
      structure1H !==
      direction12H
    ) {
      currentState.status =
        "WAIT";

      console.log(
        `[SCAN] ${pair} | STATUS: WAIT — 1H STRUCTURE ${structure1H}`
      );

      return;
    }

    /*
     * 15M DATA
     */

    const candles15M =
      await getCandles(
        pair,
        900
      );

    /*
     * 15M TREND
     */

    const trend15M =
      get15MTrend(
        candles15M,
        direction12H
      );

    currentState.trend15M =
      trend15M.direction;

    /*
     * -----------------------------------------------------
     * EXISTING SETUP
     * -----------------------------------------------------
     */

    if (
      currentState.setup &&
      currentState.setup.active
    ) {
      /*
       * Check retest
       */

      const retest =
        checkRetest(
          candles15M,
          direction12H,
          currentState.setup
        );

      if (
        retest &&
        retest.expired
      ) {
        console.log(
          `[SCAN] ${pair} | Retest expired`
        );

        clearSetup(
          currentState
        );

        currentState.retest15M =
          "WAIT";

        currentState.confirm5M =
          "WAIT";
      } else if (
        retest &&
        retest.confirmed
      ) {
        currentState.retest15M =
          direction12H;

        currentState.setup.retestEpoch =
          retest.epoch;

        console.log(
          `[SCAN] ${pair} | 15M RETEST CONFIRMED`
        );

        /*
         * 5M DATA
         */

        const candles5M =
          await getCandles(
            pair,
            300
          );

        const confirmation5M =
          check5MConfirmation(
            candles5M,
            direction12H,
            retest.epoch
          );

        currentState.confirm5M =
          confirmation5M.confirmed
            ? direction12H
            : "WAIT";

        if (
          confirmation5M.confirmed
        ) {
          const signal =
            buildSignal(
              pair,
              direction12H,
              candles5M
            );

          if (signal) {
            currentState.signal =
              signal;

            currentState.entry =
              signal.entry;

            currentState.stopLoss =
              signal.stopLoss;

            currentState.takeProfit =
              signal.takeProfit;

            currentState.status =
              direction12H ===
              "BULLISH"
                ? "BUY"
                : "SELL";

            const alertKey =
              `${pair}|${direction12H}|${currentState.setup.retestEpoch}|${confirmation5M.epoch}`;

            if (
              currentState.lastAlertKey !==
              alertKey
            ) {
              currentState.lastAlertKey =
                alertKey;

              await sendTelegram(
                formatSignal(
                  signal
                )
              );
            }

            console.log(
              `[SCAN] ${pair} | SIGNAL: ${currentState.status}`
            );

            clearSetup(
              currentState
            );

            return;
          }
        }

        currentState.status =
          "WAIT";

        return;
      }

      currentState.status =
        "WAIT";

      return;
    }

    /*
     * -----------------------------------------------------
     * LOOK FOR NEW 15M BREAK
     * -----------------------------------------------------
     */

    const break15M =
      check15MBreak(
        candles15M,
        direction12H
      );

    if (!break15M) {
      currentState.status =
        "WAIT";

      return;
    }

    currentState.break15M =
      break15M.confirmed
        ? direction12H
        : "WAIT";

    if (
      break15M.confirmed
    ) {
      currentState.setup = {
        active: true,

        direction:
          direction12H,

        breakEpoch:
          break15M.epoch,

        line:
          break15M.line,

        linePrice:
          break15M.linePrice,

        retestEpoch: null
      };

      console.log(
        `[SCAN] ${pair} | 15M BREAK CONFIRMED — WAITING FOR RETEST`
      );

      currentState.status =
        "WAIT";

      return;
    }

    currentState.status =
      "WAIT";
  } catch (error) {
    const message =
      error.message ||
      String(error);

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
        `[SCAN] ${pair}: MARKET CLOSED`
      );

      return;
    }

    currentState.status =
      "ERROR";

    currentState.error =
      message;

    console.error(
      `[SCAN] ${pair} ERROR:`,
      message
    );
  }
}

/* =========================================================
   SCAN ALL PAIRS
========================================================= */

async function scanAllPairs() {
  if (scannerRunning) {
    console.log(
      "[SCAN] Scanner already running"
    );

    return;
  }

  if (!derivConnected) {
    console.log(
      "[SCAN] Deriv not connected"
    );

    return;
  }

  if (!activeSymbolsLoaded) {
    console.log(
      "[SCAN] Active symbols not loaded"
    );

    await loadActiveSymbols();
  }

  scannerRunning = true;

  marketClosed = false;

  lastScan =
    new Date().toISOString();

  console.log(
    `[SCAN] Starting scan of ${PAIRS.length} markets`
  );

  try {
    for (
      const pair of PAIRS
    ) {
      console.log(
        `[SCAN] Checking ${pair}`
      );

      await scanPair(
        pair
      );

      /*
       * Small delay so we don't
       * hammer the Deriv connection.
       */

      await new Promise(
        resolve =>
          setTimeout(
            resolve,
            150
          )
      );
    }
  } catch (error) {
    console.error(
      "[SCAN] Global error:",
      error.message
    );
  } finally {
    scannerRunning = false;

    lastScan =
      new Date().toISOString();

    console.log(
      "[SCAN] Scan completed"
    );
  }
}

/* =========================================================
   API STATUS
========================================================= */

app.get(
  "/api/status",
  (req, res) => {
    res.json({
      success: true,

      system: {
        derivConnected,

        activeSymbolsLoaded,

        marketClosed,

        scannerRunning,

        lastScan,

        pairCount:
          PAIRS.length,

        forexPairs: 30,

        goldPairs: 1,

        mappedPairs:
          Object.keys(
            symbolMap
          ).length
      },

      pairs: state
    });
  }
);

/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/health",
  (req, res) => {
    res.json({
      status: "ok",

      derivConnected,

      activeSymbolsLoaded,

      marketClosed,

      scannerRunning,

      pairCount:
        PAIRS.length,

      mappedPairs:
        Object.keys(
          symbolMap
        ).length,

      time:
        new Date().toISOString()
    });
  }
);

/* =========================================================
   MANUAL SCAN
========================================================= */

app.post(
  "/api/scan",
  (req, res) => {
    if (scannerRunning) {
      return res.json({
        success: false,

        message:
          "Scanner already running"
      });
    }

    scanAllPairs();

    res.json({
      success: true,

      message:
        "Scan started"
    });
  }
);

/* =========================================================
   SYMBOL API
========================================================= */

app.get(
  "/api/symbols",
  (req, res) => {
    res.json({
      success: true,

      total:
        PAIRS.length,

      mapped:
        Object.keys(
          symbolMap
        ).length,

      symbols:
        symbolMap
    });
  }
);

/* =========================================================
   DASHBOARD
========================================================= */

app.get(
  "/",
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

/* =========================================================
   START SERVER
========================================================= */

app.listen(
  PORT,
  () => {
    console.log(
      "=============================================="
    );

    console.log(
      `Server running on port ${PORT}`
    );

    console.log(
      `Watching ${PAIRS.length} markets`
    );

    console.log(
      "Forex pairs: 30"
    );

    console.log(
      "Gold: XAU/USD"
    );

    console.log(
      "Strategy: 12H → 1H → 15M Fractal → Break → Retest → 5M"
    );

    console.log(
      "Risk/Reward: 1:2"
    );

    console.log(
      "Signal only — no automatic trading"
    );

    console.log(
      "=============================================="
    );

    connectDeriv();

    /*
     * First scan after connection
     */

    setTimeout(
      () => {
        scanAllPairs();
      },
      10000
    );

    /*
     * Automatic scan every 5 minutes
     */

    setInterval(
      () => {
        scanAllPairs();
      },
      SCAN_INTERVAL
    );
  }
);

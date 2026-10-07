const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/* =========================================================
   SETTINGS
========================================================= */

const RISK_REWARD = 2;

const FRACTAL_PERIODS = 2;

const MAX_CANDLES = 150;

const SCAN_INTERVAL_MS = 5 * 60 * 1000;

// A breakout must be retested within 2 hours.
const MAX_BREAK_AGE_15M = 2 * 60 * 60;

// 5M confirmation must happen within 30 minutes of retest.
const MAX_CONFIRM_AGE_5M = 30 * 60;

// Tight tolerance around the ORIGINAL broken trendline.
const RETEST_ATR_MULTIPLIER = 0.20;

const SIGNAL_SCORE_THRESHOLD = 8;

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
   TIMEFRAMES
========================================================= */

const M5 = 300;
const M15 = 900;
const H1 = 3600;

/* =========================================================
   STATE
========================================================= */

const symbolMap = {};

let activeSymbols = [];

let derivSocket = null;

let derivConnected = false;

let requestId = 1;

const pendingRequests = new Map();

let scanRunning = false;

let lastScanTime = null;

/* =========================================================
   EMPTY SETUP
========================================================= */

function emptySetup() {
  return {
    active: false,

    direction: null,

    phase: "WAIT",

    breakEpoch: null,

    breakIndex: null,

    breakType: null,

    line: null,

    linePrice: null,

    retestEpoch: null,

    retestLinePrice: null,

    confirm5MEpoch: null
  };
}

/* =========================================================
   PAIR STATE
========================================================= */

const pairStates = {};

for (const pair of PAIRS) {
  pairStates[pair] = {
    pair,

    status: "WAIT",

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

    score: 0,

    setup: emptySetup(),

    error: null,

    lastUpdate: null,

    lastSignalEpoch: null,

    lastProcessedBreakEpoch: null
  };
}

/* =========================================================
   HELPERS
========================================================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getDecimals(pair) {
  if (pair === "XAU/USD") return 2;

  if (pair.includes("JPY")) return 3;

  return 5;
}

function roundPrice(value, pair) {
  return Number(
    Number(value).toFixed(getDecimals(pair))
  );
}

function normalizeCode(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

/* =========================================================
   DERIV SYMBOL MAPPING
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

  const values = [name, symbol]
    .filter(Boolean)
    .map(normalizeCode);

  /* -------------------------
     NORMAL FOREX
  ------------------------- */

  for (const value of values) {
    if (aliases[value]) {
      return aliases[value];
    }

    if (
      value.startsWith("FRX") &&
      aliases[value.substring(3)]
    ) {
      return aliases[value.substring(3)];
    }
  }

  /* -------------------------
     GOLD

     IMPORTANT:
     Do NOT broadly match every
     symbol containing "GOLD".

     We only accept proper
     XAU/USD style symbols.
  ------------------------- */

  for (const value of values) {
    if (
      value === "XAUUSD" ||
      value === "FRXXAUUSD" ||
      value === "GOLDUSD" ||
      value === "FRXGOLDUSD"
    ) {
      return "XAU/USD";
    }
  }

  const textName = String(name || "").toUpperCase();

  if (
    /XAU\s*\/?\s*USD/.test(textName) ||
    /GOLD\s*\/?\s*USD/.test(textName)
  ) {
    return "XAU/USD";
  }

  return null;
}

/* =========================================================
   DERIV REQUEST
========================================================= */

function sendDerivRequest(request) {
  return new Promise((resolve, reject) => {
    if (
      !derivSocket ||
      derivSocket.readyState !== WebSocket.OPEN
    ) {
      reject(new Error("Deriv WebSocket is not connected"));
      return;
    }

    const reqId = requestId++;

    const timeout = setTimeout(() => {
      pendingRequests.delete(reqId);

      reject(
        new Error("Deriv request timeout")
      );
    }, 15000);

    pendingRequests.set(reqId, {
      resolve,
      reject,
      timeout
    });

    derivSocket.send(
      JSON.stringify({
        ...request,
        req_id: reqId
      })
    );
  });
}

/* =========================================================
   LOAD ACTIVE SYMBOLS
========================================================= */

async function loadActiveSymbols() {
  try {
    const response = await sendDerivRequest({
      active_symbols: "brief"
    });

    if (response.error) {
      console.error(
        "[DERIV] Active symbols error:",
        response.error.message
      );

      return false;
    }

    activeSymbols = response.active_symbols || [];

    console.log(
      `[DERIV] Received ${activeSymbols.length} active symbols`
    );

    /* Clear previous mapping */

    for (const pair of PAIRS) {
      delete symbolMap[pair];
    }

    /* Map symbols */

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

      const pair = normalizeDerivPair(
        name,
        symbol
      );

      if (pair && PAIRS.includes(pair)) {
        if (!symbolMap[pair]) {
          symbolMap[pair] = symbol;
        }
      }
    }

    const mapped = PAIRS.filter(
      pair => symbolMap[pair]
    );

    console.log(
      `[DERIV] Mapping complete: ${mapped.length}/${PAIRS.length}`
    );

    console.log("[DERIV] MAPPED:");

    for (const pair of mapped) {
      console.log(
        `  ${pair} -> ${symbolMap[pair]}`
      );
    }

    const notMapped = PAIRS.filter(
      pair => !symbolMap[pair]
    );

    if (notMapped.length) {
      console.log("[DERIV] NOT MAPPED:");

      for (const pair of notMapped) {
        console.log(`  ${pair}`);
      }
    }

    /* Gold debugging */

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
        name.includes("XAU") ||
        name.includes("GOLD") ||
        symbol.includes("XAU") ||
        symbol.includes("GOLD")
      ) {
        console.log(
          `[DERIV GOLD DEBUG] name="${name}" symbol="${symbol}"`
        );
      }
    }

    return mapped.length > 0;

  } catch (error) {
    console.error(
      "[DERIV] Symbol loading failed:",
      error.message
    );

    return false;
  }
}

/* =========================================================
   CONNECT DERIV
========================================================= */

function connectDeriv() {
  return new Promise(resolve => {
    if (
      derivSocket &&
      derivSocket.readyState === WebSocket.OPEN
    ) {
      resolve(true);
      return;
    }

    console.log(
      "[DERIV] Connecting..."
    );

    derivSocket = new WebSocket(
      DERIV_WS_URL
    );

    derivSocket.on("open", async () => {
      console.log(
        "[DERIV] Connected"
      );

      derivConnected = true;

      try {
        await loadActiveSymbols();

        resolve(true);

      } catch (error) {
        console.error(
          "[DERIV] Startup error:",
          error.message
        );

        resolve(false);
      }
    });

    derivSocket.on("message", raw => {
      try {
        const data =
          JSON.parse(raw.toString());

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

            pending.resolve(data);
          }
        }

      } catch (error) {
        console.error(
          "[DERIV] Message parse error:",
          error.message
        );
      }
    });

    derivSocket.on("close", () => {
      derivConnected = false;

      console.log(
        "[DERIV] Connection closed"
      );

      setTimeout(() => {
        connectDeriv().catch(() => {});
      }, 5000);
    });

    derivSocket.on("error", error => {
      console.error(
        "[DERIV] WebSocket error:",
        error.message
      );

      derivConnected = false;
    });
  });
}

/* =========================================================
   GET CANDLES
========================================================= */

async function getCandles(
  pair,
  granularity,
  count = MAX_CANDLES
) {
  const symbol = symbolMap[pair];

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

  const candles =
    response.candles.map(
      candle => ({
        epoch: Number(candle.epoch),
        open: Number(candle.open),
        high: Number(candle.high),
        low: Number(candle.low),
        close: Number(candle.close)
      })
    );

  /* Useful XAU diagnostic */

  if (
    pair === "XAU/USD" &&
    candles.length
  ) {
    const latest =
      candles[candles.length - 1];

    console.log(
      `[XAU/USD] Symbol=${symbol} Latest=${latest.close}`
    );
  }

  return candles;
}

/* =========================================================
   BUILD 12H CANDLES FROM 1H
========================================================= */

function build12HCandles(
  candles1H
) {
  if (
    !candles1H ||
    candles1H.length < 12
  ) {
    return [];
  }

  /*
    Remove the currently forming
    1H candle.
  */

  const completed1H =
    candles1H.slice(0, -1);

  const groups = {};

  for (const candle of completed1H) {
    const date =
      new Date(
        candle.epoch * 1000
      );

    const year =
      date.getUTCFullYear();

    const month =
      date.getUTCMonth();

    const day =
      date.getUTCDate();

    const hour =
      date.getUTCHours();

    const halfDay =
      hour < 12 ? 0 : 12;

    const key =
      `${year}-${month}-${day}-${halfDay}`;

    if (!groups[key]) {
      groups[key] = [];
    }

    groups[key].push(candle);
  }

  const result = [];

  for (const key of Object.keys(groups)) {
    const group =
      groups[key];

    if (group.length < 12) {
      continue;
    }

    group.sort(
      (a, b) =>
        a.epoch - b.epoch
    );

    let consecutive = true;

    for (
      let i = 1;
      i < group.length;
      i++
    ) {
      if (
        group[i].epoch -
          group[i - 1].epoch !==
        3600
      ) {
        consecutive = false;
        break;
      }
    }

    if (!consecutive) {
      continue;
    }

    const first =
      group[0];

    const last =
      group[group.length - 1];

    result.push({
      epoch: first.epoch,

      open: first.open,

      high: Math.max(
        ...group.map(
          c => c.high
        )
      ),

      low: Math.min(
        ...group.map(
          c => c.low
        )
      ),

      close: last.close
    });
  }

  result.sort(
    (a, b) =>
      a.epoch - b.epoch
  );

  return result;
}

/* =========================================================
   12H DIRECTION
========================================================= */

function get12HDirection(
  candles
) {
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

  /*
    Strong bullish structure.
  */

  if (
    last.close > last.open &&
    last.close > previous.close &&
    previous.close >= older.close
  ) {
    return "BULLISH";
  }

  /*
    Strong bearish structure.
  */

  if (
    last.close < last.open &&
    last.close < previous.close &&
    previous.close <= older.close
  ) {
    return "BEARISH";
  }

  /*
    Normal directional bias.
  */

  if (
    last.close >
    previous.close
  ) {
    return "BULLISH";
  }

  if (
    last.close <
    previous.close
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/* =========================================================
   1H STRUCTURE
========================================================= */

function get1HStructure(
  candles
) {
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

  /*
    Softer directional fallback.
  */

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
    i <
      candles.length - n;
    i++
  ) {
    const current =
      candles[i];

    let downFractal = true;

    let upFractal = true;

    /*
      DOWN FRACTAL:
      middle candle has highest high.
    */

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
    }

    /*
      UP FRACTAL:
      middle candle has lowest low.
    */

    for (
      let j = 1;
      j <= n;
      j++
    ) {
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

        epoch:
          current.epoch,

        price:
          current.high
      });
    }

    if (upFractal) {
      upFractals.push({
        index: i,

        epoch:
          current.epoch,

        price:
          current.low
      });
    }
  }

  return {
    upFractals,
    downFractals
  };
}

/* =========================================================
   CALCULATE TRENDLINE
========================================================= */

function calculateTrendLine(
  candles,
  direction
) {
  const {
    upFractals,
    downFractals
  } = getFractals(candles);

  /*
    BULLISH:
    Use descending fractal highs
    as resistance.
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

    if (
      second.price >=
      first.price
    ) {
      return null;
    }

    return {
      type: "RESISTANCE",

      first,

      second
    };
  }

  /*
    BEARISH:
    Use ascending fractal lows
    as support.
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

    if (
      second.price <=
      first.price
    ) {
      return null;
    }

    return {
      type: "SUPPORT",

      first,

      second
    };
  }

  return null;
}

/* =========================================================
   TRENDLINE PRICE
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
    ) / recent.length
  );
}

/* =========================================================
   FIND 15M BREAK
========================================================= */

/*
   IMPORTANT:

   This function looks backwards through
   recently completed candles.

   The trendline is calculated ONLY from
   fractals that were already confirmed
   BEFORE the breakout candle.

   This prevents future candles from
   changing the original trendline.
*/

function findLatest15MBreak(
  candles,
  direction
) {
  if (
    !candles ||
    candles.length < 40
  ) {
    return null;
  }

  const completed =
    candles.slice(0, -1);

  const now =
    Math.floor(
      Date.now() / 1000
    );

  const start =
    Math.max(
      2,
      completed.length - 40
    );

  let latestBreak = null;

  for (
    let i = start;
    i < completed.length;
    i++
  ) {
    const previous =
      completed[i - 1];

    const current =
      completed[i];

    /*
      Ignore very old breakouts.
    */

    if (
      now - current.epoch >
      MAX_BREAK_AGE_15M
    ) {
      continue;
    }

    /*
      IMPORTANT:
      Trendline is built using candles
      available at the time of the break.
    */

    const history =
      completed.slice(
        0,
        i + 1
      );

    const line =
      calculateTrendLine(
        history,
        direction
      );

    if (!line) {
      continue;
    }

    const previousLinePrice =
      trendLinePrice(
        line,
        previous.epoch
      );

    const currentLinePrice =
      trendLinePrice(
        line,
        current.epoch
      );

    if (
      !Number.isFinite(
        previousLinePrice
      ) ||
      !Number.isFinite(
        currentLinePrice
      )
    ) {
      continue;
    }

    /*
      BULLISH BREAK

      Previous candle stayed below/
      at resistance.

      Current COMPLETED candle
      CLOSED above it.
    */

    if (
      direction === "BULLISH" &&
      previous.close <=
        previousLinePrice &&
      current.close >
        currentLinePrice
    ) {
      latestBreak = {
        direction: "BULLISH",

        epoch:
          current.epoch,

        index: i,

        line,

        linePrice:
          currentLinePrice,

        type:
          "RESISTANCE_BREAK"
      };
    }

    /*
      BEARISH BREAK

      Previous candle stayed above/
      at support.

      Current COMPLETED candle
      CLOSED below it.
    */

    if (
      direction === "BEARISH" &&
      previous.close >=
        previousLinePrice &&
      current.close <
        currentLinePrice
    ) {
      latestBreak = {
        direction: "BEARISH",

        epoch:
          current.epoch,

        index: i,

        line,

        linePrice:
          currentLinePrice,

        type:
          "SUPPORT_BREAK"
      };
    }
  }

  return latestBreak;
}

/* =========================================================
   FIND RETEST OF ORIGINAL LINE
========================================================= */

/*
   The trendline is NOT recalculated here.

   We use:

       setup.line

   which is the exact line that was
   broken.

   This is the important retest correction.
*/

function find15MRetest(
  candles,
  setup
) {
  if (
    !setup ||
    !setup.active ||
    !setup.breakEpoch ||
    !setup.line
  ) {
    return {
      confirmed: false,
      expired: false
    };
  }

  const completed =
    candles.slice(0, -1);

  const afterBreak =
    completed.filter(
      candle =>
        candle.epoch >
        setup.breakEpoch
    );

  if (!afterBreak.length) {
    return {
      confirmed: false,
      expired: false
    };
  }

  const latest =
    afterBreak[
      afterBreak.length - 1
    ];

  const age =
    latest.epoch -
    setup.breakEpoch;

  if (
    age >
    MAX_BREAK_AGE_15M
  ) {
    return {
      confirmed: false,

      expired: true,

      reason:
        "BREAK_TOO_OLD"
    };
  }

  const atr =
    calculateATR(
      completed
    );

  const tolerance =
    atr
      ? atr *
        RETEST_ATR_MULTIPLIER
      : 0;

  /*
    Check every completed 15M
    candle after the breakout.

    This prevents the scanner from
    missing a retest that happened
    between scans.
  */

  for (
    const candle of afterBreak
  ) {
    const linePrice =
      trendLinePrice(
        setup.line,
        candle.epoch
      );

    if (
      !Number.isFinite(
        linePrice
      )
    ) {
      continue;
    }

    /*
      BUY RETEST

      Price returns to the SAME
      broken resistance.

      Wick can touch/penetrate line.

      Candle must close bullish
      and back above line.
    */

    if (
      setup.direction ===
      "BULLISH"
    ) {
      const touchedLine =
        candle.low <=
          linePrice +
            tolerance &&
        candle.high >=
          linePrice -
            tolerance;

      const rejectedUp =
        candle.close >
          candle.open &&
        candle.close >
          linePrice;

      if (
        touchedLine &&
        rejectedUp
      ) {
        return {
          confirmed: true,

          expired: false,

          epoch:
            candle.epoch,

          linePrice
        };
      }

      /*
        If price comes back and
        actually CLOSES decisively
        below the broken line,
        invalidate the setup.
      */

      if (
        candle.close <
        linePrice -
          tolerance
      ) {
        return {
          confirmed: false,

          expired: true,

          reason:
            "BULLISH_BREAK_FAILED"
        };
      }
    }

    /*
      SELL RETEST

      Price returns to the SAME
      broken support.

      Wick can touch/penetrate line.

      Candle must close bearish
      and back below line.
    */

    if (
      setup.direction ===
      "BEARISH"
    ) {
      const touchedLine =
        candle.high >=
          linePrice -
            tolerance &&
        candle.low <=
          linePrice +
            tolerance;

      const rejectedDown =
        candle.close <
          candle.open &&
        candle.close <
          linePrice;

      if (
        touchedLine &&
        rejectedDown
      ) {
        return {
          confirmed: true,

          expired: false,

          epoch:
            candle.epoch,

          linePrice
        };
      }

      /*
        If price comes back and
        actually CLOSES decisively
        above the broken support,
        invalidate the setup.
      */

      if (
        candle.close >
        linePrice +
          tolerance
      ) {
        return {
          confirmed: false,

          expired: true,

          reason:
            "BEARISH_BREAK_FAILED"
        };
      }
    }
  }

  return {
    confirmed: false,

    expired: false,

    epoch:
      latest.epoch,

    linePrice:
      trendLinePrice(
        setup.line,
        latest.epoch
      )
  };
}

/* =========================================================
   5M CONFIRMATION
========================================================= */

function find5MConfirmation(
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

      expired: false,

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

  if (!afterRetest.length) {
    return {
      confirmed: false,

      expired: false,

      epoch: null
    };
  }

  const latest =
    afterRetest[
      afterRetest.length - 1
    ];

  const age =
    latest.epoch -
    retestEpoch;

  /*
    Do not allow a very late
    confirmation.
  */

  if (
    age >
    MAX_CONFIRM_AGE_5M
  ) {
    return {
      confirmed: false,

      expired: true,

      epoch:
        latest.epoch
    };
  }

  /*
    Examine EVERY completed 5M
    candle after retest.

    The FIRST valid confirmation
    is used.
  */

  for (
    const candle of afterRetest
  ) {
    if (
      candle.epoch -
        retestEpoch >
      MAX_CONFIRM_AGE_5M
    ) {
      break;
    }

    const index =
      completed.indexOf(
        candle
      );

    if (index <= 0) {
      continue;
    }

    const previous =
      completed[index - 1];

    /*
      BUY confirmation
    */

    if (
      direction === "BULLISH" &&
      candle.close >
        candle.open &&
      candle.close >
        previous.high
    ) {
      return {
        confirmed: true,

        expired: false,

        epoch:
          candle.epoch
      };
    }

    /*
      SELL confirmation
    */

    if (
      direction === "BEARISH" &&
      candle.close <
        candle.open &&
      candle.close <
        previous.low
    ) {
      return {
        confirmed: true,

        expired: false,

        epoch:
          candle.epoch
      };
    }
  }

  return {
    confirmed: false,

    expired: false,

    epoch:
      latest.epoch
  };
}

/* =========================================================
   BUILD SIGNAL
========================================================= */

function buildSignal(
  pair,
  direction,
  candles5M,
  confirmationEpoch
) {
  const completed =
    candles5M.slice(0, -1);

  const index =
    completed.findIndex(
      candle =>
        candle.epoch ===
        confirmationEpoch
    );

  if (index < 0) {
    return null;
  }

  const confirmation =
    completed[index];

  /*
    Calculate ATR using candles
    available at confirmation time.

    This prevents future-data
    lookahead.
  */

  const history =
    completed.slice(
      0,
      index + 1
    );

  const atr =
    calculateATR(history);

  if (!atr) {
    return null;
  }

  const entry =
    confirmation.close;

  let stopLoss;

  let takeProfit;

  if (
    direction === "BULLISH"
  ) {
    stopLoss =
      confirmation.low -
      atr * 0.20;

    const risk =
      entry -
      stopLoss;

    if (risk <= 0) {
      return null;
    }

    takeProfit =
      entry +
      risk *
        RISK_REWARD;
  }

  else if (
    direction === "BEARISH"
  ) {
    stopLoss =
      confirmation.high +
      atr * 0.20;

    const risk =
      stopLoss -
      entry;

    if (risk <= 0) {
      return null;
    }

    takeProfit =
      entry -
      risk *
        RISK_REWARD;
  }

  else {
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

    timeframe: "5M",

    strategy:
      "12H → 1H → 15M Williams Fractal Break → SAME LINE Retest → 5M",

    signalType:
      direction ===
      "BULLISH"
        ? "BUY"
        : "SELL",

    confirmationEpoch
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

      return;
    }

    console.log(
      "[TELEGRAM] Signal sent"
    );

  } catch (error) {
    console.error(
      "[TELEGRAM] Error:",
      error.message
    );
  }
}

/* =========================================================
   TELEGRAM SIGNAL MESSAGE
========================================================= */

function createTelegramMessage(
  signal,
  state
) {
  const emoji =
    signal.direction ===
    "BULLISH"
      ? "🟢 BUY"
      : "🔴 SELL";

  return `
🚨 FOREX SIGNAL

${emoji}

Pair: ${signal.pair}

Entry: ${signal.entry}

Stop Loss: ${signal.stopLoss}

Take Profit: ${signal.takeProfit}

Risk/Reward: 1:${signal.riskReward}

━━━━━━━━━━━━━━

12H: ${state.direction12H}

1H: ${state.structure1H}

15M Break: CONFIRMED

15M Retest: CONFIRMED

5M Confirmation: CONFIRMED

Score: ${state.score}/10

━━━━━━━━━━━━━━

Strategy:
12H → 1H → 15M Fractal Break
→ SAME LINE Retest
→ 5M Confirmation

Signal only.
`;
}

/* =========================================================
   RESET DISPLAY
========================================================= */

function resetDisplay(
  state
) {
  state.status =
    "SCANNING";

  state.signal =
    null;

  state.entry =
    null;

  state.stopLoss =
    null;

  state.takeProfit =
    null;

  state.break15M =
    "WAIT";

  state.retest15M =
    "WAIT";

  state.confirm5M =
    "WAIT";

  state.score = 0;

  state.error = null;

  state.lastUpdate =
    new Date().toISOString();
}

/* =========================================================
   CLEAR SETUP
========================================================= */

function clearSetup(
  state
) {
  state.setup =
    emptySetup();
}

/* =========================================================
   SCORE
========================================================= */

function calculateScore(
  state
) {
  let score = 0;

  if (
    state.direction12H !==
    "WAIT"
  ) {
    score += 2;
  }

  if (
    state.structure1H !==
    "WAIT"
  ) {
    score += 2;
  }

  if (
    state.break15M ===
    "CONFIRMED"
  ) {
    score += 2;
  }

  if (
    state.retest15M ===
    "CONFIRMED"
  ) {
    score += 2;
  }

  if (
    state.confirm5M ===
    "CONFIRMED"
  ) {
    score += 2;
  }

  return score;
}

/* =========================================================
   15M TREND STATUS
========================================================= */

function get15MTrendStatus(
  candles,
  direction
) {
  const completed =
    candles.slice(0, -1);

  const line =
    calculateTrendLine(
      completed,
      direction
    );

  if (!line) {
    return "WAIT";
  }

  if (
    direction ===
    "BULLISH"
  ) {
    return "BULLISH";
  }

  if (
    direction ===
    "BEARISH"
  ) {
    return "BEARISH";
  }

  return "WAIT";
}

/* =========================================================
   MAIN SCAN
========================================================= */

async function scanPair(
  pair
) {
  const state =
    pairStates[pair];

  resetDisplay(state);

  try {
    if (!symbolMap[pair]) {
      state.status =
        "NOT MAPPED";

      state.error =
        "No Deriv symbol mapped";

      state.lastUpdate =
        new Date().toISOString();

      return;
    }

    /*
      Get all required timeframes.
    */

    const [
      candles1H,
      candles15M,
      candles5M
    ] =
      await Promise.all([
        getCandles(
          pair,
          H1
        ),

        getCandles(
          pair,
          M15
        ),

        getCandles(
          pair,
          M5
        )
      ]);

    /*
      Build 12H from completed 1H
      candles.
    */

    const candles12H =
      build12HCandles(
        candles1H
      );

    /*
      Calculate higher timeframe
      direction.
    */

    state.direction12H =
      get12HDirection(
        candles12H
      );

    /*
      Calculate 1H structure.
    */

    state.structure1H =
      get1HStructure(
        candles1H
      );

    /*
      15M current trend.
    */

    state.trend15M =
      get15MTrendStatus(
        candles15M,
        state.direction12H
      );

    /* =====================================================
       HIGHER TIMEFRAME PROTECTION
    ===================================================== */

    /*
      If an existing setup is active,
      it must continue to agree with
      the current 12H and 1H.
    */

    if (
      state.setup.active
    ) {
      if (
        state.direction12H !==
          state.setup.direction ||
        state.structure1H !==
          state.setup.direction
      ) {
        console.log(
          `[${pair}] Setup cancelled: higher timeframe changed`
        );

        clearSetup(state);

        state.status =
          "WAIT";

        state.lastUpdate =
          new Date().toISOString();

        return;
      }
    }

    /* =====================================================
       CREATE NEW BREAK SETUP
    ===================================================== */

    if (
      !state.setup.active
    ) {
      /*
        We ONLY look for a break in
        the 12H direction.

        Therefore:

        12H BEARISH
        -> only SELL break

        12H BULLISH
        -> only BUY break
      */

      if (
        (
          state.direction12H ===
          "BULLISH" ||
          state.direction12H ===
          "BEARISH"
        ) &&
        state.structure1H ===
          state.direction12H
      ) {
        const breakSignal =
          findLatest15MBreak(
            candles15M,
            state.direction12H
          );

        /*
          Do not process an old break
          that was already handled.
        */

        if (
          breakSignal &&
          (
            !state.lastProcessedBreakEpoch ||
            breakSignal.epoch >
              state.lastProcessedBreakEpoch
          )
        ) {
          /*
            STORE THE ORIGINAL LINE.

            This line will remain fixed
            while waiting for retest.
          */

          state.setup = {
            active: true,

            direction:
              breakSignal.direction,

            phase:
              "WAITING_RETEST",

            breakEpoch:
              breakSignal.epoch,

            breakIndex:
              breakSignal.index,

            breakType:
              breakSignal.type,

            line:
              breakSignal.line,

            linePrice:
              breakSignal.linePrice,

            retestEpoch:
              null,

            retestLinePrice:
              null,

            confirm5MEpoch:
              null
          };

          state.lastProcessedBreakEpoch =
            breakSignal.epoch;

          state.break15M =
            "CONFIRMED";

          state.status =
            "WAITING RETEST";

          console.log(
            `[${pair}] ${breakSignal.direction} 15M BREAK CONFIRMED`
          );

          console.log(
            `[${pair}] Waiting for retest of SAME trendline`
          );
        }
      }
    }

    /* =====================================================
       EXISTING SETUP
    ===================================================== */

    if (
      state.setup.active
    ) {
      /*
        BREAK IS CONFIRMED
      */

      state.break15M =
        "CONFIRMED";

      /*
        WAIT FOR RETEST
      */

      if (
        !state.setup.retestEpoch
      ) {
        const retest =
          find15MRetest(
            candles15M,
            state.setup
          );

        if (
          retest.expired
        ) {
          console.log(
            `[${pair}] Retest setup expired: ${retest.reason}`
          );

          clearSetup(state);

          state.status =
            "WAIT";

          state.lastUpdate =
            new Date().toISOString();

          return;
        }

        if (
          retest.confirmed
        ) {
          state.setup.retestEpoch =
            retest.epoch;

          state.setup.retestLinePrice =
            retest.linePrice;

          state.setup.phase =
            "WAITING_5M";

          state.retest15M =
            "CONFIRMED";

          console.log(
            `[${pair}] 15M RETEST CONFIRMED`
          );

          console.log(
            `[${pair}] SAME broken trendline was retested`
          );
        } else {
          state.retest15M =
            "WAITING";

          state.confirm5M =
            "WAIT";

          state.score =
            calculateScore(
              state
            );

          state.status =
            "WAITING RETEST";

          state.lastUpdate =
            new Date().toISOString();

          return;
        }
      }

      /* ===================================================
         RETEST HAS BEEN CONFIRMED
      =================================================== */

      if (
        state.setup.retestEpoch
      ) {
        state.retest15M =
          "CONFIRMED";

        /*
          Now ONLY look for 5M
          confirmation.
        */

        const confirmation =
          find5MConfirmation(
            candles5M,

            state.setup.direction,

            state.setup.retestEpoch
          );

        if (
          confirmation.expired
        ) {
          console.log(
            `[${pair}] 5M confirmation window expired`
          );

          clearSetup(state);

          state.status =
            "WAIT";

          state.lastUpdate =
            new Date().toISOString();

          return;
        }

        if (
          !confirmation.confirmed
        ) {
          state.confirm5M =
            "WAITING";

          state.score =
            calculateScore(
              state
            );

          state.status =
            "WAITING 5M CONFIRMATION";

          state.lastUpdate =
            new Date().toISOString();

          return;
        }

        /*
          5M CONFIRMED
        */

        state.setup.confirm5MEpoch =
          confirmation.epoch;

        state.setup.phase =
          "CONFIRMED";

        state.confirm5M =
          "CONFIRMED";

        state.score =
          calculateScore(
            state
          );

        /*
          HARD DIRECTION PROTECTION
        */

        if (
          state.direction12H !==
          state.setup.direction
        ) {
          console.log(
            `[${pair}] Signal blocked: 12H direction mismatch`
          );

          clearSetup(state);

          state.status =
            "WAIT";

          return;
        }

        if (
          state.structure1H !==
          state.setup.direction
        ) {
          console.log(
            `[${pair}] Signal blocked: 1H structure mismatch`
          );

          clearSetup(state);

          state.status =
            "WAIT";

          return;
        }

        /*
          Signal only when the complete
          sequence is finished.
        */

        if (
          state.score >=
          SIGNAL_SCORE_THRESHOLD
        ) {
          const signal =
            buildSignal(
              pair,

              state.setup.direction,

              candles5M,

              confirmation.epoch
            );

          if (!signal) {
            state.status =
              "WAIT";

            state.error =
              "Could not build signal";

            clearSetup(state);

            return;
          }

          /*
            Prevent duplicate alerts.
          */

          if (
            state.lastSignalEpoch ===
            confirmation.epoch
          ) {
            state.status =
              "SIGNAL ALREADY SENT";

            clearSetup(state);

            return;
          }

          state.signal =
            signal.direction ===
            "BULLISH"
              ? "BUY"
              : "SELL";

          state.entry =
            signal.entry;

          state.stopLoss =
            signal.stopLoss;

          state.takeProfit =
            signal.takeProfit;

          state.lastSignalEpoch =
            confirmation.epoch;

          state.status =
            signal.direction ===
            "BULLISH"
              ? "BUY SIGNAL"
              : "SELL SIGNAL";

          console.log(
            `[${pair}] =================================`
          );

          console.log(
            `[${pair}] ${state.status}`
          );

          console.log(
            `[${pair}] Entry: ${signal.entry}`
          );

          console.log(
            `[${pair}] SL: ${signal.stopLoss}`
          );

          console.log(
            `[${pair}] TP: ${signal.takeProfit}`
          );

          console.log(
            `[${pair}] =================================`
          );

          await sendTelegram(
            createTelegramMessage(
              signal,
              state
            )
          );

          /*
            Setup is now complete.
          */

          clearSetup(state);
        }
      }
    }

    state.score =
      calculateScore(
        state
      );

    if (
      state.status ===
      "SCANNING"
    ) {
      state.status =
        "WAIT";
    }

    state.lastUpdate =
      new Date().toISOString();

  } catch (error) {
    console.error(
      `[${pair}] ERROR:`,
      error.message
    );

    state.error =
      error.message;

    state.status =
      "ERROR";

    state.lastUpdate =
      new Date().toISOString();
  }
}

/* =========================================================
   SCAN ALL
========================================================= */

async function scanAll() {
  if (scanRunning) {
    console.log(
      "[SCAN] Already running"
    );

    return;
  }

  if (!derivConnected) {
    console.log(
      "[SCAN] Deriv not connected"
    );

    return;
  }

  scanRunning = true;

  lastScanTime =
    new Date().toISOString();

  console.log(
    "========================================"
  );

  console.log(
    `[SCAN] Starting ${PAIRS.length} markets`
  );

  console.log(
    "========================================"
  );

  try {
    /*
      Sequential scanning helps reduce
      pressure on the Deriv connection.
    */

    for (const pair of PAIRS) {
      await scanPair(pair);

      await sleep(250);
    }

  } catch (error) {
    console.error(
      "[SCAN] Error:",
      error.message
    );

  } finally {
    scanRunning = false;

    console.log(
      "[SCAN] Complete"
    );
  }
}

/* =========================================================
   API: STATUS
========================================================= */

app.get(
  "/api/status",
  (req, res) => {
    res.json({
      success: true,

      derivConnected,

      scanRunning,

      lastScanTime,

      totalPairs:
        PAIRS.length,

      mappedPairs:
        PAIRS.filter(
          pair =>
            !!symbolMap[pair]
        ).length,

      pairs:
        pairStates
    });
  }
);

/* =========================================================
   API: SYMBOLS
========================================================= */

app.get(
  "/api/symbols",
  (req, res) => {
    res.json({
      success: true,

      symbols:
        symbolMap
    });
  }
);

/* =========================================================
   MANUAL SCAN
========================================================= */

app.post(
  "/api/scan",
  async (req, res) => {
    if (scanRunning) {
      return res.json({
        success: false,

        message:
          "Scanner is already running"
      });
    }

    scanAll().catch(
      error =>
        console.error(
          "[MANUAL SCAN]",
          error.message
        )
    );

    res.json({
      success: true,

      message:
        "Scan started"
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

      scanRunning,

      time:
        new Date().toISOString()
    });
  }
);

/* =========================================================
   HOME
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
  async () => {
    console.log(
      "========================================"
    );

    console.log(
      "DERIV FOREX SIGNAL BOT"
    );

    console.log(
      "========================================"
    );

    console.log(
      `Server running on port ${PORT}`
    );

    console.log(
      `Markets: ${PAIRS.length}`
    );

    console.log(
      "Strategy:"
    );

    console.log(
      "12H → 1H → 15M Fractal Break → SAME LINE Retest → 5M Confirmation"
    );

    console.log(
      "========================================"
    );

    await connectDeriv();

    /*
      Initial scan after connection.
    */

    setTimeout(() => {
      scanAll().catch(
        error =>
          console.error(
            "[INITIAL SCAN]",
            error.message
          )
      );
    }, 5000);

    /*
      Automatic scan every 5 minutes.
    */

    setInterval(() => {
      scanAll().catch(
        error =>
          console.error(
            "[AUTO SCAN]",
            error.message
          )
      );
    }, SCAN_INTERVAL_MS);
  }
);

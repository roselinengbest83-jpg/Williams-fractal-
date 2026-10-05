const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

/*
===========================================================
DERIV PUBLIC MARKET DATA
===========================================================
No Deriv API key is required for this public market-data
connection.
*/

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
===========================================================
TELEGRAM
===========================================================
*/

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;

/*
===========================================================
MARKETS
===========================================================
30 FOREX PAIRS + GOLD
===========================================================
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

/*
===========================================================
TIMEFRAMES
===========================================================
*/

const M5 = 300;
const M15 = 900;
const H1 = 3600;

const FRACTAL_PERIODS = 2;

const RISK_REWARD = 2;

const SCAN_INTERVAL =
  60 * 1000;

const MAX_CANDLES = 200;

/*
How long a 15M break remains valid.
12 x 15M = 3 hours.
*/

const MAX_BREAK_AGE_15M =
  12 * M15;

/*
How long the retest can remain valid
after the first retest interaction.
*/

const MAX_RETEST_AGE_15M =
  8 * M15;

/*
Retest tolerance based on ATR.
*/

const RETEST_ATR_MULTIPLIER =
  0.35;

/*
===========================================================
GLOBAL STATE
===========================================================
*/

let derivSocket = null;

let derivConnected = false;

let scannerRunning = false;

let marketClosed = false;

let lastScan = null;

let symbolMap = {};

let activeSymbolsLoaded = false;

/*
Pending Deriv WebSocket requests.
*/

const pendingRequests =
  new Map();

let requestId = 1;

/*
===========================================================
PAIR STATE
===========================================================
*/

const state = {};

for (const pair of PAIRS) {

  state[pair] = {

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

/*
===========================================================
HELPERS
===========================================================
*/

function sleep(ms) {

  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );

}

function safeNumber(value) {

  const number =
    Number(value);

  return Number.isFinite(number)
    ? number
    : null;

}

/*
===========================================================
NORMALIZE DERIV SYMBOLS
===========================================================
*/

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
        .replace(
          /[^A-Z0-9]/g,
          ""
        )
    );

  /*
  Forex
  */

  for (const value of values) {

    if (aliases[value]) {

      return aliases[value];

    }

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

  /*
  Gold
  */

  for (const value of values) {

    if (
      value.includes("XAUUSD") ||
      value.includes("GOLD") ||
      value === "XAU"
    ) {

      return "XAU/USD";

    }

  }

  return null;

}

/*
===========================================================
DERIV CONNECTION
===========================================================
*/

function connectDeriv() {

  if (derivSocket) {

    try {

      derivSocket.close();

    } catch (_) {}

  }

  console.log(
    "[DERIV] Connecting..."
  );

  derivSocket =
    new WebSocket(
      DERIV_WS_URL
    );

  derivSocket.on(
    "open",
    () => {

      derivConnected = true;

      console.log(
        "[DERIV] WebSocket connected"
      );

      loadActiveSymbols();

    }
  );

  derivSocket.on(
    "message",
    rawMessage => {

      let data;

      try {

        data =
          JSON.parse(
            rawMessage.toString()
          );

      } catch (error) {

        console.error(
          "[DERIV] Invalid JSON:",
          error.message
        );

        return;

      }

      const reqId =
        data.req_id;

      if (
        reqId &&
        pendingRequests.has(
          reqId
        )
      ) {

        const pending =
          pendingRequests.get(
            reqId
          );

        pendingRequests.delete(
          reqId
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

      }

    }
  );

  derivSocket.on(
    "error",
    error => {

      console.error(
        "[DERIV] WebSocket error:",
        error.message
      );

    }
  );

  derivSocket.on(
    "close",
    (code, reason) => {

      derivConnected = false;

      activeSymbolsLoaded = false;

      console.log(
        `[DERIV] WebSocket closed. Code: ${code} Reason: ${reason || ""}`
      );

      /*
      Reconnect after 5 seconds.
      */

      setTimeout(
        connectDeriv,
        5000
      );

    }
  );

}

/*
===========================================================
SEND DERIV REQUEST
===========================================================
*/

function sendDerivRequest(
  payload
) {

  return new Promise(
    (resolve, reject) => {

      if (
        !derivSocket ||
        derivSocket.readyState !==
          WebSocket.OPEN
      ) {

        reject(
          new Error(
            "Deriv WebSocket is not connected"
          )
        );

        return;

      }

      const reqId =
        requestId++;

      const message = {

        ...payload,

        req_id: reqId

      };

      const timeout =
        setTimeout(
          () => {

            if (
              pendingRequests.has(
                reqId
              )
            ) {

              pendingRequests.delete(
                reqId
              );

              reject(
                new Error(
                  "Deriv request timeout"
                )
              );

            }

          },
          15000
        );

      pendingRequests.set(
        reqId,
        {

          resolve: data => {

            clearTimeout(
              timeout
            );

            resolve(data);

          },

          reject: error => {

            clearTimeout(
              timeout
            );

            reject(error);

          }

        }
      );

      try {

        derivSocket.send(
          JSON.stringify(message)
        );

      } catch (error) {

        clearTimeout(
          timeout
        );

        pendingRequests.delete(
          reqId
        );

        reject(error);

      }

    }
  );

}

/*
===========================================================
ACTIVE SYMBOLS
===========================================================
*/

async function loadActiveSymbols() {

  try {

    console.log(
      "[DERIV] Loading active symbols..."
    );

    const response =
      await sendDerivRequest({
        active_symbols:
          "brief"
      });

    if (
      !response.active_symbols
    ) {

      throw new Error(
        "No active_symbols returned"
      );

    }

    symbolMap = {};

    for (
      const item
      of response.active_symbols
    ) {

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
        symbol
      ) {

        symbolMap[pair] =
          symbol;

      }

    }

    activeSymbolsLoaded = true;

    console.log(
      `[DERIV] Received ${response.active_symbols.length} active symbols`
    );

    console.log(
      `[DERIV] Successfully mapped ${Object.keys(symbolMap).length}/${PAIRS.length} markets`
    );

    for (const pair of PAIRS) {

      if (
        !symbolMap[pair]
      ) {

        console.log(
          `[DERIV] Not mapped: ${pair}`
        );

      }

    }

  } catch (error) {

    activeSymbolsLoaded = false;

    console.error(
      "[DERIV] Active symbols error:",
      error.message
    );

  }

}

/*
===========================================================
GET CANDLES
===========================================================
*/

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

      ticks_history:
        symbol,

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

  if (
    !response.candles
  ) {

    throw new Error(
      "No candle data returned by Deriv"
    );

  }

  return response.candles
    .map(candle => ({

      epoch:
        Number(
          candle.epoch
        ),

      open:
        Number(
          candle.open
        ),

      high:
        Number(
          candle.high
        ),

      low:
        Number(
          candle.low
        ),

      close:
        Number(
          candle.close
        )

    }))
    .filter(candle =>
      Number.isFinite(
        candle.epoch
      ) &&
      Number.isFinite(
        candle.open
      ) &&
      Number.isFinite(
        candle.high
      ) &&
      Number.isFinite(
        candle.low
      ) &&
      Number.isFinite(
        candle.close
      )
    );

}

/*
===========================================================
MARKET CLOSED DETECTION
===========================================================
*/

function isMarketClosedError(
  message
) {

  const text =
    String(
      message || ""
    ).toLowerCase();

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

/*
===========================================================
12H CANDLES BUILT FROM COMPLETED 1H CANDLES
===========================================================
*/

function build12HCandles(
  candles1H
) {

  if (
    !candles1H ||
    candles1H.length < 24
  ) {

    return [];

  }

  /*
  Remove the currently forming 1H candle.
  */

  const completed =
    candles1H.slice(0, -1);

  const groups =
    new Map();

  for (
    const candle
    of completed
  ) {

    /*
    UTC 12-hour bucket.
    */

    const bucket =
      Math.floor(
        candle.epoch /
        (12 * 3600)
      ) *
      (12 * 3600);

    if (
      !groups.has(bucket)
    ) {

      groups.set(
        bucket,
        []
      );

    }

    groups
      .get(bucket)
      .push(candle);

  }

  const twelveHourCandles = [];

  for (
    const [
      epoch,
      group
    ]
    of groups
  ) {

    /*
    Only use a fully completed
    12-hour block.
    */

    if (
      group.length < 12
    ) {

      continue;

    }

    group.sort(
      (a, b) =>
        a.epoch - b.epoch
    );

    twelveHourCandles.push({

      epoch,

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

  return twelveHourCandles
    .sort(
      (a, b) =>
        a.epoch - b.epoch
    );

}

/*
===========================================================
12H DIRECTION
===========================================================
*/

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
    candles[
      candles.length - 1
    ];

  const previous =
    candles[
      candles.length - 2
    ];

  const older =
    candles[
      candles.length - 3
    ];

  /*
  Strong bullish structure.
  */

  if (

    last.close >
      last.open &&

    last.close >
      previous.close &&

    previous.close >=
      older.close

  ) {

    return "BULLISH";

  }

  /*
  Strong bearish structure.
  */

  if (

    last.close <
      last.open &&

    last.close <
      previous.close &&

    previous.close <=
      older.close

  ) {

    return "BEARISH";

  }

  return "WAIT";

}

/*
===========================================================
1H STRUCTURE
===========================================================
*/

function get1HStructure(
  candles
) {

  if (
    !candles ||
    candles.length < 20
  ) {

    return "WAIT";

  }

  /*
  Ignore current incomplete candle.
  */

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
  Flexible fallback so the bot
  does not become unnecessarily
  restrictive.
  */

  if (
    completed.length >= 7
  ) {

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

/*
===========================================================
WILLIAMS FRACTALS
===========================================================
n = 2 = 5 candle fractal
===========================================================
*/

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

    let downFractal =
      true;

    let upFractal =
      true;

    for (
      let j = 1;
      j <= n;
      j++
    ) {

      /*
      Down fractal:
      middle candle has highest high.
      */

      if (

        candles[i - j].high >=
          current.high ||

        candles[i + j].high >=
          current.high

      ) {

        downFractal =
          false;

      }

      /*
      Up fractal:
      middle candle has lowest low.
      */

      if (

        candles[i - j].low <=
          current.low ||

        candles[i + j].low <=
          current.low

      ) {

        upFractal =
          false;

      }

    }

    if (
      downFractal
    ) {

      downFractals.push({

        index: i,

        epoch:
          current.epoch,

        price:
          current.high

      });

    }

    if (
      upFractal
    ) {

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

/*
===========================================================
TRENDLINE
===========================================================
For bullish:
latest 2 DOWN fractals = resistance

For bearish:
latest 2 UP fractals = support
===========================================================
*/

function calculateTrendLine(
  candles,
  direction
) {

  const {
    upFractals,
    downFractals
  } =
    getFractals(
      candles
    );

  if (
    direction ===
    "BULLISH"
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

      type:
        "RESISTANCE",

      first,

      second

    };

  }

  if (
    direction ===
    "BEARISH"
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

      type:
        "SUPPORT",

      first,

      second

    };

  }

  return null;

}

/*
===========================================================
TRENDLINE PRICE
===========================================================
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

  if (
    x2 === x1
  ) {

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
===========================================================
15M TREND
===========================================================
*/

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
    direction ===
    "BULLISH"
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
    direction ===
    "BEARISH"
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

/*
===========================================================
15M BREAK
===========================================================
IMPORTANT:
The break is based on a COMPLETED 15M
candle CLOSE beyond the fractal trendline.
===========================================================
*/

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

  const linePrice =
    trendLinePrice(
      line,
      last.epoch
    );

  if (
    direction ===
    "BULLISH"
  ) {

    if (
      last.close >
      linePrice
    ) {

      return {

        confirmed: true,

        epoch:
          last.epoch,

        line,

        linePrice

      };

    }

  }

  if (
    direction ===
    "BEARISH"
  ) {

    if (
      last.close <
      linePrice
    ) {

      return {

        confirmed: true,

        epoch:
          last.epoch,

        line,

        linePrice

      };

    }

  }

  return {

    confirmed: false,

    epoch:
      last.epoch,

    line,

    linePrice

  };

}

/*
===========================================================
ATR
===========================================================
*/

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

  if (
    !recent.length
  ) {

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

/*
===========================================================
RETEST / REJECTION
===========================================================
*/

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

  /*
  There is no retest until a candle
  after the break exists.
  */

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

  /*
  Break has become too old.
  */

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

  /*
  =========================================================
  BULLISH RETEST
  =========================================================
  Price comes back toward the old resistance
  and rejects upward.
  */

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

        epoch:
          last.epoch,

        linePrice

      };

    }

  }

  /*
  =========================================================
  BEARISH RETEST
  =========================================================
  Price comes back toward the old support
  and rejects downward.
  */

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

        epoch:
          last.epoch,

        linePrice

      };

    }

  }

  return {

    confirmed: false,

    expired: false,

    epoch:
      last.epoch,

    linePrice

  };

}

/*
===========================================================
5M CONFIRMATION
===========================================================
*/

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
    completed.indexOf(last);

  if (
    lastIndex <= 0
  ) {

    return {

      confirmed: false,

      epoch:
        last.epoch

    };

  }

  const previous =
    completed[
      lastIndex - 1
    ];

  /*
  BUY confirmation.
  */

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

  /*
  SELL confirmation.
  */

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

    epoch:
      last.epoch

  };

}

/*
===========================================================
PRICE DECIMALS
===========================================================
*/

function getDecimals(
  pair
) {

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

/*
===========================================================
BUILD SIGNAL
===========================================================
*/

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

  /*
  =========================================================
  BUY
  =========================================================
  */

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

  }

  /*
  =========================================================
  SELL
  =========================================================
  */

  else if (
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
     

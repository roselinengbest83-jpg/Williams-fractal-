const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;


/*
========================================================
DERIV
========================================================
*/

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";


/*
========================================================
TELEGRAM
========================================================
*/

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;


/*
========================================================
20 FOREX PAIRS
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
  "NZD/JPY"
];


/*
========================================================
TIMEFRAMES
========================================================
*/

const M5 = 300;
const M15 = 900;
const H1 = 3600;

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

    lastScan: null,

    /*
      Setup memory.

      This is what allows the bot to remember:

      BREAK
        ↓
      later RETEST
        ↓
      later 5M CONFIRMATION
    */

    setup: {

      active: false,

      direction: null,

      breakEpoch: null,

      line: null,

      retestEpoch: null

    }

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

  return new Promise(resolve => {

    setTimeout(resolve, ms);

  });

}


function normalizeName(value) {

  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z]/g, "");

}


/*
========================================================
DERIV PAIR MAPPING
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

    NZDJPY: "NZD/JPY"

  };


  const values = [
    name,
    symbol
  ]
    .filter(Boolean)
    .map(value =>
      normalizeName(value)
    );


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


  return null;

}


/*
========================================================
DERIV CONNECTION
========================================================
*/

function connectDeriv() {

  console.log(
    "[DERIV] Connecting..."
  );


  derivWS =
    new WebSocket(
      DERIV_WS_URL
    );


  derivWS.on(
    "open",
    async () => {

      derivConnected = true;

      console.log(
        "[DERIV] WebSocket connected"
      );


      try {

        await loadActiveSymbols();

      } catch (error) {

        console.error(
          "[DERIV] Active symbols error:",
          error.message
        );

      }

    }
  );


  derivWS.on(
    "message",
    message => {

      try {

        const data =
          JSON.parse(
            message.toString()
          );


        if (
          data.req_id &&
          pendingRequests.has(
            data.req_id
          )
        ) {

          const request =
            pendingRequests.get(
              data.req_id
            );


          clearTimeout(
            request.timeout
          );


          pendingRequests.delete(
            data.req_id
          );


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

    }
  );


  derivWS.on(
    "error",
    error => {

      derivConnected = false;

      console.error(
        "[DERIV] WebSocket error:",
        error.message
      );

    }
  );


  derivWS.on(
    "close",
    (code, reason) => {

      derivConnected = false;

      console.log(
        `[DERIV] WebSocket closed. Code: ${code} Reason: ${reason || ""}`
      );


      setTimeout(
        connectDeriv,
        5000
      );

    }
  );

}


/*
========================================================
SEND DERIV REQUEST
========================================================
*/

function sendDerivRequest(
  payload,
  timeoutMs = 15000
) {

  return new Promise(
    (resolve, reject) => {

      if (
        !derivWS ||
        derivWS.readyState !==
        WebSocket.OPEN
      ) {

        reject(
          new Error(
            "Deriv WebSocket is not connected"
          )
        );

        return;

      }


      const req_id =
        requestId++;


      const timeout =
        setTimeout(
          () => {

            pendingRequests.delete(
              req_id
            );

            reject(
              new Error(
                "Deriv request timed out"
              )
            );

          },
          timeoutMs
        );


      pendingRequests.set(
        req_id,
        {
          resolve,
          reject,
          timeout
        }
      );


      derivWS.send(
        JSON.stringify({

          ...payload,

          req_id

        })
      );

    }
  );

}


/*
========================================================
ACTIVE SYMBOLS
========================================================
*/

async function loadActiveSymbols() {

  const response =
    await sendDerivRequest({

      active_symbols: "brief"

    });


  const symbols =
    response.active_symbols || [];


  console.log(
    `[DERIV] Received ${symbols.length} active symbols`
  );


  symbolMap = {};


  for (
    const item of symbols
  ) {

    const pair =
      normalizeDerivPair(

        item.underlying_symbol_name ||
        item.display_name ||
        item.name,

        item.underlying_symbol ||
        item.symbol

      );


    if (
      pair &&
      PAIRS.includes(pair)
    ) {

      symbolMap[pair] =
        item.underlying_symbol ||
        item.symbol;

    }

  }


  const mappedCount =
    PAIRS.filter(
      pair =>
        symbolMap[pair]
    ).length;


  console.log(
    `[DERIV] Successfully mapped ${mappedCount}/${PAIRS.length} forex pairs`
  );


  for (
    const pair of PAIRS
  ) {

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


  if (
    !response.candles
  ) {

    throw new Error(
      "No candle data returned by Deriv"
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


/*
========================================================
MARKET CLOSED
========================================================
*/

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


/*
========================================================
12H CANDLES FROM 1H
========================================================
*/

function build12HCandlesFrom1H(
  candles
) {

  const groups =
    new Map();


  for (
    const candle of candles
  ) {

    const date =
      new Date(
        candle.epoch * 1000
      );


    const hour =
      date.getUTCHours();


    const blockHour =
      hour < 12 ? 0 : 12;


    const blockDate =
      new Date(
        Date.UTC(
          date.getUTCFullYear(),
          date.getUTCMonth(),
          date.getUTCDate(),
          blockHour
        )
      );


    const key =
      blockDate.getTime();


    if (
      !groups.has(key)
    ) {

      groups.set(
        key,
        []
      );

    }


    groups
      .get(key)
      .push(candle);

  }


  const result = [];


  for (
    const [, group] of groups
  ) {

    group.sort(
      (a, b) =>
        a.epoch - b.epoch
    );


    if (
      group.length < 12
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
            c => c.high
          )
        ),

      low:
        Math.min(
          ...group.map(
            c => c.low
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
      a.epoch - b.epoch
  );

}


/*
========================================================
12H DIRECTION
========================================================
*/

function get12HDirection(
  candles
) {

  if (
    !candles ||
    candles.length < 4
  ) {

    return "WAIT";

  }


  const completed =
    candles.slice(0, -1);


  if (
    completed.length < 3
  ) {

    return "WAIT";

  }


  const last =
    completed[
      completed.length - 1
    ];


  const previous =
    completed[
      completed.length - 2
    ];


  const older =
    completed[
      completed.length - 3
    ];


  /*
    Strong bullish confirmation:
    last candle bullish +
    higher close +
    previous structure rising.
  */

  if (
    last.close > last.open &&
    last.close > previous.close &&
    previous.close >= older.close
  ) {

    return "BULLISH";

  }


  /*
    Strong bearish confirmation.
  */

  if (
    last.close < last.open &&
    last.close < previous.close &&
    previous.close <= older.close
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

function get1HStructure(
  candles
) {

  if (
    !candles ||
    candles.length < 12
  ) {

    return "WAIT";

  }


  const completed =
    candles.slice(0, -1);


  const last =
    completed[
      completed.length - 1
    ];


  /*
    Compare the last completed candle
    against the PREVIOUS six candles.

    This fixes the old logic where the
    last candle was included in highest/lowest.
  */

  const previousWindow =
    completed.slice(
      -7,
      -1
    );


  if (
    previousWindow.length < 5
  ) {

    return "WAIT";

  }


  const previousHigh =
    Math.max(
      ...previousWindow.map(
        c => c.high
      )
    );


  const previousLow =
    Math.min(
      ...previousWindow.map(
        c => c.low
      )
    );


  if (
    last.close >
    previousHigh
  ) {

    return "BULLISH";

  }


  if (
    last.close <
    previousLow
  ) {

    return "BEARISH";

  }


  /*
    If there is no clean breakout,
    compare recent closes.
  */

  const reference =
    completed[
      completed.length - 5
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
        middle candle has the highest high.
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
        middle candle has the lowest low.
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
========================================================
TREND LINE
========================================================

IMPORTANT:

BUY:
  Uses the most recent two DOWN fractals
  as resistance.

SELL:
  Uses the most recent two UP fractals
  as support.

This gives us:

BUY:
  candle closes ABOVE resistance line

SELL:
  candle closes BELOW support line
========================================================
*/

function calculateTrendLine(
  candles,
  direction
) {

  const {
    upFractals,
    downFractals
  } =
    getFractals(candles);


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

      type: "RESISTANCE",

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

      type: "SUPPORT",

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
========================================================
15M TREND
========================================================
*/

function get15MTrend(
  candles,
  direction
) {

  if (
    !candles ||
    candles.length < 30
  ) {

    return "WAIT";

  }


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


  const last =
    completed[
      completed.length - 1
    ];


  const linePrice =
    trendLinePrice(
      line,
      last.epoch
    );


  /*
    Before the breakout:

    BUY:
      price should be at/under
      resistance.

    SELL:
      price should be at/above
      support.
  */

  if (
    direction ===
    "BULLISH"
  ) {

    if (
      last.close >
      linePrice
    ) {

      return "BULLISH";

    }

    return "WAIT";

  }


  if (
    direction ===
    "BEARISH"
  ) {

    if (
      last.close <
      linePrice
    ) {

      return "BEARISH";

    }

    return "WAIT";

  }


  return "WAIT";

}


/*
========================================================
15M BREAK
========================================================

Only a COMPLETED 15M candle can confirm
the break.
========================================================
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
========================================================
REAL RETEST / REJECTION
========================================================

The retest must happen AFTER the break.

BUY:
  price comes back to the broken
  resistance line and rejects upward.

SELL:
  price comes back to the broken
  support line and rejects downward.
========================================================
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


  /*
    Only candles after the break
    can perform the retest.
  */

  const afterBreak =
    completed.filter(
      candle =>
        candle.epoch >
        setup.breakEpoch
    );


  if (
    afterBreak.length === 0
  ) {

    return null;

  }


  /*
    Use the most recent completed
    candle after the break.
  */

  const last =
    afterBreak[
      afterBreak.length - 1
    ];


  const linePrice =
    trendLinePrice(
      setup.line,
      last.epoch
    );


  /*
    Allow a reasonable touch zone.

    This prevents requiring the wick
    to hit the line by exactly one tick.
  */

  const candleRange =
    Math.max(
      last.high - last.low,
      0.00001
    );


  const tolerance =
    candleRange * 0.25;


  if (
    direction ===
    "BULLISH"
  ) {

    const touchedLine =
      last.low <=
      linePrice + tolerance;


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

        epoch:
          last.epoch,

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
      linePrice - tolerance;


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

        epoch:
          last.epoch,

        linePrice

      };

    }

  }


  return {

    confirmed: false,

    epoch:
      last.epoch,

    linePrice

  };

}


/*
========================================================
5M CONFIRMATION
========================================================

5M must confirm AFTER the 15M retest.
========================================================
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

    return false;

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

    return false;

  }


  const last =
    afterRetest[
      afterRetest.length - 1
    ];


  const previous =
    completed[
      completed.length - 2
    ];


  if (
    direction ===
    "BULLISH"
  ) {

    return (

      last.close >
      last.open &&

      last.close >
      previous.high

    );

  }


  if (
    direction ===
    "BEARISH"
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
========================================================
PRICE PRECISION
========================================================
*/

function getDecimals(pair) {

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
========================================================
BUILD SIGNAL
========================================================
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

  } else {

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


/*
========================================================
TELEGRAM
========================================================
*/

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

        body:
          JSON.stringify({

            chat_id:
              TELEGRAM_CHAT_ID,

            text:
              message

          })

      }
    );

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

function formatSignal(
  signal
) {

  const direction =
    signal.direction ===
    "BULLISH"
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
15M Candle Close Break
↓
15M Retest / Rejection
↓
5M Confirmation

Signal only — no automatic trade.
`;

}


/*
========================================================
RESET PAIR STATE
========================================================
*/

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

}


/*
========================================================
SCAN ONE PAIR
========================================================
*/

async function scanPair(
  pair
) {

  const currentState =
    state[pair];


  currentState.lastScan =
    new Date().toISOString();


  resetPairForScan(
    currentState
  );


  try {

    /*
    ================================================
    1H
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
    12H
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


    if (
      direction12H ===
      "WAIT"
    ) {

      currentState.status =
        "WAIT";

      console.log(
        `[SCAN] ${pair}: 12H WAIT`
      );

      return;

    }


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


    if (
      structure1H !==
      direction12H
    ) {

      currentState.status =
        "WAIT";

      console.log(
        `[SCAN] ${pair}: 12H ${direction12H} | 1H ${structure1H} | WAIT`
      );

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
        candles15M,
        direction12H
      );


    currentState.trend15M =
      trend15M;


    /*
    ================================================
    EXISTING SETUP
    ================================================
    */

    const setup =
      currentState.setup;


    /*
    ================================================
    FIND NEW BREAK
    ================================================
    */

    const breakResult =
      check15MBreak(
        candles15M,
        direction12H
      );


    if (
      !setup.active &&
      breakResult &&
      breakResult.confirmed
    ) {

      setup.active =
        true;

      setup.direction =
        direction12H;

      setup.breakEpoch =
        breakResult.epoch;

      setup.line =
        breakResult.line;

      setup.retestEpoch =
        null;


      currentState.break15M =
        "CONFIRMED";


      console.log(
        `[SETUP] ${pair}: ${direction12H} 15M BREAK CONFIRMED`
      );

    }


    /*
    ================================================
    SHOW EXISTING BREAK
    ================================================
    */

    if (
      setup.active
    ) {

      currentState.break15M =
        "CONFIRMED";

    } else {

      currentState.break15M =
        "WAIT";

    }


    /*
    ================================================
    RETEST
    ================================================
    */

    if (
      setup.active &&
      !setup.retestEpoch
    ) {

      const retest =
        checkRetest(
          candles15M,
          direction12H,
          setup
        );


      if (
        retest &&
        retest.confirmed
      ) {

        setup.retestEpoch =
          retest.epoch;


        currentState.retest15M =
          "CONFIRMED";


        console.log(
          `[SETUP] ${pair}: 15M RETEST CONFIRMED`
        );

      } else {

        currentState.retest15M =
          "WAIT";

        currentState.status =
          "WAIT — RETEST";

        return;

      }

    }


    if (
      !setup.active
    ) {

      currentState.status =
        "WAIT — 15M BREAK";

      return;

    }


    if (
      !setup.retestEpoch
    ) {

      currentState.status =
        "WAIT — RETEST";

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
        direction12H,
        setup.retestEpoch
      );


    currentState.confirm5M =
      confirmation5M
        ? "CONFIRMED"
        : "WAIT";


    if (
      !confirmation5M
    ) {

      currentState.status =
        "WAIT — 5M CONFIRMATION";

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
        direction12H,
        candles5M
      );


    if (!signal) {

      currentState.status =
        "WAIT";

      return;

    }


    /*
    ================================================
    SIGNAL
    ================================================
    */

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
      direction12H ===
      "BULLISH"
        ? "BUY SIGNAL"
        : "SELL SIGNAL";


    console.log(
      `[SIGNAL] ${pair} ${direction12H} ENTRY=${signal.entry} SL=${signal.stopLoss} TP=${signal.takeProfit}`
    );


    await sendTelegram(
      formatSignal(
        signal
      )
    );


    /*
      Reset setup after signal.

      This prevents the same setup from
      sending the same signal repeatedly.
    */

    currentState.setup = {

      active: false,

      direction: null,

      breakEpoch: null,

      line: null,

      retestEpoch: null

    };


  } catch (error) {

    const message =
      error.message ||
      "Unknown error";


    if (
      isMarketClosedError(
        message
      )
    ) {

      marketClosed =
        true;


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

  if (
    scannerRunning
  ) {

    return;

  }


  if (
    !derivConnected
  ) {

    console.log(
      "[SCAN] Deriv not connected yet"
    );

    return;

  }


  if (
    Object.keys(symbolMap).length === 0
  ) {

    console.log(
      "[SCAN] Deriv symbols not loaded yet"
    );

    return;

  }


  scannerRunning =
    true;


  marketClosed =
    false;


  lastScan =
    new Date().toISOString();


  console.log(
    `[SCAN] Starting scan of ${PAIRS.length} pairs`
  );


  try {

    for (
      const pair of PAIRS
    ) {

      console.log(
        `[SCAN] Checking ${pair}...`
      );


      await scanPair(
        pair
      );


      await sleep(
        300
      );

    }

  } catch (error) {

    console.error(
      "[SCAN] Global error:",
      error.message
    );

  } finally {

    scannerRunning =
      false;


    console.log(
      "[SCAN] Scan complete"
    );

  }

}


/*
========================================================
STATUS API
========================================================
*/

app.get(
  "/api/status",
  (req, res) => {

    res.json({

      success: true,

      system: {

        derivConnected,

        marketClosed,

        scannerRunning,

        lastScan,

        pairCount:
          PAIRS.length

      },

      pairs:
        state

    });

  }
);


/*
========================================================
HEALTH
========================================================
*/

app.get(
  "/health",
  (req, res) => {

    res.json({

      status: "ok",

      derivConnected,

      marketClosed,

      pairCount:
        PAIRS.length,

      mappedPairs:
        Object.keys(symbolMap).length,

      time:
        new Date().toISOString()

    });

  }
);


/*
========================================================
MANUAL SCAN
========================================================
*/

app.post(
  "/api/scan",
  async (req, res) => {

    if (
      scannerRunning
    ) {

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


/*
========================================================
DASHBOARD
========================================================
*/

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


/*
========================================================
START SERVER
========================================================
*/

app.listen(
  PORT,
  () => {

    console.log(
      `Server running on port ${PORT}`
    );


    console.log(
      `Watching ${PAIRS.length} forex pairs`
    );


    connectDeriv();


    /*
      First scan after connection.
    */

    setTimeout(
      () => {

        scanAllPairs();

      },
      10000
    );


    /*
      Continue scanning every minute.
    */

    setInterval(
      () => {

        scanAllPairs();

      },
      SCAN_INTERVAL
    );

  }
);

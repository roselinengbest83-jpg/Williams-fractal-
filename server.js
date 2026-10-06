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
   PAIRS
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

const RISK_REWARD = 2;

const FRACTAL_PERIODS = 2;

const MAX_CANDLES = 150;


/*
   A 15M break is valid for up to 2 hours.
*/

const MAX_BREAK_AGE_15M =
  60 * 60 * 2;


/*
   Retest tolerance.

   0.20 ATR is deliberately moderate.
   It allows a realistic retest without
   making the retest excessively loose.
*/

const RETEST_ATR_MULTIPLIER = 0.20;


/*
   Small buffer beyond recent 5M structure
   when calculating SL.
*/

const SL_ATR_BUFFER = 0.20;


/*
   Automatic scan every 5 minutes.
*/

const SCAN_INTERVAL_MS =
  5 * 60 * 1000;


/* =========================================================
   STATE
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

    setup: {

      active: false,

      direction: null,

      breakEpoch: null,

      line: null,

      linePrice: null,

      retestEpoch: null
    },

    error: null,

    lastUpdate: null,

    lastSignalEpoch: null
  };
}


/* =========================================================
   DERIV CONNECTION STATE
========================================================= */

let derivSocket = null;

let derivConnected = false;

let activeSymbolsLoaded = false;

let activeSymbols = [];

const symbolMap = {};

let requestId = 1;

const pendingRequests = new Map();


/* =========================================================
   HELPERS
========================================================= */

function sleep(ms) {

  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}


function getDecimals(pair) {

  if (pair === "XAU/USD") {
    return 2;
  }

  if (pair.includes("JPY")) {
    return 3;
  }

  return 5;
}


function roundPrice(value, pair) {

  return Number(
    Number(value).toFixed(
      getDecimals(pair)
    )
  );
}


/* =========================================================
   DERIV SYMBOL NORMALIZER
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
    Direct matches.
  */

  for (const value of values) {

    if (aliases[value]) {
      return aliases[value];
    }
  }


  /*
    FRX symbols.
  */

  for (const value of values) {

    if (value.startsWith("FRX")) {

      const forexCode =
        value.substring(3);

      if (aliases[forexCode]) {
        return aliases[forexCode];
      }
    }
  }


  /*
    Embedded pair codes.
  */

  for (const value of values) {

    for (
      const code of Object.keys(aliases)
    ) {

      if (value.includes(code)) {
        return aliases[code];
      }
    }
  }


  /*
    Gold.
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

  if (!derivConnected) {

    throw new Error(
      "Deriv socket is not connected"
    );
  }


  try {

    const response =
      await sendDerivRequest({

        active_symbols:
          "brief"
      });


    if (response.error) {

      console.error(
        "[DERIV] Active symbols error:",
        response.error.message
      );

      activeSymbolsLoaded =
        false;

      return false;
    }


    activeSymbols =
      response.active_symbols || [];


    console.log(
      `[DERIV] Received ${activeSymbols.length} active symbols`
    );


    if (!activeSymbols.length) {

      activeSymbolsLoaded =
        false;

      return false;
    }


    /*
      Clear previous mapping.
    */

    for (
      const key of Object.keys(symbolMap)
    ) {

      delete symbolMap[key];
    }


    /*
      Debug Deriv symbols.
    */

    console.log(
      "=========================================="
    );

    console.log(
      "[DERIV] FOREX / GOLD SYMBOL DEBUG"
    );

    console.log(
      "=========================================="
    );


    for (
      const item of activeSymbols
    ) {

      const name =
        String(
          item.underlying_symbol_name ||
          item.display_name ||
          item.name ||
          ""
        ).toUpperCase();


      const symbol =
        String(
          item.underlying_symbol ||
          item.symbol ||
          ""
        ).toUpperCase();


      const type =
        String(
          item.underlying_symbol_type ||
          item.market ||
          item.submarket ||
          ""
        ).toLowerCase();


      if (
        type.includes("forex") ||
        name.includes("/") ||
        symbol.startsWith("FRX") ||
        name.includes("GOLD") ||
        name.includes("XAU")
      ) {

        console.log(
          `[DERIV FOREX] name="${name}" symbol="${symbol}"`
        );
      }
    }


    console.log(
      "=========================================="
    );


    /*
      Map requested pairs.
    */

    for (
      const item of activeSymbols
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


      const normalized =
        normalizeDerivPair(
          name,
          symbol
        );


      if (
        normalized &&
        PAIRS.includes(normalized) &&
        !symbolMap[normalized]
      ) {

        symbolMap[normalized] =
          symbol;


        console.log(
          `[DERIV] Mapped ${normalized} -> ${symbol}`
        );
      }
    }


    /*
      Gold fallback.
    */

    if (!symbolMap["XAU/USD"]) {

      for (
        const item of activeSymbols
      ) {

        const name =
          String(
            item.underlying_symbol_name ||
            item.display_name ||
            item.name ||
            ""
          ).toUpperCase();


        const symbol =
          String(
            item.underlying_symbol ||
            item.symbol ||
            ""
          ).toUpperCase();


        if (
          name.includes("GOLD") ||
          name.includes("XAU") ||
          symbol.includes("XAU")
        ) {

          symbolMap["XAU/USD"] =
            symbol;


          console.log(
            `[DERIV] Gold fallback: XAU/USD -> ${symbol}`
          );

          break;
        }
      }
    }


    /*
      Final report.
    */

    console.log(
      "=========================================="
    );

    console.log(
      "[DERIV] FINAL SYMBOL MAPPING"
    );

    console.log(
      "=========================================="
    );


    let mappedCount = 0;


    for (
      const pair of PAIRS
    ) {

      if (symbolMap[pair]) {

        mappedCount++;

        console.log(
          `[DERIV] ${pair} -> ${symbolMap[pair]}`
        );

      } else {

        console.warn(
          `[DERIV] NOT MAPPED: ${pair}`
        );
      }
    }


    console.log(
      "=========================================="
    );


    console.log(
      `[DERIV] Mapping complete: ${mappedCount}/${PAIRS.length}`
    );


    activeSymbolsLoaded =
      mappedCount > 0;


    return activeSymbolsLoaded;

  } catch (error) {

    console.error(
      "[DERIV] Failed loading active symbols:",
      error.message
    );


    activeSymbolsLoaded =
      false;


    return false;
  }
}


/* =========================================================
   DERIV REQUEST
========================================================= */

function sendDerivRequest(payload) {

  return new Promise(
    (resolve, reject) => {

      if (
        !derivSocket ||
        derivSocket.readyState !==
          WebSocket.OPEN
      ) {

        reject(
          new Error(
            "Deriv socket not connected"
          )
        );

        return;
      }


      const reqId =
        requestId++;


      const message = {

        ...payload,

        req_id:
          reqId
      };


      const timeout =
        setTimeout(
          () => {

            pendingRequests.delete(
              reqId
            );

            reject(
              new Error(
                "Deriv request timeout"
              )
            );

          },
          30000
        );


      pendingRequests.set(
        reqId,
        {
          resolve,
          reject,
          timeout
        }
      );


      try {

        derivSocket.send(
          JSON.stringify(message)
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


/* =========================================================
   CONNECT DERIV
========================================================= */

function connectDeriv() {

  return new Promise(
    (resolve, reject) => {

      console.log(
        "[DERIV] Connecting..."
      );


      derivSocket =
        new WebSocket(
          DERIV_WS_URL
        );


      let settled = false;


      derivSocket.on(
        "message",
        raw => {

          try {

            const data =
              JSON.parse(
                raw.toString()
              );


            if (
              data.req_id &&
              pendingRequests.has(
                data.req_id
              )
            ) {

              const pending =
                pendingRequests.get(
                  data.req_id
                );


              clearTimeout(
                pending.timeout
              );


              pendingRequests.delete(
                data.req_id
              );


              pending.resolve(
                data
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


      derivSocket.on(
        "open",
        async () => {

          console.log(
            "[DERIV] WebSocket connected"
          );


          derivConnected =
            true;


          try {

            await loadActiveSymbols();


            if (!settled) {

              settled = true;

              resolve();
            }

          } catch (error) {

            console.error(
              "[DERIV] Symbol loading error:",
              error.message
            );


            if (!settled) {

              settled = true;

              resolve();
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


          derivConnected =
            false;


          if (!settled) {

            settled = true;

            reject(error);
          }
        }
      );


      derivSocket.on(
        "close",
        () => {

          console.warn(
            "[DERIV] WebSocket closed"
          );


          derivConnected =
            false;


          activeSymbolsLoaded =
            false;


          for (
            const [
              id,
              pending
            ]
            of pendingRequests
          ) {

            clearTimeout(
              pending.timeout
            );


            pending.reject(
              new Error(
                "Deriv WebSocket closed"
              )
            );


            pendingRequests.delete(
              id
            );
          }


          setTimeout(
            () => {

              connectDeriv()
                .catch(error => {

                  console.error(
                    "[DERIV] Reconnect failed:",
                    error.message
                  );

                });

            },
            5000
          );
        }
      );
    }
  );
}


/* =========================================================
   MARKET CLOSED
========================================================= */

function isMarketClosedError(
  message
) {

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

      ticks_history:
        symbol,

      end:
        "latest",

      count,

      style:
        "candles",

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


/* =========================================================
   BUILD 12H FROM 1H
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
    Remove currently forming 1H candle.
  */

  const completed1H =
    candles1H.slice(0, -1);


  const groups = {};


  for (
    const candle of completed1H
  ) {

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
      hour < 12
        ? 0
        : 12;


    const key =
      `${year}-${month}-${day}-${halfDay}`;


    if (!groups[key]) {
      groups[key] = [];
    }


    groups[key].push(
      candle
    );
  }


  const result = [];


  for (
    const key of Object.keys(groups)
  ) {

    const group =
      groups[key];


    if (
      group.length < 12
    ) {

      continue;
    }


    group.sort(
      (a, b) =>
        a.epoch -
        b.epoch
    );


    let consecutive =
      true;


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

        consecutive =
          false;

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

      epoch:
        first.epoch,

      open:
        first.open,

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
        last.close
    });
  }


  result.sort(
    (a, b) =>
      a.epoch -
      b.epoch
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
    Strong bullish 12H direction.
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
    Strong bearish 12H direction.
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


    /*
      Higher high + higher low.
    */

    if (
      latestHigh.high >
        previousHigh.high &&
      latestLow.low >
        previousLow.low
    ) {

      return "BULLISH";
    }


    /*
      Lower high + lower low.
    */

    if (
      latestHigh.high <
        previousHigh.high &&
      latestLow.low <
        previousLow.low
    ) {

      return "BEARISH";
    }
  }


  /*
    Softer directional fallback.
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


/* =========================================================
   WILLIAMS FRACTAL
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
        Down fractal =
        highest high.
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
        Up fractal =
        lowest low.
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


    if (downFractal) {

      downFractals.push({

        index:
          i,

        epoch:
          current.epoch,

        price:
          current.high
      });
    }


    if (upFractal) {

      upFractals.push({

        index:
          i,

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
   FRACTAL TRENDLINE
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
    Bullish direction:

    Use two recent DOWN fractals
    to create resistance.
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

      type:
        "RESISTANCE",

      first,

      second
    };
  }


  /*
    Bearish direction:

    Use two recent UP fractals
    to create support.
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

      type:
        "SUPPORT",

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
   15M TREND
========================================================= */

function get15MTrend(
  candles,
  direction
) {

  if (
    !candles ||
    candles.length < 10
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


  return direction;
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


  /*
    Only use CLOSED candles.
  */

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
    BUY BREAK

    Previous candle was at/below
    resistance.

    Latest CLOSED candle closed
    above resistance.
  */

  if (
    direction === "BULLISH" &&
    previous.close <=
      previousLinePrice &&
    last.close >
      linePrice
  ) {

    return {

      confirmed:
        true,

      epoch:
        last.epoch,

      line,

      linePrice
    };
  }


  /*
    SELL BREAK

    Previous candle was at/above
    support.

    Latest CLOSED candle closed
    below support.
  */

  if (
    direction === "BEARISH" &&
    previous.close >=
      previousLinePrice &&
    last.close <
      linePrice
  ) {

    return {

      confirmed:
        true,

      epoch:
        last.epoch,

      line,

      linePrice
    };
  }


  return {

    confirmed:
      false,

    epoch:
      last.epoch,

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


/* =========================================================
   RETEST QUALITY
========================================================= */

function isGoodRetestCandle(
  candle,
  direction,
  linePrice,
  tolerance
) {

  const body =
    Math.abs(
      candle.close -
      candle.open
    );


  const range =
    Math.max(
      candle.high -
      candle.low,
      0.00000001
    );


  /*
    BUY RETEST
  */

  if (
    direction === "BULLISH"
  ) {

    /*
      Price must actually reach
      or come very close to the line.
    */

    const touched =
      candle.low <=
        linePrice ||
      Math.abs(
        candle.low -
        linePrice
      ) <= tolerance;


    /*
      Candle must close back above
      the trendline.
    */

    const rejected =
      candle.close >
        linePrice &&
      candle.close >
        candle.open;


    /*
      Lower wick should show
      rejection from the line.

      We keep this moderate so
      the bot does not become
      excessively restrictive.
    */

    const lowerWick =
      Math.min(
        candle.open,
        candle.close
      ) -
      candle.low;


    const rejection =
      lowerWick >=
        Math.max(
          body * 0.15,
          range * 0.10
        );


    return (
      touched &&
      rejected &&
      rejection
    );
  }


  /*
    SELL RETEST
  */

  if (
    direction === "BEARISH"
  ) {

    const touched =
      candle.high >=
        linePrice ||
      Math.abs(
        candle.high -
        linePrice
      ) <= tolerance;


    /*
      Candle must close back
      below the trendline.
    */

    const rejected =
      candle.close <
        linePrice &&
      candle.close <
        candle.open;


    /*
      Upper wick should show
      rejection.
    */

    const upperWick =
      candle.high -
      Math.max(
        candle.open,
        candle.close
      );


    const rejection =
      upperWick >=
        Math.max(
          body * 0.15,
          range * 0.10
        );


    return (
      touched &&
      rejected &&
      rejection
    );
  }


  return false;
}


/* =========================================================
   15M RETEST
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


  /*
    IMPORTANT:

    Do not allow the actual
    breakout candle to also
    be considered the retest.

    Retest must happen AFTER
    the break candle.
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

    return {

      confirmed:
        false,

      expired:
        false
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

      confirmed:
        false,

      expired:
        true,

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

      confirmed:
        false,

      expired:
        false
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
        0.25;


  const confirmed =
    isGoodRetestCandle(
      last,
      direction,
      linePrice,
      tolerance
    );


  if (confirmed) {

    return {

      confirmed:
        true,

      expired:
        false,

      epoch:
        last.epoch,

      linePrice
    };
  }


  return {

    confirmed:
      false,

    expired:
      false,

    epoch:
      last.epoch,

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

      confirmed:
        false,

      epoch:
        null
    };
  }


  const completed =
    candles.slice(0, -1);


  /*
    Only candles AFTER
    the 15M retest.
  */

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

      confirmed:
        false,

      epoch:
        null
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

      confirmed:
        false,

      epoch:
        last.epoch
    };
  }


  const previous =
    completed[
      lastIndex - 1
    ];


  /*
    BUY confirmation:

    Bullish 5M candle
    closes above previous
    candle high.
  */

  if (
    direction === "BULLISH"
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
    SELL confirmation:

    Bearish 5M candle
    closes below previous
    candle low.
  */

  if (
    direction === "BEARISH"
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

    confirmed:
      false,

    epoch:
      last.epoch
  };
}


/* =========================================================
   BUILD SIGNAL
========================================================= */

function buildSignal(
  pair,
  direction,
  candles5M
) {

  const completed5M =
    candles5M.slice(0, -1);


  if (
    completed5M.length < 15
  ) {

    return null;
  }


  const last =
    completed5M[
      completed5M.length - 1
    ];


  const atr =
    calculateATR(
      completed5M
    );


  if (!atr) {
    return null;
  }


  const entry =
    last.close;


  let stopLoss;

  let takeProfit;


  /*
    BUY
  */

  if (
    direction === "BULLISH"
  ) {

    /*
      Use recent 5M structure
      instead of relying only on
      the signal candle's low.
    */

    const recent =
      completed5M.slice(-3);


    const structureLow =
      Math.min(
        ...recent.map(
          candle =>
            candle.low
        )
      );


    stopLoss =
      structureLow -
      atr *
        SL_ATR_BUFFER;


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
    SELL
  */

  else if (
    direction === "BEARISH"
  ) {

    /*
      Use recent 5M structure
      high plus ATR buffer.
    */

    const recent =
      completed5M.slice(-3);


    const structureHigh =
      Math.max(
        ...recent.map(
          candle =>
            candle.high
        )
      );


    stopLoss =
      structureHigh +
      atr *
        SL_ATR_BUFFER;


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


  /*
    Safety check.
  */

  if (
    direction === "BULLISH" &&
    (
      stopLoss >= entry ||
      takeProfit <= entry
    )
  ) {

    return null;
  }


  if (
    direction === "BEARISH" &&
    (
      stopLoss <= entry ||
      takeProfit >= entry
    )
  ) {

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
   FORMAT SIGNAL
========================================================= */

function formatSignal(
  signal
) {

  const direction =
    signal.direction ===
    "BULLISH"
      ? "BUY"
      : "SELL";


  return (
`🚨 FOREX SIGNAL

${direction} ${signal.pair}

Entry: ${signal.entry}
SL: ${signal.stopLoss}
TP: ${signal.takeProfit}

Risk/Reward: 1:${signal.riskReward}

Confirmation:
12H → 1H → 15M Fractal Break → Retest → 5M

Timeframe: 5M

⚠️ Signal only — not automatic trading.`
  );
}


/* =========================================================
   CLEAR SETUP
========================================================= */

function clearSetup(
  currentState
) {

  currentState.setup = {

    active:
      false,

    direction:
      null,

    breakEpoch:
      null,

    line:
      null,

    linePrice:
      null,

    retestEpoch:
      null
  };
}


/* =========================================================
   RESET SCAN STATE
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

  const state =
    pairStates[pair];


  resetPairForScan(
    state
  );


  if (!symbolMap[pair]) {

    state.status =
      "NOT MAPPED";

    state.error =
      "No Deriv symbol mapped";

    return state;
  }


  try {

    /*
      1H
    */

    const candles1H =
      await getCandles(
        pair,
        3600,
        150
      );


    /*
      12H
    */

    const candles12H =
      build12HCandles(
        candles1H
      );


    /*
      15M
    */

    const candles15M =
      await getCandles(
        pair,
        900,
        150
      );


    /*
      5M
    */

    const candles5M =
      await getCandles(
        pair,
        300,
        150
      );


    /* =====================================================
       12H DIRECTION
    ===================================================== */

    const direction12H =
      get12HDirection(
        candles12H
      );


    state.direction12H =
      direction12H;


    if (
      direction12H ===
      "WAIT"
    ) {

      clearSetup(
        state
      );

      state.status =
        "WAIT";

      return state;
    }


    /* =====================================================
       1H STRUCTURE
    ===================================================== */

    const structure1H =
      get1HStructure(
        candles1H
      );


    state.structure1H =
      structure1H;


    /*
      12H and 1H must agree.
    */

    if (
      structure1H !==
      direction12H
    ) {

      /*
        If the old setup belongs
        to the opposite direction,
        remove it.
      */

      if (
        state.setup.active &&
        state.setup.direction !==
          direction12H
      ) {

        clearSetup(
          state
        );
      }


      state.status =
        "WAIT";

      return state;
    }


    /* =====================================================
       15M TREND
    ===================================================== */

    const trend15M =
      get15MTrend(
        candles15M,
        direction12H
      );


    state.trend15M =
      trend15M;


    if (
      trend15M ===
      "WAIT"
    ) {

      state.status =
        "WAIT";

      return state;
    }


    /* =====================================================
       15M BREAK
    ===================================================== */

    const break15M =
      check15MBreak(
        candles15M,
        direction12H
      );


    if (!break15M) {

      state.break15M =
        "WAIT";

      state.status =
        "WAIT";

      return state;
    }


    state.break15M =
      break15M.confirmed
        ? "CONFIRMED"
        : "WAIT";


    /*
      Only create a NEW setup when
      a new 15M breakout candle appears.
    */

    if (
      break15M.confirmed &&
      (
        !state.setup.active ||
        state.setup.breakEpoch !==
          break15M.epoch
      )
    ) {

      state.setup = {

        active:
          true,

        direction:
          direction12H,

        breakEpoch:
          break15M.epoch,

        line:
          break15M.line,

        linePrice:
          break15M.linePrice,

        retestEpoch:
          null
      };


      console.log(
        `[${pair}] NEW 15M ${direction12H} trendline break`
      );
    }


    /*
      No active setup = wait.
    */

    if (
      !state.setup.active
    ) {

      state.status =
        "WAIT";

      return state;
    }


    /*
      Make sure the setup direction
      still matches the current
      higher timeframe direction.
    */

    if (
      state.setup.direction !==
      direction12H
    ) {

      clearSetup(
        state
      );

      state.status =
        "WAIT";

      return state;
    }


    /* =====================================================
       15M RETEST
    ===================================================== */

    const retest =
      checkRetest(
        candles15M,
        direction12H,
        state.setup
      );


    if (!retest) {

      state.status =
        "WAIT";

      return state;
    }


    if (
      retest.expired
    ) {

      console.log(
        `[${pair}] 15M retest expired`
      );


      clearSetup(
        state
      );


      state.status =
        "WAIT";

      return state;
    }


    state.retest15M =
      retest.confirmed
        ? "CONFIRMED"
        : "WAIT";


    if (
      !retest.confirmed
    ) {

      state.status =
        "WAIT";

      return state;
    }


    /*
      Save the actual retest candle.
    */

    state.setup.retestEpoch =
      retest.epoch;


    /* =====================================================
       5M CONFIRMATION
    ===================================================== */

    const confirmation5M =
      check5MConfirmation(
        candles5M,
        direction12H,
        retest.epoch
      );


    state.confirm5M =
      confirmation5M.confirmed
        ? "CONFIRMED"
        : "WAIT";


    if (
      !confirmation5M.confirmed
    ) {

      state.status =
        "WAIT";

      return state;
    }


    /* =====================================================
       BUILD SIGNAL
    ===================================================== */

    const signal =
      buildSignal(
        pair,
        direction12H,
        candles5M
      );


    if (!signal) {

      state.status =
        "WAIT";

      return state;
    }


    const signalEpoch =
      confirmation5M.epoch;


    /*
      Prevent duplicate Telegram
      alerts from the same 5M candle.
    */

    if (
      state.lastSignalEpoch ===
      signalEpoch
    ) {

      state.signal =
        signal;

      state.entry =
        signal.entry;

      state.stopLoss =
        signal.stopLoss;

      state.takeProfit =
        signal.takeProfit;

      state.status =
        "SIGNAL";

      return state;
    }


    state.signal =
      signal;

    state.entry =
      signal.entry;

    state.stopLoss =
      signal.stopLoss;

    state.takeProfit =
      signal.takeProfit;

    state.status =
      "SIGNAL";

    state.lastSignalEpoch =
      signalEpoch;


    console.log(
      "========================================"
    );

    console.log(
      `[${pair}] ${signal.direction} SIGNAL`
    );

    console.log(
      `Entry: ${signal.entry}`
    );

    console.log(
      `SL: ${signal.stopLoss}`
    );

    console.log(
      `TP: ${signal.takeProfit}`
    );

    console.log(
      "========================================"
    );


    await sendTelegram(
      formatSignal(
        signal
      )
    );


    /*
      Clear the setup after
      the signal is issued.
    */

    clearSetup(
      state
    );


    return state;

  } catch (error) {

    if (
      isMarketClosedError(
        error.message
      )
    ) {

      state.status =
        "MARKET CLOSED";

      state.error =
        error.message;

      return state;
    }


    state.status =
      "ERROR";

    state.error =
      error.message;


    console.error(
      `[${pair}] ERROR:`,
      error.message
    );


    return state;
  }
}


/* =========================================================
   SCAN ALL PAIRS
========================================================= */

let scanRunning =
  false;

let lastScanTime =
  null;


async function scanAllPairs() {

  if (scanRunning) {

    console.log(
      "[SCAN] Scan already running"
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


    try {

      await loadActiveSymbols();

    } catch (error) {

      console.error(
        "[SCAN] Symbol reload failed:",
        error.message
      );

      return;
    }
  }


  scanRunning =
    true;


  lastScanTime =
    new Date().toISOString();


  console.log(
    "========================================"
  );

  console.log(
    "[SCAN] Starting scan..."
  );

  console.log(
    "========================================"
  );


  try {

    for (
      const pair of PAIRS
    ) {

      await scanPair(
        pair
      );


      await sleep(350);
    }

  } catch (error) {

    console.error(
      "[SCAN] Fatal scan error:",
      error.message
    );

  } finally {

    scanRunning =
      false;


    console.log(
      "[SCAN] Scan complete"
    );
  }
}


/* =========================================================
   STATUS API
========================================================= */

app.get(
  "/api/status",
  (req, res) => {

    const mappedPairs =
      PAIRS.filter(
        pair =>
          Boolean(
            symbolMap[pair]
          )
      );


    res.json({

      success:
        true,

      bot: {

        online:
          true,

        derivConnected,

        activeSymbolsLoaded,

        totalPairs:
          PAIRS.length,

        mappedPairs:
          mappedPairs.length,

        unmappedPairs:
          PAIRS.filter(
            pair =>
              !symbolMap[pair]
          ),

        scanRunning,

        lastScan:
          lastScanTime
      },

      pairs:
        pairStates,

      symbols:
        symbolMap
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

      success:
        true,

      activeSymbolsLoaded,

      totalActiveSymbols:
        activeSymbols.length,

      totalPairs:
        PAIRS.length,

      mappedPairs:
        PAIRS.filter(
          pair =>
            Boolean(
              symbolMap[pair]
            )
        ),

      unmappedPairs:
        PAIRS.filter(
          pair =>
            !symbolMap[pair]
        ),

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

      return res
        .status(409)
        .json({

          success:
            false,

          message:
            "Scan already running"
        });
    }


    scanAllPairs();


    res.json({

      success:
        true,

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

      status:
        "ok",

      derivConnected,

      activeSymbolsLoaded,

      pairs:
        PAIRS.length
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
      `Pairs configured: ${PAIRS.length}`
    );

    console.log(
      "Deriv is the market-price source"
    );

    console.log(
      "12H constructed from completed 1H candles"
    );

    console.log(
      "Strategy:"
    );

    console.log(
      "12H → 1H → 15M Williams Fractal → Break → Retest → 5M"
    );

    console.log(
      "Risk/Reward: 1:2"
    );

    console.log(
      "Signal only — no automatic trading"
    );

    console.log(
      "========================================"
    );


    try {

      await connectDeriv();


      console.log(
        "[DERIV] Initial connection complete"
      );


      setTimeout(
        () => {

          scanAllPairs();

        },
        5000
      );

    } catch (error) {

      console.error(
        "[DERIV] Initial connection failed:",
        error.message
      );
    }
  }
);


/* =========================================================
   AUTOMATIC SCANNER
========================================================= */

setInterval(
  () => {

    if (
      derivConnected &&
      activeSymbolsLoaded &&
      !scanRunning
    ) {

      scanAllPairs();
    }

  },
  SCAN_INTERVAL_MS
);

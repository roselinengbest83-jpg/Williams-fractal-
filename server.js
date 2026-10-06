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
  Maximum age of a 15M breakout:
  2 hours.
*/
const MAX_BREAK_AGE_15M =
  60 * 60 * 2;

/*
  Retest tolerance.
*/
const RETEST_ATR_MULTIPLIER = 0.35;

/*
  Automatic scanner.
*/
const SCAN_INTERVAL_MS =
  5 * 60 * 1000;

/*
  Signal score.

  12H = 2
  1H  = 2
  15M break = 2
  Retest = 2
  5M = 2

  8/10 is required.
*/
const SIGNAL_SCORE_THRESHOLD = 8;


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

    score: 0,

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
  return new Promise(resolve => setTimeout(resolve, ms));
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
    Normal forex matches.
  */

  for (const value of values) {

    if (aliases[value]) {
      return aliases[value];
    }
  }


  /*
    FRXEURUSD
    FRXGBPJPY
    etc.
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
    Search inside longer strings.
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
    GOLD / XAU.
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
   LOAD DERIV ACTIVE SYMBOLS
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
        active_symbols: "brief"
      });


    if (response.error) {

      console.error(
        "[DERIV] Active symbols error:",
        response.error.message
      );

      activeSymbolsLoaded = false;

      return false;
    }


    activeSymbols =
      response.active_symbols || [];


    console.log(
      `[DERIV] Received ${activeSymbols.length} active symbols`
    );


    if (!activeSymbols.length) {

      console.error(
        "[DERIV] No active symbols returned"
      );

      activeSymbolsLoaded = false;

      return false;
    }


    /*
      Clear old mapping.
    */

    for (
      const key of Object.keys(symbolMap)
    ) {

      delete symbolMap[key];
    }


    console.log(
      "=========================================="
    );

    console.log(
      "[DERIV] FOREX / GOLD SYMBOL DEBUG"
    );

    console.log(
      "=========================================="
    );


    /*
      Debug symbols.

      This helps identify unmapped
      Deriv instruments.
    */

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
      Map pairs.
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
      Extra Gold fallback.
    */

    if (
      !symbolMap["XAU/USD"]
    ) {

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

      if (
        symbolMap[pair]
      ) {

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
   SEND DERIV REQUEST
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
        req_id: reqId
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
    Remove current forming 1H candle.
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
    Strong bullish sequence.
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
    Strong bearish sequence.
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


  /*
    If the strict condition fails,
    use the latest completed candle
    against the previous one.
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
    Softer fallback.
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
   CALCULATE TRENDLINE
========================================================= */

/*
   IMPORTANT

   BUY:
   Use two recent DOWN fractals.
   They create descending resistance.

   SELL:
   Use two recent UP fractals.
   They create ascending support.
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


    /*
      For BUY we want a
      descending resistance.

      If it is not descending,
      it is not the desired
      resistance structure.
    */

    if (
      second.price >=
      first.price
    ) {

      return null;
    }


    return {

      type:
        "RESISTANCE",

      first,

      second
    };
  }


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


    /*
      For SELL we want an
      ascending support.
    */

    if (
      second.price <=
      first.price
    ) {

      return null;
    }


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


/* =========================================================
   15M BREAK DETECTION
========================================================= */

/*
   This checks the LAST COMPLETED 15M candle.

   BUY:
   previous close <= resistance
   LAST close > resistance

   SELL:
   previous close >= support
   LAST close < support

   WICKS DO NOT COUNT.

   The candle must CLOSE beyond the line.
*/

function detect15MBreak(
  candles
) {

  if (
    !candles ||
    candles.length < 30
  ) {

    return null;
  }


  const completed =
    candles.slice(0, -1);


  const last =
    completed[
      completed.length - 1
    ];

  const previous =
    completed[
      completed.length - 2
    ];


  /*
    ==========================
    BUY BREAK
    ==========================
  */

  const bullishLine =
    calculateTrendLine(
      completed,
      "BULLISH"
    );


  if (bullishLine) {

    const currentLinePrice =
      trendLinePrice(
        bullishLine,
        last.epoch
      );


    const previousLinePrice =
      trendLinePrice(
        bullishLine,
        previous.epoch
      );


    if (
      Number.isFinite(
        currentLinePrice
      ) &&
      Number.isFinite(
        previousLinePrice
      )
    ) {

      if (
        previous.close <=
          previousLinePrice &&
        last.close >
          currentLinePrice
      ) {

        return {

          direction:
            "BULLISH",

          epoch:
            last.epoch,

          line:
            bullishLine,

          linePrice:
            currentLinePrice,

          type:
            "RESISTANCE_BREAK"
        };
      }
    }
  }


  /*
    ==========================
    SELL BREAK
    ==========================
  */

  const bearishLine =
    calculateTrendLine(
      completed,
      "BEARISH"
    );


  if (bearishLine) {

    const currentLinePrice =
      trendLinePrice(
        bearishLine,
        last.epoch
      );


    const previousLinePrice =
      trendLinePrice(
        bearishLine,
        previous.epoch
      );


    if (
      Number.isFinite(
        currentLinePrice
      ) &&
      Number.isFinite(
        previousLinePrice
      )
    ) {

      if (
        previous.close >=
          previousLinePrice &&
        last.close <
          currentLinePrice
      ) {

        return {

          direction:
            "BEARISH",

          epoch:
            last.epoch,

          line:
            bearishLine,

          linePrice:
            currentLinePrice,

          type:
            "SUPPORT_BREAK"
        };
      }
    }
  }


  return null;
}


/* =========================================================
   15M TREND STATUS
========================================================= */

function get15MTrendStatus(
  candles
) {

  if (
    !candles ||
    candles.length < 30
  ) {

    return {
      bullish: false,
      bearish: false
    };
  }


  const completed =
    candles.slice(0, -1);


  const bullishLine =
    calculateTrendLine(
      completed,
      "BULLISH"
    );


  const bearishLine =
    calculateTrendLine(
      completed,
      "BEARISH"
    );


  return {

    bullish:
      Boolean(bullishLine),

    bearish:
      Boolean(bearishLine)
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
   RETEST
========================================================= */

function checkRetest(
  candles,
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
        0.35;


  /*
    ==========================
    BUY RETEST
    ==========================
  */

  if (
    setup.direction ===
    "BULLISH"
  ) {

    const touchedLine =
      last.low <=
        linePrice +
          tolerance &&
      last.high >=
        linePrice -
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

        confirmed:
          true,

        expired:
          false,

        epoch:
          last.epoch,

        linePrice
      };
    }
  }


  /*
    ==========================
    SELL RETEST
    ==========================
  */

  if (
    setup.direction ===
    "BEARISH"
  ) {

    const touchedLine =
      last.high >=
        linePrice -
          tolerance &&
      last.low <=
        linePrice +
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

        confirmed:
          true,

        expired:
          false,

        epoch:
          last.epoch,

        linePrice
      };
    }
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
    BUY confirmation.

    Strong bullish candle
    closes above previous high.
  */

  if (
    direction ===
    "BULLISH"
  ) {

    const confirmed =
      last.close >
        last.open &&
      last.close >
        previous.high;


    return {

      confirmed,

      epoch:
        last.epoch
    };
  }


  /*
    SELL confirmation.

    Strong bearish candle
    closes below previous low.
  */

  if (
    direction ===
    "BEARISH"
  ) {

    const confirmed =
      last.close <
        last.open &&
      last.close <
        previous.low;


    return {

      confirmed,

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
   SCORE SETUP
========================================================= */

function calculateSignalScore(
  direction,
  direction12H,
  structure1H,
  breakConfirmed,
  retestConfirmed,
  confirm5M
) {

  let score = 0;


  /*
    12H = +2
  */

  if (
    direction12H ===
    direction
  ) {

    score += 2;
  }


  /*
    1H = +2
  */

  if (
    structure1H ===
    direction
  ) {

    score += 2;
  }


  /*
    15M break = +2
  */

  if (
    breakConfirmed
  ) {

    score += 2;
  }


  /*
    Retest = +2
  */

  if (
    retestConfirmed
  ) {

    score += 2;
  }


  /*
    5M = +2
  */

  if (
    confirm5M
  ) {

    score += 2;
  }


  return score;
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
    BUY
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
    SELL
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
      RISK_REWARD,

    timeframe:
      "5M",

    strategy:
      "12H → 1H → 15M Williams Fractal Break → Retest → 5M",

    signalType:
      direction === "BULLISH"
        ? "15M Resistance Break"
        : "15M Support Break"
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


    if (
      !response.ok
    ) {

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
  signal,
  score
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

Score: ${score}/10

Confirmation:
12H → 1H → 15M Fractal Break → Retest → 5M

Trigger:
${signal.signalType}

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
   RESET SCAN DISPLAY
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

  currentState.score =
    0;

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


  /*
    IMPORTANT:

    Do not clear an active setup
    during every scan.

    The setup must survive while
    waiting for the retest.
  */

  const previousSetup =
    state.setup &&
    state.setup.active
      ? {
          ...state.setup
        }
      : null;


  resetPairForScan(
    state
  );


  /*
    Restore active setup.
  */

  if (previousSetup) {

    state.setup =
      previousSetup;

    state.status =
      "WAITING RETEST";
  }


  if (
    !symbolMap[pair]
  ) {

    state.status =
      "NOT MAPPED";

    state.error =
      "No Deriv symbol mapped";

    return state;
  }


  try {

    /*
      ==========================
      1H
      ==========================
    */

    const candles1H =
      await getCandles(
        pair,
        3600,
        150
      );


    /*
      ==========================
      12H
      ==========================
    */

    const candles12H =
      build12HCandles(
        candles1H
      );


    /*
      ==========================
      15M
      ==========================
    */

    const candles15M =
      await getCandles(
        pair,
        900,
        150
      );


    /*
      ==========================
      5M
      ==========================
    */

    const candles5M =
      await getCandles(
        pair,
        300,
        150
      );


    /*
      ==========================
      12H
      ==========================
    */

    const direction12H =
      get12HDirection(
        candles12H
      );


    state.direction12H =
      direction12H;


    /*
      ==========================
      1H
      ==========================
    */

    const structure1H =
      get1HStructure(
        candles1H
      );


    state.structure1H =
      structure1H;


    /*
      ==========================
      15M TRENDLINES
      ==========================
    */

    const trendStatus =
      get15MTrendStatus(
        candles15M
      );


    if (
      trendStatus.bullish &&
      trendStatus.bearish
    ) {

      state.trend15M =
        "BOTH";

    } else if (
      trendStatus.bullish
    ) {

      state.trend15M =
        "BULLISH";

    } else if (
      trendStatus.bearish
    ) {

      state.trend15M =
        "BEARISH";

    } else {

      state.trend15M =
        "WAIT";
    }


    /*
      ==========================
      DETECT NEW 15M BREAK
      ==========================

      Only detect a new break if
      we don't already have an
      active setup.
    */

    if (
      !state.setup.active
    ) {

      const newBreak =
        detect15MBreak(
          candles15M
        );


      if (newBreak) {

        state.setup = {

          active:
            true,

          direction:
            newBreak.direction,

          breakEpoch:
            newBreak.epoch,

          line:
            newBreak.line,

          linePrice:
            newBreak.linePrice,

          retestEpoch:
            null
        };


        state.break15M =
          "CONFIRMED";


        state.status =
          "WAITING RETEST";


        console.log(
          `[${pair}] 15M ${newBreak.direction} BREAK detected`
        );


        console.log(
          `[${pair}] Break price: ${newBreak.linePrice}`
        );
      }

    } else {

      /*
        Existing setup.

        Keep break confirmed.
      */

      state.break15M =
        "CONFIRMED";
    }


    /*
      No setup yet.
    */

    if (
      !state.setup.active
    ) {

      state.status =
        "WAIT";

      return state;
    }


    /*
      ==========================
      RETEST
      ==========================
    */

    const retest =
      checkRetest(
        candles15M,
        state.setup
      );


    if (!retest) {

      state.status =
        "WAITING RETEST";

      return state;
    }


    /*
      Break expired.
    */

    if (
      retest.expired
    ) {

      console.log(
        `[${pair}] 15M setup expired`
      );


      clearSetup(
        state
      );


      state.break15M =
        "WAIT";

      state.retest15M =
        "WAIT";

      state.confirm5M =
        "WAIT";

      state.status =
        "WAIT";

      return state;
    }


    /*
      Retest not yet confirmed.
    */

    if (
      !retest.confirmed
    ) {

      state.retest15M =
        "WAIT";


      state.confirm5M =
        "WAIT";


      /*
        Score the setup so the
        dashboard can see progress.
      */

      state.score =
        calculateSignalScore(

          state.setup.direction,

          direction12H,

          structure1H,

          true,

          false,

          false
        );


      state.status =
        "WAITING RETEST";


      return state;
    }


    /*
      Retest confirmed.
    */

    state.retest15M =
      "CONFIRMED";


    state.setup.retestEpoch =
      retest.epoch;


    /*
      ==========================
      5M CONFIRMATION
      ==========================
    */

    const confirmation5M =
      check5MConfirmation(

        candles5M,

        state.setup.direction,

        retest.epoch
      );


    state.confirm5M =
      confirmation5M.confirmed
        ? "CONFIRMED"
        : "WAIT";


    /*
      Calculate current score.
    */

    const score =
      calculateSignalScore(

        state.setup.direction,

        direction12H,

        structure1H,

        true,

        true,

        confirmation5M.confirmed
      );


    state.score =
      score;


    /*
      No 5M confirmation yet.
    */

    if (
      !confirmation5M.confirmed
    ) {

      state.status =
        "WAITING 5M";


      return state;
    }


    /*
      ==========================
      SCORE CHECK
      ==========================
    */

    if (
      score <
      SIGNAL_SCORE_THRESHOLD
    ) {

      state.status =
        "LOW SCORE";


      console.log(
        `[${pair}] Setup rejected: score ${score}/10`
      );


      return state;
    }


    /*
      ==========================
      BUILD SIGNAL
      ==========================
    */

    const signal =
      buildSignal(

        pair,

        state.setup.direction,

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
      ==========================
      DUPLICATE PROTECTION
      ==========================
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


    /*
      ==========================
      SAVE SIGNAL
      ==========================
    */

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
      `Score: ${score}/10`
    );

    console.log(
      "========================================"
    );


    /*
      TELEGRAM
    */

    await sendTelegram(
      formatSignal(
        signal,
        score
      )
    );


    /*
      Signal has been sent.

      Clear setup so the bot
      waits for a completely
      new breakout.
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

  if (
    scanRunning
  ) {

    console.log(
      "[SCAN] Scan already running"
    );

    return;
  }


  if (
    !derivConnected
  ) {

    console.log(
      "[SCAN] Deriv not connected"
    );

    return;
  }


  if (
    !activeSymbolsLoaded
  ) {

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


      /*
        Small delay between
        requests.
      */

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
          lastScanTime,

        signalScoreThreshold:
          SIGNAL_SCORE_THRESHOLD
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

    if (
      scanRunning
    ) {

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
        PAIRS.length,

      scoreThreshold:
        SIGNAL_SCORE_THRESHOLD
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
      "12H = market bias"
    );

    console.log(
      "1H = market structure"
    );

    console.log(
      "15M = Williams Fractal trendline breakout"
    );

    console.log(
      "BUY = close above descending resistance"
    );

    console.log(
      "SELL = close below ascending support"
    );

    console.log(
      "Retest required"
    );

    console.log(
      "5M confirmation required"
    );

    console.log(
      `Signal score threshold: ${SIGNAL_SCORE_THRESHOLD}/10`
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

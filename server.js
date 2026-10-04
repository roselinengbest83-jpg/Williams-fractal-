const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

const FRACTAL_PERIODS = 2;
const SCAN_INTERVAL = 60 * 1000;
const RETEST_LOOKBACK_15M = 8;

const PAIRS = [
  "EUR/USD","GBP/USD","USD/JPY","GBP/JPY","EUR/JPY",
  "AUD/USD","USD/CAD","USD/CHF","AUD/JPY","NZD/USD"
];

const symbolMap = {};
const pairState = {};
for (const pair of PAIRS) {
  pairState[pair] = {
    pair, symbol: null, price: null,
    direction12H: "WAIT", structure1H: "WAIT",
    trend15M: "WAIT", fractalHigh: null, fractalLow: null,
    trendLine: null, lineBreak: "WAIT",
    retest: "WAIT", confirmation5M: "WAIT",
    signal: "WAIT", entry: null, stopLoss: null,
    takeProfit: null, riskReward: "1:2",
    lastSignalTime: null, lastError: null
  };
}

let ws = null;
let connected = false;
let requestId = 1;
const pendingRequests = new Map();

function sendRequest(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return null;
  const reqId = requestId++;
  payload.req_id = reqId;
  ws.send(JSON.stringify(payload));
  return reqId;
}

function connectDeriv() {
  console.log("[DERIV] Connecting...");
  ws = new WebSocket("wss://ws.binaryws.com/websockets/v3");

  ws.on("open", () => {
    connected = true;
    console.log("[DERIV] Connected");
    sendRequest({ active_symbols: "brief" });
  });

  ws.on("message", raw => {
    try { handleDerivMessage(JSON.parse(raw.toString())); }
    catch (e) { console.error("[DERIV] Message error:", e.message); }
  });

  ws.on("error", e => {
    connected = false;
    console.error("[DERIV] WebSocket error:", e.message);
  });

  ws.on("close", () => {
    connected = false;
    console.log("[DERIV] Disconnected. Reconnecting...");
    setTimeout(connectDeriv, 5000);
  });
}

function handleDerivMessage(data) {
  if (data.req_id && pendingRequests.has(data.req_id)) {
    const r = pendingRequests.get(data.req_id);
    clearTimeout(r.timeout);
    pendingRequests.delete(data.req_id);
    if (data.error) r.reject(new Error(data.error.message));
    else r.resolve(data);
    return;
  }

  if (data.error) {
    console.error("[DERIV ERROR]", data.error.code, data.error.message);
    return;
  }

  if (data.msg_type === "active_symbols") {
    resolveForexSymbols(data.active_symbols || []);
  }
}

function normalizeName(x) {
  return String(x || "").toUpperCase().replace(/\s+/g, "").replace("-", "/");
}

function resolveForexSymbols(symbols) {
  for (const pair of PAIRS) {
    const wanted = normalizeName(pair);
    const found = symbols.find(s => {
      const name = normalizeName(s.underlying_symbol_name || s.display_name || "");
      return name === wanted;
    });

    if (found) {
      const code = found.underlying_symbol || found.symbol;
      symbolMap[pair] = code;
      pairState[pair].symbol = code;
      console.log(`[SYMBOL] ${pair} -> ${code}`);
    } else {
      pairState[pair].lastError = "Deriv symbol not found";
      console.log(`[SYMBOL] ${pair} NOT FOUND`);
    }
  }
  startScanner();
}

function getCandles(symbol, granularity, count) {
  return new Promise((resolve, reject) => {
    const reqId = sendRequest({
      ticks_history: symbol, end: "latest", count,
      style: "candles", granularity, subscribe: 0
    });
    if (!reqId) return reject(new Error("Deriv WebSocket not connected"));

    const timeout = setTimeout(() => {
      pendingRequests.delete(reqId);
      reject(new Error("Deriv candle request timeout"));
    }, 15000);

    pendingRequests.set(reqId, { resolve, reject, timeout });
  });
}

function convertCandles(response) {
  return (response.candles || []).map(c => ({
    time: Number(c.epoch), open: Number(c.open),
    high: Number(c.high), low: Number(c.low), close: Number(c.close)
  })).filter(c => [c.open,c.high,c.low,c.close].every(Number.isFinite));
}

function build12HCandles(oneHour) {
  const groups = new Map();

  for (const c of oneHour) {
    const d = new Date(c.time * 1000);
    const startHour = d.getUTCHours() < 12 ? 0 : 12;
    const start = Date.UTC(
      d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), startHour
    ) / 1000;
    if (!groups.has(start)) groups.set(start, []);
    groups.get(start).push(c);
  }

  const result = [];
  for (const [time, group] of groups) {
    group.sort((a,b) => a.time-b.time);
    if (group.length < 12) continue;
    result.push({
      time, open: group[0].open,
      high: Math.max(...group.map(c=>c.high)),
      low: Math.min(...group.map(c=>c.low)),
      close: group[group.length-1].close
    });
  }
  return result;
}

function determineDirection(candles) {
  if (candles.length < 4) return "WAIT";
  const a=candles[candles.length-4], b=candles[candles.length-3],
        c=candles[candles.length-2], d=candles[candles.length-1];

  if (d.high > c.high && c.high >= b.high && d.low > c.low && c.low >= b.low)
    return "BULLISH";
  if (d.high < c.high && c.high <= b.high && d.low < c.low && c.low <= b.low)
    return "BEARISH";

  if (d.close > d.open) return "BULLISH";
  if (d.close < d.open) return "BEARISH";
  return "WAIT";
}

function findFractals(candles, n=2) {
  const highs=[], lows=[];
  for (let i=n; i<candles.length-n; i++) {
    let down=true, up=true;
    for (let j=1;j<=n;j++) {
      if (candles[i-j].high >= candles[i].high || candles[i+j].high >= candles[i].high) down=false;
      if (candles[i-j].low <= candles[i].low || candles[i+j].low <= candles[i].low) up=false;
    }
    if (down) highs.push({index:i,time:candles[i].time,price:candles[i].high});
    if (up) lows.push({index:i,time:candles[i].time,price:candles[i].low});
  }
  return {highs,lows};
}

function calculateTrendLine(fractals, direction) {
  if (direction === "BULLISH" && fractals.lows.length >= 2) {
    return {type:"BULLISH",a:fractals.lows.at(-2),b:fractals.lows.at(-1)};
  }
  if (direction === "BEARISH" && fractals.highs.length >= 2) {
    return {type:"BEARISH",a:fractals.highs.at(-2),b:fractals.highs.at(-1)};
  }
  return null;
}

function trendLinePrice(line,time) {
  if (!line || line.b.time === line.a.time) return line?.b.price ?? null;
  return line.a.price + ((line.b.price-line.a.price)/(line.b.time-line.a.time))*(time-line.a.time);
}

/* The 1H structure is intentionally separate from the 12H direction. */
function determine1HStructure(candles) {
  if (candles.length < 6) return "WAIT";
  const last = candles.slice(-6);
  const highs = last.map(c=>c.high), lows=last.map(c=>c.low);
  const hh = highs[5] > highs[3] && highs[3] >= highs[1];
  const hl = lows[5] > lows[3] && lows[3] >= lows[1];
  const lh = highs[5] < highs[3] && highs[3] <= highs[1];
  const ll = lows[5] < lows[3] && lows[3] <= lows[1];
  if (hh && hl) return "BULLISH";
  if (lh && ll) return "BEARISH";
  return determineDirection(candles);
}

/* Step 1: candle CLOSES beyond the 15M trend line. */
function detectLineBreak(candles, line, direction) {
  if (!line || candles.length < 2) return null;
  const prev=candles.at(-2), cur=candles.at(-1);
  const prevLine=trendLinePrice(line,prev.time);
  const curLine=trendLinePrice(line,cur.time);

  if (direction==="BULLISH" && prev.close <= prevLine && cur.close > curLine)
    return {type:"BULLISH", candle:cur};
  if (direction==="BEARISH" && prev.close >= prevLine && cur.close < curLine)
    return {type:"BEARISH", candle:cur};
  return null;
}

/*
 Step 2: after the break, wait for a RETEST/REJECTION.
 Bullish: price comes back toward/through the line and closes back above it.
 Bearish: price comes back toward/through the line and closes back below it.
 This is evaluated on completed 15M candles only.
*/
function detectRetestRejection(candles, line, direction, breakInfo) {
  if (!breakInfo || !line) return null;
  const breakIndex = candles.findIndex(c=>c.time===breakInfo.candle.time);
  if (breakIndex < 0) return null;

  const start = Math.max(breakIndex+1, candles.length-RETEST_LOOKBACK_15M);
  for (let i=start;i<candles.length;i++) {
    const c=candles[i];
    const lp=trendLinePrice(line,c.time);
    if (lp == null) continue;

    const tolerance = Math.max(Math.abs(lp)*0.00015, 0.00001);
    const touched = c.low <= lp+tolerance && c.high >= lp-tolerance;

    if (direction==="BULLISH" && touched && c.close > lp && c.close > c.open) {
      return {type:"BULLISH",candle:c};
    }
    if (direction==="BEARISH" && touched && c.close < lp && c.close < c.open) {
      return {type:"BEARISH",candle:c};
    }
  }
  return null;
}

/* Step 3: 5M confirmation after retest/rejection. */
function confirm5M(candles, direction) {
  if (candles.length < 3) return false;
  const c1=candles.at(-1), c2=candles.at(-2);
  if (direction==="BULLISH") return c1.close>c1.open && c1.close>c2.high;
  if (direction==="BEARISH") return c1.close<c1.open && c1.close<c2.low;
  return false;
}

function calculateATR(candles, period=14) {
  if (candles.length < period+1) return null;
  const trs=[];
  for (let i=1;i<candles.length;i++) {
    const c=candles[i], p=candles[i-1];
    trs.push(Math.max(c.high-c.low,Math.abs(c.high-p.close),Math.abs(c.low-p.close)));
  }
  const recent=trs.slice(-period);
  return recent.reduce((a,b)=>a+b,0)/recent.length;
}

function decimalsForPair(pair) {
  if (pair.includes("JPY")) return 3;
  return 5;
}

function roundPrice(pair, value) {
  return Number(value.toFixed(decimalsForPair(pair)));
}

function createSignal(pair,direction,candles5M,atr) {
  if (!atr || atr<=0) return null;
  const price=candles5M.at(-1).close;
  const entry=price;
  const stopLoss=direction==="BULLISH" ? price-atr : price+atr;
  const takeProfit=direction==="BULLISH" ? price+atr*2 : price-atr*2;

  return {
    pair, signal:direction==="BULLISH"?"BUY":"SELL",
    entry:roundPrice(pair,entry),
    stopLoss:roundPrice(pair,stopLoss),
    takeProfit:roundPrice(pair,takeProfit),
    riskReward:"1:2",
    time:new Date().toISOString()
  };
}

async function sendTelegram(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const url=`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    await fetch(url,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({chat_id:TELEGRAM_CHAT_ID,text:message})
    });
  } catch(e) {
    console.error("[TELEGRAM]",e.message);
  }
}

function formatSignal(s) {
  return `🚨 DERIV FOREX SIGNAL

Pair: ${s.pair}
Signal: ${s.signal}

Entry: ${s.entry}
SL: ${s.stopLoss}
TP: ${s.takeProfit}
Risk/Reward: 1:2

12H Direction
→ 1H Structure
→ 15M Fractal Trend Line
→ 15M Candle Close Break
→ Retest/Rejection
→ 5M Confirmation

⚠️ Signal only — verify before trading.`;
}

async function scanPair(pair) {
  const state=pairState[pair], symbol=symbolMap[pair];
  if (!symbol) return;

  try {
    const h1=convertCandles(await getCandles(symbol,3600,120));
    if (h1.length<30) return;
    const completedH1=h1.slice(0,-1);

    const h12=build12HCandles(completedH1);
    const direction12H=determineDirection(h12);
    const structure1H=determine1HStructure(completedH1);

    state.direction12H=direction12H;
    state.structure1H=structure1H;

    const m15=convertCandles(await getCandles(symbol,900,180));
    if (m15.length<30) return;
    const completed15M=m15.slice(0,-1);

    const fractals=findFractals(completed15M,FRACTAL_PERIODS);
    const alignedDirection =
      direction12H===structure1H ? direction12H : "WAIT";

    state.trend15M=alignedDirection;
    state.fractalHigh=fractals.highs.at(-1)||null;
    state.fractalLow=fractals.lows.at(-1)||null;

    const line=calculateTrendLine(fractals,alignedDirection);
    state.trendLine=line;

    const breakInfo=detectLineBreak(completed15M,line,alignedDirection);
    state.lineBreak=breakInfo ? "CONFIRMED" : "WAIT";

    const retest=detectRetestRejection(
      completed15M,line,alignedDirection,breakInfo
    );
    state.retest=retest ? "CONFIRMED" : "WAIT";

    const m5=convertCandles(await getCandles(symbol,300,120));
    if (m5.length<25) return;
    const completed5M=m5.slice(0,-1);

    state.price=completed5M.at(-1).close;

    const confirmation =
      retest ? confirm5M(completed5M,alignedDirection) : false;

    state.confirmation5M=confirmation ? "CONFIRMED" : "WAIT";

    if (alignedDirection!=="WAIT" && breakInfo && retest && confirmation) {
      const atr=calculateATR(completed5M,14);
      const signal=createSignal(pair,alignedDirection,completed5M,atr);
      if (!signal) return;

      const signalTime=completed5M.at(-1).time;
      if (state.lastSignalTime===signalTime) return;

      state.signal=signal.signal;
      state.entry=signal.entry;
      state.stopLoss=signal.stopLoss;
      state.takeProfit=signal.takeProfit;
      state.lastSignalTime=signalTime;

      console.log(`[SIGNAL] ${pair} ${signal.signal}`);
      await sendTelegram(formatSignal(signal));
    } else {
      state.signal="WAIT";
      state.entry=null;
      state.stopLoss=null;
      state.takeProfit=null;
    }

    state.lastError=null;
  } catch(e) {
    state.lastError=e.message;
    console.error(`[${pair}]`,e.message);
  }
}

let scanning=false;
async function scanAllPairs() {
  if (scanning) return;
  scanning=true;
  try {
    for (const pair of PAIRS) {
      await scanPair(pair);
      await new Promise(r=>setTimeout(r,700));
    }
  } finally {
    scanning=false;
  }
}

function startScanner() {
  console.log("[SCAN] Scanner started");
  scanAllPairs();
  setInterval(scanAllPairs,SCAN_INTERVAL);
}

app.get("/api/status",(req,res)=>{
  res.json({
    connected,
    strategy:[
      "12H direction",
      "1H structure",
      "15M Williams Fractal trend line",
      "15M candle close beyond line",
      "15M retest/rejection",
      "5M confirmation",
      "signal"
    ],
    pairs:pairState
  });
});

app.get("/health",(req,res)=>{
  res.json({status:"online",derivConnected:connected});
});

app.listen(PORT,()=>{
  console.log(`Server running on port ${PORT}`);
  connectDeriv();
});
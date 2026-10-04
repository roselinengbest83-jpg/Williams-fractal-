const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

const DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3";

let status = "Starting...";
let lastMessage = null;

function connectDeriv() {
  console.log("[DERIV] Connecting to:", DERIV_WS_URL);

  const ws = new WebSocket(DERIV_WS_URL);

  ws.on("open", () => {
    status = "CONNECTED";
    console.log("[DERIV] WebSocket CONNECTED");

    // Test Deriv connectivity
    ws.send(JSON.stringify({
      ping: 1,
      req_id: 1
    }));

    // Test market symbols
    ws.send(JSON.stringify({
      active_symbols: "brief",
      req_id: 2
    }));
  });

  ws.on("message", (message) => {
    const data = JSON.parse(message.toString());

    console.log("[DERIV] MESSAGE:", JSON.stringify(data));

    lastMessage = data;

    if (data.msg_type === "ping") {
      console.log("[DERIV] PING SUCCESS");
    }

    if (data.msg_type === "active_symbols") {
      console.log(
        "[DERIV] ACTIVE SYMBOLS RECEIVED:",
        data.active_symbols?.length
      );
    }

    if (data.error) {
      console.log("[DERIV] API ERROR:", data.error);
    }
  });

  ws.on("error", (error) => {
    status = "ERROR";
    console.log("[DERIV] WebSocket ERROR:", error.message);
  });

  ws.on("close", (code, reason) => {
    status = "CLOSED";

    console.log(
      "[DERIV] WebSocket CLOSED. Code:",
      code,
      "Reason:",
      reason.toString()
    );

    setTimeout(connectDeriv, 5000);
  });
}

connectDeriv();

app.get("/", (req, res) => {
  res.json({
    bot: "Deriv Connection Test",
    status,
    lastMessage
  });
});

app.get("/health", (req, res) => {
  res.json({
    status,
    deriv: status
  });
});

app.listen(PORT, () => {
  console.log(`[SERVER] Running on port ${PORT}`);
});

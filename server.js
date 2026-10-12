/**
 * Game Night — multiplayer WebSocket server
 *
 * Simple room-based relay server. Clients connect over WebSocket and exchange
 * JSON messages. Rooms are identified by a 4-character code chosen by the
 * host; no authentication is needed.
 *
 * Message protocol (all JSON):
 *   Client -> Server
 *     {"type":"host-ready","code":"ABCD"}          Host creates/joins room
 *     {"type":"join-request","code":"ABCD","name":"Player","clientId":"..."}
 *     {"type":"join-accepted","code":"ABCD","name":"Player","clientId":"..."}
 *     {"type":"game-event","code":"ABCD","event":{...}}
 *
 *   Server -> Client
 *     {"type":"room-ready","code":"ABCD"}           Ack after host-ready
 *     (join-request / join-accepted / game-event are forwarded as received)
 */

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

// code (uppercased) -> Set of WebSocket clients in that room
const rooms = new Map();

/** Send a JSON payload to a single socket, ignoring dead connections. */
function sendJson(socket, payload) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

/** Broadcast a JSON payload to every socket in a room, optionally skipping one. */
function broadcast(room, payload, exceptSocket) {
  const clients = rooms.get(room);
  if (!clients) return;
  const data = JSON.stringify(payload);
  for (const client of clients) {
    if (client !== exceptSocket && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

/** Remove a socket from every room it belongs to; drop empty rooms. */
function removeFromRooms(socket) {
  for (const [code, clients] of rooms) {
    if (clients.delete(socket) && clients.size === 0) {
      rooms.delete(code);
      console.log(`room ${code} closed (empty)`);
    }
  }
}

// Plain HTTP server: health check for hosting platforms + WebSocket upgrade.
const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        rooms: rooms.size,
        service: "game-night-server",
      })
    );
    return;
  }
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not found" }));
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (socket) => {
  // Track which rooms this socket joined (for fast cleanup on disconnect).
  const myRooms = new Set();

  socket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      sendJson(socket, { type: "error", error: "invalid-json" });
      return;
    }

    if (typeof msg.type !== "string") {
      sendJson(socket, { type: "error", error: "missing-type" });
      return;
    }

    // Normalize the room code: uppercase, trimmed.
    const code =
      typeof msg.code === "string" ? msg.code.trim().toUpperCase() : null;

    switch (msg.type) {
      case "host-ready": {
        if (!code) {
          sendJson(socket, { type: "error", error: "missing-code" });
          return;
        }
        if (!rooms.has(code)) rooms.set(code, new Set());
        rooms.get(code).add(socket);
        myRooms.add(code);
        sendJson(socket, { type: "room-ready", code });
        console.log(`host-ready: room ${code} (clients: ${rooms.get(code).size})`);
        break;
      }

      case "join-request": {
        if (!code || !rooms.has(code)) {
          sendJson(socket, {
            type: "join-rejected",
            code: code || null,
            reason: "room-not-found",
          });
          return;
        }
        rooms.get(code).add(socket);
        myRooms.add(code);
        // Forward the request to the host(s); guest waits for join-accepted.
        broadcast(code, {
          type: "join-request",
          code,
          name: msg.name || "Guest",
          clientId: msg.clientId || null,
        });
        console.log(`join-request: ${msg.name} -> room ${code}`);
        break;
      }

      case "join-accepted": {
        if (!code || !rooms.has(code)) return;
        // Forward to everyone in the room; the guest filters by name/clientId.
        // CRITICAL: forward the welcome `event` (map, tokens, fog, objects, chat)
        // so joining/rejoining players receive the full table state.
        broadcast(code, {
          type: "join-accepted",
          code,
          name: msg.name || null,
          clientId: msg.clientId || null,
          event: msg.event || {},
        });
        console.log(`join-accepted: ${msg.name} in room ${code}`);
        break;
      }

      case "game-event": {
        if (!code || !rooms.has(code)) return;
        // Broadcast to the room, excluding the sender so it doesn't echo back.
        // Preserve routing fields (sender, direction, target, exclude) so clients can route correctly.
        broadcast(
          code,
          {
            type: "game-event",
            code,
            event: msg.event || {},
            sender: msg.sender || null,
            direction: msg.direction || null,
            target: msg.target || null,
            exclude: msg.exclude || null,
          },
          socket
        );
        break;
      }

      default:
        sendJson(socket, { type: "error", error: "unknown-type" });
    }
  });

  socket.on("close", () => {
    removeFromRooms(socket);
  });

  socket.on("error", () => {
    // Errors are followed by 'close'; nothing extra to do here.
  });
});

server.listen(PORT, () => {
  console.log(`game-night-server listening on port ${PORT}`);
});

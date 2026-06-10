const http = require('http');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const ROOT = path.join(__dirname, 'src');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.wav': 'audio/wav',
  '.mp3': 'audio/mpeg'
};

// --- Static file server (serves the tools from src/) ---
const server = http.createServer((req, res) => {
  let urlPath;
  try {
    urlPath = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  } catch {
    res.writeHead(400);
    res.end('Bad request');
    return;
  }

  if (urlPath.endsWith('/')) urlPath += 'index.html';

  const filePath = path.normalize(path.join(ROOT, urlPath));
  if (!filePath.startsWith(ROOT + path.sep)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    const type = MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': type });
    res.end(data);
  });
});

// --- Signaling / sync server (same port as HTTP) ---
const wss = new WebSocket.Server({ server });

// Most recent sequencer state, replayed to clients that join late
let lastState = null;

wss.on('connection', (ws) => {
  if (lastState) ws.send(JSON.stringify(lastState));

  ws.on('message', (message) => {
    let msg;
    try {
      msg = JSON.parse(message.toString());
    } catch {
      return; // Don't relay malformed messages — they'd break every client's onmessage
    }

    if (msg.id) ws.clientId = msg.id;
    if (msg.type === 'state_update') lastState = msg;

    broadcast(msg, ws);
  });

  ws.on('close', () => {
    if (ws.clientId) {
      broadcast({ type: 'client_left', id: ws.clientId }, ws);
    }
  });
});

function broadcast(msg, except) {
  const data = JSON.stringify(msg);
  wss.clients.forEach((client) => {
    if (client !== except && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

server.listen(PORT, () => {
  console.log(`Web audio tools running on http://localhost:${PORT} (WebSocket on same port)`);
});

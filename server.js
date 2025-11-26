const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: 8080 });

console.log("Signaling server running on port 8080");

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    // Broadcast incoming message to all OTHER clients
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message.toString());
      }
    });
  });
});
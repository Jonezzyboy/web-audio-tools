// Room-based WebSocket relay for the tone sequencer, mirroring server.js:
// broadcast every JSON message to the room, remember the latest state_update
// and replay it to late joiners. One Durable Object instance per room code.

export class Room {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected a WebSocket', { status: 426 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    // acceptWebSocket (not accept) so the object can hibernate while idle
    this.ctx.acceptWebSocket(server);
    const lastState = await this.ctx.storage.get('lastState');
    if (lastState) server.send(lastState);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    if (typeof message !== 'string') return;
    let msg;
    try {
      msg = JSON.parse(message);
    } catch {
      return; // Don't relay malformed messages — they'd break every client's onmessage
    }

    // Attachment survives hibernation, unlike instance fields
    if (msg.id) ws.serializeAttachment({ clientId: msg.id });
    if (msg.type === 'state_update') await this.ctx.storage.put('lastState', message);

    this.broadcast(message, ws);
  }

  webSocketClose(ws) {
    this.dropClient(ws);
  }

  webSocketError(ws) {
    this.dropClient(ws);
  }

  dropClient(ws) {
    const att = ws.deserializeAttachment();
    if (att && att.clientId) {
      this.broadcast(JSON.stringify({ type: 'client_left', id: att.clientId }), ws);
    }
  }

  broadcast(data, except) {
    for (const client of this.ctx.getWebSockets()) {
      if (client === except) continue;
      try {
        client.send(data);
      } catch {
        // Client already gone; close event will handle it
      }
    }
  }
}

export default {
  fetch(request, env) {
    const match = new URL(request.url).pathname.match(/^\/room\/([\w-]{1,32})$/);
    if (!match) return new Response('Not found', { status: 404 });
    return env.ROOMS.get(env.ROOMS.idFromName(match[1])).fetch(request);
  },
};

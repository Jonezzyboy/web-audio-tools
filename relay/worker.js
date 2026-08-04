// Room-based WebSocket relay for the tone sequencer, mirroring server.js:
// broadcast every JSON message to the room, remember the latest state_update
// and replay it to late joiners. One Durable Object instance per room code.
//
// The first client to present a hostKey claims the room as host; the key lives
// in the creator's localStorage so refreshing keeps host status. Hosts receive
// a roster of connected users and may kick, which bans the target's IP for the
// life of the room (unless it matches the host's own IP — same-network safety).

export class Room {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected a WebSocket', { status: 426 });
    }
    const ip = request.headers.get('CF-Connecting-IP') || '';
    const banned = (await this.ctx.storage.get('bannedIps')) || [];
    if (ip && banned.includes(ip)) {
      return new Response('Removed from this session', { status: 403 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    // acceptWebSocket (not accept) so the object can hibernate while idle
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ ip });
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
    const att = ws.deserializeAttachment() || {};

    if (msg.type === 'join') {
      att.clientId = msg.id;
      att.color = msg.color;
      let hostKey = await this.ctx.storage.get('hostKey');
      if (msg.hostKey && !hostKey) {
        await this.ctx.storage.put('hostKey', msg.hostKey);
        hostKey = msg.hostKey;
      }
      att.isHost = Boolean(msg.hostKey) && msg.hostKey === hostKey;
      ws.serializeAttachment(att);
      ws.send(JSON.stringify({ type: 'host_status', isHost: att.isHost }));
      this.broadcast(message, ws);
      this.broadcastRoster();
      return;
    }

    if (msg.type === 'kick') {
      if (!att.isHost) return;
      const target = this.ctx
        .getWebSockets()
        .find((w) => (w.deserializeAttachment() || {}).clientId === msg.targetId);
      if (!target || target === ws) return;

      const targetAtt = target.deserializeAttachment() || {};
      if (targetAtt.ip && targetAtt.ip !== att.ip) {
        const banned = (await this.ctx.storage.get('bannedIps')) || [];
        if (!banned.includes(targetAtt.ip)) {
          banned.push(targetAtt.ip);
          await this.ctx.storage.put('bannedIps', banned);
        }
      }
      try {
        target.send(JSON.stringify({ type: 'kicked' }));
        target.close(4001, 'kicked');
      } catch {}
      this.broadcast(JSON.stringify({ type: 'client_left', id: msg.targetId }), target);
      this.broadcastRoster(target);
      return;
    }

    // Cursor moves arrive ~20/sec — only persist the attachment when it changes
    if (msg.id && att.clientId !== msg.id) {
      att.clientId = msg.id;
      ws.serializeAttachment(att);
    }
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
    const att = ws.deserializeAttachment() || {};
    if (att.clientId) {
      this.broadcast(JSON.stringify({ type: 'client_left', id: att.clientId }), ws);
    }
    this.broadcastRoster(ws);
  }

  broadcastRoster(except) {
    const users = this.ctx
      .getWebSockets()
      .filter((w) => w !== except)
      .map((w) => w.deserializeAttachment() || {})
      .filter((a) => a.clientId)
      .map((a) => ({ id: a.clientId, color: a.color, isHost: Boolean(a.isHost) }));
    this.broadcast(JSON.stringify({ type: 'roster', users }), except);
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

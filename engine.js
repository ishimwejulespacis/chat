const WebSocket = require('ws');
const http = require('http');
const url = require('url');
const { randomUUID } = require('crypto');

class MessagingEngine {
  constructor(port = 3000) {
    this.port = port;
    this.server = null;
    this.wss = null;
    this.users = new Map();         // userId -> WebSocket
    this.userNames = new Map();     // userId -> display name
    this.groups = new Map();        // groupId -> { id, name, members: Set<userId> }
  }

  start() {
    this.server = http.createServer((req, res) => this.handleHttp(req, res));
    this.wss = new WebSocket.Server({ server: this.server });

    this.wss.on('connection', (ws) => {
      let userId = null;

      // First message MUST be registration
      ws.once('message', (raw) => {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.type !== 'register' || !msg.userId) {
            ws.close(4001, 'First message must be { type: "register", userId: "..." }');
            return;
          }

          // Prevent duplicate connections for the same user
          if (this.users.has(msg.userId)) {
            ws.send(JSON.stringify({ type: 'error', message: 'User ID already connected' }));
            ws.close();
            return;
          }

          userId = msg.userId;
          const userName = msg.name || userId;   // use provided name or fallback to userId
          this.users.set(userId, ws);
          this.userNames.set(userId, userName);
          ws.send(JSON.stringify({ type: 'registered', userId, userName }));
          console.log(`User connected: ${userId} (${userName})`);

          // Now handle further messages
          ws.on('message', (data) => this.handleMessage(ws, userId, data));
        } catch (e) {
          ws.close(4001, 'Invalid registration JSON');
        }
      });

      ws.on('close', () => {
        if (userId) {
          this.users.delete(userId);
          this.userNames.delete(userId);
          // Remove user from all groups
          for (const [groupId, group] of this.groups) {
            group.members.delete(userId);
            if (group.members.size === 0) {
              this.groups.delete(groupId);
            }
          }
          console.log(`User disconnected: ${userId}`);
        }
      });

      ws.on('error', (err) => console.error(`WebSocket error for ${userId}:`, err.message));
    });

    this.server.listen(this.port, () => {
      console.log(`🚀 Messaging engine running on port ${this.port}`);
    });
  }

  handleHttp(req, res) {
    const parsed = url.parse(req.url, true);
    const method = req.method;

    if (parsed.pathname === '/health' && method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', users: this.users.size, groups: this.groups.size }));
      return;
    }

    if (parsed.pathname === '/send' && method === 'POST') {
      let body = '';
      req.on('data', chunk => body += chunk);
      req.on('end', () => {
        try {
          const { to, type = 'chat', payload } = JSON.parse(body);
          const target = this.users.get(to);
          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify({ from: 'api', type, payload }));
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true }));
          } else {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: 'User not connected' }));
          }
        } catch (e) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'Invalid JSON' }));
        }
      });
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  }

  handleMessage(ws, userId, rawData) {
    let msg;
    try {
      msg = JSON.parse(rawData.toString());
    } catch (e) {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
      return;
    }

    switch (msg.type) {
      case 'chat':
        this.handleDirectChat(ws, userId, msg);
        break;
      case 'file':
        this.handleDirectFile(ws, userId, msg);
        break;
      case 'call-offer':
      case 'call-answer':
      case 'ice-candidate':
        this.relayDirect(msg.to, {
          type: msg.type,
          from: userId,
          sdp: msg.sdp,
          candidate: msg.candidate
        });
        break;

      case 'create-group':
        this.createGroup(ws, userId, msg);
        break;
      case 'join-group':
        this.joinGroup(ws, userId, msg);
        break;
      case 'leave-group':
        this.leaveGroup(ws, userId, msg);
        break;
      case 'group-message':
        this.handleGroupMessage(ws, userId, msg);
        break;
      case 'group-file':
        this.handleGroupFile(ws, userId, msg);
        break;

      // NEW: Return list of connected users with their names
      case 'get-users':
        const userList = [];
        for (const [id, name] of this.userNames) {
          userList.push({ userId: id, userName: name });
        }
        ws.send(JSON.stringify({ type: 'user-list', users: userList }));
        break;

      default:
        ws.send(JSON.stringify({ type: 'error', message: `Unknown message type: ${msg.type}` }));
    }
  }

  // ---------- Direct messaging ----------
  handleDirectChat(ws, userId, { to, content }) {
    const target = this.users.get(to);
    if (!target || target.readyState !== WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'error', message: `User "${to}" not connected` }));
      return;
    }
    target.send(JSON.stringify({
      type: 'chat',
      from: userId,
      content,
      timestamp: Date.now()
    }));
  }

  handleDirectFile(ws, userId, { to, filename, mimeType, data }) {
    const target = this.users.get(to);
    if (!target || target.readyState !== WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'error', message: `User "${to}" not connected` }));
      return;
    }
    target.send(JSON.stringify({
      type: 'file',
      from: userId,
      filename,
      mimeType,
      data,
      timestamp: Date.now()
    }));
  }

  relayDirect(to, message) {
    const target = this.users.get(to);
    if (target && target.readyState === WebSocket.OPEN) {
      target.send(JSON.stringify(message));
    }
  }

  // ---------- Groups ----------
  createGroup(ws, userId, { groupName }) {
    const groupId = randomUUID().slice(0, 8); // short join code
    this.groups.set(groupId, {
      id: groupId,
      name: groupName || 'Unnamed Group',
      members: new Set([userId])
    });
    ws.send(JSON.stringify({
      type: 'group-created',
      groupId,
      groupName: this.groups.get(groupId).name
    }));
  }

  joinGroup(ws, userId, { groupId }) {
    const group = this.groups.get(groupId);
    if (!group) {
      ws.send(JSON.stringify({ type: 'error', message: 'Group not found' }));
      return;
    }
    if (group.members.has(userId)) {
      ws.send(JSON.stringify({ type: 'error', message: 'Already a member' }));
      return;
    }
    group.members.add(userId);
    ws.send(JSON.stringify({
      type: 'group-joined',
      groupId,
      groupName: group.name
    }));
    // Notify other members
    this.broadcastToGroup(groupId, {
      type: 'group-notification',
      groupId,
      message: `${userId} joined the group`
    }, userId);
  }

  leaveGroup(ws, userId, { groupId }) {
    const group = this.groups.get(groupId);
    if (!group || !group.members.has(userId)) {
      ws.send(JSON.stringify({ type: 'error', message: 'Not a member of this group' }));
      return;
    }
    group.members.delete(userId);
    ws.send(JSON.stringify({ type: 'group-left', groupId }));
    if (group.members.size === 0) {
      this.groups.delete(groupId);
    } else {
      this.broadcastToGroup(groupId, {
        type: 'group-notification',
        groupId,
        message: `${userId} left the group`
      }, userId);
    }
  }

  handleGroupMessage(ws, userId, { groupId, content }) {
    const group = this.groups.get(groupId);
    if (!group || !group.members.has(userId)) {
      ws.send(JSON.stringify({ type: 'error', message: 'Cannot send – not a member of this group' }));
      return;
    }
    this.broadcastToGroup(groupId, {
      type: 'group-message',
      groupId,
      from: userId,
      content,
      timestamp: Date.now()
    }, userId);
  }

  handleGroupFile(ws, userId, { groupId, filename, mimeType, data }) {
    const group = this.groups.get(groupId);
    if (!group || !group.members.has(userId)) {
      ws.send(JSON.stringify({ type: 'error', message: 'Cannot send file – not a member of this group' }));
      return;
    }
    this.broadcastToGroup(groupId, {
      type: 'group-file',
      groupId,
      from: userId,
      filename,
      mimeType,
      data,
      timestamp: Date.now()
    }, userId);
  }

  broadcastToGroup(groupId, message, excludeUserId = null) {
    const group = this.groups.get(groupId);
    if (!group) return;
    for (const memberId of group.members) {
      if (memberId === excludeUserId) continue;
      const memberWs = this.users.get(memberId);
      if (memberWs && memberWs.readyState === WebSocket.OPEN) {
        memberWs.send(JSON.stringify(message));
      }
    }
  }
}

module.exports = MessagingEngine;

if (require.main === module) {
  const engine = new MessagingEngine(process.env.PORT || 3000);
  engine.start();
}
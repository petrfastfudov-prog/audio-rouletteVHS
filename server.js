const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

const searchQueues = {
  general: [],
  flirt: [],
  adult: []
};

const rooms = new Map();         // socketId -> roomName
const tokenToRoom = new Map();   // token -> roomName
const tokenToSocket = new Map(); // token -> socketId
const socketToToken = new Map(); // socketId -> token

const requestLog = new Map();
const blockedUsers = new Map();

function checkRateLimit(socketId) {
  const now = Date.now();
  if (blockedUsers.has(socketId)) {
    if (now < blockedUsers.get(socketId)) return true;
    blockedUsers.delete(socketId);
  }
  if (!requestLog.has(socketId)) requestLog.set(socketId, []);
  const timestamps = requestLog.get(socketId);
  const recent = timestamps.filter(t => now - t < 3000);
  recent.push(now);
  requestLog.set(socketId, recent);
  if (recent.length > 5) {
    blockedUsers.set(socketId, now + 30000);
    return true;
  }
  return false;
}

io.on('connection', (socket) => {
  const token = socket.handshake.query.token;
  const wasInCall = socket.handshake.query.wasInCall === 'true';
  console.log(`User connected: ${socket.id} (Token: ${token}, WasInCall: ${wasInCall})`);

  if (token) {
    tokenToSocket.set(token, socket.id);
    socketToToken.set(socket.id, token);

    if (wasInCall && tokenToRoom.has(token)) {
      const roomName = tokenToRoom.get(token);
      rooms.set(socket.id, roomName);
      socket.join(roomName);

      let partnerSocketId = null;
      for (let [sId, rName] of rooms.entries()) {
        if (rName === roomName && sId !== socket.id) {
          partnerSocketId = sId;
          break;
        }
      }

      if (partnerSocketId) {
        const isInitiator = socket.id > partnerSocketId;
        console.log(`Session restored instantly for token ${token} in room ${roomName}`);
        socket.emit('session_restored', { room: roomName, partnerId: partnerSocketId, initiator: isInitiator });
      } else {
        socket.emit('session_failed');
      }
    } else if (wasInCall) {
      socket.emit('session_failed');
    }
  }

  updateOnlineCount();

  socket.on('find_partner', (data) => {
    if (checkRateLimit(socket.id)) {
      socket.emit('rate_limited', { message: 'СПАМ-БЛОК: Превышен лимит запросов' });
      return;
    }

    const mode = (data && data.mode && ['general', 'flirt', 'adult'].includes(data.mode)) 
                 ? data.mode 
                 : 'general';
    const ignored = data && data.ignored ? data.ignored : [];

    removeFromAllQueues(socket.id);

    socket.currentMode = mode;
    socket.ignoredPartners = ignored;

    console.log(`User ${socket.id} looking for partner in mode: ${mode}`);
    tryMatch(socket);
  });

  socket.on('signal', (data) => {
    if (data.room) {
      socket.to(data.room).emit('signal', { signal: data.signal, sender: socket.id });
    }
  });

  socket.on('leave_room', () => {
    handleUserLeave(socket);
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    const t = socketToToken.get(socket.id);
    
    setTimeout(() => {
      const currentSockId = tokenToSocket.get(t);
      if (!currentSockId || currentSockId === socket.id) {
        handleUserLeave(socket);
        if (t) {
          tokenToRoom.delete(t);
          tokenToSocket.delete(t);
        }
      }
    }, 6000);

    removeFromAllQueues(socket.id);
    requestLog.delete(socket.id);
    blockedUsers.delete(socket.id);
    socketToToken.delete(socket.id);
    updateOnlineCount();
  });
});

function tryMatch(socket) {
  const mode = socket.currentMode || 'general';
  const queue = searchQueues[mode];
  if (!queue) return;

  let partnerIndex = -1;
  for (let i = 0; i < queue.length; i++) {
    const candidateId = queue[i];
    if (candidateId !== socket.id && 
        !socket.ignoredPartners.includes(candidateId)) {
      partnerIndex = i;
      break;
    }
  }

  if (partnerIndex !== -1) {
    const partnerId = queue.splice(partnerIndex, 1)[0];
    const partnerSocket = io.sockets.sockets.get(partnerId);

    if (!partnerSocket) {
      tryMatch(socket);
      return;
    }

    const roomName = `room_${socket.id}_${partnerId}`;

    socket.join(roomName);
    partnerSocket.join(roomName);

    rooms.set(socket.id, roomName);
    rooms.set(partnerId, roomName);

    const token1 = socketToToken.get(socket.id);
    const token2 = socketToToken.get(partnerId);
    if (token1) tokenToRoom.set(token1, roomName);
    if (token2) tokenToRoom.set(token2, roomName);

    socket.emit('matched', { room: roomName, partnerId: partnerId, initiator: true });
    partnerSocket.emit('matched', { room: roomName, partnerId: socket.id, initiator: false });

    console.log(`Matched [Mode: ${mode}]: ${socket.id} <-> ${partnerId}`);
  } else {
    if (!queue.includes(socket.id)) {
      queue.push(socket.id);
    }
    socket.emit('waiting');
  }
}

function handleUserLeave(socket) {
  const room = rooms.get(socket.id);
  if (room) {
    socket.to(room).emit('partner_left');
    io.in(room).socketsLeave(room);
    
    for (let [sId, rName] of rooms.entries()) {
      if (rName === room) {
        rooms.delete(sId);
        const t = socketToToken.get(sId);
        if (t) tokenToRoom.delete(t);
      }
    }
  }
  removeFromAllQueues(socket.id);
}

function removeFromAllQueues(socketId) {
  for (let mode in searchQueues) {
    searchQueues[mode] = searchQueues[mode].filter(id => id !== socketId);
  }
}

function updateOnlineCount() {
  io.emit('online', io.engine.clientsCount);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
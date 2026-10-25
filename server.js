const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cookieParser = require('cookie-parser');
const { v4: uuidv4 } = require('uuid');
const {
  initializeDatabase,
  findOrCreateUser,
  getUserById,
  getRandomQuestions,
  getCategories,
  createGameRoom,
  updateGameResult,
  incrementUserGames,
  getLeaderboard,
} = require('./database');

initializeDatabase();

const app = express();
app.use(cookieParser());
app.use(express.json());
app.use(express.static('public'));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingTimeout: 20000,
  pingInterval: 10000,
});

// ── In-memory state ────────────────────────────────────────────────
const connectedUsers = new Map();   // socketId → { userId, nickname }
const matchingQueue  = new Map();   // category → [{ socketId, userId, nickname, timer }]
const activeGames    = new Map();   // roomId → Game

const QUESTION_TIME   = 15;         // seconds per question
const QUESTIONS_PER_GAME = 5;
const MATCH_TIMEOUT   = 30000;      // ms

// ── REST API ───────────────────────────────────────────────────────
app.get('/api/categories', (_req, res) => {
  res.json(getCategories());
});

app.get('/api/leaderboard', (_req, res) => {
  res.json(getLeaderboard());
});

// ── Scoring ────────────────────────────────────────────────────────
function calculateScore(answerTimeMs, isCorrect) {
  if (!isCorrect) return 0;
  const seconds = Math.floor(answerTimeMs / 1000);
  if (seconds <= 3) return 100;
  return Math.max(0, 100 - seconds * 5);
}

// ── Matching ───────────────────────────────────────────────────────
function addToQueue(socketId, userId, nickname, category) {
  if (!matchingQueue.has(category)) matchingQueue.set(category, []);
  const queue = matchingQueue.get(category);

  const timer = setTimeout(() => {
    const idx = queue.findIndex(p => p.socketId === socketId);
    if (idx !== -1) {
      queue.splice(idx, 1);
      io.to(socketId).emit('match_timeout');
    }
  }, MATCH_TIMEOUT);

  queue.push({ socketId, userId, nickname, timer });

  if (queue.length >= 2) {
    const p1 = queue.shift();
    const p2 = queue.shift();
    clearTimeout(p1.timer);
    clearTimeout(p2.timer);
    createGame(p1, p2, category);
  } else {
    io.to(socketId).emit('waiting');
  }
}

function removeFromQueue(socketId) {
  for (const [category, queue] of matchingQueue) {
    const idx = queue.findIndex(p => p.socketId === socketId);
    if (idx !== -1) {
      clearTimeout(queue[idx].timer);
      queue.splice(idx, 1);
      return;
    }
  }
}

// ── Game lifecycle ─────────────────────────────────────────────────
function createGame(p1, p2, category) {
  const roomId = uuidv4();
  const rawQuestions = getRandomQuestions(category, QUESTIONS_PER_GAME);

  if (rawQuestions.length < QUESTIONS_PER_GAME) {
    io.to(p1.socketId).emit('match_error', { message: '题目不足，请重试' });
    io.to(p2.socketId).emit('match_error', { message: '题目不足，请重试' });
    return;
  }

  const questions = rawQuestions.map(q => ({
    id: q.id,
    text: q.question_text,
    options: JSON.parse(q.options),
    correctAnswer: q.correct_answer,
  }));

  createGameRoom(roomId, p1.userId, p2.userId, category);

  const game = {
    id: roomId,
    category,
    players: [
      { socketId: p1.socketId, userId: p1.userId, nickname: p1.nickname, score: 0, currentAnswer: null },
      { socketId: p2.socketId, userId: p2.userId, nickname: p2.nickname, score: 0, currentAnswer: null },
    ],
    questions,
    currentQuestion: 0,
    questionStartTime: null,
    questionTimer: null,
  };

  activeGames.set(roomId, game);

  io.to(p1.socketId).emit('matched', { opponent: p2.nickname, roomId, totalQuestions: QUESTIONS_PER_GAME });
  io.to(p2.socketId).emit('matched', { opponent: p1.nickname, roomId, totalQuestions: QUESTIONS_PER_GAME });

  setTimeout(() => sendQuestion(roomId, 0), 2000);
}

function sendQuestion(roomId, index) {
  const game = activeGames.get(roomId);
  if (!game) return;

  game.currentQuestion = index;
  game.players.forEach(p => { p.currentAnswer = null; });
  game.questionStartTime = Date.now();

  const q = game.questions[index];
  const payload = {
    index,
    text: q.text,
    options: q.options,
    totalTime: QUESTION_TIME,
  };

  game.players.forEach(p => {
    io.to(p.socketId).emit('question', payload);
  });

  game.questionTimer = setTimeout(() => handleTimeUp(roomId), QUESTION_TIME * 1000);
}

function handleTimeUp(roomId) {
  const game = activeGames.get(roomId);
  if (!game) return;

  game.players.forEach(p => {
    if (p.currentAnswer === null) {
      p.currentAnswer = { optionIndex: -1, correct: false, score: 0, answerTime: QUESTION_TIME * 1000 };
    }
  });

  emitQuestionResult(roomId);
}

function processAnswer(roomId, socketId, questionIndex, optionIndex) {
  const game = activeGames.get(roomId);
  if (!game) return;
  if (questionIndex !== game.currentQuestion) return;

  const player = game.players.find(p => p.socketId === socketId);
  if (!player || player.currentAnswer !== null) return;

  const answerTime = Date.now() - game.questionStartTime;
  const q = game.questions[questionIndex];
  const correct = optionIndex === q.correctAnswer;
  const score = calculateScore(answerTime, correct);

  player.currentAnswer = { optionIndex, correct, score, answerTime };
  player.score += score;

  io.to(socketId).emit('answer_ack', { questionIndex, correct, score, totalScore: player.score });

  const opponent = game.players.find(p => p.socketId !== socketId);
  if (opponent) io.to(opponent.socketId).emit('opponent_answered', { questionIndex });

  if (game.players.every(p => p.currentAnswer !== null)) {
    clearTimeout(game.questionTimer);
    emitQuestionResult(roomId);
  }
}

function emitQuestionResult(roomId) {
  const game = activeGames.get(roomId);
  if (!game) return;

  const [p1, p2] = game.players;
  const q = game.questions[game.currentQuestion];

  const resultPayload = {
    questionIndex: game.currentQuestion,
    correctAnswer: q.correctAnswer,
    playerScore: p1.score,
    opponentScore: p2.score,
    playerAnswer: p1.currentAnswer ? p1.currentAnswer.optionIndex : -1,
    opponentAnswer: p2.currentAnswer ? p2.currentAnswer.optionIndex : -1,
    playerCorrect: p1.currentAnswer ? p1.currentAnswer.correct : false,
    opponentCorrect: p2.currentAnswer ? p2.currentAnswer.correct : false,
  };

  io.to(p1.socketId).emit('question_result', resultPayload);
  io.to(p2.socketId).emit('question_result', {
    ...resultPayload,
    playerScore: p2.score,
    opponentScore: p1.score,
    playerAnswer: p2.currentAnswer ? p2.currentAnswer.optionIndex : -1,
    opponentAnswer: p1.currentAnswer ? p1.currentAnswer.optionIndex : -1,
    playerCorrect: p2.currentAnswer ? p2.currentAnswer.correct : false,
    opponentCorrect: p1.currentAnswer ? p1.currentAnswer.correct : false,
  });

  if (game.currentQuestion < QUESTIONS_PER_GAME - 1) {
    setTimeout(() => sendQuestion(roomId, game.currentQuestion + 1), 2500);
  } else {
    setTimeout(() => finishGame(roomId), 2500);
  }
}

function finishGame(roomId) {
  const game = activeGames.get(roomId);
  if (!game) return;

  const [p1, p2] = game.players;
  let winnerId = null;
  if (p1.score > p2.score) winnerId = p1.userId;
  else if (p2.score > p1.score) winnerId = p2.userId;

  updateGameResult(roomId, p1.score, p2.score, winnerId);
  incrementUserGames(p1.userId, p1.score > p2.score);
  incrementUserGames(p2.userId, p2.score > p1.score);

  io.to(p1.socketId).emit('game_over', {
    playerScore: p1.score,
    opponentScore: p2.score,
    result: p1.score > p2.score ? 'win' : p1.score < p2.score ? 'lose' : 'draw',
    opponentNickname: p2.nickname,
  });

  io.to(p2.socketId).emit('game_over', {
    playerScore: p2.score,
    opponentScore: p1.score,
    result: p2.score > p1.score ? 'win' : p2.score < p1.score ? 'lose' : 'draw',
    opponentNickname: p1.nickname,
  });

  activeGames.delete(roomId);
}

// ── Socket.IO ──────────────────────────────────────────────────────
io.on('connection', (socket) => {

  socket.on('join', (data) => {
    const nickname = (data.nickname || '').trim().slice(0, 8);
    if (!nickname) return socket.emit('auth_error', { message: '昵称不能为空' });

    let userId = data.userId || socket.handshake.cookies?.quiz_uid;
    if (!userId) {
      userId = uuidv4();
      socket.emit('set_cookie', { userId });
    }

    findOrCreateUser(userId, nickname);
    connectedUsers.set(socket.id, { userId, nickname });
    socket.emit('auth_success', { userId, nickname });
  });

  socket.on('select_category', (data) => {
    const user = connectedUsers.get(socket.id);
    if (!user) return;

    const categories = getCategories().map(c => c.category);
    if (!categories.includes(data.category)) return;

    removeFromQueue(socket.id);
    addToQueue(socket.id, user.userId, user.nickname, data.category);
  });

  socket.on('cancel_match', () => {
    removeFromQueue(socket.id);
  });

  socket.on('answer', (data) => {
    const user = connectedUsers.get(socket.id);
    if (!user) return;

    for (const [roomId, game] of activeGames) {
      const player = game.players.find(p => p.socketId === socket.id);
      if (player) {
        processAnswer(roomId, socket.id, data.questionIndex, data.optionIndex);
        return;
      }
    }
  });

  socket.on('disconnect', () => {
    const user = connectedUsers.get(socket.id);
    if (!user) return;

    removeFromQueue(socket.id);

    for (const [roomId, game] of activeGames) {
      const player = game.players.find(p => p.socketId === socket.id);
      if (player) {
        const opponent = game.players.find(p => p.socketId !== socket.id);
        if (opponent) {
          io.to(opponent.socketId).emit('opponent_disconnected');
        }
        activeGames.delete(roomId);
        break;
      }
    }

    connectedUsers.delete(socket.id);
  });
});

// ── Start ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Quiz PK server running on http://localhost:${PORT}`);
});

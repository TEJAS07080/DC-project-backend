// server.js - Enhanced backend server for content moderation
const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { writeFileAtomic } = require('fs-nextra'); // For atomic file writes

const app = express();
const PORT = process.env.PORT || 3001;
const WORKER_PORTS = [4001, 4002, 4003];
const DATA_STORE = path.join(__dirname, 'data', 'posts.json');

// Ensure data directory exists
if (!fs.existsSync(path.join(__dirname, 'data'))) {
  fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
}

// Initialize empty posts file if it doesn't exist
if (!fs.existsSync(DATA_STORE)) {
  fs.writeFileSync(DATA_STORE, JSON.stringify([]));
}

app.use(cors());
app.use(express.json());

// RabbitMQ Connection
let channel;
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    channel = await connection.createChannel();
    await channel.assertQueue('moderation_queue', { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('RabbitMQ connection error:', error);
    console.error('Make sure RabbitMQ is running on localhost:5672');
    setTimeout(connectRabbitMQ, 5000);
  }
}

// Read posts safely
async function readPosts() {
  try {
    if (fs.existsSync(DATA_STORE)) {
      const fileContent = await fs.promises.readFile(DATA_STORE, 'utf8');
      const posts = JSON.parse(fileContent);
      return Array.isArray(posts) ? posts : [];
    }
    return [];
  } catch (error) {
    console.error('Error reading posts:', error);
    return [];
  }
}

// Write posts atomically
async function writePosts(posts) {
  try {
    await writeFileAtomic(DATA_STORE, JSON.stringify(posts, null, 2));
    // Write to backup
    const backupDir = path.join(__dirname, 'data', 'backups');
    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }
    await writeFileAtomic(
      path.join(backupDir, `posts_backup_${Date.now()}.json`),
      JSON.stringify(posts, null, 2)
    );
  } catch (error) {
    console.error('Error writing posts:', error);
    throw error;
  }
}

// Compute moderation statistics
function computeStats(posts) {
  return {
    total: posts.length,
    approved: posts.filter(p => p.status === 'approved').length,
    rejected: posts.filter(p => p.status === 'rejected').length,
    pending: posts.filter(p => p.status === 'pending').length,
  };
}

// API Routes
app.get('/api/posts', async (req, res) => {
  try {
    const { filter = 'all' } = req.query;
    let posts = await readPosts();

    // Apply filter
    if (filter !== 'all') {
      posts = posts.filter(p => p.status === filter);
    }

    res.json({
      posts,
      stats: computeStats(await readPosts()), // Compute stats on all posts
    });
  } catch (error) {
    console.error('Error fetching posts:', error);
    res.status(500).json({ error: 'Failed to fetch posts' });
  }
});

app.post('/api/posts', async (req, res) => {
  try {
    const { title, content, author, server } = req.body;

    if (!title || !content || !author) {
      return res.status(400).json({ error: 'Title, content, and author are required' });
    }

    const post = {
      id: uuidv4(),
      title,
      content,
      author,
      status: 'pending',
      server: server || 'server1',
      worker: null,
      moderationDetails: null,
      createdAt: new Date().toISOString(),
    };

    const posts = await readPosts();
    posts.push(post);
    await writePosts(posts);

    // Send to message queue
    if (channel) {
      channel.sendToQueue('moderation_queue', Buffer.from(JSON.stringify(post)), {
        persistent: true,
      });
    } else {
      console.warn('RabbitMQ channel not available, post will not be processed');
    }

    res.status(201).json(post);
  } catch (error) {
    console.error('Error adding post:', error);
    res.status(500).json({ error: 'Failed to add post' });
  }
});

// System status endpoint
app.get('/api/status', async (req, res) => {
  const statuses = {
    server: true,
    // server2: Math.random() > 0.2,
    // server3: Math.random() > 0.3,
    rabbitmq: !!channel,
  };

  try {
    await Promise.all(
      WORKER_PORTS.map(async (port, index) => {
        try {
          await axios.get(`http://localhost:${port}/health`, { timeout: 1000 });
          statuses[`worker${index + 1}`] = true;
        } catch {
          statuses[`worker${index + 1}`] = false;
        }
      })
    );
  } catch (error) {
    console.error('Error checking worker status:', error);
  }

  res.json(statuses);
});

// Start server
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await connectRabbitMQ();
});
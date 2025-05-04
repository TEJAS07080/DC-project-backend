require('dotenv').config();
const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { writeFileAtomic } = require('fs-nextra');

const app = express();
const PORT = process.env.PORT || 3001;
const WORKER_PORTS = [4001, 4002, 4003];
const DATA_STORE = path.join(__dirname, 'data', 'posts.json');

if (!fs.existsSync(path.join(__dirname, 'data'))) {
  fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
}

if (!fs.existsSync(DATA_STORE)) {
  fs.writeFileSync(DATA_STORE, JSON.stringify([]));
}

app.use(cors());
app.use(express.json());

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

async function writePosts(posts) {
  try {
    await writeFileAtomic(DATA_STORE, JSON.stringify(posts, null, 2));
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

function computeStats(posts) {
  return {
    total: posts.length,
    approved: posts.filter(p => p.status === 'approved').length,
    rejected: posts.filter(p => p.status === 'rejected').length,
    pending: posts.filter(p => p.status === 'pending').length,
    processing: posts.filter(p => p.status === 'processing').length,
    needs_review: posts.filter(p => p.status === 'needs_review').length,
  };
}

function filterPostsByPeriod(posts, period) {
  const now = new Date();
  let startDate;
  switch (period) {
    case 'day':
      startDate = new Date(now.setHours(0, 0, 0, 0));
      break;
    case 'week':
      startDate = new Date(now.setDate(now.getDate() - 7));
      break;
    case 'month':
      startDate = new Date(now.setMonth(now.getMonth() - 1));
      break;
    case 'year':
      startDate = new Date(now.setFullYear(now.getFullYear() - 1));
      break;
    default:
      return posts;
  }
  return posts.filter(p => new Date(p.createdAt) >= startDate);
}

app.get('/api/posts', async (req, res) => {
  try {
    const { filter = 'all', period } = req.query;
    let posts = await readPosts();

    if (period) {
      posts = filterPostsByPeriod(posts, period);
    }

    if (filter !== 'all') {
      posts = posts.filter(p => p.status === filter);
    }

    res.json({
      posts,
      stats: computeStats(await readPosts()),
    });
  } catch (error) {
    console.error('Error fetching posts:', error);
    res.status(500).json({ error: 'Failed to fetch posts' });
  }
});

app.post('/api/posts', async (req, res) => {
  try {
    const { title, content, author, server, category } = req.body;

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
      processingTime: null,
      toxicityScore: null,
      reviewReason: null,
      category: category || 'general',
    };

    const posts = await readPosts();
    posts.push(post);
    await writePosts(posts);

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

app.get('/api/status', async (req, res) => {
  const statuses = {
    server1: true,
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

app.get('/api/processing-times', async (req, res) => {
  try {
    const { period } = req.query;
    let posts = await readPosts();

    if (period) {
      posts = filterPostsByPeriod(posts, period);
    }

    const workerStats = {};

    posts.forEach(post => {
      if (post.worker) {
        if (!workerStats[post.worker]) {
          workerStats[post.worker] = { 
            totalTime: 0, 
            count: 0, 
            approved: 0, 
            rejected: 0, 
            needs_review: 0 
          };
        }
        if (post.processingTime) {
          workerStats[post.worker].totalTime += post.processingTime;
          workerStats[post.worker].count += 1;
        }
        if (post.status === 'approved') {
          workerStats[post.worker].approved += 1;
        } else if (post.status === 'rejected') {
          workerStats[post.worker].rejected += 1;
        } else if (post.status === 'needs_review') {
          workerStats[post.worker].needs_review += 1;
        }
      }
    });

    const processingTimes = Object.keys(workerStats).map(worker => ({
      worker,
      averageTime: workerStats[worker].count > 0 
        ? (workerStats[worker].totalTime / workerStats[worker].count / 1000).toFixed(1) 
        : 0,
      processed: workerStats[worker].count,
      approved: workerStats[worker].approved,
      rejected: workerStats[worker].rejected,
      needs_review: workerStats[worker].needs_review,
    }));

    res.json(processingTimes);
  } catch (error) {
    console.error('Error fetching processing times:', error);
    res.status(500).json({ error: 'Failed to fetch processing times' });
  }
});

app.get('/api/activity', async (req, res) => {
  try {
    const { period } = req.query;
    let posts = await readPosts();
    
    if (period) {
      posts = filterPostsByPeriod(posts, period);
    }

    const days = period === 'day' ? 1 : period === 'month' ? 30 : period === 'year' ? 365 : 7;
    const activity = [];

    for (let i = days - 1; i >= 0; i--) {
      const date = new Date();
      date.setDate(date.getDate() - i);
      const dateString = date.toISOString().split('T')[0];

      const dayPosts = posts.filter(post => post.createdAt.startsWith(dateString));
      const approved = dayPosts.filter(p => p.status === 'approved').length;
      const rejected = dayPosts.filter(p => p.status === 'rejected').length;
      const pending = dayPosts.filter(p => p.status === 'pending' || p.status === 'processing').length;
      const needs_review = dayPosts.filter(p => p.status === 'needs_review').length;

      activity.push({
        day: date.toLocaleDateString('en-US', { weekday: 'short' }),
        approved,
        rejected,
        pending,
        needs_review,
      });
    }

    res.json(activity);
  } catch (error) {
    console.error('Error fetching activity data:', error);
    res.status(500).json({ error: 'Failed to fetch activity data' });
  }
});

app.get('/api/categories', async (req, res) => {
  try {
    const { period } = req.query;
    let posts = await readPosts();

    if (period) {
      posts = filterPostsByPeriod(posts, period);
    }

    const categoryCounts = {};
    posts.forEach(post => {
      const category = post.category || 'general';
      categoryCounts[category] = (categoryCounts[category] || 0) + 1;
    });

    const categories = Object.keys(categoryCounts).map(category => ({
      name: category.charAt(0).toUpperCase() + category.slice(1),
      value: categoryCounts[category],
    }));

    res.json(categories);
  } catch (error) {
    console.error('Error fetching category data:', error);
    res.status(500).json({ error: 'Failed to fetch category data' });
  }
});

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await connectRabbitMQ();
});
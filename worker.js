// worker.js - Enhanced Worker Node for Content Moderation
const express = require('express');
const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const { writeFileAtomic } = require('fs-nextra');

const app = express();
const PORT = process.env.PORT || 4001;
const WORKER_ID = process.env.WORKER_ID || `worker-${PORT}`;
const DATA_STORE = path.join(__dirname, 'data', 'posts.json');

app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send({ status: 'ok', workerId: WORKER_ID });
});

// Simple moderation logic (keyword-based)
function moderateContent(content) {
  const blockedKeywords = ['spam', 'offensive', 'inappropriate', 'hate'];
  const lowercaseContent = content.toLowerCase();
  const foundKeyword = blockedKeywords.find(keyword => lowercaseContent.includes(keyword));
  
  if (foundKeyword) {
    return {
      status: 'rejected',
      moderationDetails: `Content rejected due to keyword: ${foundKeyword}`,
    };
  }
  
  return {
    status: 'approved',
    moderationDetails: 'Content approved after review',
  };
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
    const dataDir = path.dirname(DATA_STORE);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    await writeFileAtomic(DATA_STORE, JSON.stringify(posts, null, 2));
  } catch (error) {
    console.error('Error writing posts:', error);
    throw error;
  }
}

// Update post status
async function updatePostStatus(postId, status, workerId, moderationDetails = null) {
  try {
    const posts = await readPosts();
    const postIndex = posts.findIndex(p => p.id === postId);

    if (postIndex !== -1) {
      posts[postIndex].status = status;
      posts[postIndex].worker = workerId;
      if (moderationDetails) {
        posts[postIndex].moderationDetails = moderationDetails;
      }
      if (status === 'approved' || status === 'rejected') {
        posts[postIndex].completedAt = new Date().toISOString();
      }
      await writePosts(posts);
      console.log(`Updated post ${postId} status to ${status}`);
    } else {
      console.log(`Post ${postId} not found`);
    }
  } catch (error) {
    console.error(`Error updating post ${postId}:`, error);
  }
}

// Connect to RabbitMQ and process posts
async function connectAndConsume() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    console.log(`${WORKER_ID} connected to RabbitMQ`);
    
    const channel = await connection.createChannel();
    await channel.assertQueue('moderation_queue', { durable: true });
    channel.prefetch(1);

    console.log(`${WORKER_ID} waiting for posts...`);

    channel.consume('moderation_queue', async (msg) => {
      if (msg !== null) {
        const post = JSON.parse(msg.content.toString());
        console.log(`${WORKER_ID} processing post ${post.id}`);

        await updatePostStatus(post.id, 'processing', WORKER_ID);

        // Simulate processing time
        const processingTime = Math.random() * 3000 + 1000;
        await new Promise(resolve => setTimeout(resolve, processingTime));

        // Moderate content
        const { status, moderationDetails } = moderateContent(post.content);

        await updatePostStatus(post.id, status, WORKER_ID, moderationDetails);

        channel.ack(msg);
      }
    });

    connection.on('close', () => {
      console.error(`${WORKER_ID} lost connection to RabbitMQ, reconnecting...`);
      setTimeout(connectAndConsume, 5000);
    });
  } catch (error) {
    console.error(`${WORKER_ID} RabbitMQ connection error:`, error);
    setTimeout(connectAndConsume, 5000);
  }
}

// Start server
app.listen(PORT, () => {
  console.log(`${WORKER_ID} running on port ${PORT}`);
  connectAndConsume();
});
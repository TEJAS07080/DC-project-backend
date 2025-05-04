require('dotenv').config();
const express = require('express');
const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const axios = require('axios');
const { writeFileAtomic } = require('fs-nextra');

const app = express();
const PORT = process.env.PORT || 4001;
const WORKER_ID = process.env.WORKER_ID || `worker-${PORT}`;
const DATA_STORE = path.join(__dirname, 'data', 'posts.json');
const PERSPECTIVE_API_KEY = process.env.PERSPECTIVE_API_KEY;

if (!PERSPECTIVE_API_KEY) {
  console.error('PERSPECTIVE_API_KEY environment variable is required');
  process.exit(1);
}

app.use(cors());
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).send({ status: 'ok', workerId: WORKER_ID });
});

async function moderateContent(content) {
  // Step 1: Rule-based check for egregious content
  const blockedKeywords = ['fuck', 'nigger']; // Add more as needed
  const lowercaseContent = content.toLowerCase();
  const foundKeyword = blockedKeywords.find(keyword => lowercaseContent.includes(keyword));
  
  if (foundKeyword) {
    return {
      status: 'rejected',
      moderationDetails: `Content rejected due to egregious keyword: ${foundKeyword}`,
      toxicityScore: null,
      reviewReason: null,
    };
  }

  // Step 2: Perspective API analysis
  try {
    const response = await axios.post(
      `https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze?key=${PERSPECTIVE_API_KEY}`,
      {
        comment: { text: content },
        requestedAttributes: {
          TOXICITY: {},
          INSULT: {},
          PROFANITY: {},
        },
        languages: ['en'],
      }
    );

    const scores = response.data.attributeScores;
    const toxicityScore = scores.TOXICITY.summaryScore.value;
    const insultScore = scores.INSULT.summaryScore.value;
    const profanityScore = scores.PROFANITY.summaryScore.value;
    const maxScore = Math.max(toxicityScore, insultScore, profanityScore);

    // Step 3: Decision logic
    if (maxScore > 0.8) {
      return {
        status: 'rejected',
        moderationDetails: `Content rejected due to high scores: toxicity=${toxicityScore.toFixed(2)}, insult=${insultScore.toFixed(2)}, profanity=${profanityScore.toFixed(2)}`,
        toxicityScore: maxScore,
        reviewReason: null,
      };
    } else if (maxScore >= 0.5) {
      return {
        status: 'needs_review',
        moderationDetails: `Content flagged for human review: toxicity=${toxicityScore.toFixed(2)}, insult=${insultScore.toFixed(2)}, profanity=${profanityScore.toFixed(2)}`,
        toxicityScore: maxScore,
        reviewReason: 'Borderline content requiring human evaluation',
      };
    } else {
      return {
        status: 'approved',
        moderationDetails: `Content approved: toxicity=${toxicityScore.toFixed(2)}, insult=${insultScore.toFixed(2)}, profanity=${profanityScore.toFixed(2)}`,
        toxicityScore: maxScore,
        reviewReason: null,
      };
    }
  } catch (error) {
    console.error('Perspective API error:', error.response?.data || error.message);
    return {
      status: 'needs_review',
      moderationDetails: 'Failed to analyze content with Perspective API',
      toxicityScore: null,
      reviewReason: 'API error, requires human review',
    };
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

async function updatePostStatus(postId, status, workerId, moderationDetails = null, processingTime = null, toxicityScore = null, reviewReason = null) {
  try {
    const posts = await readPosts();
    const postIndex = posts.findIndex(p => p.id === postId);

    if (postIndex !== -1) {
      posts[postIndex].status = status;
      posts[postIndex].worker = workerId;
      if (moderationDetails) {
        posts[postIndex].moderationDetails = moderationDetails;
      }
      if (processingTime) {
        posts[postIndex].processingTime = processingTime;
      }
      if (toxicityScore !== null) {
        posts[postIndex].toxicityScore = toxicityScore;
      }
      if (reviewReason) {
        posts[postIndex].reviewReason = reviewReason;
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

        const startTime = Date.now();
        const { status, moderationDetails, toxicityScore, reviewReason } = await moderateContent(post.content);
        const processingTime = Date.now() - startTime;

        await updatePostStatus(post.id, status, WORKER_ID, moderationDetails, processingTime, toxicityScore, reviewReason);

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

app.listen(PORT, () => {
  console.log(`${WORKER_ID} running on port ${PORT}`);
  connectAndConsume();
});
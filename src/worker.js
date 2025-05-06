require('dotenv').config();
   const amqp = require('amqplib');
   const { MongoClient } = require('mongodb');
   const axios = require('axios');

   const WORKER_ID = process.env.WORKER_ID || 'worker-unknown';
   const PERSPECTIVE_API_KEY = process.env.PERSPECTIVE_API_KEY;
   const mongoUri = process.env.MONGO_URI || 'mongodb://root:secret@mongodb:27017';
   const dbName = process.env.MONGO_DB || 'content_moderation';
   const rabbitMQUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

   if (!PERSPECTIVE_API_KEY) {
     console.error(`${WORKER_ID}: PERSPECTIVE_API_KEY environment variable is required`);
     process.exit(1);
   }

   let db;

   async function connectToMongo() {
     try {
       const client = new MongoClient(mongoUri);
       await client.connect();
       console.log(`${WORKER_ID}: Connected to MongoDB`);
       db = client.db(dbName);
     } catch (error) {
       console.error(`${WORKER_ID}: MongoDB connection error:`, error);
       process.exit(1);
     }
   }

   async function moderateContent(content) {
     // Step 1: Rule-based check for egregious content
     const blockedKeywords = ['fuck', 'nigger'];
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
       console.error(`${WORKER_ID}: Perspective API error:`, error.response?.data || error.message);
       return {
         status: 'needs_review',
         moderationDetails: 'Failed to analyze content with Perspective API',
         toxicityScore: null,
         reviewReason: 'API error, requires human review',
       };
     }
   }

   async function updatePostStatus(postId, status, workerId, moderationDetails = null, processingTime = null, toxicityScore = null, reviewReason = null) {
     try {
       const update = {
         status,
         worker: workerId,
       };
       if (moderationDetails) update.moderationDetails = moderationDetails;
       if (processingTime) update.processingTime = processingTime;
       if (toxicityScore !== null) update.toxicityScore = toxicityScore;
       if (reviewReason) update.reviewReason = reviewReason;
       if (status === 'approved' || status === 'rejected') update.completedAt = new Date();

       const result = await db.collection('posts').updateOne(
         { id: postId },
         { $set: update }
       );

       if (result.matchedCount === 0) {
         console.log(`${WORKER_ID}: Post ${postId} not found`);
       } else {
         console.log(`${WORKER_ID}: Updated post ${postId} status to ${status}`);
       }
     } catch (error) {
       console.error(`${WORKER_ID}: Error updating post ${postId}:`, error);
     }
   }

   async function connectAndConsume() {
     try {
       const connection = await amqp.connect(rabbitMQUrl);
       console.log(`${WORKER_ID}: Connected to RabbitMQ`);
       
       const channel = await connection.createChannel();
       await channel.assertQueue('moderation_queue', { durable: true });
       channel.prefetch(1);

       console.log(`${WORKER_ID}: Waiting for posts...`);

       channel.consume('moderation_queue', async (msg) => {
         if (msg !== null) {
           const post = JSON.parse(msg.content.toString());
           console.log(`${WORKER_ID}: Processing post ${post.id}`);

           await updatePostStatus(post.id, 'processing', WORKER_ID);

           const startTime = Date.now();
           const { status, moderationDetails, toxicityScore, reviewReason } = await moderateContent(post.content);
           const processingTime = Date.now() - startTime;

           await updatePostStatus(post.id, status, WORKER_ID, moderationDetails, processingTime, toxicityScore, reviewReason);

           channel.ack(msg);
         }
       }, { noAck: false });

       connection.on('close', () => {
         console.error(`${WORKER_ID}: Lost connection to RabbitMQ, reconnecting...`);
         setTimeout(connectAndConsume, 5000);
       });
     } catch (error) {
       console.error(`${WORKER_ID}: RabbitMQ connection error:`, error);
       setTimeout(connectAndConsume, 5000);
     }
   }

   connectToMongo();
   connectAndConsume();
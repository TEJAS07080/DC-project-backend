require('dotenv').config();
   const express = require('express');
   const cors = require('cors');
   const amqp = require('amqplib');
   const { MongoClient } = require('mongodb');
   const { v4: uuidv4 } = require('uuid');

   const app = express();
   const port = process.env.PORT || 3001;

   app.use(cors());
   app.use(express.json());

   // MongoDB connection
   const mongoUri = process.env.MONGO_URI || 'mongodb://root:secret@mongodb:27017';
   const dbName = process.env.MONGO_DB || 'content_moderation';
   let db;

   async function connectToMongo() {
     try {
       const client = new MongoClient(mongoUri);
       await client.connect();
       console.log('Connected to MongoDB');
       db = client.db(dbName);
     } catch (error) {
       console.error('MongoDB connection error:', error);
       process.exit(1);
     }
   }

   // RabbitMQ connection
   const rabbitMQUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
   let channel;

   async function connectToRabbitMQ() {
     try {
       const connection = await amqp.connect(rabbitMQUrl);
       channel = await connection.createChannel();
       await channel.assertQueue('moderation_queue', { durable: true });
       console.log('Connected to RabbitMQ');
     } catch (error) {
       console.error('RabbitMQ connection error:', error);
       setTimeout(connectToRabbitMQ, 5000);
     }
   }

   connectToMongo();
   connectToRabbitMQ();

   // API Endpoints
   app.get('/api/posts', async (req, res) => {
     try {
       const { filter = 'all', period } = req.query;
       let query = {};
       
       if (filter !== 'all') {
         query.status = filter;
       }

       if (period) {
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
             startDate = null;
         }
         if (startDate) {
           query.createdAt = { $gte: startDate };
         }
       }

       const posts = await db.collection('posts')
         .find(query)
         .sort({ createdAt: -1 })
         .toArray();

       const allPosts = await db.collection('posts').find().toArray();
       const stats = {
         total: allPosts.length,
         approved: allPosts.filter(p => p.status === 'approved').length,
         rejected: allPosts.filter(p => p.status === 'rejected').length,
         pending: allPosts.filter(p => p.status === 'pending').length,
         processing: allPosts.filter(p => p.status === 'processing').length,
         needs_review: allPosts.filter(p => p.status === 'needs_review').length,
       };

       res.json({ posts, stats });
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
         createdAt: new Date(),
         processingTime: null,
         toxicityScore: null,
         reviewReason: null,
         category: category || 'general',
       };

       await db.collection('posts').insertOne(post);

       if (channel) {
         channel.sendToQueue('moderation_queue', Buffer.from(JSON.stringify(post)), {
           persistent: true,
         });
         await db.collection('posts').updateOne(
           { id: post.id },
           { $set: { status: 'processing' } }
         );
         post.status = 'processing';
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
     try {
       const statuses = {
         server1: true,
         rabbitmq: !!channel,
       };

       // Check status for exactly three workers
       const workers = ['worker-1', 'worker-2', 'worker-3'];
       for (const workerId of workers) {
         const processingPosts = await db.collection('posts')
           .find({ worker: workerId, status: 'processing' })
           .toArray();
         statuses[workerId] = processingPosts.length > 0 ? 'busy' : 'idle';
       }

       res.json(statuses);
     } catch (error) {
       console.error('Error checking status:', error);
       res.status(500).json({ error: 'Failed to check status' });
     }
   });

   app.get('/api/processing-times', async (req, res) => {
     try {
       const { period } = req.query;
       let query = {};

       if (period) {
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
             startDate = null;
         }
         if (startDate) {
           query.createdAt = { $gte: startDate };
         }
       }

       const posts = await db.collection('posts')
         .find({ ...query, processingTime: { $exists: true } })
         .toArray();

       const workerStats = {};
       const workers = ['worker-1', 'worker-2', 'worker-3'];
       workers.forEach(worker => {
         workerStats[worker] = { totalTime: 0, count: 0, approved: 0, rejected: 0, needs_review: 0 };
       });

       posts.forEach(post => {
         const worker = post.worker || 'unknown';
         if (workerStats[worker]) {
           if (post.processingTime) {
             workerStats[worker].totalTime += post.processingTime;
             workerStats[worker].count += 1;
           }
           if (post.status === 'approved') workerStats[worker].approved += 1;
           else if (post.status === 'rejected') workerStats[worker].rejected += 1;
           else if (post.status === 'needs_review') workerStats[worker].needs_review += 1;
         }
       });

       const processingTimes = workers.map(worker => ({
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
       let query = {};

       if (period) {
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
             startDate = null;
         }
         if (startDate) {
           query.createdAt = { $gte: startDate };
         }
       }

       const posts = await db.collection('posts')
         .find(query)
         .sort({ createdAt: -1 })
         .toArray();

       const days = period === 'day' ? 1 : period === 'month' ? 30 : period === 'year' ? 365 : 7;
       const activity = [];

       for (let i = days - 1; i >= 0; i--) {
         const date = new Date();
         date.setDate(date.getDate() - i);
         const startOfDay = new Date(date.setHours(0, 0, 0, 0));
         const endOfDay = new Date(date.setHours(23, 59, 59, 999));

         const dayPosts = posts.filter(post => {
           const createdAt = new Date(post.createdAt);
           return createdAt >= startOfDay && createdAt <= endOfDay;
         });

         activity.push({
           day: startOfDay.toLocaleDateString('en-US', { weekday: 'short' }),
           approved: dayPosts.filter(p => p.status === 'approved').length,
           rejected: dayPosts.filter(p => p.status === 'rejected').length,
           pending: dayPosts.filter(p => p.status === 'pending' || p.status === 'processing').length,
           needs_review: dayPosts.filter(p => p.status === 'needs_review').length,
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
       let query = {};

       if (period) {
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
             startDate = null;
         }
         if (startDate) {
           query.createdAt = { $gte: startDate };
         }
       }

       const posts = await db.collection('posts').find(query).toArray();
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

   app.listen(port, () => {
     console.log(`Server running on port ${port}`);
   });
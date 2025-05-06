# Use official Node.js 18 LTS image
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package.json and install dependencies
COPY package.json package-lock.json ./
RUN npm install --production

# Copy source code
COPY src/ ./src/

# Expose port
EXPOSE 3001

# Start the server (overridden for worker)
CMD ["node", "src/server.js"]
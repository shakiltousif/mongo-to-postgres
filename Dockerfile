# Use Node.js official image
FROM node:18

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package.json package-lock.json ./

# Install dependencies
RUN npm install

# Copy the entire project
COPY . .

# Expose port (Optional, if needed for monitoring)
EXPOSE 3000

# Start the Node.js application
CMD ["node", "index.js"]

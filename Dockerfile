# Stage 1: Build
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# Stage 2: Production
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/dist ./dist
COPY src/dashboard/public ./dist/dashboard/public
COPY src/queue/scripts ./dist/queue/scripts

EXPOSE 3000 3001
CMD ["node", "dist/index.js"]

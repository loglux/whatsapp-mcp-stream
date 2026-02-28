# WhatsApp MCP Stream Dockerfile
# Node.js 20 with FFmpeg

FROM node:20-bullseye-slim AS builder

# Install system dependencies for building (with retry for flaky apt in CI)
RUN set -e; \
    for i in 1 2 3; do \
      apt-get update && apt-get install -y --fix-missing \
        git \
        python3 \
        make \
        g++ \
        && rm -rf /var/lib/apt/lists/* && break; \
      echo "apt-get failed (attempt $i), retrying..." >&2; \
      sleep 2; \
    done

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install all dependencies (including dev dependencies for building)
RUN npm ci

# Copy source code
COPY . .

# Build TypeScript
RUN npm run build

# Production stage
FROM node:20-bullseye-slim

# Install runtime dependencies: FFmpeg and certificates (with retry for flaky apt)
RUN set -e; \
    for i in 1 2 3; do \
      apt-get update && apt-get install -y --fix-missing \
        ffmpeg \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/* && break; \
      echo "apt-get failed (attempt $i), retrying..." >&2; \
      sleep 2; \
    done

# Create a non-root user to run the application
RUN groupadd -r mcpuser && useradd -r -g mcpuser -G audio,video mcpuser \
    && mkdir -p /home/mcpuser/Downloads /app /app/whatsapp-sessions \
    && chown -R mcpuser:mcpuser /home/mcpuser /app

WORKDIR /app

# Copy package files from builder stage
COPY --from=builder --chown=mcpuser:mcpuser /app/package*.json ./

# Install production dependencies only
RUN npm ci --only=production

# Copy built application from builder stage
COPY --from=builder --chown=mcpuser:mcpuser /app/dist ./dist

# Copy other necessary files
COPY --from=builder --chown=mcpuser:mcpuser /app/.env.example ./
COPY --from=builder --chown=mcpuser:mcpuser /app/LICENSE ./
COPY --from=builder --chown=mcpuser:mcpuser /app/README.md ./
COPY --from=builder --chown=mcpuser:mcpuser /app/admin.html ./

# Switch to non-root user
USER mcpuser

# Expose default port (optional, for documentation)
EXPOSE 3001

# Start the server
CMD ["node", "--no-deprecation", "dist/index.js", "--http"]

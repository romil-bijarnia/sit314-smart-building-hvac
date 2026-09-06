FROM docker.io/node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY src ./src
COPY public ./public
COPY scripts ./scripts
RUN mkdir /data && chown node:node /data
USER node
ENV DATA_DIR=/data CERT_DIR=/certs PORT=8080
EXPOSE 8080
CMD ["node","src/service.cjs"]

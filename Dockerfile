FROM node:22-alpine

WORKDIR /app
COPY . .

RUN apk add --no-cache --virtual .build-deps git
RUN npm ci && \
    npm ci --prefix client && \
    npm run build && \
    npm run generate:git-info && \
    npm prune --production && \
    npm prune --production --prefix client
RUN apk del .build-deps

CMD ["node", "./dist/src/index.js", "&&", "node", "./client/dist/src/index.js"]

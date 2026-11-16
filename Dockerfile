FROM node:24.22.3-alpine@sha256:116b800c3754b440ee0aa3ed2f20f5bf3d99a554bfd9adeea09ea8017f6e85f5

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

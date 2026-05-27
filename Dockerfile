# syntax=docker/dockerfile:1.7

ARG NODE_IMAGE=docker.io/library/node:22-bookworm

FROM ${NODE_IMAGE} AS base
WORKDIR /app

ENV COREPACK_ENABLE_PROJECT_SPEC=0
ARG NPM_CONFIG_REGISTRY=http://verdaccio:4873/
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY}
ENV npm_config_registry=${NPM_CONFIG_REGISTRY}

RUN npm install --global pnpm@9.15.9 --registry "${NPM_CONFIG_REGISTRY}"

FROM base AS builder
COPY components/api-indexer/package.json components/api-indexer/package.json
COPY components/api-indexer/tsconfig.json components/api-indexer/tsconfig.json
COPY components/api-indexer/src components/api-indexer/src
RUN cd components/api-indexer && pnpm install --prod=false --registry "${NPM_CONFIG_REGISTRY}"
RUN cd components/api-indexer && pnpm build

FROM ${NODE_IMAGE} AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/components/api-indexer/dist components/api-indexer/dist
CMD ["node", "components/api-indexer/dist/index.js"]

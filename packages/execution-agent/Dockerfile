# syntax=docker/dockerfile:1.7
FROM node:24-bookworm-slim AS build

ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH
WORKDIR /src

RUN sed -i 's|http://deb.debian.org/debian|http://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://deb.debian.org/debian-security|http://mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install --yes --no-install-recommends g++ make python3 \
 && rm -rf /var/lib/apt/lists/* \
 && corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.base.json ./
COPY packages/run-context/package.json packages/run-context/tsconfig.json packages/run-context/
COPY packages/execution-protocol/package.json packages/execution-protocol/tsconfig.json packages/execution-protocol/
COPY packages/execution-agent/package.json packages/execution-agent/tsconfig.json packages/execution-agent/
RUN pnpm install --frozen-lockfile --filter @dsh-cloud/execution-agent...
COPY packages/run-context/src packages/run-context/src
COPY packages/execution-protocol/src packages/execution-protocol/src
COPY packages/execution-agent/src packages/execution-agent/src
RUN pnpm exec tsc -b packages/run-context packages/execution-protocol packages/execution-agent

FROM node:24-bookworm-slim AS runtime

ENV NODE_ENV=production
ENV DSH_CLOUD_WORKSPACE_ROOT=/workspace
ENV DSH_CLOUD_RUNTIME_ROOT=/tmp/dsh-cloud-runtime
ENV DSH_CLOUD_EXECUTION_PORT=49984
WORKDIR /app

RUN sed -i 's|http://deb.debian.org/debian|http://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://deb.debian.org/debian-security|http://mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install --yes --no-install-recommends bash ca-certificates git openssh-client python3 tini \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /workspace /tmp/dsh-cloud-runtime \
 && chown -R node:node /workspace /tmp/dsh-cloud-runtime
COPY --from=build --chown=node:node /src/node_modules /app/node_modules
COPY --from=build --chown=node:node /src/packages/run-context/package.json /app/packages/run-context/package.json
COPY --from=build --chown=node:node /src/packages/run-context/lib /app/packages/run-context/lib
COPY --from=build --chown=node:node /src/packages/run-context/node_modules /app/packages/run-context/node_modules
COPY --from=build --chown=node:node /src/packages/execution-protocol/package.json /app/packages/execution-protocol/package.json
COPY --from=build --chown=node:node /src/packages/execution-protocol/lib /app/packages/execution-protocol/lib
COPY --from=build --chown=node:node /src/packages/execution-protocol/node_modules /app/packages/execution-protocol/node_modules
COPY --from=build --chown=node:node /src/packages/execution-agent/package.json /app/packages/execution-agent/package.json
COPY --from=build --chown=node:node /src/packages/execution-agent/lib /app/packages/execution-agent/lib
COPY --from=build --chown=node:node /src/packages/execution-agent/node_modules /app/packages/execution-agent/node_modules

USER node
EXPOSE 49984
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "packages/execution-agent/lib/main.js"]

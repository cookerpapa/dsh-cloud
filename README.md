# DSH Cloud

DSH Cloud is a self-hosted cloud distribution of [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). It keeps the official DSH Web UI and Agent Loop, while moving durable sessions and the code-execution world behind cloud-safe service boundaries.

This is a separate project from AgentDock. AgentDock remains the Pi-based reference implementation; DSH Cloud follows DSH's Cordis plugin model instead of forcing both harnesses through one runtime adapter.

## Current milestone

The first runnable slice provides:

- the official DSH Web profile and frontend;
- an append-only PostgreSQL implementation of DSH's native `SessionPersistence` contract;
- bounded event batching through DSH's upstream persistence coordinator;
- monotonic writer fencing for future stateless Worker handoff;
- a Cloud Web launcher that applies the cloud profile without modifying a user's normal DSH home.

Tool execution still uses DSH's local development providers in this milestone. The next milestone replaces `ctx.fs` and `ctx.subprocess` with a remote CubeSandbox execution-world provider; local execution will then be removed from the production profile.

## Target architecture

```text
Browser (official DSH Web UI)
        |
        v
Cloud Host / Control Plane
        |
        +---- PostgreSQL Session event log
        |
        +---- durable Run scheduler
                    |
                    v
             Trusted DSH Workers
             Agent Loop + model auth
                    |
                    v
          Remote fs/subprocess providers
                    |
                    v
             CubeSandbox KVM
```

The Session log is the model-context authority. PostgreSQL stores DSH's native events rather than a separately reconstructed `messages[]` array, so compaction, steering, tool outcomes, request headers, and interrupted-turn recovery keep upstream semantics.

## Local development

Requirements: Node.js 22.19+ or 24+, pnpm 11, and PostgreSQL.

```bash
cp .env.example .env
pnpm install
pnpm build
set -a; . ./.env; set +a
pnpm start
```

Open the URL printed by DSH. The launcher stores the generated Web profile and other local state under `.data/dsh-home`.

Run the verification suite with:

```bash
pnpm check
pnpm db:up
pnpm test:integration
```

`test:integration` runs the Session contract against real PostgreSQL and boots
the released DSH Web profile through the Cloud overlay before fetching the
official frontend. CI runs both gates on every push.

See [the architecture document](docs/architecture.md) for ownership and failure boundaries.

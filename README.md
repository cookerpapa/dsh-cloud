# DSH Cloud

DSH Cloud is a self-hosted cloud distribution of [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). It keeps the official DSH Web UI and Agent Loop, while moving durable sessions and the code-execution world behind cloud-safe service boundaries.

This is a separate project from AgentDock. AgentDock remains the Pi-based reference implementation; DSH Cloud follows DSH's Cordis plugin model instead of forcing both harnesses through one runtime adapter.

## Current implementation

The runnable cloud slice provides:

- the official DSH Web profile and frontend;
- an append-only PostgreSQL implementation of DSH's native `SessionPersistence` contract;
- bounded event batching through DSH's upstream persistence coordinator;
- lossless packing of adjacent token deltas into ranged PostgreSQL records while preserving DSH's logical event sequence;
- Workspace-scoped monotonic writer fencing for safe cross-Worker handoff;
- a Cloud Web launcher that applies the cloud profile without modifying a user's normal DSH home;
- remote implementations of DSH's native filesystem, subprocess, terminal, and sandbox services;
- a trusted Sandbox Manager that derives Cube identity from Run authority and rejects stale fences;
- one credential-free execution agent per CubeSandbox KVM;
- public registration/login with tenant-filtered Session and Workspace APIs;
- a PostgreSQL transactional Run queue with idempotent admission, per-Workspace serialization, fair claims, leases, fencing, cancellation, and crash reconciliation;
- a horizontally scalable pool of short-lived DSH Agent runs; `LISTEN/NOTIFY` is only a latency hint and polling preserves correctness;
- PostgreSQL durability barriers before live Session events are forwarded to a browser;
- one stable Cube Volume per tenant Workspace, reattached to replacement KVMs.

The production profile disables DSH's local filesystem and subprocess providers. There is no Docker, `runc`, or local-process compatibility fallback: user-generated effects can only enter the execution world through the authenticated Cube path.

## Target architecture

```text
Browser (official DSH Web UI)
        |
        v
Multi-tenant Gateway
        |
        +---- PostgreSQL auth / Run queue / Session event log
        |
        +---- transactional Run claims
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

The Session log is the model-context authority. PostgreSQL stores DSH's native events rather than a separately reconstructed `messages[]` array, so compaction, steering, tool outcomes, request headers, and interrupted-turn recovery keep upstream semantics. Temporal is intentionally not part of this design: the current AgentDock architecture and DSH Cloud both use PostgreSQL as the sole product-state and Run-queue authority.

## Local development

Requirements: Node.js 22.19+ or 24+, pnpm 11, and PostgreSQL.

```bash
cp .env.example .env
pnpm install
pnpm build
set -a; . ./.env; set +a
pnpm start:manager
# in separate terminals:
pnpm start
pnpm start:gateway
```

Open `http://127.0.0.1:8080`, register, and use the official DSH UI through the authenticated Gateway. The Worker launcher stores its generated Web profile and other local state under `.data/dsh-home`. Add more Workers with distinct `DSH_CLOUD_WORKER_ID`, `DSH_HOME`, `DSH_CLOUD_PORT`, and `DSH_CLOUD_WORKER_URL`; all replicas compete through PostgreSQL without a second scheduler.

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
The latest reproducible Docker/Kubernetes, real-model, and Cube KVM results are
recorded in the [production acceptance report](docs/reports/production-acceptance-latest.md).

## One-host deployment

The one-host profile requires Docker Compose plus an existing CubeSandbox
control/compute cluster and a Cube Volume driver. Build and publish the
credential-free execution image, register it as described below, then run:

```bash
./install.sh
# edit deploy/production/production.env with the model and Cube values
./install.sh
```

The first invocation creates a mode-0600 environment file and generates the
platform-owned secrets. The second validates the configuration, builds the
Gateway/Worker/Manager images, starts PostgreSQL and two Workers, and waits for
the health gates. `./install.sh check` renders Compose without changing the
deployment; `./install.sh down` stops services while retaining PostgreSQL and
Worker profile volumes.

Important lifecycle values are deliberately shared across components:

| Variable | Default | Contract |
| --- | ---: | --- |
| `DSH_CLOUD_RUN_LEASE_SECONDS` | 20 s | Used by both Worker reconciliation and Sandbox Manager authority checks; configure one identical value everywhere. |
| `DSH_CLOUD_WORKER_DRAIN_TIMEOUT_MS` | 540 s | Maximum graceful Run drain before Worker abort; must remain below the orchestrator termination grace period. |
| `DSH_CLOUD_SANDBOX_IDLE_TTL_MS` | 30 min | Warm KVM lifetime after its last Tool operation; Workspace bytes remain in the Cube Volume. |
| `DSH_CLOUD_WORKER_SLOTS` | 4 | Concurrent active Agent Runs admitted by one Worker process. |

Workers heartbeat every five seconds. The default 20-second Run lease therefore
tolerates transient database delays while still fencing an abandoned Attempt
quickly. Changing the lease in only the Manager or only the Workers is an
invalid deployment.

## Kubernetes deployment

The Helm chart deploys independent Gateway, Worker and Sandbox Manager replica
sets. PostgreSQL and Cube remain external authorities. KEDA scales only the
Worker Deployment from the PostgreSQL ready-Run backlog; it is not a second
scheduler and losing KEDA does not lose queued work.

```bash
helm lint deploy/helm/dsh-cloud --set sandbox.cube.templateId=your-template
helm upgrade --install dsh-cloud deploy/helm/dsh-cloud \
  --namespace dsh-cloud --create-namespace \
  --set sandbox.cube.templateId=your-template
```

Create the PostgreSQL, model and Cube Secrets named in `values.yaml` before
installation. When autoscaling is enabled, install KEDA first. The chart adds
PodDisruptionBudgets, non-root/read-only security contexts, default-deny
NetworkPolicies, Worker drain grace, health probes and Prometheus scrape
annotations.

KEDA runs in its own namespace, so the PostgreSQL URL stored in the Secret must
use a fully qualified in-cluster service name (for example,
`postgres.database.svc.cluster.local`) or an externally resolvable address. A
short service name that resolves only inside the DSH Cloud namespace will let
the application work while silently preventing KEDA from reading the backlog.

`sandbox.cube.apiPort` must match the port embedded in
`sandbox.cube.apiUrl`; it is kept explicit so the default-deny NetworkPolicy
can admit only the configured Cube control and proxy ports.
For an installation whose Worker nodes require an HTTP proxy for model calls,
set `worker.proxy.enabled=true` and the three proxy values; Node's environment
proxy support is enabled only in that explicit profile.

## Cube execution image

Build the credential-free in-VM agent from the repository root:

```bash
docker build -f packages/execution-agent/Dockerfile \
  -t registry.example/dsh-cloud/execution-agent:0.1.0 .
```

Push it to the registry visible to Cube, then use
`deploy/cube/register-template.sh` to create a template. The script requires
the Cube CLI, master address, and image through environment variables and
prints Cube's JSON response; store the returned template id in
`DSH_CLOUD_CUBE_TEMPLATE_ID`. The health probe is `/health/live` on port
`49984`.

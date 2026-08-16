# DSH Cloud

DSH Cloud is a self-hosted cloud distribution of [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). It keeps the official DSH Web UI and Agent Loop, while moving durable sessions and the code-execution world behind cloud-safe service boundaries.

This is a separate project from Pi Cloud. Pi Cloud remains the Pi-based reference implementation; DSH Cloud follows DSH's Cordis plugin model instead of forcing both harnesses through one runtime adapter.

## Current implementation

The runnable cloud slice provides:

- the official DSH Web profile and frontend;
- a Cordis `SessionPersistence` plugin with one compressed immutable PostgreSQL segment per settled Turn;
- a replaceable `SessionLiveLog` Provider (Kafka by default) for the exact unfinished native event suffix;
- a replaceable `SessionLiveProjection` Provider (Valkey by default) applied before browser visibility;
- Workspace-scoped monotonic writer fencing for safe cross-Worker handoff;
- a Cloud Web launcher that applies the cloud profile without modifying a user's normal DSH home;
- remote implementations of DSH's native filesystem, subprocess, terminal, and sandbox services;
- a trusted Tool Broker that admits fenced Tool calls and routes them through a replaceable Cube provider;
- one credential-free execution agent per CubeSandbox KVM;
- public registration/login with tenant-filtered Session and Workspace APIs;
- a PostgreSQL transactional Run queue with idempotent admission, per-Workspace serialization, fair claims, leases, fencing, cancellation, and crash reconciliation;
- a horizontally scalable pool of short-lived DSH Agent runs; `LISTEN/NOTIFY` is only a latency hint and polling preserves correctness;
- bounded process-local Agent residency: after an ordinary user Turn is durably flushed, DSH's public `AgentHandle` is disposed and the next Run resumes through shared SessionPersistence;
- deployment-approved per-Session Harness profiles, with the chosen DSH Agent preset preserved across Worker handoff;
- a shared Gateway event outlet that aggregates every healthy Worker's native downlink, so browser delivery and future Runs do not depend on user/Session placement;
- projection watermarks that prevent live Session events from reaching a browser before Kafka and Valkey acknowledge them;
- one stable Cube Volume per tenant Workspace, reattached to replacement KVMs.

The production profile disables DSH's local filesystem and subprocess providers. There is no Docker, `runc`, or local-process compatibility fallback: user-generated effects can only enter the execution world through the authenticated Cube path.

## Target architecture

```text
Browser (official DSH Web UI)
        |
        v
Multi-tenant Gateway
        |\
        | +---- shared Worker event outlet <---- all healthy Worker downlinks
        |
        +---- PostgreSQL auth / Run queue / sealed Turn segments
        |             ^
        |             |
        |      Kafka active suffix -> Valkey live projection
        |
        +---- transactional Run claims
                    |
                    v
             Trusted DSH Workers
             Agent Loop + model auth
                    |
                    v
          Remote fs/subprocess plugins
                    |
                    v
              Tool Broker
                    |
                    v
             CubeSandbox KVM provider
```

The Session log is the model-context authority. PostgreSQL stores DSH's native events rather than a separately reconstructed `messages[]` array, so compaction, steering, tool outcomes, request headers, and interrupted-turn recovery keep upstream semantics. Successful Compaction also creates a verified Session runtime baseline containing the effective surface and runtime state; a cold Worker reads it plus only the later physical suffix, while canonical Turn segments remain the human-history, audit and fallback source. Temporal is intentionally not part of this design: Pi Cloud and DSH Cloud both use PostgreSQL as the sole product-state and Run-queue authority.

## Local development

Requirements: Node.js 22.19+ or 24+, pnpm 11, PostgreSQL, Kafka, and Valkey.

```bash
cp .env.example .env
pnpm install
pnpm build
docker compose -f deploy/dev/compose.yaml up -d --wait postgres kafka valkey
set -a; . ./.env; set +a
pnpm start:broker
# in separate terminals:
pnpm start
pnpm start:gateway
```

Open `http://127.0.0.1:8080`, register, and use the official DSH UI through the authenticated Gateway. The Worker launcher stores its generated Web profile and other local state under `.data/dsh-home`. Add more Workers with distinct `DSH_CLOUD_WORKER_ID`, `DSH_HOME`, `DSH_CLOUD_PORT`, and `DSH_CLOUD_WORKER_URL`; all replicas compete through PostgreSQL without a second scheduler or durable Worker placement.

Run the verification suite with:

```bash
pnpm check
pnpm db:up
pnpm test:integration
```

`test:integration` runs the Session contract against real PostgreSQL and boots
the released DSH Web profile through the Cloud overlay before fetching the
official frontend. CI runs both gates on every push.

Real-model multi-user journeys and isolated Kafka/PostgreSQL capacity probes
are intentionally separate from CI. Their reproducible commands, workload
models and grading rules are documented in [Testing DSH Cloud](docs/testing.md).

See [the documentation map](docs/README.md) and
[architecture document](docs/architecture.md) for ownership and failure
boundaries. Operator-facing settings and their cross-component contracts are
listed in [configuration](docs/configuration.md).
The [Pi Cloud alignment review](docs/pi-cloud-alignment.md) records which
cloud invariants are shared and why DSH-specific event persistence remains
different.
The latest automated, real-model, and Cube KVM results are
recorded in the [production acceptance report](docs/reports/production-acceptance-latest.md).

## One-host deployment

The one-host profile requires Docker Compose plus a DSH-owned CubeSandbox API
security domain and Cube Volume driver. It must use the DSH authorizer and an
API credential distinct from Pi Cloud or any other product; the checked-in
[Cube deployment guide](deploy/cube/README.md) describes that boundary. Set
`DSH_CLOUD_CUBE_CONTROL_NETWORK` to the external Docker network that exposes
the trusted Cube API relay named by `DSH_CLOUD_CUBE_API_URL`; the Tool Broker joins
that network, while Workers and the Gateway do not. Build and publish the
credential-free execution image, register it as described below, then run:

```bash
./install.sh
# edit deploy/production/production.env with the model and Cube values
./install.sh
```

The first invocation creates a mode-0600 environment file and generates the
platform-owned secrets, including the DSH Cube API key. The second validates the configuration, builds the
Gateway/Worker/Tool-Broker images, starts PostgreSQL, Kafka, Valkey and two Workers, and waits for
the health gates. `./install.sh check` renders Compose without changing the
deployment; `./install.sh down` stops services while retaining PostgreSQL and
Worker profile volumes.

Important lifecycle values are deliberately shared across components:

| Variable | Default | Contract |
| --- | ---: | --- |
| `DSH_CLOUD_RUN_LEASE_SECONDS` | 20 s | Used by both Worker reconciliation and Tool Broker authority checks; configure one identical value everywhere. |
| `DSH_CLOUD_WORKER_DRAIN_TIMEOUT_MS` | 540 s | Maximum graceful Run drain before Worker abort; must remain below the orchestrator termination grace period. |
| `DSH_CLOUD_SANDBOX_IDLE_TTL_MS` | 30 min | Warm KVM lifetime after its last Tool operation; Workspace bytes remain in the Cube Volume. |
| `DSH_CLOUD_WORKER_SLOTS` | 4 | Concurrent active Agent Runs admitted by one Worker process. |
| `DSH_CLOUD_LIVE_EVENT_RETENTION_SECONDS` | 24 h | Retention of the rebuildable Valkey live projection; settled history remains in PostgreSQL Turn segments. |
| `DSH_CLOUD_KAFKA_EVENT_RETENTION_MS` | 30 d | Maximum recovery horizon for an unfinished Turn; it must exceed the Valkey replay window. Configure existing external topics to the same value. |
| `DSH_CLOUD_EVENT_PROJECTION_TIMEOUT_MS` | 90 s | Gateway visibility deadline; permits one bounded Kafka retry before failing closed. |
| `DSH_CLOUD_AGENT_PRESETS` | `standard,code` | Trusted Agent presets selectable for a blank Session; every Worker image must ship the same compositions. |

Workers heartbeat every five seconds. The default 20-second Run lease therefore
tolerates transient database delays while still fencing an abandoned Attempt
quickly. Changing the lease in only the Tool Broker or only the Workers is an
invalid deployment. The Compose Workers use a 600-second stop grace, matching
the Helm termination grace and keeping the 540-second application drain inside
the orchestrator deadline.

## Kubernetes deployment

The Helm chart deploys independent Gateway, Worker and Tool Broker replica
sets. PostgreSQL, Kafka, Valkey and Cube remain external authorities. KEDA scales only the
Worker Deployment from the PostgreSQL ready-Run backlog; it is not a second
scheduler and losing KEDA does not lose queued work.

```bash
helm lint deploy/helm/dsh-cloud --set sandbox.cube.templateId=your-template
helm upgrade --install dsh-cloud deploy/helm/dsh-cloud \
  --namespace dsh-cloud --create-namespace \
  --set sandbox.cube.templateId=your-template
```

Create the PostgreSQL, model and Cube Secrets named in `values.yaml`, and configure the external Kafka/Valkey endpoints, before
installation. When autoscaling is enabled, install KEDA first. The chart adds
PodDisruptionBudgets, non-root/read-only security contexts, default-deny
NetworkPolicies, Worker drain grace, health probes and Prometheus scrape
annotations.

The Cube Secret must contain the same dedicated DSH credential installed in
`dsh-cloud-cube-api-credential`. Tool Broker startup rejects a Pi Cloud policy
or a credential that can access both `dsh-*` and `adw-*` Volume identities.

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

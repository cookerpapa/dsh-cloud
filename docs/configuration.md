# Configuration

DSH Cloud has one shared lifecycle contract and three independently scalable
services: Gateway, Worker, and Tool Broker. Start from `.env.example` for local
development or `deploy/production/production.env.example` for one-host
deployment. Kubernetes values and Secret names live under
`deploy/helm/dsh-cloud`.

## Shared authority

| Setting | Purpose |
| --- | --- |
| `DSH_CLOUD_DATABASE_URL` | PostgreSQL product, Run, fence and Session authority. |
| `DSH_CLOUD_NAMESPACE` | Logical isolation namespace used by all services. |
| `DSH_CLOUD_RUN_LEASE_SECONDS` | Attempt lease checked by Workers and Tool Broker; the value must be identical in both. |
| `DSH_CLOUD_TOOL_BROKER_URL` | Trusted Tool RPC endpoint used by Gateway/Worker-side providers. |
| `DSH_CLOUD_TOOL_BROKER_TOKEN` | Internal Tool Broker credential; never injected into Cube. |

## Session stream

| Setting | Purpose |
| --- | --- |
| `DSH_CLOUD_KAFKA_BROKERS` | Durable live-log Provider endpoints. |
| `DSH_CLOUD_KAFKA_SESSION_TOPIC` | Exact unfinished native Session-event topic. |
| `DSH_CLOUD_KAFKA_PARTITIONS` | Topic partitions created by the development/bootstrap path. |
| `DSH_CLOUD_KAFKA_REPLICATION_FACTOR` | Topic replication requested by bootstrap. Production should match broker count. |
| `DSH_CLOUD_KAFKA_EVENT_RETENTION_MS` | Maximum unfinished-Turn recovery horizon; must exceed the live projection window. |
| `DSH_CLOUD_VALKEY_URL` | Rebuildable low-latency Session projection. |
| `DSH_CLOUD_LIVE_EVENT_RETENTION_SECONDS` | Valkey projection retention. |
| `DSH_CLOUD_EVENT_PROJECTION_TIMEOUT_MS` | Maximum Gateway wait before it fails closed instead of exposing a non-durable event. |

Fine token payloads live in Kafka only while a Turn is unfinished. A settled
Turn is stored as one compressed native PostgreSQL segment; Valkey remains a
rebuildable projection.

## Gateway and Worker

| Setting | Purpose |
| --- | --- |
| `DSH_CLOUD_GATEWAY_HOST` / `DSH_CLOUD_GATEWAY_PORT` | Public authenticated Gateway bind. |
| `DSH_CLOUD_GATEWAY_DB_POOL` | Gateway PostgreSQL pool size; default `20`. |
| `DSH_CLOUD_PUBLIC_ORIGIN` | Optional canonical browser origin. |
| `DSH_CLOUD_SECURE_COOKIES` | Set to `1` behind HTTPS. |
| `DSH_CLOUD_WORKER_ID` | Unique current Worker identity, not durable Session placement. |
| `DSH_CLOUD_WORKER_SLOTS` | Maximum concurrent Agent Runs in one Worker process. |
| `DSH_CLOUD_WORKER_URL` | Trusted address advertised to Gateway for the current Worker. |
| `DSH_CLOUD_WORKER_CONTROL_HOST` / `DSH_CLOUD_WORKER_CONTROL_PORT` | Private Worker Control Channel bind. |
| `DSH_CLOUD_WORKER_DRAIN_TIMEOUT_MS` | Graceful active-Run drain deadline. |
| `DSH_CLOUD_WORKER_METRICS_PORT` | Worker Prometheus endpoint. |

`DSH_CLOUD_WORKER_ENABLED=0` is an internal profile switch used when launching
the upstream Host without a queue consumer. It is not a second deployment
mode and creates no local execution fallback.

## Cube execution plane

| Setting | Purpose |
| --- | --- |
| `DSH_CLOUD_CUBE_API_URL` / `DSH_CLOUD_CUBE_API_KEY` | Trusted Cube control endpoint and credential, held only by Tool Broker. |
| `DSH_CLOUD_CUBE_TEMPLATE_ID` | Registered credential-free execution image. |
| `DSH_CLOUD_CUBE_VOLUME_DRIVER` | Persistent Workspace Volume driver; default `dsh-cloud-posix`. |
| `DSH_CLOUD_CUBE_PROXY_NODE_IP`, `DSH_CLOUD_CUBE_PROXY_PORT`, `DSH_CLOUD_CUBE_PROXY_SCHEME`, `DSH_CLOUD_CUBE_DOMAIN` | Private ingress route from Tool Broker to the execution agent. |
| `DSH_CLOUD_CUBE_EGRESS_PROXY_IP` | Only trusted egress-proxy destination made available to the KVM. |
| `DSH_CLOUD_SANDBOX_ENCRYPTION_KEY` | Tool Broker encryption key for stored activation credentials. |
| `DSH_CLOUD_SANDBOX_IDLE_TTL_MS` | Warm Cube idle lifetime; persistent Volume bytes outlive activation loss. |
| `DSH_CLOUD_TOOL_BROKER_PORT` | Private Tool Broker listen port; default `3090`. |

`DSH_CLOUD_EXECUTION_PORT`, `DSH_CLOUD_WORKSPACE_ROOT`, and
`DSH_CLOUD_RUNTIME_ROOT` configure the credential-free execution image. They
are image internals, not tenant-controlled settings.

## Valid lifecycle relationships

- Worker heartbeat is five seconds; the default 20-second lease permits brief
  PostgreSQL stalls while fencing abandoned Attempts promptly.
- Worker drain timeout must be shorter than the orchestrator termination grace
  period.
- Kafka retention must be longer than Valkey retention because Kafka is the
  exact unfinished suffix and Valkey is only its projection.
- A larger warm-sandbox TTL consumes more KVM capacity but does not change
  Workspace durability.
- Secrets must be supplied by deployment Secret/file mechanisms and must not
  enter Worker logs, Session events, Workspace files, or Cube environment.

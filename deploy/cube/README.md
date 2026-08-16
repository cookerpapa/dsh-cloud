# DSH Cloud Cube API boundary

DSH Cloud owns its Cube API credential and authorization policy. Do not point
the Tool Broker at Pi Cloud's Cube API frontend or reuse Pi Cloud's API key.
The DSH policy admits only the control calls used by the Tool Broker and only
`dsh-<48 hex>` persistent Volume item paths; `adw-*` Pi Cloud Volumes are
denied.

The recommended deployment is a DSH-owned CubeSandbox control/compute
installation. An operator may attach a separately deployed, stateless DSH
CubeAPI frontend to trusted shared Cube infrastructure, but the API key,
callback, Service and route exposed to DSH Cloud must remain distinct. The
Tool Brokers are trusted principals; Cube's callback protocol does not include
request bodies and therefore cannot create a hostile boundary between two
control clients sharing one CubeMaster.

Build the authorizer image and make it visible to the target Kubernetes
cluster:

```bash
docker build -f deploy/docker/Dockerfile.services \
  --target cube-api-authorizer \
  -t dsh-cloud/cube-api-authorizer:local .
```

Create the credential Secret from the same value used as
`DSH_CLOUD_CUBE_API_KEY`, then install the callback:

```bash
kubectl create namespace cube-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cube-system create secret generic dsh-cloud-cube-api-credential \
  --from-literal=api-key="$DSH_CLOUD_CUBE_API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/cube/authorizer.yaml
```

Merge `values-dsh-cloud.yaml` into the upstream CubeSandbox v0.6.0 Helm
installation. CubeAPI then calls the DSH authorizer for every request. Configure
the Tool Broker's `DSH_CLOUD_CUBE_API_URL` to that DSH-owned CubeAPI Service,
not to another product's relay.

Register [`cube-volume-dsh-cloud-posix.sh`](cube-volume-dsh-cloud-posix.sh) as
the `dsh-cloud-posix` binary Volume Plugin in both CubeMaster and Cubelet. Both
processes must mount that executable at their configured `binary_path`, and the
Cubelet must expose the persistent POSIX root at `/data/cube-shared/volume`.
The relevant upstream configuration entries are:

```yaml
# CubeMaster conf.yaml
volume_plugins:
  - name: dsh-cloud-posix
    type: binary
    binary_path: /usr/local/services/cubetoolbox/CubeMaster/plugin/cube-volume-dsh-cloud-posix
```

```toml
# Cubelet config.toml
[[plugins."io.cubelet.internal.v1.storage".volume_plugins]]
name = "dsh-cloud-posix"
type = "binary"
binary_path = "/usr/local/services/cubetoolbox/Cubelet/plugin/cube-volume-dsh-cloud-posix"
```

The backing POSIX filesystem is a deployment responsibility. A single-host
installation may use a host-mounted XFS directory; a multi-node installation
must use shared storage with the required consistency and durability. Do not
point this driver at Pi Cloud's Workspace root.

At startup the Tool Broker verifies both halves of the policy: an authorized,
nonexistent `dsh-*` Volume must return 404, while an `adw-*` probe must return
401. A mismatched or shared credential therefore fails readiness before any
model-generated Tool operation can run.

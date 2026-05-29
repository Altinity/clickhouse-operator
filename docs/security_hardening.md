# ClickHouse operator hardening guide

## Overview

This section provides an overview of the **clickhouse-operator** security model.

With the default settings, the ClickHouse operator deploys ClickHouse with two users protected by network restriction rules to block unauthorized access.

### The 'default' user

The '**default**' user is used to connect to ClickHouse instance from a pod where it is running, and also for distributed queries. It is deployed with an **empty password** that was a long-time default for ClickHouse out-of-the-box installation.

For security purposes, we recommend that you disable the `default` user altogether. As an example, create a file named `remove_default_user.xml` and place it in the `users.d` directory. This markup does the trick:

```xml
<clickhouse>
   <users>
      <default remove="1"/>
   </users>
</clickhouse>
```

However, if you do use the `default` user, the operator applies network security rules that restrict connections to the pods running the ClickHouse cluster, and nothing else.

Before version **0.19.0**  `hostRegexp` was applied that captured pod names. This did not work correctly in some Kubernetes distributions, such as GKE. In later versions, the operator additionally applies a restrictive set of pod IP addresses and rebuilds this set if the IP address of a pod changes for whatever reason.

The following `users.xml` is set up by operator for a cluster that has two nodes. In this configuration, a '**default**' user can not connect from outside of the cluster.

```xml
<users>
  <default>
    <networks>
      <host_regexp>(chi-my-cluster-[^.]+\d+-\d+|clickhouse\-my-cluster\.test\.svc\.cluster\.local$</host_regexp>
      <ip>::1</ip>
      <ip>127.0.0.1</ip>
      <ip>172.17.0.4</ip>
      <ip>172.17.0.12</ip>
    </networks>
    <profile>default</profile>
    <quota>default</quota>
  </default>
</users>
```

### The 'clickhouse_operator' user

The '**clickhouse_operator**' user is used by the operator itself to perform DMLs when adding or removing ClickHouse replicas and shards, and also for collecting monitoring data. The **user** and **password** values are stored in a secret.

The following example shows how **secret** is referenced in the **clickhouse_operator** configuration:

```yaml
clickhouse:
  access:
    secret:
      # Empty `namespace` means that k8s secret would be looked
      # in the same namespace where the operator's pod is running.
      namespace: ""
      name: "clickhouse-operator"
```

The following example shows a **secret**:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-operator
type: Opaque
stringData:
  username: clickhouse_operator
  password: chpassword
```

We recommend that you do not include the **user** and **password** within the operator configuration without a **secret**, though it is also supported.

To change '**clickhouse_operator**' user password you can modify `etc-clickhouse-operator-files` configmap or create `ClickHouseOperatorConfiguration` object, then restart the operator to apply the change.

See [operator configuration](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_configuration.md) for more information about operator configuration files.

The operator also protects access for the '**clickhouse\_operator**' user using an IP mask. When deploying a user into a ClickHouse server, access is restricted to the IP address of the pod where the operator is running, and nothing else. Therefore, the '**clickhouse_operator**' user can not be used outside of this pod.

## Securing ClickHouse users

More ClickHouse users can be created using SQL `CREATE USER` statement or in a dedicated section of `ClickHouseInstallation`.

To make sure passwords are not exposed, for the `ClickHouseInstallation` the operator provides the following:

### Using hashed passwords

User passwords in `ClickHouseInstallation` can be specified in plain, as sha256 and double sha1 hashes.

When a password is specified in plaintext, the operator hashes it when deploying to ClickHouse, but that is still left in unsecure plaintext format in the `ClickHouseInstallation`.

Altinity recommends providing hashes explicitly as follows:

```yaml
spec:
  useTemplates:
    - name: clickhouse-version
  configuration:
    users:
      user1/password: pwduser1  # This will be hashed in ClickHouse config files, but this NOT RECOMMENDED
      user2/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
      user3/password_double_sha1_hex: cbe205a7351dd15397bf423957559512bd4be395
```

### Using secrets

The operator also allows user to specify passwords and password hashes in a Kubernetes secret as follows:

```yaml
spec:
  configuration:
    users:
      user1/password:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser1
      user2/password_sha256_hex:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser2          
      user3/password_double_sha1_hex:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser3                
```

The following example refers to the secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-secret
type: Opaque
stringData:
  pwduser1: pwduser1
  pwduser2: e106728c3541ec3694822a29d4b8b5f1f473508adc148fcb58a60c42bcf3234c
  pwduser3: cbe205a7351dd15397bf423957559512bd4be395

```

**DEPRECATED**: Since version 0.23.x the syntax to read passwords and password hashes from a secret using special 'k8s\_secret\_' and 'k8s\_secret\_env\_' prefixes is deprecated:

```yaml
spec:
  configuration:
    users:
      user1/k8s_secret_password: clickhouse-secret/pwduser1
      user2/k8s_secret_password_sha256_hex: clickhouse-secret/pwduser2
      user3/k8s_secret_password_double_sha1_hex: clickhouse-secret/pwduser3
```

```yaml
spec:
  configuration:
    users:
      user1/k8s_secret_env_password: clickhouse-secret/pwduser1
      user2/k8s_secret_env_password_sha256_hex: clickhouse-secret/pwduser2
      user3/k8s_secret_env_password_double_sha1_hex: clickhouse-secret/pwduser3
```

### Securing the 'default' user

While the '**default**' user is protected by network rules, passwordless operation is often not allowed by infosec teams. The password for the '**default**' user can be changed the same way as for other users. However, the '**default**' user is also used by ClickHouse to run distributed queries. If the password changes, distributed queries may stop working.

To keep distributed queries running without exposing the password, configure ClickHouse to use a secret token for inter-cluster communications instead of 'default' user credentials.


The operator supports the following:

#### 'auto' token

The following example shows how to let ClickHouse generate the secret token automatically. This is the simplest and recommended way.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: default
        secret:
          auto: "true"
```

#### Custom token

The following example shows how to define a token.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: "default"
        secret:
          value: "my_secret"
```

#### Custom token from Kubernetes secret

The following example shows how to define a token within a secret.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: "default"
        secret:
          valueFrom:
            secretKeyRef:
              name: "secure-inter-cluster-communications"
              key: "secret"
```

## Securing ClickHouse server settings

Some ClickHouse server settings may contain sensitive data, for example, passwords or keys to access external systems. ClickHouse allows a user to keep connection information for external systems in [Named Collections](https://clickhouse.com/docs/en/operations/named-collections) defined by DDL, but sometimes it is more convenient to store keys in server configuration files. In order to do it securely, sensitive information needs to be stored in secrets.

For example, in order to access S3 bucket one may define the following secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-credentials
type: Opaque
stringData:
  AWS_SECRET_ACCESS_KEY: *****
  AWS_ACCESS_KEY_ID: *****
```

Secret can be referred in ```ClickHouseInstallation``` as follows:

```yaml
spec:
  configuration:
    settings:
      s3/my_bucket/endpoint: "https://my-bucket.s3.amazonaws.com/sample/"
      s3/my_bucket/secret_access_key:
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: AWS_SECRET_ACCESS_KEY
      s3/my_bucket/access_key:
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: AWS_ACCESS_KEY_ID
```

Under the hood, secrets settings are mapped to environment variables and referred in XML configuration files using ```from_env``` syntax. So the snippet above is equivalent to the following:

```yaml
spec:
  templates:
    podTemplates:
      - name: default
        spec:
          containers:
          - name: clickhouse
            image: altinity/clickhouse-server:24.3.12.76.altinitystable
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: s3-credentials
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: s3-credentials
                  key: AWS_SECRET_ACCESS_KEY
  configuration:
    files:
      config.d/s3.xml: |
        <clickhouse>
          <s3>
            <my_bucket>
               <endpoint>https://my-bucket.s3.amazonaws.com/sample/</endpoint>
               <access_key_id from_env="AWS_ACCESS_KEY_ID"></access_key_id>
               <secret_access_key from_env="AWS_SECRET_ACCESS_KEY"></secret_access_key>
            </my_bucket>
          </s3>
        </clickhouse>
```

## Securing the network

This section covers how to secure your network.

### Network Overview

With default settings, the operator deploys pods and services that expose 3 ports:

* 8123 -- HTTP interface
* 9000 -- TCP interface
* 9009 -- used for replication protocol between cluster nodes (HTTP)

For every pod, there is one service created, and also load balancer service is created to access the cluster. Additional load balancers and custom services may be created using service templates.

### Enabling secure connections to clickhouse-server

[ClickHouse Network Hardening Guide](https://docs.altinity.com/operationsguide/security/clickhouse-hardening-guide/network-hardening/) describes steps required to secure ClickHouse server. Some of them are manual, others are outomated by operator.

The ClickHouse HTTPS/TLS configuration requires the following steps:

* Generate the following certificate files:

  * `server.crt`
  * `server.key`
  * `dhparam.pem`

* Add generated files into the `files` section of `ClickHouseInstallation`
* Add **openSSL configuration** for the server (and client, if ClickHouse needs to connect to other nodes by SSL)
* Enabling **secure ports** in a ClickHouse configuration
* Defining a custom **podTemplate** with secure ports
* Defining a custom **serviceTemplate** if needed

The **podTemplate** is automated by the operator using a '**secure**' flag in the cluster definition rather than the user doing it.

The following example shows a typical secure configuration:

```yaml
spec:
  configuration:
    clusters:
    - name: default
      secure: "yes"
    settings:
      tcp_port: 9000 # keep for localhost
      tcp_port_secure: 9440
      https_port: 8443
    files:
      openssl.xml: |
        <clickhouse>
          <openSSL>
            <server>
              <certificateFile>/etc/clickhouse-server/config.d/server.crt</certificateFile>
              <privateKeyFile>/etc/clickhouse-server/config.d/server.key</privateKeyFile>
              <dhParamsFile>/etc/clickhouse-server/config.d/dhparam.pem</dhParamsFile>
              <verificationMode>none</verificationMode>
              <loadDefaultCAFile>true</loadDefaultCAFile>
              <cacheSessions>true</cacheSessions>
              <disableProtocols>sslv2,sslv3</disableProtocols>
              <preferServerCiphers>true</preferServerCiphers>
            </server>
          </openSSL>
        </clickhouse>
      config.d/server.crt: |
        ***

      config.d/server.key: |
        ***

      config.d/dhparam.pem: |
        ***

```

Certificate files can also be stored in secrets: 

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-certs
type: Opaque
stringData:
  server.crt: |
        ***

  server.key: |
        ***

  dhparam.pem: |
        ***
```

and referred as below:


```yaml
spec:
  configuration:
    files:
      openssl.xml: |
        <clickhouse>
          <openSSL>
            <server>
              <certificateFile>/etc/clickhouse-server/secrets.d/server.crt/clickhouse-certs/server.crt</certificateFile>
              <privateKeyFile>/etc/clickhouse-server/secrets.d/server.key/clickhouse-certs/server.key</privateKeyFile>
              <dhParamsFile>/etc/clickhouse-server/secrets.d/dhparam.pem/clickhouse-certs/dhparam.pem</dhParamsFile>
              <verificationMode>none</verificationMode>
              <loadDefaultCAFile>true</loadDefaultCAFile>
              <cacheSessions>true</cacheSessions>
              <disableProtocols>sslv2,sslv3</disableProtocols>
              <preferServerCiphers>true</preferServerCiphers>
            </server>
          </openSSL>
        </clickhouse>
      server.crt:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: server.crt
      server.key:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: server.key
      dhparam.pem:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: dhparam.pem

```

**NOTE**: secret files are mapped into `secrets.d` configuration folder using the following rule:
 `/etc/clickhouse-server/secrets.d/<config_file_name>/<secret_name>/<secret_key>`.

### Disabling insecure connections

The operator automatically adjusts services used to access individual pods when '**secure**' flag is used. Additionally, '**inscure: "no"**' flag can be added as of version 0.21.x in order to disable insecure ports:

```
spec:
  configuration:
    clusters:
    - name: default
      secure: "yes"
      insecure: "no"
```

That adjusts pod services properly, but it does not adjust load balancer service. In real environments, the load balancer is managed by a separate **serviceTemplate**. User must define the available ports explicitly.

The following example shows how to define ports for load balancer service:

```yaml
spec:
  templates:
    serviceTemplates:
      - generateName: clickhouse-{chi}
        metadata:
          annotations:
           # cloud specific annotations to configure load balancer
        name: default-service-template
        spec:
          ports:
            - name: https
              port: 8443
            - name: secureclient
              port: 9440
          type: LoadBalancer
```

### Using secure connections for distributed queries

If ClickHouse is configured using the '**secure**' flag as described above, secure connections for distributed installations are already enabled:

```yaml
spec:
  configuration:
    clusters:
      - name: "default"
        secure: "yes"
        secret:
          auto: "yes"
```

Under the hood operator changes **remote_servers** configuration automatically, providing a secure port and flag:

```xml
<shard>
   <internal_replication>False</internal_replication>
      <replica>
          <host>***</host>
          <port>9440</port>
          <secure>1</secure>
      </replica>
   </shard>
```

It may also require an openSSL client configuration:

```yaml
spec:
  configuration:
    files:
      openssl_client.xml: |
        <clickhouse>
          <openSSL>
            <client>
                <loadDefaultCAFile>true</loadDefaultCAFile>
                <cacheSessions>true</cacheSessions>
                <disableProtocols>sslv2,sslv3</disableProtocols>
                <preferServerCiphers>true</preferServerCiphers>
                <verificationMode>none</verificationMode>
                <invalidCertificateHandler>
                    <name>AcceptCertificateHandler</name>
                </invalidCertificateHandler>
            </client>
          </openSSL>
        </clickhouse>
```

**Note:** To secure connections for external users only, but keep inter-cluster communications insecure, instead of using the '**secure**' flag, specify the **podTemplate** explicitly and open the proper ports:

```yaml
spec:
  templates:
    podTemplates:
    - name: default
      containers:
      - name: clickhouse-pod
        image: altinity/clickhouse-server:24.3.12.76.altinitystable
        ports:
        - name: http
          containerPort: 8123
        - name: https
          containerPort: 8443
        - name: client
          containerPort: 9000
        - name: secureclient
          containerPort: 9440
        - name: interserver
          containerPort: 9009
```

### Forcing HTTPS for operator connections

To have ClickHouse use HTTPS, use following YAML example to set the operator configuration for the '**clickhouse_operator**' user. It switches all interactions with ClickHouse to HTTPS, including health checks.

```yaml
configuration:
  access:
    scheme: https
    port: 8443
```

### Forcing HTTPS for replication

To force ClickHouse replication to use HTTPS on a securerly configured `ClickHouseInstallation`, set the required ClickHouse ports as follows:

```yaml
spec:
  configuration:
    settings:
      interserver_http_port: _removed_
      interserver_https_port: 9009
```

### Forcing HTTPS for ZooKeeper

**TODO**:

- Coming soon

---

# Operator-side security toggles (added in 0.27.1)

> The sections above document ClickHouse-side hardening (users, passwords,
> secrets, network). The sections below document the per-component **operator-side**
> security toggles introduced in 0.27.1 — TLS verification on the operator's
> outbound clients (ClickHouse / ZooKeeper / Kubernetes API), the
> operator↔metrics-exporter IPC channel, and the transport-hardening master
> switch.
>
> FIPS-specific controls (the cryptographic-module gate, the workload-image
> policy gate, the ACVP responder, the FIPS-built image, and per-release
> evidence) are documented in
> [`security_hardening_fips.md`](./security_hardening_fips.md).

The toggles below are opt-in: with no configuration changes the operator behaves
exactly as in 0.26.x. Each toggle is independent — the operator does not infer
one knob from another. This lets users harden one surface (e.g. ClickHouse
client TLS) without disturbing others.

## What is configurable

| Surface | Knob | Default | Strict value |
|---|---|---|---|
| ClickHouse client TLS | `verify` | (legacy `InsecureSkipVerify=true`) | `Strict` |
| ClickHouse client TLS | `minVersion` | Go default (1.2) | `"1.3"` |
| ClickHouse client TLS | `serverName` | derived from dial host | explicit |
| ClickHouse client TLS | `rootCA` | from chopconf `access.rootCA` | (unchanged; per-cluster override available) |
| ZooKeeper / Keeper TLS | `verify` | strict (RootCAs+ServerName always set) | `Strict` |
| ZooKeeper / Keeper TLS | `minVersion` | Go default | `"1.3"` |
| Kubernetes client (chopconf only) | `tls.verify` | (unset; kubeconfig wins) | `Strict` |
| Kubernetes client (chopconf only) | `tls.minVersion` | (unset; declared but not yet wired to K8s transport) | `"1.3"` |
| Operator↔exporter IPC | `mode` | `Plain` | `Secure` |
| Operator↔exporter IPC | `bindHost` | (Plain: all interfaces) | `127.0.0.1` |

## Where toggles live

Toggles surface at two levels with 3-level inheritance (CHOP-config → CHI spec
→ cluster spec). Lower level wins; empty fields fall through.

### Operator-wide (chopconf — `ClickHouseOperatorConfiguration` CR)

```yaml
spec:
  security:
    clickhouse:
      tls:
        verify: Strict
        minVersion: "1.2"
    zookeeper:
      tls:
        minVersion: "1.2"
    kubernetes:
      tls:
        verify: Strict
    ipc:
      mode: Secure
    policy: Enforced     # TLS-hardening master switch (orthogonal to fips)
    fips:
      enforced: true     # cryptographic-module gate — Fatals if binary lacks GOFIPS140
    images:
      policy: FIPSRequired   # workload supply-chain gate — refuse non-FIPS CH/Keeper images
```

**Location note**: `security:` lives at the top level of the chopconf spec —
sibling of `spec.clickhouse`, not under it. The block covers more than ClickHouse
(ZooKeeper TLS, Kubernetes client posture, operator-internal IPC, operator-wide
FIPS), so nesting it under `clickhouse` would be misleading.

### Per-CHI (ClickHouseInstallation CR — `spec.configuration.clusters[].security`)

```yaml
spec:
  configuration:
    clusters:
      - name: prod
        security:
          clickhouse:
            tls:
              verify: Strict
              rootCA: |
                -----BEGIN CERTIFICATE-----
                ...
                -----END CERTIFICATE-----
```

The CHI block does NOT contain `ipc` or `kubernetes` — IPC is between the
operator and the metrics-exporter sidecar (both in the operator's own Pod),
and the Kubernetes API client is operator-process-scoped (one kubeconfig per
operator pod). Both are configurable in ClickHouseOperatorConfiguration only.

## How each toggle works

### `security.clickhouse.tls.verify`

Controls peer-certificate + hostname verification on outbound TLS connections
the operator makes to ClickHouse hosts (schema reconciliation, metrics fetch).

- `Strict` — verify the chain against `rootCA` (or system trust store if
  unset) and check ServerName against the cert SANs. Handshake fails if the
  cert is invalid.
- `None` — equivalent to today's behavior: `InsecureSkipVerify=true`. Skip
  chain and hostname validation. Useful for local development against
  self-signed certs without ship-able CA bundles.
- Unset (`""`) — preserve legacy behavior. Same as `None` today.

The default is intentionally `None` (via "unset") so existing CHIs reconcile
unchanged on upgrade.

### `security.clickhouse.tls.minVersion`

Floors the TLS protocol version negotiated with ClickHouse hosts. Accepts
`"1.2"` or `"1.3"`. Empty uses Go's default (`1.2` today).

If the ClickHouse server doesn't support the requested floor, the handshake
fails with a clear `protocol version not supported` error.

### `security.clickhouse.tls.serverName`

Overrides the SNI / hostname used during TLS verification. By default the
operator uses the dial host (per-host FQDN derived from the CHI's cluster
shape). Set this when your certs are issued for a single reference name that
doesn't match per-pod FQDNs.

### `security.clickhouse.tls.rootCA`

PEM-encoded CA bundle (or base64-wrapped PEM — auto-detected). Used to verify
the ClickHouse host's cert when `verify: Strict`. If empty, the operator falls
back to the chopconf `access.rootCA`, then to the system trust store.

**Back-compat note**: setting `rootCA` alone — without `verify: Strict` — preserves
pre-0.27.1 behavior (`InsecureSkipVerify=true`). The bundle is loaded but not
enforced. To actually verify the chain against your CA, set `verify: Strict`
explicitly.

### `security.zookeeper.tls.verify` / `.minVersion`

Same semantics as the ClickHouse TLS knobs but applied to the operator's
ZooKeeper / Keeper client (used during initial cluster setup).

**These knobs apply ONLY when a TLS-enabled ZK dial is in flight** — i.e. when
the underlying `zookeeper.ConnectionParams` has `CertFile` / `KeyFile` set.
With a plain-TCP ZK endpoint (the default in many development environments),
the knobs are inert: there is no TLS handshake to configure. Setting
`verify: Strict` on a plain-TCP ZK does NOT upgrade the dial to TLS; it is
silently a no-op. To enforce TLS-protected ZK end-to-end, also configure
client cert/key/CA via the chopconf-level ZK section.

When TLS IS active, these knobs add explicit `MinVersion` and an
`InsecureSkipVerify` opt-out on top of the existing ZK TLS pipeline (which
always supplies `RootCAs` and `ServerName`).

### `security.kubernetes.tls.verify` (chopconf-only)

Operator-process-scoped — there is no CHI/cluster-level override. Gates startup
against the loaded kubeconfig's `TLSClientConfig.Insecure` field.

- `Strict` — refuse to start if the kubeconfig has `Insecure=true`. Typical
  for production operators that must never honor an accidentally-insecure
  kubeconfig.
- `None` — explicit opt-in to permit an insecure kubeconfig (development).
- Unset — preserve current behavior (kubeconfig wins).

Unlike `clickhouse.tls.verify` and `zookeeper.tls.verify`, this knob is a
LOAD-TIME GATE: the operator does not build the kubeconfig's `tls.Config`
(client-go does), so `Strict` only refuses startup — it does not actively
override the kubeconfig at the wire.

The check runs once at startup, after the chopconf is loaded — if the gate
fires, the operator exits with a `Fatal` log line including the exact problem.

### `security.kubernetes.tls.minVersion` (chopconf-only)

Declared for shape uniformity with ClickHouse/Zookeeper and coerced under FIPS,
but not yet enforced on the operator's K8s API transport. A future enhancement
will wire it into `rest.Config` when the operator wraps the kubeconfig with
stricter TLS settings.

### `security.ipc.mode`

Hardens the operator↔metrics-exporter REST channel. The exporter runs as a
sidecar in the operator's Pod and exposes `/chi` on port 8888 for the operator
to push CR/host registration. Without hardening, this endpoint binds all
interfaces and accepts unauthenticated requests from any pod on the same node
network.

- `Plain` (default) — preserve today's behavior. `/chi` binds all interfaces,
  no auth.
- `Secure` — the `/chi` handler enforces two checks:
  1. The remote address of the request must be loopback (`127.0.0.1`).
  2. An `X-CHOP-Token` header must match the per-Pod token at
     `/etc/clickhouse-operator-ipc/token`.

The token is generated by the operator at startup (32 bytes from
`crypto/rand`, hex-encoded → 64 chars) and written to a shared
`emptyDir{medium:Memory}` volume mounted into both containers. Token lifetime
= Pod lifetime; no rotation is required.

**Note**: In Secure mode the `/metrics` Prometheus endpoint still binds all
interfaces — only the `/chi` handler is loopback-restricted. ServiceMonitor /
Prometheus scrapes are unaffected.

**Container UID note**: The token file is written with mode `0o400` (owner-only
read). Operator and metrics-exporter containers MUST share a UID — or be in a
shared group via Pod-level `securityContext.fsGroup` — for the exporter to read
the token. The default Deployment satisfies this (both containers run as the
same user); custom per-container `securityContext` overrides that diverge UIDs
will break Secure mode at exporter startup.

### Externally-managed token (advanced)

The default flow generates a fresh per-Pod random token. If you need the token
to come from a Kubernetes Secret instead — for GitOps reproducibility, external
rotation by Vault / cert-manager / sealed-secrets, or audit/compliance reasons —
**no operator change is required**. Override the `clickhouse-operator-ipc`
volume in the operator Deployment from `emptyDir` to a `secret` projection
mounted at the same path:

```yaml
volumes:
  - name: clickhouse-operator-ipc
    secret:
      secretName: my-ipc-token
      items:
        - key: token
          path: token
          mode: 0400
```

The operator's startup logic at `pkg/chop/ipc_token.go` reuses any non-empty
file already present at `tokenPath`, so a pre-populated Secret-backed file is
adopted as-is and no random token is generated. Both containers continue to
read the same file via the shared mount — no client/server code path changes.

**Trade-offs vs the default `emptyDir{medium: Memory}` flow:**

- The token is now persisted in `etcd`. Verify your cluster has
  [encryption-at-rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
  enabled before relying on this; without it the token leaks into etcd backups.
- Rotation is your responsibility. Both containers cache the token in process
  memory after startup, so Secret edits do NOT propagate live — you must
  restart the operator Pod to pick up a rotated value.
- Entropy is your responsibility. The default flow guarantees 32 bytes from
  `crypto/rand`; a hand-rolled Secret could weaken this if it contains a short
  or predictable value. Recommend ≥32 random bytes hex-encoded.

This is intentionally a Deployment-level escape hatch, not a CRD field — the
IPC token is operator-internal trust material and users who don't need this
should keep the default `emptyDir` flow.

## Setup

The toggles below are pure CR fields. No new images, no new RBAC.

### Operator-wide via Helm

```yaml
# values.yaml
configs:
  files:
    config.yaml: |
      security:
        clickhouse:
          tls:
            verify: Strict
            minVersion: "1.3"
        ipc:
          mode: Secure
```

Then `helm upgrade clickhouse-operator altinity/clickhouse-operator -f values.yaml`.

### Operator-wide via ClickHouseOperatorConfiguration CR

```yaml
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseOperatorConfiguration
metadata:
  name: chop-config
spec:
  security:
    clickhouse:
      tls:
        verify: Strict
```

If `RestartOnOperatorConfigurationChange` is enabled (chopconf option), the
operator auto-restarts when this CR changes; otherwise restart the operator
pod manually for changes to apply.

### Per-CHI

```yaml
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: my-chi
spec:
  configuration:
    clusters:
      - name: c1
        security:
          clickhouse:
            tls:
              verify: Strict
              rootCA: ...
```

Apply with `kubectl apply -f chi.yaml`. The operator re-normalizes on next
reconcile.

## Debug

### Verify a toggle is loaded

The operator logs the resolved chopconf at startup (level INFO):

```
$ kubectl logs -n kube-system deploy/clickhouse-operator -c clickhouse-operator | grep -i security
... Config parsed ...
security:
  clickhouse:
    tls:
      verify: "Strict"
      minVersion: "1.3"
  ipc:
    mode: "Secure"
```

### Check a CHI's resolved Security after inheritance

The 3-level merged values aren't directly exposed in CHI status. To inspect:

```bash
kubectl get chi <name> -o yaml | yq '.spec.configuration.clusters[].security'
```

That shows ONLY what's set at the cluster level. To see the effective merged
value, watch the operator logs during reconcile — TLS-setup paths emit:

```
TLS setup OK - root Cert registered (verify=Strict minVersion=1.3)
```

### TLS verify rejecting connections

If `verify: Strict` is set and connections start failing, check:

1. **Cert chain** — does your `rootCA` actually sign the ClickHouse server cert?
   ```bash
   openssl verify -CAfile rootca.pem clickhouse-server.crt
   ```
2. **SAN / hostname mismatch** — the cert's SANs must include the dial host
   the operator uses. Find the dial host in operator logs:
   ```
   Establishing connection: http://chi-<name>-<cluster>-<shard>-<replica>...
   ```
   Then inspect the cert:
   ```bash
   openssl s_client -connect <host>:8443 </dev/null 2>/dev/null \
     | openssl x509 -noout -text | grep -A1 'Subject Alternative Name'
   ```
   If the SANs don't match, either re-issue the cert with proper SANs (cleanest)
   or set `serverName:` to one of the existing SANs (workaround).
3. **MinVersion mismatch** — if the ClickHouse server is old and only supports
   1.2 but you set `minVersion: "1.3"`, handshakes will fail.

### IPC Secure mode debugging

If `ipc.mode: Secure` is enabled and the operator's CR registrations fail:

1. **Token presence on the exporter side**:
   ```bash
   kubectl exec -n kube-system deploy/clickhouse-operator -c metrics-exporter \
     -- sh -c '[ -r /etc/clickhouse-operator-ipc/token ] && echo readable'
   ```
   Should print `readable`. (The operator/exporter images are distroless and
   ship only `bash`/`sh`/`curl`; there is no `ls` or `stat` available.)

2. **Token presence on the operator side** (same path, same file — it's a
   shared volume):
   ```bash
   kubectl exec -n kube-system deploy/clickhouse-operator -c clickhouse-operator \
     -- sh -c '[ -r /etc/clickhouse-operator-ipc/token ] && echo readable'
   ```

3. **Reject reason in exporter logs**:
   ```bash
   kubectl logs -n kube-system deploy/clickhouse-operator -c metrics-exporter \
     | grep "IPC:"
   ```
   - `rejected non-loopback request from <addr>` — the operator is dialing
     non-loopback (shouldn't happen in normal operation — file a bug).
   - `rejected request from <addr> missing or invalid X-CHOP-Token header` —
     the operator's read of the token failed or the file content drifted.

4. **Operator side errors**:
   ```bash
   kubectl logs ... -c clickhouse-operator | grep "IPC Secure mode but token unreadable"
   ```

### Kubernetes tls.verify=Strict firing

The check runs once at startup. If you set it and the operator crash-loops
with the message:
```
kubeconfig declares TLSClientConfig.Insecure=true but
security.kubernetes.tls.verify=Strict — refusing to start
```
…then your kubeconfig (either the file mounted into the operator or the
in-cluster auto-discovered one) has `insecure-skip-tls-verify: true`. Either:
- Fix the kubeconfig (issue a CA-signed API-server cert and provide its CA in
  the kubeconfig), or
- Set `kubernetes.tls.verify: None` if you explicitly accept the dev posture,
  or leave the field unset to preserve pre-0.27.1 behavior.

## Upgrade compatibility

All toggles default to nil / unset such that an upgrade from 0.26.x
preserves identical runtime behavior:

- ClickHouse client still runs with `InsecureSkipVerify=true` until you opt in.
- ZooKeeper / Keeper TLS unchanged.
- Kubernetes client honors kubeconfig as before (unset `tls.verify` ≡ permissive).
- Operator↔exporter IPC is plain HTTP on all interfaces.
- Prometheus scrape topology unaffected (`/metrics` stays open even in Secure IPC mode; only `/chi` is loopback-restricted).

No CRD field is required, no Helm value is required. Existing chopconfs and
CHIs validate and apply unchanged.

**Upgrade ordering**: the new CRDs (with `security:` schema blocks) must be
applied BEFORE any CR using the `security:` block. `helm upgrade` handles this
automatically (CRDs ship in the chart). Manual installs must apply
`deploy/operator/clickhouse-operator-install-bundle.yaml` (which includes the
CRDs) first, then the CR. Applying a `security:`-bearing CR against a
pre-0.27.1 CRD returns `BadRequest: unknown field "spec.security"`.

**Typo handling**: the security schema blocks use
`x-kubernetes-preserve-unknown-fields: true`, so `kubectl apply` accepts unknown
field names without error. A typo like `tls.verifi: Strict` (missing 'y') is
silently swallowed and the operator falls through to default behavior. To
confirm a setting took effect, grep the operator logs for the relevant marker —
e.g. `TLS setup OK - root Cert registered (verify=Strict ...)` for TLS verify,
or `IPC: Secure mode — provisioned token` for IPC mode.


## Hardening axes overview

The operator's hardening posture splits across three orthogonal chopconf
knobs. Only the transport-hardening axis is documented in detail below; the
two FIPS axes live in [`security_hardening_fips.md`](./security_hardening_fips.md).

| Axis | Knob | Concern | Detailed docs |
|---|---|---|---|
| Transport hardening | `security.policy` | TLS verification + IPC + scheme coercion across the operator's outbound clients (CH / ZK / K8s) | below |
| Cryptographic-module gate | `security.fips.enforced` | Runtime assertion that the operator binary links the Go FIPS 140-3 module (`GOFIPS140=v1.0.0`) and is running under `GODEBUG=fips140=only` / `fips140=on` | [`security_hardening_fips.md`](./security_hardening_fips.md) |
| Workload supply-chain gate | `security.images.policy` | Admission + post-Ready check that every CH/Keeper container image carries `fips` in its tag and reports `fips` in `SELECT version()` | [`security_hardening_fips.md`](./security_hardening_fips.md) |

Each axis is opt-in and orthogonal — a deployment may enable any one, any two,
or all three. The 2×2 over the two operator-runtime axes (workload axis
flipped on/off independently):

| `security.policy` | `security.fips.enforced` | Operator posture |
|---|---|---|
| `Permissive` (default) | `false` (default) | Pre-0.27.1 behavior. No coercion, no FIPS gate, no image gate. |
| `Enforced` | `false` | TLS-only hardening (described below). |
| `Permissive` | `true` | FIPS module gate — see [`security_hardening_fips.md`](./security_hardening_fips.md). |
| `Enforced` | `true` | Full operator-side FIPS posture — see [`security_hardening_fips.md`](./security_hardening_fips.md). |

### `security.policy: Enforced` — TLS-hardening master switch

`security.policy` (default `Permissive`) is the master switch for the
operator's outbound TLS posture. When `Enforced` at startup, the operator:

1. **Coerces every per-component toggle to its Strict position** — logged at INFO
   per-field:
   - `security.clickhouse.tls.verify` → `Strict`
   - `security.clickhouse.tls.minVersion` → `1.3`
   - `security.zookeeper.tls.verify` → `Strict`
   - `security.zookeeper.tls.minVersion` → `1.3`
   - `security.kubernetes.tls.verify` → `Strict`
   - `security.kubernetes.tls.minVersion` → `1.3`
   - `security.ipc.mode` → `Secure`

   User-set values in any of these fields are overridden (one-way tightening).

2. **Re-registers the legacy ClickHouse TLS config to verifying mode**
   (`InsecureSkipVerify=false`), so DSNs that didn't go through the per-CHI
   security pipeline still get a verified handshake.

3. **Coerces `clickhouse.access.scheme: http` → `https`** so a hardened
   deployment cannot silently dial unencrypted ClickHouse. `auto` and `https`
   pass through unchanged.

4. **Rejects CHIs that reference plain-text external ZooKeeper.** Each
   `spec.configuration.zookeeper.nodes[].secure: true` is required; any node
   missing `secure: true` causes the CHI to land in `status: Aborted` with the
   bracketed reason `[FIPSValidationFailed]` in the error stream. The
   `secure: true` field is the enforced proxy for "FIPS-compatible ClickHouse
   Keeper over TLS" — plain-text ZK is not permitted under transport-hardened
   mode.

5. **Rejects ZK `digest:` auth files** — the vendored go-zookeeper digest
   scheme hashes user:password pairs with SHA-1 (see "ZooKeeper digest-auth
   policy" below).

`security.policy: Enforced` no longer Fatals on a non-FIPS-built binary. It
governs transport hardening only — the cryptographic-module assertion is a
separate axis (`security.fips.enforced`), documented in
[`security_hardening_fips.md`](./security_hardening_fips.md).

## Related

- FIPS-specific controls (cryptographic-module gate, image policy, ACVP, FIPS build, release evidence): [`security_hardening_fips.md`](./security_hardening_fips.md)
- Per-release verification recipes (digest / SBOM / cosign): [`fips_evidence_verification.md`](./fips_evidence_verification.md)
- Concrete YAML examples: `docs/chi-examples/24-security-*.yaml`
- Operator-config surface: `docs/chi-examples/70-chop-config.yaml`

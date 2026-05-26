# Security hardening

This document describes the per-component security toggles introduced in 0.27.1+.
The toggles are opt-in: with no configuration changes the operator behaves
exactly as in 0.26.x.

Each toggle is independent — the operator does not infer one knob from another.
This lets users harden one surface (e.g. ClickHouse client TLS) without
disturbing others.

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

## Orthogonal hardening axes

0.27.1 splits the operator's "hardening posture" into three orthogonal axes —
each is its own chopconf knob, each is enabled independently, and each gates
a distinct concern:

| Axis | Knob | Concern | Default |
|---|---|---|---|
| Transport hardening | `security.policy` | TLS verification + IPC + scheme coercion across the operator's outbound clients (CH / ZK / K8s) | `Permissive` |
| Cryptographic-module gate | `security.fips.enforced` | Runtime assertion that the operator binary links the Go FIPS 140-3 module (`GOFIPS140=v1.0.0`) and is running under `GODEBUG=fips140=only` (default) or `fips140=on` (escape hatch) | `false` |
| Workload supply-chain gate | `security.images.policy` | Admission + post-Ready check that every CH/Keeper container image carries `fips` in its tag and reports `fips` in `SELECT version()` | `Permissive` |

Each axis is opt-in and orthogonal — a deployment may enable any one, any two,
or all three. The most common postures, expressed as a 2×2 over the two
operator-runtime axes, with the workload axis flipped on or off independently:

| `security.policy` | `security.fips.enforced` | Operator posture |
|---|---|---|
| `Permissive` (default) | `false` (default) | Pre-0.27.1 behavior. No coercion, no FIPS gate, no image gate. |
| `Enforced` | `false` | TLS-only hardening. Operator coerces every TLS knob to Strict + IPC to Secure + rejects plain-text external ZK + refuses ZK `digest:` auth + coerces `clickhouse.access.scheme` http→https. No assertion that the binary is FIPS-linked. |
| `Permissive` | `true` | Pure FIPS module gate. Operator Fatals at startup if the binary is not `GOFIPS140`-built. ALSO triggers the same TLS coercions as Enforced (FIPS implies verified TLS) — see below. |
| `Enforced` | `true` | Full operator-side FIPS posture: TLS hardening + cryptographic-module gate. |

Set `security.images.policy: FIPSRequired` on top of any row above to add the
workload supply-chain gate (refusing non-FIPS-tagged ClickHouse/Keeper images).

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
separate axis (`security.fips.enforced`, below).

### `security.fips.enforced: true` — FIPS cryptographic-module gate

`security.fips.enforced` (default `false`) is the runtime assertion that the
operator binary was built with the Go FIPS 140-3 cryptographic module
(`GOFIPS140=v1.0.0`) AND is running with `GODEBUG=fips140=only` (shipped
default) or `fips140=on` (escape-hatch). The gate lives in `cmd/operator/app/fips_gate.go` (and the
mirror in `cmd/metrics_exporter/app/fips_gate.go`); both binaries enforce it
symmetrically because the metrics-exporter ships its own copy of the FIPS
module.

When `true` at startup:

- If `crypto/fips140` reports not-Enabled (binary built without `GOFIPS140`),
  the operator logs `Fatal` and exits. `security.policy: Enforced` alone does
  NOT fire this gate.
- Side-effect: triggers the same TLS coercions listed under
  `security.policy: Enforced` above, AND re-registers the legacy ClickHouse
  TLS config to verifying mode. Rationale: a FIPS-asserted operator
  necessarily wants verified TLS — there is no realistic posture in which the
  cryptographic-module gate is on while the operator dials with
  `InsecureSkipVerify=true`.

Setting `security.policy: Enforced` and `security.fips.enforced: true`
together is supported and idempotent: the TLS coercions fire once, the
FIPS-binary assertion fires once, and the operator logs both decisions.

**Spec-deviation note**: the internal Altinity FIPS scope specification
(§6 step 2) names this knob `operator.security.fips.enabled`. The operator
ships it as `security.fips.enforced` because the gate Fatals at startup on
mismatch — `enforced` more accurately describes the strict-failure semantics
than
`enabled` (which would suggest a soft toggle). The two names refer to the
same control surface; this rename is a wording deviation only, not a
behavioral one. Either-switch fan-out (TLS coercions firing when EITHER
`security.policy=Enforced` OR `security.fips.enforced=true`) is implemented
via the shared `OperatorConfigSecurity.RequiresHardening()` accessor so that
the narrower `fips.enforced=true` posture is never weaker than the broader
`policy=Enforced` posture at the per-CR gate level (plain-text ZK rejection,
ZK digest-auth rejection, `rejectFIPSBypass`).

### `security.images.policy: FIPSRequired` — workload supply-chain gate

Orthogonal to the two operator-runtime axes above. This knob does NOT
constrain the operator binary itself; it constrains the CH/Keeper container
images the operator deploys.

| `security.images.policy` | Effect |
|---|---|
| `Permissive` (default) | No image-tag gating. Any image accepted. |
| `FIPSRequired` | Reject CRs whose CH/Keeper images lack `fips` in the tag (admission-time, in normalize). After the pod is Ready, also reject CRs whose `SELECT version()` reply lacks `fips`. Rejection lands the CR in `status: Aborted` with the bracketed reason `[FIPSImagePolicyViolation]`. |

`FIPSRequired` is the current wire value; the older bare `Required` spelling
(pre-0.27.1 internal usage, never released) is still accepted by the
normalizer as a defensive alias but is not documented as a supported value.

### Detection signals

- **Admission (deploy-time)** — the operator's normalizer inspects each host's
  resolved PodTemplate `spec.containers[clickhouse].image` (or `keeper` for CHK)
  for the substring `fips` (case-insensitive) in the TAG portion of the
  reference. Registry-path substrings don't count. Hosts with no PodTemplate
  use the operator default `clickhouse/clickhouse-server:latest` (or the
  Keeper equivalent), which by construction fails Required.
- **Runtime (post-Ready)** — after the host responds to `SELECT version()`,
  the operator checks the reply for the same substring. Altinity FIPS builds
  bake the tag suffix (e.g. `.altinityfips`) into the binary's version
  string. Fails OPEN on transient SQL errors — a query hiccup against a
  running CR does NOT flip it to Aborted; the next reconcile re-evaluates.

### Failure reason

Reason tag `[FIPSImagePolicyViolation]` (distinct from
`[FIPSValidationFailed]`) is prepended to `status.errors`. Dashboards grep
either tag to distinguish operator-config bypass from per-CR image policy.

### Recovery from `[FIPSImagePolicyViolation]`

Edit the CHI/CHK to point at a FIPS-tagged image
(`altinity/clickhouse-server:<ver>.altinityfips`,
`altinity/clickhouse-keeper:<ver>.altinityfips`), then `kubectl apply`. The
informer's `UpdateFunc` re-enqueues; normalize re-runs cleanly.

Auto-recovery from pod-Ready transitions skips this reason (per
`shouldTriggerAutoRecovery`) — a pod flip can't fix a manifest that pins the
wrong image.

### Recovery from `[FIPSValidationFailed]`

The reason is encoded in `status.errors` (no first-class `reason` field; that
would be a CRD schema change). Operators and dashboards grep the error stream
for the bracketed tag:

```bash
kubectl get chi -o json | jq -r '.items[].status.errors[]? | select(startswith("[FIPSValidationFailed]"))'
```

Recovery is via spec edit: `kubectl apply` a corrected CHI (set `secure: true`
on every ZK node, or remove the `zookeeper:` block and use a CHK reference).
The informer's `UpdateFunc` re-enqueues the CR and normalize re-runs cleanly.
Note: this recovery path does NOT depend on `recovery.from.aborted.onPodReady`
— that path requires pod-readiness transitions, which never fire for CHIs
rejected at the normalizer (pods are never created).

### Out of scope for this knob

- **Forcing `/metrics` HTTPS** — ticket bullet deferred to a follow-up PR.
  Forcing HTTPS requires cert/key plumbing in the operator Deployment and a
  conditional ServiceMonitor scheme block in the Helm chart; both are non-
  trivial and break existing Prometheus scrape topology without warning.
- **OperatorHub `features.operators.openshift.io/fips-compliant: "true"`
  label** — Red Hat policy additionally requires a UBI-based image with
  signing/attestation; the label stays `"false"` until that work lands. The
  build itself is FIPS-enabled (see "FIPS build" below), but the label flip
  is gated separately.
- **ClickHouseKeeperInstallation (CHK) controller** — the security toggles
  documented above apply to CHK as well via the shared ClusterSecurity type
  (spec-level + cluster-level), with the same chopconf inheritance and FIPS
  bypass-reject semantics. Symmetric Keeper-side TLS additions are tracked
  separately.

## ACVP (Automated Cryptographic Validation Protocol)

### What ACVP is

[ACVP](https://pages.nist.gov/ACVP/) is NIST's machine-readable protocol for
streaming cryptographic test vectors at a module and comparing the responses
against expected outputs. It is the wire format that the
[CAVP](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program)
suite uses to drive algorithm testing, and CAVP results in turn feed the
[CMVP](https://csrc.nist.gov/Projects/cryptographic-module-validation-program)
certification process. The three are easy to confuse: CMVP certifies a module
end-to-end, CAVP validates individual algorithms inside that module, and ACVP
is the protocol the CAVP harness speaks to the implementation under test.

The Go cryptographic module `crypto/fips140` v1.0.0 that the operator links
against carries a CAVP algorithm-validation listing (A6650) and is on the
CMVP In Process list as of Go's published documentation — full CMVP
certification has not yet been issued. The operator therefore claims FIPS
140-3 *compatibility* (uses a module that has cleared CAVP and is awaiting
CMVP), not certification, and does not re-do upstream's algorithm-validation
work. See `## FIPS build` below for the authoritative module status text.

### Why an embedded wrapper

Downstream auditors typically ask "can I re-run the vectors against the exact
binary you shipped?" rather than "is your module certified?" — applications
that consume a validated module are not themselves separately validated, but
they still need to demonstrate that the binary in production exercises the
validated module's code paths and produces bit-identical outputs for a fixed
test vector set. The embedded ACVP wrapper exists to answer that question
reproducibly: it lets anyone with the source replay the same vectors against
the same binary and confirm the responder output matches a pinned reference.

This is *supplementary evidence* for downstream audits, not a substitute for
Go's upstream CMVP certification. The wrapper would surface a regression if a
build accidentally swapped in a non-validated primitive or if a future Go
toolchain upgrade altered the FIPS module's externally observable behaviour.

### How to invoke

The wrapper is gated behind a build tag so the default operator image does
NOT ship the responder. To build and run it:

```bash
# Build with the wrapper compiled in
go build -tags acvp_wrapper -o dev/bin/clickhouse-operator ./cmd/operator
ln -snf clickhouse-operator dev/bin/clickhouse-operator-acvp

# Drive the responder against BoringSSL's acvptool with pinned vectors
bash pkg/util/fips/acvp/run.sh
```

The same trampoline pattern lives in `cmd/metrics_exporter/`, producing
`metrics-exporter-acvp`. Each binary statically links its own copy of the Go
FIPS module, so both must be exercised to claim reproducibility for both
shipped images. The argv[0] dispatch (basename ending in `-acvp`) is the only
runtime trigger — running the operator binary normally has no ACVP code path.

### Scope — algorithms exercised

The wrapper exercises only public `crypto/...` APIs from the Go standard
library that map to FIPS-approved algorithms. The current vector set covers
roughly 38 algorithm suites: SHA2 family, SHA3 family, SHAKE, HMAC, HKDF,
PBKDF2, the DRBG random source, AES in CBC/CTR/GCM modes, CMAC, ECDSA,
EdDSA (Ed25519), RSA (PKCS#1 v1.5 and PSS), and the TLS 1.2/1.3 KDFs.

Two algorithms in the FIPS module are deliberately **excluded**:

- **ML-KEM (FIPS 203)** — the Kyber-derived post-quantum KEM. Its
  deterministic seed-based key generation entry point is internal to the Go
  FIPS module and not surfaced through any public `crypto/...` API.
- **ML-DSA (FIPS 204)** — the Dilithium-derived post-quantum signature
  scheme. Same constraint as ML-KEM.

A wrapper that drove these would have to import internal Go packages, which
is unsupported and would break across toolchain upgrades. The pragmatic
trade-off is to validate the broad classical-cryptography surface that the
operator actually uses (TLS handshakes, HMAC, certificate verification, AES)
and accept that the two post-quantum primitives carry only the upstream
CMVP/CAVP evidence.

### Security note — not a production binary

The responder exposes raw cryptographic primitives over stdin: callers
supply IVs directly, inject DRBG seed material, and observe per-primitive
outputs without higher-level authentication. This is by design for the test
harness, but it makes the `-acvp` binary unsuitable for production
deployment. Mitigations:

- The wrapper compiles only with the `acvp_wrapper` build tag. Default
  images do NOT carry it.
- A separate `image-acvp` Dockerfile stage is used for ACVP CI; that stage
  is never tagged as the default `:latest` or `:<version>` image.
- The argv[0] suffix check requires the binary to be invoked as
  `*-acvp` — a binary built with the tag still runs as the normal operator
  when launched as `clickhouse-operator`. This protects against accidental
  dispatch but is NOT a security boundary on its own; the build-tag gating
  is.

### Where outputs land

The [`acvp_test.yaml`](../.github/workflows/acvp_test.yaml) GitHub Actions
workflow runs the driver against both binaries on every push to `master`
and on PRs that touch the wrapper, the `cmd/` entrypoints, or the
dockerfiles. Each run uploads `acvp-evidence-<binary>-<sha>.tar.gz`
containing the BoringSSL `acvptool` run log, the pinned BoringSSL and
testdata commits, and the binary's `go version -m` output. This artifact is
the per-release reproducibility trail that the Release Gate item #12
(release-evidence archival) expects — see `pkg/util/fips/acvp/README.md`
for the wrapper's local-reproduction instructions and pinned upstream
commits.

## FIPS build

Operator and metrics-exporter binaries are built with `GOFIPS140=v1.0.0`,
linking the Go FIPS 140-3 cryptographic module (`crypto/fips140` v1.0.0).
**Module status**: as of Go's published documentation, v1.0.0 is **in CMVP
review** — it is **not** a completed CMVP-validated module. This operator
therefore claims FIPS 140-3 *compatibility*, not certification. See the
[Go FIPS 140-3 documentation](https://go.dev/doc/security/fips140) for the
authoritative module status.

This is not full product certification. The boundary is the operator and
metrics-exporter binaries only — it does not cover ClickHouse server,
ClickHouse Keeper, the Kubernetes API server, etcd, or any other component
the operator talks to.

Base image: `gcr.io/distroless/static-debian13` (distroless remains supported
under FIPS — the Go FIPS module is statically linked into the binary, no
glibc/OpenSSL dependency). Supported architectures: `linux/amd64`,
`linux/arm64` (see `dockerfile/operator/Dockerfile` `image-base-amd64` /
`image-base-arm64` stages).

### Runtime mode

The operator and metrics-exporter images ship with `GODEBUG=fips140=only` as the
default since 0.27.1. This is the strictest mode: any invocation of a non-FIPS
primitive panics at call time. TLS versions, cipher suites, signature
algorithms, key exchanges, and certificate chains are filtered to FIPS-approved
primitives. The operator's identifier-derivation code (object-version labels,
env-var name disambiguation) uses inline pure-Go bitwise implementations that do
not invoke `crypto/sha1` or `crypto/md5` and remain compatible — see
"Non-security hash exclusions" below.

| Mode | `GODEBUG` value | What it does | How to select |
|---|---|---|---|
| Strict (default) | `fips140=only` | Filters TLS as in `on` AND panics on any `crypto/...` call that touches a non-approved primitive | Shipped default in `altinity/clickhouse-operator:<version>` |
| Permissive (escape hatch) | `fips140=on` | Filters TLS versions, cipher suites, signature algorithms, key exchanges, FIPS-compatible certificate chains; allows non-FIPS calls outside the TLS layer | `-e GODEBUG=fips140=on` at container start, OR Pod `env:`, OR rebuild with `--build-arg GODEBUG_FIPS140=on` |
| Off (debug) | (unset / `fips140=off`) | Go runtime default; no FIPS gating | `-e GODEBUG=` |

If the shipped strict default surfaces a regression in a customer's
environment, the recommended escape hatch is `kubectl set env
deploy/clickhouse-operator -n <ns> GODEBUG=fips140=on --containers='*'` — the
operator pod rolls in ~30s and reconciliation resumes. Mirror the command for
the metrics-exporter deployment when applicable. The default image is the
only published image — there is no separate `:<version>`-suffixed FIPS build.

### Knobs

- Build-time: `GOFIPS140` in `dev/go_build_config.sh` (default `v1.0.0`).
  Pass `GOFIPS140=` (empty) to opt out for local non-FIPS builds.
- Build-time: `GODEBUG_FIPS140` in `dev/go_build_config.sh` (default `only`).
  Sets the runtime mode baked into the image via the Dockerfile `ARG`
  (`GODEBUG_FIPS140`). Override at build with `GODEBUG_FIPS140=on
  ./dev/image_build_all.sh` or `docker buildx build --build-arg
  GODEBUG_FIPS140=on …`.
- Runtime: `ENV GODEBUG=fips140=${GODEBUG_FIPS140}` in each Dockerfile's
  `image-prod` stage (resolves to `fips140=only` by default). Override at
  container-run time with `-e GODEBUG=fips140=on` for the permissive mode
  without rebuilding, or `-e GODEBUG=` to disable.
- `security.policy` chopconf knob: when `Enforced`, the operator coerces
  every per-component TLS toggle to Strict positions, rejects CHIs that
  cannot be served in a FIPS-compatible posture, and re-registers the
  ClickHouse legacy TLS config to verifying mode (no `InsecureSkipVerify`).
  Transport-hardening only — does NOT assert that the binary was built with
  `GOFIPS140`. For that, set the orthogonal `security.fips.enforced: true`.
- `security.fips.enforced` chopconf knob: when `true`, the operator Fatals at
  startup if the binary was not built with `GOFIPS140` and the runtime does
  not report `crypto/fips140` Enabled. Also triggers the same TLS coercions
  as `security.policy: Enforced` (a FIPS-asserted operator necessarily wants
  verified TLS). Independent of `security.policy`.

### Verify a built image

```bash
docker run --rm --entrypoint=/bin/sh altinity/clickhouse-operator:<tag> \
    -c 'echo $GODEBUG'   # expect: fips140=only
go version -m dev/bin/clickhouse-operator | grep GOFIPS140
```

The operator banner at startup also reports the module version:

```
FIPS: chopconf.policy=… build.enabled=… runtime.enforced=true module=v1.0.0
FIPS env: GODEBUG=fips140=only DefaultGODEBUG=fips140=only GOFIPS140=v1.0.0
```

The second `FIPS env:` line is new in 0.27.1 and is emitted by both the operator
and the metrics-exporter. It echoes the **raw** `GODEBUG` environment variable
as the process sees it, alongside the `DefaultGODEBUG` and `GOFIPS140` build
settings read from `runtime/debug.ReadBuildInfo`. This disambiguates two
postures that otherwise produce identical `crypto/fips140.Enabled=true` and
`runtime.enforced=false` interpretations in the first banner line:

- **Case 1** — binary built with `GOFIPS140=v1.0.0`, container started with
  `GODEBUG` unset → `FIPS env: GODEBUG= DefaultGODEBUG=fips140=only GOFIPS140=v1.0.0`.
  The runtime is in strict `only` mode courtesy of the baked-in `DefaultGODEBUG`.
- **Case 2** — binary built with `GOFIPS140=v1.0.0`, container started with
  `-e GODEBUG=fips140=on` → `FIPS env: GODEBUG=fips140=on DefaultGODEBUG=fips140=only GOFIPS140=v1.0.0`.
  The env override wins — the pod runs in permissive `on` mode.

Reading the raw `GODEBUG` value is the recommended first step when triaging
"is my pod actually in the FIPS mode I configured?" — a downstream override
(`-e GODEBUG=` to disable, `-e GODEBUG=fips140=on` to relax) shows up
verbatim in this line, whereas the first banner line only collapses the
posture into Enabled/Enforced booleans.

### E2E coverage

`tests/e2e/test_operator.py::test_010076` reads the operator startup banner
emitted by `cmd/operator/app/fips_gate.go` and fails the run if
`build.enabled` reports `false`. The shipped image asserts the build linkage
against the Go FIPS 140-3 module.

`tests/e2e/test_operator.py::test_010078` verifies the strict-FIPS master
switch cannot be subverted by a per-CHI override: it applies a CHI with
`security.clickhouse.tls.verify: None` while the chopconf has
`security.policy: Enforced`, and asserts the CHI lands in `status: Aborted`
with `[FIPSValidationFailed]` in `status.errors` — admission rejects the
weaker per-CHI knob rather than silently honoring it. A companion assertion
greps the `FIPS env:` banner line and confirms the recorded `GODEBUG` /
`DefaultGODEBUG` / `GOFIPS140` triple matches the deployed posture.

Local e2e (`tests/e2e/run_tests_local.sh`) rebuilds operator + metrics-
exporter via `dev/image_build_all_dev.sh`, which defaults `GOFIPS140=v1.0.0`
and runs the `image-prod` Dockerfile stage. The `image-debug` stage
(reachable only via `deploy/devspace/docker-build.sh --debug=delve`) does
NOT set `GODEBUG` so delve can single-step crypto paths; that path is not
reachable from `run_tests_*` and is excluded from CI.

### Non-security hash exclusions (scanner allow-list)

Per the FIPS scope document (§3 "Security-Sensitive Crypto Only"), the
following sites are explicitly **outside the FIPS cryptographic boundary**.
Both compute deterministic identifiers for K8s label uniqueness and shell
env-var name disambiguation; they are NOT signatures, NOT MACs, NOT key
derivation, NOT integrity proofs — just deterministic byte-mixing whose only
contract is collision rarity and stability across releases.

- `pkg/util/hash.go::HashIntoString` — produces the 40-char hex fingerprint
  used by `Fingerprint()` and the K8s
  `clickhouse.altinity.com/object-version` label value. The digest is
  computed by an **inline pure-Go bitwise implementation** of the algorithm
  specified by FIPS PUB 180-4 §6.1.2 / RFC 3174 — `crypto/sha1` is not
  imported. The standard is cited only as a reference for byte-output
  compatibility (existing labels stay byte-identical across the upgrade, no
  StatefulSet rollout), not as a claim of cryptographic protection.
- `pkg/util/shell.go::BuildShellEnvVarName` — appends a 32-char hex
  uniqueness suffix when a generated env-var name exceeds the base length
  budget. The suffix is computed by an **inline pure-Go bitwise
  implementation** of the algorithm specified by RFC 1321 — `crypto/md5` is
  not imported. As above, the RFC is cited only as a reference for
  byte-output compatibility (existing env-var names stay byte-identical), not
  as a claim of cryptographic protection.

Because neither call site references `crypto/sha1` or `crypto/md5`, the
operator binary runs cleanly under the strict `GODEBUG=fips140=only` runtime
mode — which is the shipped default since 0.27.1. Customers who need to
relax to the permissive `fips140=on` mode (e.g. for a downstream vendored
dependency that touches a non-FIPS primitive in a code path not yet inlined)
can set `-e GODEBUG=fips140=on` at container start or `kubectl set env
deploy/clickhouse-operator GODEBUG=fips140=on`. The byte-identical output
guarantee means changing the runtime mode never re-hashes a K8s object's
label or env-var name on upgrade. Scanner reports against these two files
are out of scope per the internal Altinity FIPS scope specification §3
"Security-Sensitive Crypto Only".

In addition, the following vendored telemetry libraries contain internal
non-security hashing / sampling that is **outside the FIPS cryptographic
boundary** per spec §4:

- `vendor/github.com/prometheus/client_golang/**` — Prometheus client
  internals (label-set cardinality hashing, histogram bucket selection).
- `vendor/go.opentelemetry.io/**` — OpenTelemetry SDK internals (trace
  sampling, span ID generation).

Scanner reports against these vendor paths are out of scope.

### Prerequisites for the deployment

Under both `fips140=only` (default) and `fips140=on` (escape-hatch) modes, the
runtime filters TLS chains for FIPS-approved primitives. The handshake fails
at use time, not at load time, so a non-FIPS chain may sit dormant until the
first dial.

- **Kubeconfig CA**: must be signed with SHA-256 or later. SHA-1- or
  MD5-signed CAs cause a TLS handshake failure the first time the operator
  dials the API server. Modern managed K8s (EKS, GKE, AKS, OpenShift ≥4) is
  fine; ad-hoc kind/k3s clusters with old certs may need rotation.
- **ClickHouse server certificates** (when `security.clickhouse.tls.rootCA`
  or `verify: Strict` is configured): same constraint.
- **ZooKeeper / Keeper certificates** (when ZK TLS is enabled): same.
- **The operator itself never generates or accepts SHA-1 in TLS**; the
  prerequisite is about the certificates you point it at.

- Code-side audit: `pkg/util/shell.go::BuildShellEnvVarName` and
  `pkg/util/hash.go::HashIntoString` derive deterministic identifiers using
  inline pure-Go bitwise implementations of the algorithms specified by
  RFC 1321 and FIPS PUB 180-4 §6.1.2 / RFC 3174 respectively. Neither
  imports `crypto/md5` or `crypto/sha1`. Documented as outside the FIPS
  cryptographic boundary per the internal Altinity FIPS scope specification
  §3; both the shipped `fips140=on` runtime and the strict opt-in
  `fips140=only` override permit these paths (see
  [Go FIPS 140-3 mode](https://go.dev/doc/security/fips140) for Go-side
  runtime semantics).

### ZooKeeper digest-auth policy

The ZooKeeper `digest` authentication scheme hashes user:password pairs with
SHA-1 inside the vendored `go-zookeeper` library. Under
`security.policy: Enforced` the operator **rejects** `digest:` auth files
(`pkg/model/zookeeper/connection.go::connectionAddAuth`) — the dial proceeds
without auth and the operator logs an error pointing operators at non-digest
schemes (`sasl`, `x509`). Deployments that must use ZooKeeper auth under FIPS
should switch to one of those schemes; deployments that don't need ZooKeeper
auth at all (the common case for in-cluster Keeper) are unaffected.

### Default (non-FIPS) HTTPS posture is intentionally back-compat

When `security.policy: Permissive` (the default), the legacy global TLS
config registered at `pkg/model/clickhouse/connection.go::setupTLSBasic`
keeps `InsecureSkipVerify=true` for ClickHouse DSNs that have no explicit
security knobs set. This preserves pre-0.27.1 behavior for upgrades.
`security.policy: Enforced` (or the orthogonal `security.fips.enforced: true`)
triggers `EnforceVerifiedLegacyTLS()` to re-register the same key with verifying
behavior. To enable verified TLS without flipping either master switch, set
`security.clickhouse.tls.verify: Strict` at the chopconf or per-CHI level —
`applyFIPSStrict` is not the only way to opt in.

### Release evidence — image digest, SBOM, build logs

Per the FIPS scope specification (Release Gate item #12, see the
operator-side boundary documented in this file together with
[Go FIPS 140-3 mode](https://go.dev/doc/security/fips140) for the Go-side
runtime semantics), a FIPS-tagged release must archive "image digest, SBOM,
build logs, and test report". The release pipeline now produces this
evidence automatically; the subsections below describe what ships, how to
verify it, and what is still planned.

#### What ships per release (automated)

Every tag-push build of `build_branch.yaml` uploads a
`release-evidence-<version>` GitHub Actions artifact containing, for each
of `clickhouse-operator` and `metrics-exporter`:

- `<bin>__<version>.digest.txt` — sha256 manifest-list digest.
- `<bin>__<version>.sbom.spdx.json` — syft SPDX-JSON SBOM of the image.
- `<bin>__<version>.manifest.json` — multi-arch image manifest.
- `<bin>-build-metadata.json` — the `buildx --metadata-file` output,
  including SBOM digests, provenance hashes, and per-platform image IDs.

In addition to the side-channel files, an inline SBOM and a SLSA provenance
attestation are attached to the image manifest itself via
`docker buildx --sbom=true --provenance=mode=max`, so anyone pulling the
image can inspect them directly without the GitHub Actions artifact. The
same trio of files is also uploaded to the matching GitHub Release page
via `gh release upload`, so customers without access to the Actions tab
can still fetch evidence by release tag.

#### How to verify

Pull the live manifest digest and compare it to the archived value:

```bash
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{.Manifest.Digest}}'
# should equal the contents of clickhouse-operator__0.27.1.digest.txt
```

Diff a freshly generated SBOM against the shipped one:

```bash
syft altinity/clickhouse-operator:0.27.1 -o spdx-json > /tmp/now.json
diff <(jq -S . /tmp/now.json) \
     <(jq -S . release-evidence/clickhouse-operator__0.27.1.sbom.spdx.json)
```

Inspect the in-image provenance attestation:

```bash
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{json .Provenance}}'
```

TestFlows e2e reports remain a separate stream: each `run_tests.yaml` run
keeps its `testflows.*.log`, operator pod logs, and event snapshot as the
ordinary workflow artifacts. They are not bundled into
`release-evidence-<version>` because they are produced by a different
workflow with a different cadence.

#### What is still pending

Cosign signing now ships as a separate workflow
(`.github/workflows/cosign_sign.yaml`) chained off `build_branch` via
`workflow_run`, signing the immutable `<image>@sha256:...` digest (not the
floating tag) via Sigstore keyless OIDC. Signatures are verified in the same
job and the resulting evidence is uploaded as `cosign-evidence-<branch>`
with 365-day retention.

One follow-up remains:

- **Reproducible builds**: `dev/go_build_universal.sh` does not currently
  pass `-trimpath` or `-buildvcs=true`, and bit-identical multi-arch
  builds are not enforced as a release-gate. The release-evidence
  pipeline pins the resulting image by digest, which preserves
  identity verifiability without requiring bit-reproducibility.

#### Retention

GitHub Actions retains tag-build evidence artifacts for 365 days and
master-push evidence for 30 days. Customers whose compliance window is
longer should pull the artifacts soon after release and mirror them into
their own evidence store; the GitHub Release attachments share the
retention of the release itself and serve as the longer-lived copy.

#### PR-time validation

`.github/workflows/release_evidence_smoketest.yaml` runs on every pull
request that touches an evidence-relevant input (Dockerfiles, build
scripts under `dev/`, and the release orchestrator). It exercises the
digest, SBOM, manifest, and metadata steps end-to-end so the pipeline
cannot silently regress between releases.

## Related

- See `docs/chi-examples/24-security-*.yaml` for concrete YAML examples.
- See `docs/chi-examples/70-chop-config.yaml` for the chopconf surface.

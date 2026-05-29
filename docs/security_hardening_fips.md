# FIPS 140-3 hardening

This document covers FIPS-specific controls in the Altinity ClickHouse
operator: the cryptographic-module gate (`security.fips.enforced`), the
workload supply-chain gate (`security.images.policy`), the embedded ACVP
responder, the FIPS-built image, and the per-release evidence pipeline.

General operator security knobs (TLS verify, MinVersion, IPC mode, the
`security.policy` master switch, the per-CHI inheritance surface) live in
[`security_hardening.md`](./security_hardening.md). See also
[`fips_evidence_verification.md`](./fips_evidence_verification.md) for digest /
SBOM / cosign verification recipes.

## How FIPS fits the three-axis hardening model

The operator's "hardening posture" splits across three orthogonal chopconf
knobs. Two of them are FIPS-specific and are documented in this file; the
third (`security.policy`) is a transport-hardening master switch covered in
the general doc.

| Axis | Knob | Concern | Default |
|---|---|---|---|
| Transport hardening | `security.policy` | TLS verification + IPC + scheme coercion across the operator's outbound clients (CH / ZK / K8s) — **see `security_hardening.md`** | `Permissive` |
| Cryptographic-module gate | `security.fips.enforced` | Runtime assertion that the operator binary links the Go FIPS 140-3 module (`GOFIPS140=v1.0.0`) and is running under `GODEBUG=fips140=only` (default) or `fips140=on` (escape hatch) | `false` |
| Workload supply-chain gate | `security.images.policy` | Admission + post-Ready check that every CH/Keeper container image carries `fips` in its tag and reports `fips` in `SELECT version()` | `Permissive` |

Each axis is opt-in and orthogonal — a deployment may enable any one, any two,
or all three. The 2×2 over the two operator-runtime axes (workload axis
flipped on/off independently):

| `security.policy` | `security.fips.enforced` | Operator posture |
|---|---|---|
| `Permissive` (default) | `false` (default) | Pre-0.27.1 behavior. No coercion, no FIPS gate, no image gate. |
| `Enforced` | `false` | TLS-only hardening. Operator coerces every TLS knob to Strict + IPC to Secure + rejects plain-text external ZK + refuses ZK `digest:` auth + coerces `clickhouse.access.scheme` http→https. No assertion that the binary is FIPS-linked. |
| `Permissive` | `true` | Pure FIPS module gate. Operator Fatals at startup if the binary is not `GOFIPS140`-built. ALSO triggers the same TLS coercions as Enforced (FIPS implies verified TLS) — see below. |
| `Enforced` | `true` | Full operator-side FIPS posture: TLS hardening + cryptographic-module gate. |

Set `security.images.policy: FIPSRequired` on top of any row above to add the
workload supply-chain gate (refusing non-FIPS-tagged ClickHouse/Keeper images).

## `security.fips.enforced: true` — FIPS cryptographic-module gate

`security.fips.enforced` (default `false`) is the runtime assertion that the
operator binary was built with the Go FIPS 140-3 cryptographic module
(`GOFIPS140=v1.0.0`) AND is running with `GODEBUG=fips140=only` (shipped
default) or `fips140=on` (escape-hatch). The gate lives in
`cmd/operator/app/fips_gate.go` (and the mirror in
`cmd/metrics_exporter/app/fips_gate.go`); both binaries enforce it
symmetrically because the metrics-exporter ships its own copy of the FIPS
module.

When `true` at startup:

- If `crypto/fips140` reports not-Enabled (binary built without `GOFIPS140`),
  the operator logs `Fatal` and exits. `security.policy: Enforced` alone does
  NOT fire this gate.
- Side-effect: triggers the same TLS coercions listed under
  `security.policy: Enforced` (see `security_hardening.md`), AND re-registers
  the legacy ClickHouse TLS config to verifying mode. Rationale: a
  FIPS-asserted operator necessarily wants verified TLS — there is no
  realistic posture in which the cryptographic-module gate is on while the
  operator dials with `InsecureSkipVerify=true`.

Setting `security.policy: Enforced` and `security.fips.enforced: true`
together is supported and idempotent: the TLS coercions fire once, the
FIPS-binary assertion fires once, and the operator logs both decisions.

**Spec-deviation note**: the internal Altinity FIPS scope specification
(§6 step 2) names this knob `operator.security.fips.enabled`. The operator
ships it as `security.fips.enforced` because the gate Fatals at startup on
mismatch — `enforced` more accurately describes the strict-failure semantics
than `enabled` (which would suggest a soft toggle). The two names refer to
the same control surface; this rename is a wording deviation only, not a
behavioral one. Either-switch fan-out (TLS coercions firing when EITHER
`security.policy=Enforced` OR `security.fips.enforced=true`) is implemented
via the shared `OperatorConfigSecurity.RequiresHardening()` accessor so that
the narrower `fips.enforced=true` posture is never weaker than the broader
`policy=Enforced` posture at the per-CR gate level (plain-text ZK rejection,
ZK digest-auth rejection, `rejectFIPSBypass`).

## `security.images.policy: FIPSRequired` — workload supply-chain gate

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

- **Forcing `/metrics` HTTPS** — deferred follow-up. Forcing HTTPS requires
  cert/key plumbing in the operator Deployment and a conditional
  ServiceMonitor scheme block in the Helm chart; both are non-trivial and
  break existing Prometheus scrape topology without warning.
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

The driver lives at `pkg/util/fips/acvp/run.sh` and is reproduced locally
against both binaries (`clickhouse-operator` and `metrics-exporter`) for
each release. There is no CI workflow that uploads an evidence artifact
today; the driver fails non-zero on any vector mismatch, so a green local
run is itself the pass/fail manifest. The pinned BoringSSL and
`geomys/acvp-testdata` commits in `run.sh` keep the run bit-for-bit
reproducible across hosts. The captured run log + the binary's
`go version -m` output form the per-release reproducibility trail that
the release-gate evidence-archival requirement expects — see
`pkg/util/fips/acvp/README.md` for the wrapper's local-reproduction
instructions and pinned upstream commits.

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

The operator and metrics-exporter images ship with `GODEBUG=fips140=off` as the
shipped default. The Go FIPS 140-3 module is **linked** into the binary
(`GOFIPS140=v1.0.0` at build time) but **dormant** at runtime — standard stdlib
crypto is the active provider. This preserves pre-0.27.1 runtime behavior on
upgrade. Customers opt the module into active mode at runtime via Pod env
without needing to rebuild the image. The operator's identifier-derivation code
(object-version labels, env-var name disambiguation) uses inline pure-Go bitwise
implementations that do not invoke `crypto/sha1` or `crypto/md5`, so it runs
cleanly under any of the four modes including the strictest `only` — see
"Non-security hash exclusions" below.

The Go runtime parses the `fips140` key of `GODEBUG` to one of **four** values:

| Mode | `GODEBUG` value | What it does | How to select |
|---|---|---|---|
| **Off (default)** | `fips140=off` | FIPS module not active. Standard stdlib crypto, no filtering, no panics. | Shipped default — baked via `ARG GODEBUG_FIPS140=off` |
| **On** | `fips140=on` | Module active. TLS / cipher / signature / cert-chain selection filtered to FIPS-approved primitives. Non-approved direct calls outside TLS still callable. | `-e GODEBUG=fips140=on` at container start, OR Pod `env:`, OR `kubectl set env`, OR rebuild with `--build-arg GODEBUG_FIPS140=on` |
| **Only (strict)** | `fips140=only` | Same as `on` plus any direct invocation of a non-FIPS-approved primitive panics at call time. Per Go upstream: best-effort, "not intended for production." Useful for CI gating. | `-e GODEBUG=fips140=only` runtime override or build-arg override |
| **Debug** | `fips140=debug` | Same as `on` plus every call into the FIPS module is logged to stderr. Verbose; diagnostic only. | `-e GODEBUG=fips140=debug` runtime override |

If a customer needs strict-FIPS posture for an audit, the recommended path is
`kubectl set env deploy/clickhouse-operator -n <ns> GODEBUG=fips140=on
--containers='*'` (or `=only` for strict). The operator pod rolls in ~30s and
reconciliation resumes. Mirror the command for the metrics-exporter deployment
when applicable. The default image is the only published image — there is no
separate `:<version>`-suffixed FIPS build.

### Knobs

- Build-time: `GOFIPS140` in `dev/go_build_config.sh` (default `v1.0.0`).
  Pass `GOFIPS140=` (empty) to opt out for local non-FIPS builds. The shipped
  image is always GOFIPS140-linked; this knob is for dev convenience only.
- Build-time: `GODEBUG_FIPS140` in `dev/go_build_config.sh` (default `off`).
  Sets the runtime mode baked into the image via the Dockerfile `ARG`
  (`GODEBUG_FIPS140`). Accepted values: `off` (default), `on`, `only`, `debug`.
  Override at build with `GODEBUG_FIPS140=on ./dev/image_build_all.sh` or
  `docker buildx build --build-arg GODEBUG_FIPS140=on …`.
- Runtime: `ENV GODEBUG=fips140=${GODEBUG_FIPS140}` in each Dockerfile's
  `image-prod` stage (resolves to `fips140=off` by default). Override at
  container-run time with `-e GODEBUG=fips140=on` (permissive), `=only`
  (strict), `=debug` (logging), or `-e GODEBUG=` (disable entirely).
- `security.policy` chopconf knob: when `Enforced`, the operator coerces
  every per-component TLS toggle to Strict positions, rejects CHIs that
  cannot be served in a FIPS-compatible posture, and re-registers the
  ClickHouse legacy TLS config to verifying mode (no `InsecureSkipVerify`).
  Transport-hardening only — does NOT assert that the binary was built with
  `GOFIPS140`. For that, set the orthogonal `security.fips.enforced: true`.
- `security.fips.enforced` chopconf knob: when `true`, the operator Fatals at
  startup if the binary was not built with `GOFIPS140` and the runtime does
  not report `crypto/fips140` Enabled. With the shipped default
  `GODEBUG=fips140=off`, setting `security.fips.enforced: true` will Fatal
  the operator unless the customer also overrides `GODEBUG` to `on` or
  `only` at the container level. Also triggers the same TLS coercions as
  `security.policy: Enforced`. Independent of `security.policy`.

### Verify a built image

```bash
docker run --rm --entrypoint=/bin/sh altinity/clickhouse-operator:<tag> \
    -c 'echo $GODEBUG'   # expect: fips140=off  (shipped default)
go version -m dev/bin/clickhouse-operator | grep GOFIPS140
```

Or use the binary's self-report (no Go toolchain required):

```bash
docker run --rm altinity/clickhouse-operator:<tag> -fips-info
```

The operator banner at startup also reports the module version:

```
FIPS: chopconf.policy=… build.enabled=true runtime.enforced=false module=v1.0.0
FIPS env: GODEBUG=fips140=off DefaultGODEBUG=fips140=off GOFIPS140=v1.0.0
```

Note that `build.enabled=true` (GOFIPS140 was linked at build time) but
`runtime.enforced=false` (the module is dormant because GODEBUG=fips140=off).
This is the shipped default. Customer overrides flip `runtime.enforced` to
`true` when GODEBUG is set to `on` or `only`.

The second `FIPS env:` line is new in 0.27.1 and is emitted by both the operator
and the metrics-exporter. It echoes the **raw** `GODEBUG` environment variable
as the process sees it, alongside the `DefaultGODEBUG` and `GOFIPS140` build
settings read from `runtime/debug.ReadBuildInfo`. This disambiguates the
shipped-default posture from customer overrides:

- **Case 1 (shipped default)** — binary built with `GOFIPS140=v1.0.0`,
  container started without env override → `FIPS env: GODEBUG= DefaultGODEBUG=fips140=off GOFIPS140=v1.0.0`.
  Module is linked but dormant.
- **Case 2 (customer opt-in to permissive)** — same binary, container started with
  `-e GODEBUG=fips140=on` → `FIPS env: GODEBUG=fips140=on DefaultGODEBUG=fips140=off GOFIPS140=v1.0.0`.
  The env override wins — the pod runs in permissive `on` mode.

Reading the raw `GODEBUG` value is the recommended first step when triaging
"is my pod actually in the FIPS mode I configured?" — a downstream override
(`-e GODEBUG=` to disable, `-e GODEBUG=fips140=on` to relax) shows up
verbatim in this line, whereas the first banner line only collapses the
posture into Enabled/Enforced booleans.

### E2E coverage

`tests/e2e/test_operator.py::test_010076` reads the operator startup banner
emitted by `cmd/operator/app/fips_gate.go` and fails the run if the
`FIPS env:` line reports an empty `GOFIPS140` build setting — that is the
unambiguous "GOFIPS140 build-arg was present at link time" signal,
independent of the runtime `GODEBUG=fips140=` mode (the shipped default is
`off`, so `build.enabled` now reads `false` even on a properly FIPS-built
binary; the `GOFIPS140` build setting is the stable linkage signal). The
shipped image asserts the build linkage against the Go FIPS 140-3 module
across both the operator and metrics-exporter containers.

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
operator binary runs cleanly under any of the four `GODEBUG=fips140` modes,
including the strictest `=only`. The shipped default is `=off` (FIPS module
dormant); customers opt the module into active mode at runtime via Pod env
(`=on` for permissive, `=only` for strict-panic, `=debug` for verbose
diagnostics) without rebuilding. The byte-identical output guarantee means
changing the runtime mode never re-hashes a K8s object's label or env-var name
on upgrade. Scanner reports against these two files are out of scope per the
internal Altinity FIPS scope specification §3 "Security-Sensitive Crypto
Only".

In addition, the following vendored telemetry libraries contain internal
non-security hashing / sampling that is **outside the FIPS cryptographic
boundary** per spec §4:

- `vendor/github.com/prometheus/client_golang/**` — Prometheus client
  internals (label-set cardinality hashing, histogram bucket selection).
- `vendor/go.opentelemetry.io/**` — OpenTelemetry SDK internals (trace
  sampling, span ID generation).

Scanner reports against these vendor paths are out of scope.

### Prerequisites for the deployment

When the FIPS module is **active** (`GODEBUG=fips140=on` or `=only` — opt-in;
not the shipped default), the runtime filters TLS chains for FIPS-approved
primitives. The handshake fails at use time, not at load time, so a non-FIPS
chain may sit dormant until the first dial. The prerequisites below only apply
to deployments that opt the module into active mode; the shipped default
(`fips140=off`) uses standard stdlib crypto and has no SHA-1-cert restriction.

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
  §3; all four `GODEBUG=fips140` modes (`off`, `on`, `only`, `debug`)
  permit these paths (see
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

Per the FIPS scope specification's release-gate "image digest, SBOM, build
logs, and test report archived" item (see the operator-side boundary
documented in this file together with
[Go FIPS 140-3 mode](https://go.dev/doc/security/fips140) for the Go-side
runtime semantics), a FIPS-tagged release must archive evidence for every
binary it ships. The release pipeline produces this evidence automatically;
the subsections below describe what ships, how to verify it, and what is
still planned.

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

The release-evidence pipeline is validated locally by running
`dev/release_evidence.sh` on a release-candidate tag; there is no CI
smoketest. Reviewers touching Dockerfiles, build scripts under `dev/`,
or the release orchestrator should run the script manually and confirm
the digest, SBOM, manifest, and metadata steps complete end-to-end.

## Related

- General security toggles (TLS verify / MinVersion / ServerName / RootCA, IPC
  mode, the `security.policy` master switch): [`security_hardening.md`](./security_hardening.md).
- Per-release verification recipes for digest / SBOM / cosign: [`fips_evidence_verification.md`](./fips_evidence_verification.md).
- Concrete YAML examples: `docs/chi-examples/24-security-*.yaml`,
  `docs/chi-examples/70-chop-config.yaml`.

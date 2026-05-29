# How to upgrade ClickHouse Operator

**Note:** Before you upgrade check releases notes if there are any backward incompatible changes between your version and the latest version.

**Supported upgrade path:** previous minor → current minor (e.g. 0.26 → 0.27). Skipping multiple minor versions is not validated in CI. To upgrade from a much older version, perform incremental minor-version upgrades. For installations running ClickHouse Keeper (CHK) on operator 0.23.x, see the manual PV-migration procedure in [keeper_migration_from_23_to_24.md](./keeper_migration_from_23_to_24.md) — direct 0.23 → current is no longer covered by CI.

**0.27.0 → 0.27.1 FIPS posture:** the default `altinity/clickhouse-operator` and `altinity/metrics-exporter` images are now FIPS 140-3 *compatible* (not certified). They are built with `GOFIPS140=v1.0.0` against Go's native FIPS 140-3 cryptographic module (currently in CMVP review) and ship with runtime `GODEBUG=fips140=off` — the FIPS module is linked into the binary but dormant by default, preserving pre-0.27.1 runtime behavior on upgrade. Customers opt the module into active mode at runtime via `kubectl set env deploy/clickhouse-operator -n <ns> GODEBUG=fips140=on --containers='*'` (permissive: TLS filtering, non-FIPS calls allowed) or `GODEBUG=fips140=only` (strict: non-FIPS calls panic). Mirror the command for the metrics-exporter deployment. The build-time default is governed by the new `GODEBUG_FIPS140` Docker `ARG` (accepted: `off`/`on`/`only`/`debug`; default `off`); rebuild with `--build-arg GODEBUG_FIPS140=on` (or `=only`) to bake an active-FIPS mode into a customer-specific image. The operator's identifier-derivation paths use inline pure-Go SHA-1/MD5 implementations (no `crypto/sha1` / `crypto/md5` imports) so they remain compatible under every mode including `only`. **Customer prerequisite (only when activating FIPS):** when `GODEBUG=fips140=on` or `=only` is set, the kubeconfig CA, ClickHouse server certificates, and ZooKeeper/Keeper certificates the operator dials must be SHA-256-or-later signed — SHA-1-signed certs will fail the very first TLS handshake. With the default `fips140=off`, this prerequisite does not apply. See [security_hardening_fips.md](./security_hardening_fips.md) for details.

**Release evidence (new for 0.27.1):** the `build_branch.yaml` CI workflow now publishes a `release-evidence-<version>` artifact alongside each tag-push image build, containing the image digest, syft SPDX SBOM, multi-arch manifest, and buildx metadata for both `clickhouse-operator` and `metrics-exporter`. Artifacts are also attached to the GitHub Release page. Inline SBOM and SLSA provenance attestations are embedded in the image manifest itself (via `docker buildx --sbom=true --provenance=mode=max`). Auditors verifying compliance with the FIPS 140-3 release gate can fetch these without rebuilding. See [`docs/fips_evidence_verification.md`](./fips_evidence_verification.md) for verification recipes.

**0.27.0 → 0.27.1 new chopconf knobs:** the `security.*` section gains per-component TLS toggles for the operator's own outbound connections — `security.clickhouse.tls.{verify,minVersion,serverName,rootCA,rootCASecretRef}` (ClickHouse), `security.zookeeper.tls.{verify,minVersion}` (ZooKeeper/Keeper), and `security.kubernetes.tls.{verify,minVersion}` (K8s API client — `verify` is a load-time gate, `minVersion` is CRD-only and not yet wired to the transport). It also adds `security.ipc.{mode,bindHost,tokenPath}` to harden the operator↔metrics-exporter REST channel, plus three orthogonal hardening axes: the TLS-hardening master switch `security.policy` (`Permissive` default, `Enforced` coerces all transport knobs to Strict and rejects FIPS-incompatible CR postures — TLS only, no longer Fatals on non-FIPS-built binaries), the FIPS cryptographic-module gate `security.fips.enforced` (`false` default, `true` Fatals at startup if the binary was not built with `GOFIPS140` and triggers the same TLS coercions as Enforced), and the workload supply-chain gate `security.images.policy` (`Permissive` default, `FIPSRequired` rejects CRs whose ClickHouse/Keeper images lack `fips` in their tag).

**0.27.0 → 0.27.1 FIPS-compatible non-security hash sites:** the two non-security hash sites used for deterministic-identifier derivation — `pkg/util/hash.go::HashIntoString` (the K8s `clickhouse.altinity.com/object-version` label fingerprint) and `pkg/util/shell.go::BuildShellEnvVarName` (the long-name uniqueness suffix for shell env-var names) — now compute their digests via inline pure-Go bitwise implementations rather than via `crypto/sha1` / `crypto/md5`. The byte-level outputs are unchanged (the inline code follows FIPS PUB 180-4 §6.1.2 / RFC 3174 and RFC 1321 respectively, cited only as reference standards for byte-output compatibility, not as cryptographic protection), so existing object-version labels and env-var names stay byte-identical across the upgrade. No StatefulSet rollout is triggered by this change. Because these inline impls do not invoke any forbidden primitive, the operator binary runs cleanly under every `GODEBUG=fips140` mode (the shipped default is `off`; customers can opt into `on` / `only` / `debug` at runtime via Pod env). The byte-identical guarantee means switching modes never re-hashes a K8s object's label or env-var name on upgrade. See [security_hardening_fips.md](./security_hardening_fips.md) § "Runtime mode" for the full mode table and § "Non-security hash exclusions" for the FIPS scope-spec authorization of non-security hashing outside the cryptographic boundary.

**0.27.0 → 0.27.1 strict-FIPS coercions:** when EITHER `security.policy: Enforced` OR `security.fips.enforced: true` is set, the operator additionally (a) coerces every per-component TLS knob to its Strict position at startup, (b) re-registers the legacy ClickHouse TLS config with `InsecureSkipVerify=false` so DSNs without explicit security knobs still get verified TLS, (c) coerces `clickhouse.access.scheme: http` → `https` and rejects plain HTTP, (d) rejects ZK `digest:` auth files (the vendored go-zookeeper digest hashes passwords with SHA-1), and (e) rejects CHIs/CHKs that reference plain-text external ZooKeeper (i.e. `zookeeper.nodes[*].secure` not set). `security.fips.enforced: true` additionally Fatals at startup if the binary was not built with `GOFIPS140` (the cryptographic-module assertion); `security.policy: Enforced` alone no longer fires that gate. Review your manifests before flipping either switch.

**0.27.0 → 0.27.1 FIPS startup banner enhancement:** both the operator and metrics-exporter now emit an additional `FIPS env:` log line at startup, immediately after the existing `FIPS:` banner. It echoes the raw `GODEBUG` environment variable, the `DefaultGODEBUG` build setting baked into the binary, and the `GOFIPS140` build setting — e.g. `FIPS env: GODEBUG=fips140=on DefaultGODEBUG=fips140=on GOFIPS140=v1.0.0`. This lets operators verify at a glance that the running container actually received the GODEBUG mode they configured (vs. silently falling back to the binary default) and is the recommended diagnostic to read before filing FIPS-posture questions. New e2e test `test_010078` additionally verifies that submitting `security.clickhouse.tls.verify: None` under `security.policy: Enforced` is rejected at admission with `[FIPSValidationFailed]` — the master switch cannot be subverted by a per-CHI knob. See [security_hardening.md](./security_hardening.md) for full banner-output semantics.

**ACVP responder** (optional, build-tagged): a NIST ACVP test responder can be embedded into operator and metrics-exporter binaries via the `acvp_wrapper` build tag. Default builds do NOT include the responder. See `docs/security_hardening_fips.md` § ACVP for the test-evidence pipeline.

**0.27.0 → 0.27.1 auto-recovery from Aborted:** the new `reconcile.recovery.from.aborted.onPodReady` knob re-enqueues a CHI when a pod transitions NotReady → Ready while the CR sits in `Aborted` status. Default is `retry` (auto-recover on pod-Ready transition); set to `none` to opt out and preserve the pre-0.27.1 manual-intervention behaviour.

**0.27.0 → 0.27.1 reconcile hooks (preview):** new `spec.reconcile.host.hooks` and `spec.reconcile.cluster.hooks` blocks accept `events:` + `sql:` / `http:` / `shell:` actions. Only `sql:` is wired end-to-end in 0.27.1; `http:` and `shell:` currently emit a "not yet implemented" Fatal at validation time, so defer adopting those action types until the corresponding runners ship.

**0.27.0 → 0.27.1 Keeper reference:** the new `spec.configuration.zookeeper.keeper` field on `ClickHouseInstallation` lets you reference a `ClickHouseKeeperInstallation` by name; the operator resolves it to the matching ZK endpoint list automatically and re-reconciles dependent CHIs when the referenced CHK's endpoint set changes.

**0.26.x → 0.27.0 CHK note:** existing ClickHouse Keeper pods will be rolled (sequentially, gated by the startup probe) due to two operator-rendered keeper-config additions: an explicit `<four_letter_word_white_list>` (so the new ruok-based liveness probe keeps working even when users add restrictive `keeper_server` overrides) and the migration of the liveness probe itself from `pgrep` to a `ruok`/`imok` 4LW handshake. No data migration is required.

**0.26.x → 0.27.0 metrics note:** the default chop config now filters per-CPU OS metrics (`metric.OSUserTimeCPU0..N`, `metric.CPUFrequencyMHz_*`, etc.) from the metrics-exporter output. Dashboards or alerts that referenced these series will go silent after upgrade. To restore the prior behaviour, set `excludeRegexp: []` in your `ClickHouseOperatorConfiguration`.

ClickHouse operator is deployed as Deployment Kubernetes resource (see: [Operator Installation Guide][operator_installation_details.md] for more details).
Supplied [clickhouse-operator-install-bundle.yaml][clickhouse-operator-install-bundle.yaml] contains the following deployment spec:
```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: clickhouse-operator
  namespace: kube-system
  labels:
    clickhouse.altinity.com/chop: 0.17.0
    app: clickhouse-operator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: clickhouse-operator
  template:
    metadata:
      labels:
        app: clickhouse-operator
      annotations:
        prometheus.io/port: '8888'
        prometheus.io/scrape: 'true'
        clickhouse-operator-metrics/port: '9999'
        clickhouse-operator-metrics/scrape: 'true'        
    spec:
      serviceAccountName: clickhouse-operator
      volumes:
        - name: etc-clickhouse-operator-folder
          configMap:
            name: etc-clickhouse-operator-files
        - name: etc-clickhouse-operator-confd-folder
          configMap:
            name: etc-clickhouse-operator-confd-files
        - name: etc-clickhouse-operator-configd-folder
          configMap:
            name: etc-clickhouse-operator-configd-files
        - name: etc-clickhouse-operator-templatesd-folder
          configMap:
            name: etc-clickhouse-operator-templatesd-files
        - name: etc-clickhouse-operator-usersd-folder
          configMap:
            name: etc-clickhouse-operator-usersd-files
      containers:
        - name: clickhouse-operator
          image: altinity/clickhouse-operator:0.17.2
          imagePullPolicy: Always
          volumeMounts:
            - name: etc-clickhouse-operator-folder
              mountPath: /etc/clickhouse-operator
            - name: etc-clickhouse-operator-confd-folder
              mountPath: /etc/clickhouse-operator/conf.d
            - name: etc-clickhouse-operator-configd-folder
              mountPath: /etc/clickhouse-operator/config.d
            - name: etc-clickhouse-operator-templatesd-folder
              mountPath: /etc/clickhouse-operator/templates.d
            - name: etc-clickhouse-operator-usersd-folder
              mountPath: /etc/clickhouse-operator/users.d
          env:
            # Pod-specific
            - name: OPERATOR_POD_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: OPERATOR_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPERATOR_POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: OPERATOR_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: OPERATOR_POD_SERVICE_ACCOUNT
              valueFrom:
                fieldRef:
                  fieldPath: spec.serviceAccountName

            # Container-specific
            - name: OPERATOR_CONTAINER_CPU_REQUEST
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: requests.cpu
            - name: OPERATOR_CONTAINER_CPU_LIMIT
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: limits.cpu
            - name: OPERATOR_CONTAINER_MEM_REQUEST
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: requests.memory
            - name: OPERATOR_CONTAINER_MEM_LIMIT
              valueFrom:
                resourceFieldRef:
                  containerName: clickhouse-operator
                  resource: limits.memory

        - name: metrics-exporter
          image: altinity/metrics-exporter:0.17.0
          imagePullPolicy: Always
          volumeMounts:
            - name: etc-clickhouse-operator-folder
              mountPath: /etc/clickhouse-operator
            - name: etc-clickhouse-operator-confd-folder
              mountPath: /etc/clickhouse-operator/conf.d
            - name: etc-clickhouse-operator-configd-folder
              mountPath: /etc/clickhouse-operator/config.d
            - name: etc-clickhouse-operator-templatesd-folder
              mountPath: /etc/clickhouse-operator/templates.d
            - name: etc-clickhouse-operator-usersd-folder
              mountPath: /etc/clickhouse-operator/users.d
          ports:
            - containerPort: 8888
              name: metrics
```

The latest available version is installed by default. If version changes, there are three ways to upgrade the operator:

* Delete existing deployment
```bash
kubectl delete deploy -n kube-system clickhouse-operator 
```
* Upgrade Custom Resource Definitions to latest version
```bash
kubectl apply -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/operator/parts/crd.yaml
```

Current deployments can be listed using the following command:
```
$ kubectl describe --all-namespaces deployment -l app=clickhouse-operator
Name:                   clickhouse-operator
Namespace:              kube-system
CreationTimestamp:      Sat, 01 Jun 2019 23:44:46 +0300
Labels:                 app=clickhouse-operator
                        version=0.13.0
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:0.17.0
   metrics-exporter:
    Image:        altinity/metrics-exporter:0.17.0
<...>
```

Version is labeled and can be also displayed with the command:
```
$ kubectl get deployment --all-namespaces -L clickhouse.altinity.com/chop -l app=clickhouse-operator 
NAMESPACE   NAME                  UP-TO-DATE   AVAILABLE   AGE       VERSION
kube-system clickhouse-operator   1            1           19h       0.17.0
```

If you want to update to the latest version, we can run following command:
  
```
$ kubectl apply -n kube-system -f https://github.com/Altinity/clickhouse-operator/raw/master/deploy/operator/clickhouse-operator-install-bundle.yaml

```
  
And then check upgrade status with:
```
$ kubectl get deployment --all-namespaces -L clickhouse.altinity.com/chop -l app=clickhouse-operator
NAMESPACE    NAME                  READY   UP-TO-DATE   AVAILABLE   AGE   VERSION
kube-system  clickhouse-operator   1/1     1            1           125d  0.18.2

$ kubectl describe --all-namespaces deployment -l app=clickhouse-operator
Name:                   clickhouse-operator
<...>
Pod Template:
  Labels:           app=clickhouse-operator
  Service Account:  clickhouse-operator
  Containers:
   clickhouse-operator:
    Image:        altinity/clickhouse-operator:0.18.2
   metrics-exporter:
    Image:        altinity/metrics-exporter:0.18.2
<...>
```

[operator_installation_details.md]: ./operator_installation_details.md
[clickhouse-operator-install-bundle.yaml]: ../deploy/operator/clickhouse-operator-install-bundle.yaml

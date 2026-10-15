# FIPS-compatible deployment example

This document presents a reference deployment of the ClickHouse Operator in a
FIPS-compatible posture: FIPS-built images, secure ports only, and verified TLS
between every component. It walks through the example manifests and highlights
the main FIPS-related option in each. The cryptographic-module gate, image
policy, and release evidence are covered in
[`security_hardening_fips.md`](./security_hardening_fips.md); the general TLS
knobs and certificate setup are covered in
[`security_hardening.md`](./security_hardening.md).

## Limitations

The FIPS boundary covers the operator and metrics-exporter binaries only, so a
few paths stay outside it. The operator-to-metrics-exporter IPC and the
Prometheus metrics endpoints (operator `:9999`, metrics-exporter
`:8888/metrics`) remain plain HTTP. TLS is server-authentication only, not
mutual TLS, because the operator has no client-certificate settings for its
health and version connections to ClickHouse.

## Shared prerequisites

Apply these before either ClickHouse cluster variant:

1. [`25-fips-01-chopconf.yaml`](chi-examples/25-fips-01-chopconf.yaml) — operator
   configuration.
2. The `clickhouse-certs` Secret — the shared TLS material; see
   [`security_hardening.md`](./security_hardening.md#certificates-and-ca-trust).
3. [`31-fips-secure-cluster.yaml`](chk-examples/31-fips-secure-cluster.yaml) —
   Keeper cluster.

## ClickHouse cluster variants

Two example manifests cover the same TLS-only cluster configuration. 

| Variant | Manifest(s) | When to use |
| ------- | ----------- | ----------- |
| **ClickHouse only** | [`25-fips-02-cluster.yaml`](chi-examples/25-fips-02-cluster.yaml) | Single self-contained CHI: FIPS image and pod template are inline; no sidecar. |
| **With backup sidecar** | [`25-fips-03-cluster-with-backup.yaml`](chi-examples/25-fips-03-cluster-with-backup.yaml) plus [`107-fips-backup-sidecar.yaml`](chit-examples/107-fips-backup-sidecar.yaml) | Reusable CHIT defines the full pod (FIPS ClickHouse + FIPS `clickhouse-backup`); CHI pulls it in via `useTemplates`. |


### ClickHouse only

Apply after the shared prerequisites:

```text
kubectl apply -f chi-examples/25-fips-02-cluster.yaml
```

The CHI sets `defaults.templates.podTemplate: fips` and defines pod template
`fips` with the FIPS-built `altinity/clickhouse-server` image.

### With backup sidecar

Apply the backup template before or alongside the cluster CHI:

```text
kubectl apply -f chit-examples/107-fips-backup-sidecar.yaml
kubectl apply -f chi-examples/25-fips-03-cluster-with-backup.yaml
```

The CHIT (`clickhouse-fips-backup-template`) supplies pod template `fips`
(clickhouse + clickhouse-backup containers, backup TLS env, and volumes). The
CHI references it through `useTemplates`. The `podTemplate` name in
`defaults.templates` must match `templates.podTemplates[].name` in the CHIT
(both are `fips`).

## Operator configuration

[`25-fips-01-chopconf.yaml`](chi-examples/25-fips-01-chopconf.yaml) turns on the
operator's FIPS posture. Setting `policy: Enforced` and `fips.enforced: true`
makes the operator coerce every outbound TLS connection to verified mode and
assert at startup that its binary is linked against the Go FIPS module.

```yaml
spec:
  security:
    policy: Enforced
    fips:
      enforced: "true"
```

## ClickHouse TLS configuration

Both cluster manifests run ClickHouse on secure ports only. Plain-text ports
are removed and replaced by TLS equivalents; the cluster trusts the shared CA
through `rootCASecretRef`. See
[`25-fips-02-cluster.yaml`](chi-examples/25-fips-02-cluster.yaml) and [`25-fips-03-cluster-with-backup.yaml`](chi-examples/25-fips-03-cluster-with-backup.yaml).

The CHI cluster uses `secure: "yes"` with `insecure: "no"`. These settings enable secure ClickHouse network interfaces
and service ports. In CHI, they do not automatically remove all plain-text ClickHouse interfaces; TLS-only deployments
should explicitly configure the desired secure ports and disable any unused plain-text ports.

```yaml
clusters:
  - name: default
    secure: "yes"
    insecure: "no"
```

```yaml
settings:
  http_port: _removed_
  tcp_port: _removed_
  interserver_http_port: _removed_
  mysql_port: _removed_
  postgresql_port: _removed_
  https_port: 8443
  tcp_port_secure: 9440
  interserver_https_port: 9010
```

The `openssl.xml` file allows TLS 1.2 and 1.3 on both the server and client
sides with FIPS-approved cipher suites. While external clients may connect using
either protocol version, the operator enforces TLS 1.3 for its outbound
connections, ensuring all operator-to-ClickHouse communication uses TLS 1.3
exclusively. The server presents its certificate without requesting one from
clients (`verificationMode: none`), while the client side validates the server
against the mounted CA in strict mode.

```xml
<client>
  <caConfig>/etc/clickhouse-server/secrets.d/ca.crt/clickhouse-certs/ca.crt</caConfig>
  <loadDefaultCAFile>false</loadDefaultCAFile>
  <verificationMode>strict</verificationMode>
  <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
  <cipherSuites>TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384</cipherSuites>
</client>
```

## Keeper cluster

[`31-fips-secure-cluster.yaml`](chk-examples/31-fips-secure-cluster.yaml) runs
ClickHouse Keeper on a FIPS-built image with TLS-only ports. Setting
`secure: "yes"` and `insecure: "no"` drops the plain-text client port 2181,
opens the secure client port 2281, and secures the Raft traffic between Keeper
nodes.

```yaml
clusters:
  - name: keeper
    secure: "yes"
    insecure: "no"
settings:
  keeper_server/raft_configuration/server/port: 9444
```

Keeper uses the same `openssl.xml` server and client blocks as ClickHouse and
pulls its image from `altinity/clickhouse-keeper:<ver>.altinityfips`.

## Backup sidecar

[`107-fips-backup-sidecar.yaml`](chit-examples/107-fips-backup-sidecar.yaml) is
the `ClickHouseInstallationTemplate` used by
[`25-fips-03-cluster-with-backup.yaml`](chi-examples/25-fips-03-cluster-with-backup.yaml).
It defines pod template `fips` with the FIPS-built ClickHouse and
`clickhouse-backup` images. The backup container serves its API over HTTPS and
connects to ClickHouse over the secure native port with CA verification enabled.

```yaml
- name: CLICKHOUSE_SECURE
  value: "true"
- name: CLICKHOUSE_TLS_CA
  value: /etc/clickhouse-backup/tls/ca.crt
- name: CLICKHOUSE_SKIP_VERIFY
  value: "false"
```

It mounts the same `clickhouse-certs` Secret used by the rest of the deployment.

This sidecar is self-managed: the CHI leaves `spec.backup` off, so the operator
neither changes it nor lets backup resources target the cluster. With
[operator-managed backup](backup.md) enabled, name this pod template in
`spec.backup.podTemplate`, as the CHI is aborted while a pod template it does
not name holds a `clickhouse-backup` container. The container then becomes the
base of the operator's sidecar, whose REST API the backup Jobs call over plain
HTTP. Drop `API_SECURE` with its certificate settings, the `command`/`args`,
the API port and any API or ClickHouse credentials, which the operator
provides; a value of `API_SECURE` that turns TLS on aborts the CHI. See
[Customizing the sidecar](backup.md#customizing-the-sidecar).

## Related

- FIPS controls, GODEBUG, image policy, and release evidence:
  [`security_hardening_fips.md`](./security_hardening_fips.md).
- TLS knobs, coercion, and certificate setup:
  [`security_hardening.md`](./security_hardening.md).
- Per-release verification recipes:
  [`fips_evidence_verification.md`](./fips_evidence_verification.md).

# QA-SRS ClickHouse Operator FIPS 140-3
# Software Requirements Specification

(c) 2026 Altinity Inc. All Rights Reserved.

**Document status:** Confidential

**Author:** Saba Momtselidze

**Date:** June 12, 2026

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Configuration Requirements](#configuration-requirements)
    * 2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.HTTPPorts](#rqsrs-026clickhouseoperatorfipshttpports)
* 3 [Build Verification](#build-verification)
    * 3.1 [RQ.SRS-026.ClickHouseOperator.FIPS.OperatorBuild.ShippedBinaries](#rqsrs-026clickhouseoperatorfipsoperatorbuildshippedbinaries)
    * 3.2 [RQ.SRS-026.ClickHouseOperator.FIPS.OperatorBuild.ShippedBinaries.StartupLogs](#rqsrs-026clickhouseoperatorfipsoperatorbuildshippedbinariesstartuplogs)
* 4 [FIPS 140-3 TLS Cipher Suites](#fips-140-3-tls-cipher-suites)
    * 4.1 [Approved TLS Cipher Suites](#approved-tls-cipher-suites)
        * 4.1.1 [RQ.SRS-026.ClickHouseOperator.FIPS.TLS.ApprovedCiphers](#rqsrs-026clickhouseoperatorfipstlsapprovedciphers)
    * 4.2 [Rejected Cipher Suites and Protocols](#rejected-cipher-suites-and-protocols)
        * 4.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.TLS.RejectedCiphers](#rqsrs-026clickhouseoperatorfipstlsrejectedciphers)
* 5 [ClickHouse Server](#clickhouse-server)
        * 5.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.CH.FIPSConfig](#rqsrs-026clickhouseoperatorfipschfipsconfig)
        * 5.2.2 [RQ.SRS-026.ClickHouseOperator.FIPS.CH.FIPSConfig.ExternalClient](#rqsrs-026clickhouseoperatorfipschfipsconfigexternalclient)
        * 5.2.3 [RQ.SRS-026.ClickHouseOperator.FIPS.CH.Rescale](#rqsrs-026clickhouseoperatorfipschrescale)
        * 5.2.4 [RQ.SRS-026.ClickHouseOperator.FIPS.CH.ConfigUpdate](#rqsrs-026clickhouseoperatorfipschconfigupdate)
* 6 [ClickHouse Keeper](#clickhouse-keeper)
        * 6.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.CHK.FIPSConfig](#rqsrs-026clickhouseoperatorfipschkfipsconfig)
        * 6.2.2 [RQ.SRS-026.ClickHouseOperator.FIPS.CHK.Rescale](#rqsrs-026clickhouseoperatorfipschkrescale)
        * 6.2.3 [RQ.SRS-026.ClickHouseOperator.FIPS.CHK.ConfigUpdate](#rqsrs-026clickhouseoperatorfipschkconfigupdate)
* 7 [ClickHouse Backup Sidecar](#clickhouse-backup-sidecar)
        * 7.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.Backup.FIPSBinary](#rqsrs-026clickhouseoperatorfipsbackupfipsbinary)
        * 7.2.2 [RQ.SRS-026.ClickHouseOperator.FIPS.Backup.FIPSConfig](#rqsrs-026clickhouseoperatorfipsbackupfipsconfig)
        * 7.2.3 [RQ.SRS-026.ClickHouseOperator.FIPS.Backup.RestoreRoundTrip](#rqsrs-026clickhouseoperatorfipsbackuprestoreroundtrip)
* 8 [FIPS Enforcement Mode](#fips-enforcement-mode)
    * 8.1 [Security Coercion](#security-coercion)
        * 8.1.1 [RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.SecurityCoercion](#rqsrs-026clickhouseoperatorfipsenforcedsecuritycoercion)
        * 8.1.2 [RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.RejectNonCompliantSpecs](#rqsrs-026clickhouseoperatorfipsenforcedrejectnoncompliantspecs)
        * 8.1.3 [RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.MinVersionScope](#rqsrs-026clickhouseoperatorfipsenforcedminversionscope)
    * 8.2 [Image Policy](#image-policy)
        * 8.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.Images.Required.RejectNonFIPS](#rqsrs-026clickhouseoperatorfipsimagesrequiredrejectnonfips)
* 9 [Runtime Connection Evidence](#runtime-connection-evidence)
    * 9.1 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.KubernetesAPI](#rqsrs-026clickhouseoperatorfipsconnectoperatorkubernetesapi)
    * 9.2 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Exporter.KubernetesAPI](#rqsrs-026clickhouseoperatorfipsconnectexporterkubernetesapi)
    * 9.3 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.ClickHouse](#rqsrs-026clickhouseoperatorfipsconnectoperatorclickhouse)
    * 9.4 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Exporter.ClickHouse](#rqsrs-026clickhouseoperatorfipsconnectexporterclickhouse)
    * 9.5 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.KeeperRestriction](#rqsrs-026clickhouseoperatorfipsconnectoperatorkeeperrestriction)
    * 9.6 [RQ.SRS-026.ClickHouseOperator.FIPS.Connect.ClickHouse.KeeperTLS](#rqsrs-026clickhouseoperatorfipsconnectclickhousekeepertls)
* 10 [Integrity Check](#integrity-check)
    * 10.1 [RQ.SRS-026.ClickHouseOperator.FIPS.Integrity.VerificationMismatch](#rqsrs-026clickhouseoperatorfipsintegrityverificationmismatch)
* 11 [CAST Failure](#cast-failure)
    * 11.1 [Operator CAST Failure](#operator-cast-failure)
        * 11.1.1 [RQ.SRS-026.ClickHouseOperator.FIPS.CAST.OperatorFail](#rqsrs-026clickhouseoperatorfipscastoperatorfail)
    * 11.2 [Exporter CAST Failure](#exporter-cast-failure)
        * 11.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.CAST.ExporterFail](#rqsrs-026clickhouseoperatorfipscastexporterfail)
* 12 [ACVP Algorithm Validation](#acvp-algorithm-validation)
    * 12.1 [Operator ACVP Validation](#operator-acvp-validation)
        * 12.1.1 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.WrapperIntegration](#rqsrs-026clickhouseoperatorfipsacvpoperatorwrapperintegration)
        * 12.1.2 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.ConfigGeneration](#rqsrs-026clickhouseoperatorfipsacvpoperatorconfiggeneration)
        * 12.1.3 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.SHA2256AFT](#rqsrs-026clickhouseoperatorfipsacvpoperatorsha2256aft)
    * 12.2 [Exporter ACVP Validation](#exporter-acvp-validation)
        * 12.2.1 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.WrapperIntegration](#rqsrs-026clickhouseoperatorfipsacvpexporterwrapperintegration)
        * 12.2.2 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.ConfigGeneration](#rqsrs-026clickhouseoperatorfipsacvpexporterconfiggeneration)
        * 12.2.3 [RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.SHA2256AFT](#rqsrs-026clickhouseoperatorfipsacvpexportersha2256aft)
* 13 [Terminology](#terminology)
    * 13.1 [SRS](#srs)
    * 13.2 [FIPS 140-3](#fips-140-3)
    * 13.3 [clickhouse-operator](#clickhouse-operator)
    * 13.4 [metrics-exporter](#metrics-exporter)
    * 13.5 [CHI](#chi)
    * 13.6 [CHK](#chk)
    * 13.7 [ACVP](#acvp)
    * 13.8 [CMVP](#cmvp)
    * 13.9 [CAVP](#cavp)

## Introduction

This specification describes FIPS 140-3 compatibility requirements for the
[clickhouse-operator] and [metrics-exporter] binaries built with Go FIPS support.

The goal is to verify that FIPS-enabled builds of the operator and metrics-exporter:
- Operate correctly under FIPS constraints
- Properly enforce cryptographic restrictions
- Use FIPS-compliant TLS for all outbound connections

Autotests that trace to these requirements live in
[`tests/e2e/test_operator.py`](../e2e/test_operator.py) and
[`tests/e2e/test_acvp.py`](../e2e/test_acvp.py).

**Boundary:** The operator and metrics-exporter run in the same pod. Internal IPC between
them is localhost HTTP and is not subject to FIPS TLS requirements. The Prometheus metrics
endpoints (operator `:9999` and metrics-exporter `:8888`) are also served over plain HTTP
and remain outside the FIPS TLS scope as a known gap. The ClickHouse Keeper readiness probe
endpoint (`:9182` `/ready`, which reflects Raft quorum status) likewise stays unconditionally
plaintext HTTP regardless of the secure/insecure knobs and is outside the FIPS TLS scope.

## Configuration Requirements

### RQ.SRS-026.ClickHouseOperator.FIPS.HTTPPorts
version: 1.0

All external connections SHALL require TLS with FIPS-compliant settings, except for localhost IPC between the operator
and metrics-exporter and the Prometheus metrics endpoints `:9999` and `:8888`.

## Build Verification

### RQ.SRS-026.ClickHouseOperator.FIPS.OperatorBuild.ShippedBinaries
version: 1.0

Each shipped pod binary — `clickhouse-operator` and `metrics-exporter` — SHALL satisfy all of the following:

* Both binaries SHALL be built with `GOFIPS140=v1.0.0` (or `certified`); `go version -m` on each binary SHALL show the `GOFIPS140` build setting when the binary is inspectable.
* Each binary SHALL identify itself as a FIPS build via `--fips-info` and startup logs containing a FIPS indicator.
* Each binary SHALL report `crypto/fips140.Version()` equal to `v1.0.0` (for example via `--fips-info` or in-process inspection).
* Each binary SHALL report `crypto/fips140.Enabled()` equal to `true` when FIPS mode is active per `GODEBUG=fips140`.

Examples:
* `go version -m clickhouse-operator` contains `GOFIPS140=v1.0.0`
* `go version -m metrics-exporter` contains `GOFIPS140=v1.0.0`
* `clickhouse-operator --fips-info` reports:

  ```yaml
  fips_module:
    version: v1.0.0
    enabled: true
  ```

* `metrics-exporter --fips-info` reports:

  ```yaml
  fips_module:
    version: v1.0.0
    enabled: true
  ```

### RQ.SRS-026.ClickHouseOperator.FIPS.OperatorBuild.ShippedBinaries.StartupLogs
version: 1.0

At startup, each binary SHALL emit a FIPS startup log line indicating build and runtime FIPS state.

When `GODEBUG=fips140=only`:

```text
FIPS: chopconf.fips.enforced=true build.linked=true module.active=true runtime.enforced=true module=v1.0.0
```

## FIPS 140-3 TLS Cipher Suites

### Approved TLS Cipher Suites

#### RQ.SRS-026.ClickHouseOperator.FIPS.TLS.ApprovedCiphers
version: 1.0

TLS-enforced external connections for [clickhouse-operator] and [metrics-exporter]
SHALL negotiate only TLS 1.3 with the following approved cipher suites.

* TLS_AES_128_GCM_SHA256
* TLS_AES_256_GCM_SHA384

Note: `TLS_CHACHA20_POLY1305_SHA256` is TLS v1.3 but not FIPS approved.

### Rejected Cipher Suites and Protocols

#### RQ.SRS-026.ClickHouseOperator.FIPS.TLS.RejectedCiphers
version: 1.0

On TLS-enforced external connections for [clickhouse-operator] and [metrics-exporter], any protocol version
older than TLS 1.3 and any cipher suite not listed in [approved ciphers](#rqsrs-026clickhouseoperatorfipstlsapprovedciphers)
SHALL be rejected by the operator in a FIPS-compliant configuration.


## ClickHouse Server

#### RQ.SRS-026.ClickHouseOperator.FIPS.CH.FIPSConfig
version: 1.0

Operator deploying a `ClickHouseInstallation` with FIPS TLS OpenSSL settings SHALL start a FIPS-compliant ClickHouse server and client.

```yaml
  configuration:
    clusters:
      - name: default
        secure: "yes"
        insecure: "no"
        layout:
          shardsCount: 1
          replicasCount: 2
    zookeeper:
      nodes:
        - host: chk-test-030003-keeper-0-0
          port: 2281
          secure: "yes"
    settings:
      http_port: _removed_
      tcp_port: _removed_
      interserver_http_port: _removed_
      mysql_port: _removed_
      postgresql_port: _removed_
      https_port: 8443
      tcp_port_secure: 9440
      interserver_https_port: 9010
    files:
      openssl.xml: |
        <yandex>
          <openSSL>
            <server>
              <certificateFile>/etc/clickhouse-server/secrets.d/server.crt/clickhouse-certs/server.crt</certificateFile>
              <privateKeyFile>/etc/clickhouse-server/secrets.d/server.key/clickhouse-certs/server.key</privateKeyFile>
              <dhParamsFile>/etc/clickhouse-server/secrets.d/dhparam.pem/clickhouse-certs/dhparam.pem</dhParamsFile>
              <!-- Server-auth TLS only: clients validate this certificate; the server does not require client certificates (not mTLS). -->
              <verificationMode>none</verificationMode>
              <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
              <cipherSuites>TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384</cipherSuites>
            </server>
            <client>
              <caConfig>/etc/clickhouse-server/secrets.d/ca.crt/clickhouse-certs/ca.crt</caConfig>
              <loadDefaultCAFile>false</loadDefaultCAFile>
              <verificationMode>strict</verificationMode>
              <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
              <cipherSuites>TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384</cipherSuites>
            </client>
          </openSSL>
        </yandex>
```

The deployed ClickHouse server SHALL use only the following ports:

* HTTPS API port 8443 (instead of 8123)
* Secure native TCP port 9440 (instead of 9000)
* Interserver HTTPS port 9010 (instead of interserver HTTP port 9009)
* Backup sidecar HTTPS API port 7171 (instead of 7180), when backups are enabled

Each exposed port SHALL support TLS communication using only FIPS-compliant protocol versions and cipher suites.

#### RQ.SRS-026.ClickHouseOperator.FIPS.CH.FIPSConfig.ExternalClient
version: 1.0

External clients connecting to the ClickHouse server SHALL be able to use any enabled TLS protocol version, including TLS 1.2.

#### RQ.SRS-026.ClickHouseOperator.FIPS.CH.Rescale
version: 1.0

Adding or removing a replica from a FIPS-configured `ClickHouseInstallation` SHALL reconcile successfully and result in the expected number of running pods.

After rescaling, all replicas SHALL continue to run the FIPS ClickHouse binary and maintain the configured TLS-only OpenSSL settings.

#### RQ.SRS-026.ClickHouseOperator.FIPS.CH.ConfigUpdate
version: 1.0

Updating TLS settings on a running CHI SHALL reload ClickHouse with the new FIPS-compliant configuration.


## ClickHouse Keeper

#### RQ.SRS-026.ClickHouseOperator.FIPS.CHK.FIPSConfig
version: 1.0

Operator deploying a `ClickHouseKeeperInstallation` with FIPS TLS OpenSSL settings SHALL start a FIPS-compliant ClickHouse
Keeper server and client.

```yaml
  configuration:
    clusters:
      - name: keeper
        secure: "yes"
        insecure: "no"
        layout:
          replicasCount: 2
    settings:
      keeper_server/log_storage_path: /var/lib/clickhouse/coordination/log
      keeper_server/snapshot_storage_path: /var/lib/clickhouse/coordination/snapshots
      keeper_server/raft_configuration/server/port: 9444
    files:
      openssl.xml: |
        <clickhouse>
          <openSSL>
              <server>
                <certificateFile>/etc/clickhouse-server/secrets.d/server.crt/clickhouse-certs/server.crt</certificateFile>
                <privateKeyFile>/etc/clickhouse-server/secrets.d/server.key/clickhouse-certs/server.key</privateKeyFile>
                <!-- Server-auth TLS only: clients validate this certificate; the server does not require client certificates (not mTLS). -->
                <verificationMode>none</verificationMode>
                <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
                <cipherSuites>TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384</cipherSuites>
              </server>
              <client>
                <caConfig>/etc/clickhouse-server/secrets.d/ca.crt/clickhouse-certs/ca.crt</caConfig>
                <loadDefaultCAFile>false</loadDefaultCAFile>
                <verificationMode>strict</verificationMode>
                <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
                <cipherSuites>TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384</cipherSuites>
              </client>
          </openSSL>
        </clickhouse>
```

The deployed ClickHouse Keeper cluster SHALL use only the following ports:

* Secure client port 2281 (instead of 2181)
* Secure Raft communication port 9444
* Plaintext HTTP readiness probe port 9182 (the `/ready` Raft-quorum health check)

Every exposed port except the readiness probe port 9182 and Raft replication port 9444 (which enforces peer-only authentication)
SHALL support TLS communication using only FIPS-compliant protocol versions and cipher suites. Port 9182 SHALL stay 
unconditionally plaintext HTTP regardless of the secure/insecure configuration (see Boundary).

#### RQ.SRS-026.ClickHouseOperator.FIPS.CHK.Rescale
version: 1.0

Adding or removing a node from a FIPS-configured `ClickHouseKeeperInstallation` SHALL reconcile successfully and result 
in the expected number of running pods.

After rescaling, all Keeper nodes SHALL continue to run the FIPS ClickHouse Keeper binary and maintain the configured 
TLS-only OpenSSL settings.

#### RQ.SRS-026.ClickHouseOperator.FIPS.CHK.ConfigUpdate
version: 1.0

Updating TLS settings on a running CHK SHALL reload ClickHouse with the new FIPS-compliant configuration.


## ClickHouse Backup Sidecar

#### RQ.SRS-026.ClickHouseOperator.FIPS.Backup.FIPSBinary
version: 1.0

The `clickhouse-backup` sidecar SHALL run a FIPS-built binary.

The sidecar binary SHALL satisfy all of the following:

* `clickhouse-backup --version` contains `fips`
* When inspectable, `go version -m` reports `GOFIPS140=v1.0.0`

#### RQ.SRS-026.ClickHouseOperator.FIPS.Backup.FIPSConfig
version: 1.0

Deploying a `ClickHouseInstallation` with a FIPS-configured backup sidecar SHALL start `clickhouse-backup` with a FIPS-compliant TLS configuration.

The deployed backup sidecar SHALL only add the following listener ports to the ClickHouse container:

* HTTPS API port 7171 (instead of 7180)

The FIPS-configured backup sidecar SHALL additionally satisfy all of the following:

* Each exposed port SHALL support TLS communication using only FIPS-compliant protocol versions and cipher suites.
* The `clickhouse-backup` sidecar SHALL connect to ClickHouse using secure native TCP with TLS enabled.

#### RQ.SRS-026.ClickHouseOperator.FIPS.Backup.RestoreRoundTrip
version: 1.0

Creating a backup and restoring it through the HTTPS API SHALL succeed over TLS.

## FIPS Enforcement Mode

**Objective:** Verify that `security.fips.enforced: "true"` coerces relaxed security settings and rejects non-compliant CHI/CHK specifications and non-FIPS images.

### Security Coercion

#### RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.SecurityCoercion
version: 1.0

When `security.fips.enforced: "true"` is set in the [ClickHouseOperatorConfiguration], the operator SHALL coerce unset or relaxed security settings as follows:

* Unset TLS verify SHALL be coerced to Strict for ClickHouse, ZooKeeper/Keeper, and Kubernetes clients.
* Unset TLS `minVersion` SHALL be coerced to `"1.3"` for the operator's outbound TLS clients (`security.clickhouse.tls`, `security.zookeeper.tls`, and `security.kubernetes.tls`).
* Explicit `minVersion: "1.2"` for those TLS clients SHALL be coerced to `"1.3"`.
* Unset IPC mode SHALL be coerced to Secure.

Example configuration with explicit `minVersion: "1.2"`:

```yaml
spec:
  security:
    fips:
      enforced: "true"
    clickhouse:
      tls:
        minVersion: "1.2"
    zookeeper:
      tls:
        minVersion: "1.2"
    kubernetes:
      tls:
        minVersion: "1.2"
```

After operator configuration normalization, the effective `minVersion` for each TLS client listed above SHALL be `"1.3"`.

#### RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.RejectNonCompliantSpecs
version: 1.0

When `security.fips.enforced: "true"` is set in the [ClickHouseOperatorConfiguration], the operator SHALL reject 
non-compliant CHI and CHK specifications with `FIPSValidationFailed` and SHALL NOT create workload StatefulSets for:

* CHI referencing plain external ZooKeeper nodes, including when `secure` is explicitly set to `"false"`.
* CHI with `clickhouse.tls.verify=None` at spec or cluster level.
* CHI with `zookeeper.tls.verify=None`.
* CHI with invalid `clickhouse.tls.minVersion`.
* CHK with TLS verify bypass at spec level.

#### RQ.SRS-026.ClickHouseOperator.FIPS.Enforced.MinVersionScope
version: 1.0

The `minVersion` coercion SHALL apply only to TLS clients created and managed by the operator.
They SHALL NOT require ClickHouse Server or ClickHouse Keeper listener endpoints to reject TLS 1.2
(see [RQ.SRS-026.ClickHouseOperator.FIPS.CH.FIPSConfig.ExternalClient](#rqsrs-026clickhouseoperatorfipschfipsconfigexternalclient)).

### Image Policy

#### RQ.SRS-026.ClickHouseOperator.FIPS.Images.Required.RejectNonFIPS
version: 1.0

With `security.fips.images.policy=Required`, non-FIPS images SHALL be rejected with `FIPSImagePolicyViolation` as follows:

* CHI with non-FIPS ClickHouse image tag SHALL be rejected at admission.
* CHK with non-FIPS Keeper image tag SHALL be rejected at admission.
* CHI with non-FIPS `clickhouse-backup` sidecar image tag SHALL be rejected at admission.
* CHI with multiple non-FIPS hosts SHALL produce a single policy violation error.
* Digest-only image references SHALL NOT be detected as FIPS at admission.
* Registry hostname containing `fips` SHALL NOT satisfy FIPS tag detection.
* CHI admitted with a FIPS-tagged ClickHouse image whose running binary lacks `fips` in `SELECT version()` SHALL fail at runtime.

## Runtime Connection Evidence

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.KubernetesAPI
version: 1.0

The clickhouse-operator SHALL access the Kubernetes API through the HTTPS endpoint on port `443`.
Plain HTTP requests to the Kubernetes API endpoint SHALL be rejected.

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Exporter.KubernetesAPI
version: 1.0

The metrics-exporter SHALL access the Kubernetes API through the HTTPS endpoint on port `443`.
Plain HTTP requests to the Kubernetes API endpoint SHALL be rejected.

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.ClickHouse
version: 1.0

The clickhouse-operator SHALL communicate with ClickHouse hosts using HTTPS port `8443`.

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Exporter.ClickHouse
version: 1.0

The metrics-exporter SHALL discover ClickHouse hosts using the HTTPS endpoint `8443`.

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.Operator.KeeperRestriction
version: 1.0

When a Keeper ensemble is configured as TLS-only, the clickhouse-operator SHALL NOT attempt plaintext ZooKeeper/Keeper
operations against it.

### RQ.SRS-026.ClickHouseOperator.FIPS.Connect.ClickHouse.KeeperTLS
version: 1.0

ClickHouse replicas SHALL connect to Keeper using secure client port `2281` with `secure=yes`.


## Integrity Check

### RQ.SRS-026.ClickHouseOperator.FIPS.Integrity.VerificationMismatch
version: 1.0

Each shipped FIPS binary — `clickhouse-operator` and `metrics-exporter` — SHALL perform a software integrity 
self-test at initialization by verifying its embedded HMAC. If the binary is tampered with or corrupted such that
the HMAC verification fails, the process SHALL immediately terminate with a `fips140: verification mismatch` panic 
to prevent the execution of a compromised cryptographic module.

## CAST Failure

**Objective:** Verify FIPS Cryptographic Algorithm Self-Test (CAST) detects failures in each binary independently.


### Operator CAST Failure

#### RQ.SRS-026.ClickHouseOperator.FIPS.CAST.OperatorFail
version: 1.0

Running `clickhouse-operator` with `GODEBUG=failfipscast=<name>` SHALL terminate with a CAST error.


### Exporter CAST Failure

#### RQ.SRS-026.ClickHouseOperator.FIPS.CAST.ExporterFail
version: 1.0

Running `metrics-exporter` with `GODEBUG=failfipscast=<name>` SHALL terminate with a CAST error.


## ACVP Algorithm Validation

**Objective:** Verify that each FIPS binary can be built with the ACVP wrapper enabled and that the embedded ACVP responder works through the modulewrapper stdin/stdout protocol.

These requirements cover the e2e ACVP smoke tests only. They do not claim full ACVP expected-output replay or suite-count validation from `pkg/util/fips/acvp/run.sh`.

### Operator ACVP Validation

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.WrapperIntegration
version: 1.0

Building `clickhouse-operator` with `-tags acvp_wrapper` SHALL produce a binary whose ACVP responder is reachable through argv0 dispatch when executed as `clickhouse-operator-acvp`.

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.ConfigGeneration
version: 1.0

The `clickhouse-operator` ACVP responder SHALL answer a `getConfig` request successfully. The returned payload SHALL be valid JSON, SHALL advertise `SHA2-256` and `ACVP-AES-GCM`, and SHALL NOT advertise `ML-KEM` or `ML-DSA`.

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Operator.SHA2256AFT
version: 1.0

The `clickhouse-operator` ACVP responder SHALL answer a `SHA2-256` algorithm functional test request for input `abc` with the digest matching `hashlib.sha256(b"abc").digest()`.

### Exporter ACVP Validation

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.WrapperIntegration
version: 1.0

Building `metrics-exporter` with `-tags acvp_wrapper` SHALL produce a binary whose ACVP responder is reachable through argv0 dispatch when executed as `metrics-exporter-acvp`.

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.ConfigGeneration
version: 1.0

The `metrics-exporter` ACVP responder SHALL answer a `getConfig` request successfully. The returned payload SHALL be valid JSON, SHALL advertise `SHA2-256` and `ACVP-AES-GCM`, and SHALL NOT advertise `ML-KEM` or `ML-DSA`.

#### RQ.SRS-026.ClickHouseOperator.FIPS.ACVP.Exporter.SHA2256AFT
version: 1.0

The `metrics-exporter` ACVP responder SHALL answer a `SHA2-256` algorithm functional test request for input `abc` with the digest matching `hashlib.sha256(b"abc").digest()`.


## Terminology

### SRS

Software Requirements Specification.

### FIPS 140-3

Federal Information Processing Standard for cryptographic module validation.

### clickhouse-operator

The Altinity ClickHouse Operator Kubernetes controller binary.

### metrics-exporter

The Prometheus metrics exporter sidecar binary shipped in the operator pod.

### CHI

ClickHouseInstallation custom resource.

### CHK

ClickHouseKeeperInstallation custom resource.

### ACVP

Automated Cryptographic Validation Protocol.

### CMVP

Cryptographic Module Validation Program.

### CAVP

Cryptographic Algorithm Validation Program.

[clickhouse-operator]: #clickhouse-operator
[metrics-exporter]: #metrics-exporter
[Kubernetes API]: https://kubernetes.io/docs/reference/kubernetes-api/
[ClickHouse Server]: #clickhouse-server
[ZooKeeper/Keeper]: #clickhouse-keeper

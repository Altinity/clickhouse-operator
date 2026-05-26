# ACVP Wrapper Repro Guide

This folder contains the embedded ACVP wrapper used by `clickhouse-operator`
and the metrics-exporter binary when built with the `acvp_wrapper` build tag
and invoked as `<binary>-acvp` (or via the `acvp` subcommand wired in by the
caller).

The package is gated by `//go:build acvp_wrapper`, so default builds never
link the ACVP handlers or the `crypto/mlkem/mlkemtest` import; only
`go build -tags acvp_wrapper` includes them.

The tracked config for public-Go-API validation is:

- `pkg/util/fips/acvp/acvp_test_fips140v1.26.public.config.json`

It intentionally excludes `ML-KEM` and `ML-DSA` vector suites because those
ACVP paths rely on internal Go crypto APIs that are not publicly exposed in
Go 1.26.

## Compliance Scope

- This wrapper is a reproducibility harness for ACVP expected-output tests.
- It is **not** itself a CMVP certificate artifact.
- The compliance-relevant cryptographic implementation is Go's native
  `crypto/internal/fips140/...` module when run with `GODEBUG=fips140=on`
  (or `only`), per Go's FIPS documentation.

## Certificate And Security Policy Context

- Go's official FIPS status and validation lifecycle is documented here:
  [FIPS 140-3 Compliance (Go docs)](https://go.dev/doc/security/fips140).
- As documented by Go, module validations are tracked in CMVP lists; at the time
  of writing, v1.26.0 is listed as "Implementation Under Test" and v1.0.0 is
  listed as "In Process" (with CAVP certificate A6650).
- Go's published FIPS documentation and blog post are the authoritative public
  references for certificate/security-policy context:
  - [The FIPS 140-3 Go Cryptographic Module (Go blog)](https://go.dev/blog/fips140)
  - [CMVP Modules In Process List](https://csrc.nist.gov/Projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list)
  - [CMVP Validated Modules Search (Go Cryptographic Module)](https://csrc.nist.gov/projects/cryptographic-module-validation-program/validated-modules/search?SearchMode=Basic&ModuleName=Go+Cryptographic+Module&CertificateStatus=Active&ValidationYear=0)
  - [CAVP Certificate A6650](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/details?validation=39260)

## Reference Traceability (Claim -> Source)

| Claim | Source |
| --- | --- |
| Go supports native FIPS 140-3 mode in the standard toolchain and documents `GODEBUG=fips140=...` behavior. | [Go FIPS 140-3 compliance docs](https://go.dev/doc/security/fips140) |
| The relevant cryptographic implementation is Go's `crypto/internal/fips140/...` module in FIPS mode. | [Go FIPS 140-3 compliance docs](https://go.dev/doc/security/fips140) |
| Go module-validation lifecycle/status is tracked via CMVP lists (Validated / In Process / IUT). | [Go FIPS 140-3 compliance docs](https://go.dev/doc/security/fips140), [CMVP MIP list](https://csrc.nist.gov/Projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list), [CMVP validated modules search](https://csrc.nist.gov/projects/cryptographic-module-validation-program/validated-modules/search?SearchMode=Basic&ModuleName=Go+Cryptographic+Module&CertificateStatus=Active&ValidationYear=0) |
| Go Cryptographic Module v1.0.0 has CAVP certificate A6650 and entered CMVP in-process workflow. | [Go FIPS blog](https://go.dev/blog/fips140), [CAVP A6650](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/details?validation=39260), [CMVP MIP list](https://csrc.nist.gov/Projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list) |
| Pins used by this repo's ACVP repro (`baaf868e6e8f`, `d893de8b8b1c`) come from upstream Go `fips140test` ACVP test constants. | Upstream Go source: `src/crypto/internal/fips140test/acvp_test.go` (constants `bsslVersion` and `goAcvpVersion`) |
| This repo's tracked config is a public-API-only derivative of Go's v1.26 ACVP config. | Upstream Go source: `src/crypto/internal/fips140test/acvp_test_fips140v1.26.config.json`, local tracked file `pkg/util/fips/acvp/acvp_test_fips140v1.26.public.config.json` |
| The reproducibility result target in this repo is `38 ACVP tests matched expectations`. | Local reproducibility flow in `pkg/util/fips/acvp/run.sh` and command output from `check_expected.go` |

## Pinned Version Provenance

Pins used by `run.sh` are taken from upstream Go's ACVP test setup for the
v1.26 stream:

- `boringssl` commit: `baaf868e6e8f`
  - From `bsslVersion = v0.0.0-20251111011041-baaf868e6e8f` in Go
    `crypto/internal/fips140test/acvp_test.go`.
- `acvp-testdata` commit: `d893de8b8b1c`
  - From `goAcvpVersion = v0.0.0-20251201200548-d893de8b8b1c` in the same Go test.
- Upstream config source:
  - `src/crypto/internal/fips140test/acvp_test_fips140v1.26.config.json`
    in Go source.
- This repo's tracked config:
  - `pkg/util/fips/acvp/acvp_test_fips140v1.26.public.config.json`
  - Derived from Go's v1.26 config, with `ML-KEM` and `ML-DSA` suites removed
    to keep the run strictly on public Go crypto APIs.

## Reproduce The Current Result

Run from the repository root.

### One-command repro

Validate the operator binary (default):

```bash
bash pkg/util/fips/acvp/run.sh
```

Validate the metrics-exporter binary:

```bash
BINARY=metrics-exporter bash pkg/util/fips/acvp/run.sh
```

Expected output includes:

- `38 ACVP tests matched expectations`

### Manual step-by-step

### 1) Build the wrapper binary

Operator:

```bash
docker run --rm \
  -v "$PWD:/work" \
  -w /work \
  golang:1.26-alpine \
  sh -lc 'export PATH=$PATH:/usr/local/go/bin && CGO_ENABLED=0 go build -tags acvp_wrapper -o clickhouse-operator ./cmd/operator && ln -sf clickhouse-operator clickhouse-operator-acvp'
```

Metrics exporter:

```bash
docker run --rm \
  -v "$PWD:/work" \
  -w /work \
  golang:1.26-alpine \
  sh -lc 'export PATH=$PATH:/usr/local/go/bin && CGO_ENABLED=0 go build -tags acvp_wrapper -o metrics-exporter ./cmd/metrics_exporter && ln -sf metrics-exporter metrics-exporter-acvp'
```

### 2) Fetch pinned upstream inputs

```bash
rm -rf /tmp/boringssl /tmp/acvp-testdata
git clone https://boringssl.googlesource.com/boringssl /tmp/boringssl
git -C /tmp/boringssl checkout baaf868e6e8f

git clone https://github.com/geomys/acvp-testdata /tmp/acvp-testdata
git -C /tmp/acvp-testdata checkout d893de8b8b1c
```

### 3) Build pinned `acvptool`

```bash
docker run --rm \
  -v /tmp/boringssl:/src \
  -w /src \
  golang:1.26-alpine \
  sh -lc 'export PATH=$PATH:/usr/local/go/bin && go build -o /src/acvptool-pinned ./util/fipstools/acvp/acvptool'
```

### 4) Run the expected-output check

```bash
docker run --rm \
  -e ACVP_WRAPPER=1 \
  -e GODEBUG=fips140=on \
  -v "$PWD:/work" \
  -v /tmp/boringssl:/tmp/boringssl:ro \
  -v /tmp/acvp-testdata:/tmp/acvp-testdata:rw \
  -w /tmp/acvp-testdata \
  golang:1.26-alpine \
  sh -lc 'export PATH=$PATH:/usr/local/go/bin && go run /tmp/boringssl/util/fipstools/acvp/acvptool/test/check_expected.go -tool /tmp/boringssl/acvptool-pinned -module-wrappers go:/work/clickhouse-operator-acvp -tests /work/pkg/util/fips/acvp/acvp_test_fips140v1.26.public.config.json'
```

Substitute `metrics-exporter-acvp` for `clickhouse-operator-acvp` to validate
the metrics-exporter binary instead.

Expected output includes:

- `38 ACVP tests matched expectations`

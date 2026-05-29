# FIPS evidence verification

This document is a recipe book for security/compliance engineers who need to
independently verify that a downloaded
`altinity/clickhouse-operator:0.27.1` image matches the artifacts the
project publishes alongside the release.

All commands target the concrete tag `0.27.1` and a concrete artifact
directory `release-evidence/`. To audit a different release, substitute the
tag everywhere `0.27.1` appears and re-run; nothing else needs to change.

For the policy context (what is in scope, what is not, what CMVP claim is
being made), see [`security_hardening_fips.md` § Release evidence — image digest,
SBOM, build logs](security_hardening_fips.md#release-evidence--image-digest-sbom-build-logs).
That section is the policy; this document is the verification procedure.

Required local tooling: `docker` (with the `buildx` plugin), `jq`,
`syft` (for SBOM re-generation), `gh` (optional, only for downloading the
GitHub Actions artifact), and `tar`. Versions current at the time of
release are acceptable — none of the steps depend on a specific tool
version.

## 1. What's available

Two independent evidence streams are published for every release tag.

**Side-channel artifact bundle.** Every push of a `MAJOR.MINOR.PATCH` tag
runs `.github/workflows/build_branch.yaml`, which uploads a GitHub
Actions artifact named `release-evidence-<tag>` and also attaches the
files inside it to the matching GitHub Release page. For each of the two
shipped images (`clickhouse-operator`, `metrics-exporter`) the bundle
contains four files:

- `<bin>__<version>.digest.txt` — the `sha256:` manifest-list digest.
- `<bin>__<version>.sbom.spdx.json` — SPDX-JSON SBOM produced by syft.
- `<bin>__<version>.manifest.json` — the raw multi-arch manifest list.
- `<bin>-build-metadata.json` — `docker buildx --metadata-file` output,
  with SBOM digests, provenance hashes, and per-platform image IDs.

Download via `gh` against the Actions run:

```bash
gh run download --repo Altinity/clickhouse-operator \
  --name release-evidence-0.27.1 \
  --dir release-evidence/
```

Or from the GitHub Releases page for tag `0.27.1`:
<https://github.com/Altinity/clickhouse-operator/releases/tag/0.27.1>.

**Inline image attestations.** The image manifest itself carries an inline
SBOM and a SLSA provenance attestation, attached at build time via
`docker buildx --sbom=true --provenance=mode=max`. These travel with the
image — they are reachable from any registry pull without needing the
side-channel bundle:

```bash
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{json .SBOM}}' | jq .
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{json .Provenance}}' | jq .
```

## 2. Verify the image digest

Pull the live registry digest and compare it to the archived value:

```bash
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{.Manifest.Digest}}'
# Expected: identical to the single sha256:... line in
# release-evidence/clickhouse-operator__0.27.1.digest.txt
```

A mismatch means the tag has been re-pushed (image rotation) or the
archived digest is stale. Either way it is a verification failure — open
an issue (see § 8) before deploying.

The manifest list itself is also archived. To compare it byte-for-byte:

```bash
docker buildx imagetools inspect --raw \
  altinity/clickhouse-operator:0.27.1 \
  | diff - release-evidence/clickhouse-operator__0.27.1.manifest.json
```

Empty diff is the pass criterion.

## 3. Verify the SBOM

Regenerate the SBOM locally with the same tool the release pipeline uses
(`syft`), then diff against the shipped SBOM:

```bash
syft altinity/clickhouse-operator:0.27.1 -o spdx-json > /tmp/local.sbom.spdx.json
diff <(jq -S . /tmp/local.sbom.spdx.json) \
     <(jq -S . release-evidence/clickhouse-operator__0.27.1.sbom.spdx.json)
```

A non-empty diff is not automatically a red flag. SPDX documents contain
several fields that are run-specific by design. Expected divergence
sources:

- `creationInfo.created` — ISO timestamp of the syft run.
- `documentNamespace` — randomized per syft invocation.
- `SPDXID` element identifiers when syft regenerates them.
- `creationInfo.creators` — embeds the local syft binary version.

What MUST match: the `packages[*]` set (name, version, licenseConcluded,
checksums, supplier), the `relationships` between packages, and the
`files` list. A practical filtered comparison:

```bash
diff <(jq -S '.packages | sort_by(.name,.versionInfo)' /tmp/local.sbom.spdx.json) \
     <(jq -S '.packages | sort_by(.name,.versionInfo)' \
         release-evidence/clickhouse-operator__0.27.1.sbom.spdx.json)
```

Empty diff on `.packages` is the pass criterion. If a package appears in
one document but not the other, treat that as a real divergence.

## 4. Inspect inline provenance

The inline SLSA provenance records what built the image, from which source
revision:

```bash
docker buildx imagetools inspect altinity/clickhouse-operator:0.27.1 \
  --format '{{json .Provenance.SLSA}}' | jq .
```

Expected shape (abbreviated):

```json
{
  "builder": { "id": "https://github.com/Altinity/clickhouse-operator/.github/workflows/build_branch.yaml@refs/heads/0.27.1" },
  "buildType": "https://mobyproject.org/buildkit@v1",
  "invocation": {
    "configSource": {
      "uri": "git+https://github.com/Altinity/clickhouse-operator@refs/heads/0.27.1",
      "digest": { "sha1": "<commit sha>" },
      "entryPoint": "dockerfile/operator/Dockerfile"
    },
    "parameters": { "frontend": "dockerfile.v0" }
  },
  "metadata": {
    "buildInvocationID": "<uuid>",
    "buildStartedOn": "...",
    "buildFinishedOn": "...",
    "completeness": { "parameters": true, "environment": true, "materials": false },
    "reproducible": false
  }
}
```

Verify three properties:

- `builder.id` resolves to the `Altinity/clickhouse-operator` org.
- `invocation.configSource.uri` matches the public repo URL.
- `invocation.configSource.digest.sha1` matches the commit tagged
  `0.27.1` (cross-check with `git rev-parse 0.27.1` against a local
  checkout).

## 5. Verify FIPS-build metadata from the binary itself

The shipped image is distroless (`gcr.io/distroless/static-debian13`),
so there is no shell or Go toolchain inside the container. Extract the
binary to the host and run `go version -m` locally:

```bash
docker create --name cho-verify altinity/clickhouse-operator:0.27.1
docker cp cho-verify:/clickhouse-operator /tmp/clickhouse-operator
docker rm cho-verify

go version -m /tmp/clickhouse-operator | grep -E 'GOFIPS140|vcs\.|build\s+-tags'
```

Expected (relevant lines only):

```
build   GOFIPS140=v1.0.0
build   -trimpath=true
build   -buildvcs=true
build   vcs.revision=<commit sha>
```

The `GOFIPS140=v1.0.0` line is the canonical proof that the binary was
linked against the Go FIPS 140-3 cryptographic module. Its absence in a
shipped image is a release failure.

A second, lighter-weight check uses the operator's own version flag
(the image entrypoint is the operator binary itself):

```bash
docker run --rm altinity/clickhouse-operator:0.27.1 -version
# Prints the version string; non-zero exit on a corrupt binary.
```

At normal startup the operator also emits a FIPS banner line that
records the runtime gate:

```
FIPS: chopconf.fips.enforced=<bool> build.enabled=<bool> runtime.enforced=<bool> module=v1.0.0
```

`build.enabled=true` and `module=v1.0.0` together confirm the same fact
that `go version -m` exposes statically. `tests/e2e/test_operator.py::test_010076`
asserts this banner on every CI run.

## 6. Verify ACVP-evidence artifact (local-only)

ACVP responder evidence is produced by `pkg/util/fips/acvp/run.sh`, which
drives BoringSSL's `acvptool` against each shipped binary's NIST ACVP
responder. There is no CI workflow that uploads the artifact today;
reviewers and release engineers run the driver locally on the release
commit and archive the output alongside the rest of the per-release
evidence bundle. The driver fails non-zero on any vector mismatch, so a
green local run is itself the pass/fail manifest.

Reproduce per binary:

```bash
GOFIPS140=v1.0.0 go build -tags acvp_wrapper \
  -o dev/bin/clickhouse-operator ./cmd/operator
pkg/util/fips/acvp/run.sh dev/bin/clickhouse-operator
go version -m dev/bin/clickhouse-operator | grep GOFIPS140
```

The pinned BoringSSL and `geomys/acvp-testdata` commits live in
`pkg/util/fips/acvp/run.sh` so the run is bit-for-bit reproducible across
hosts. `go version -m` proves the binary subjected to the test vectors
was linked against the same FIPS module that ships in the image.

The expected pattern is a long run of `PASS` lines and zero `FAIL` lines.
For wrapper internals and a procedure to reproduce the vector run
locally, see `pkg/util/fips/acvp/README.md`.

## 7. CMVP status disclosure

The Go FIPS 140-3 cryptographic module (`crypto/fips140` v1.0.0) is
in CMVP review, not yet a completed CMVP-validated module. The image
encodes this status as a label so machine-readable consumers cannot
miss it:

```bash
docker inspect altinity/clickhouse-operator:0.27.1 \
  | jq '.[0].Config.Labels | with_entries(select(.key | startswith("altinity.fips")))'
```

Expected labels:

```json
{
  "altinity.fips.module": "v1.0.0",
  "altinity.fips.module.status": "CMVP-in-review",
  "altinity.fips.build": "GOFIPS140=v1.0.0",
  "altinity.fips.runtime": "GODEBUG=fips140=on"
}
```

`altinity.fips.module.status="CMVP-in-review"` is the canonical truth
for this release. The product claims FIPS 140-3 *compatibility*, not
certification. For the authoritative module status see the
[Go FIPS 140-3 documentation](https://go.dev/doc/security/fips140).

## 8. Reporting issues

If any check above fails — digest mismatch, SBOM packages diverge, inline
provenance points at the wrong source repo, `GOFIPS140` missing from the
binary, ACVP run shows `FAIL` lines, or the CMVP status label is missing
or differs from the shipped policy — file a report at
<https://github.com/Altinity/clickhouse-operator/issues> and attach the
artifact files involved (digest, SBOM, manifest, build metadata, and any
relevant log output). Include the image reference and tag, the exact
output of the failing command, and the host platform.

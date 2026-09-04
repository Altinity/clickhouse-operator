# Red Hat certified Operator bundle

This directory provides a certification-specific bundle path without changing
the existing OperatorHub.io/community bundle. Generated files are written to
the ignored `generated/` directory.

## Prerequisites

- `yq` v4
- `envsubst`
- `operator-sdk` v1.40 or newer for full local validation
- certified and published, digest-pinned operator, metrics, operand, and helper
  images

## Generate and validate

Choose an OpenShift range that Altinity actually supports and has tested. The
example values below are illustrative, not a support declaration.

```bash
OPERATOR_IMAGE='registry.example/altinity/clickhouse-operator@sha256:<digest>' \
METRICS_IMAGE='registry.example/altinity/metrics-exporter@sha256:<digest>' \
OPENSHIFT_VERSIONS='v4.18-v4.22' \
VALID_SUBSCRIPTION='No subscription required; optional commercial support is available from Altinity' \
RELATED_IMAGES=$'clickhouse-server=registry.example/altinity/clickhouse-server@sha256:<digest>\nclickhouse-keeper=registry.example/altinity/clickhouse-keeper@sha256:<digest>\nclickhouse-log=registry.access.redhat.com/ubi9/ubi-minimal@sha256:<digest>' \
./deploy/certified-operator/build-bundle.sh

./deploy/certified-operator/verify-bundle.sh
```

`VALID_SUBSCRIPTION` is optional. It defaults to the message above and can be
overridden if Altinity's commercial policy changes.

For a later certified release, set `PREVIOUS_VERSION`. Do not point it at a
community-only CSV. Set `CERT_PROJECT_ID` to emit the `ci.yaml` required at the
operator-package root in the Red Hat certified-operators repository. The ID is
not a secret, but it must come from the matching Partner Connect Operator
Bundle certification project.

Build the bundle image from the repository root:

```bash
docker build \
  -f deploy/certified-operator/bundle.Dockerfile \
  --build-arg BUNDLE_PACKAGE=altinity-clickhouse-operator \
  --build-arg OPENSHIFT_VERSIONS='v4.18-v4.22' \
  -t registry.example/altinity/clickhouse-operator-bundle:<version> .
```

Before submission, run `preflight check operator` against the pushed bundle
image with `KUBECONFIG` and `PFLT_INDEXIMAGE` configured for a supported
OpenShift cluster. Copy `generated/manifests`, `generated/metadata`, and the
package-level `generated/ci.yaml` into a branch of
`redhat-openshift-ecosystem/certified-operators`.

## Release automation setup

Publishing a GitHub Release automatically builds, signs, and submits the two
UBI image manifests. A scheduled workflow then waits until Red Hat reports both
exact digests as certified and published, generates the bundle, and opens the
certified-operators pull request. Red Hat's hosted pipeline can merge a passing
bundle because the generated `ci.yaml` uses `merge: true`.

Add these GitHub Actions repository variables:

| Variable | Value |
| --- | --- |
| `RED_HAT_CERTIFICATION_ENABLED` | `true` when automatic release certification should run |
| `RED_HAT_REGISTRY` | Registry hostname, for example `quay.io` |
| `RED_HAT_REGISTRY_NAMESPACE` | Namespace containing the certified repositories |
| `RED_HAT_PACKAGE_NAME` | Agreed certified package name; optional until naming is finalized because the provisional default is `altinity-clickhouse-operator` |
| `RED_HAT_CERTIFICATION_START_VERSION` | First release that should enter the certified catalog, without the `release-` prefix |
| `RED_HAT_OPENSHIFT_VERSIONS` | Tested range, for example `v4.18-v4.22` |
| `RED_HAT_BUNDLE_CERT_PROJECT_ID` | Partner Connect Operator Bundle project ID |
| `RED_HAT_CERTIFIED_OPERATORS_FORK` | Writable GitHub fork, for example `Altinity/certified-operators` |
| `RED_HAT_RELATED_IMAGES` | Multiline digest references named `clickhouse-server`, `clickhouse-keeper`, and `clickhouse-log`; extra certified related images may follow |

Add these GitHub Actions secrets:

| Secret | Purpose |
| --- | --- |
| `UBI_REGISTRY_USERNAME` | Push access to the certified image repositories |
| `UBI_REGISTRY_PASSWORD` | Registry password or robot token |
| `RED_HAT_PYXIS_API_TOKEN` | Submit Preflight results and query publication state |
| `RED_HAT_OPERATOR_CERTIFICATION_COMPONENT_ID` | Operator container project/component ID |
| `RED_HAT_METRICS_CERTIFICATION_COMPONENT_ID` | Metrics-exporter container project/component ID |
| `RED_HAT_CERTIFIED_OPERATORS_TOKEN` | GitHub token able to push to the configured fork and open a pull request against the public upstream repository |

The GitHub identity behind `RED_HAT_CERTIFIED_OPERATORS_TOKEN` must be the
identity authorized for the Partner Connect project. The workflow deliberately
fails before bundle generation when `RED_HAT_RELATED_IMAGES` is empty: the
certified distribution must have digest-pinned certified defaults first.

## Decisions still required

- Confirm the certified package name with Red Hat. The default here avoids the
  existing community package name `clickhouse`.
- Select and test the supported OpenShift minor-version range.
- Supply the Partner Connect certification project ID.
- Certify or replace every default operand/helper image and supply its digest
  in `RELATED_IMAGES`. The certified bundle injects these as the operator's
  defaults. Users may explicitly override them with their own uncertified
  images; the certified distribution itself never selects an uncertified image
  by default.
- Keep `features.operators.openshift.io/fips-compliant` false unless the full
  Red Hat FIPS badge requirements are separately met.

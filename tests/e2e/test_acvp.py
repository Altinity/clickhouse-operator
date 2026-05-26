# Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end smoke tests for the ACVP (Automated Cryptographic Validation
Protocol) responder embedded in operator and metrics-exporter binaries.

These scenarios exercise the full chain: build → argv0 dispatch → embedded
wrapper → length-prefixed stdio protocol → algorithm handler → response.

They do NOT replace the full BoringSSL acvptool reproducibility run driven by
pkg/util/fips/acvp/run.sh — that lives in the .github/workflows/acvp_test.yaml
CI workflow. The point of these e2e scenarios is to catch regressions in the
operator's build wiring (build tag, symlink-style argv0 dispatch, FIPS-mode
gating) before they reach a vector-roundtrip job that takes 15+ minutes.

Why subprocess and not kubectl: ACVP is a build-time smoke test of the binary's
cryptographic wrapper, not a runtime check of operator behavior. There is no
CHI, no chopconf, no Kubernetes object involved — only the binary, GODEBUG, and
the stdin/stdout protocol. Running this in-cluster (via kubectl exec into the
operator pod) would require shipping the image-acvp variant into minikube,
which is the workflow's job; here we validate the local build.
"""

import json
import os
import shutil
import struct
import subprocess
import tempfile
import hashlib

from testflows.core import *
from testflows.asserts import error


# Repo root resolution: this file lives at tests/e2e/test_acvp.py, so two
# directory hops upward land on the operator's git root.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _frame_request(command_name, *args):
    """Encode an ACVP request in the BoringSSL modulewrapper wire format.

    The format (little-endian throughout) is:
      uint32 num_args             // command name counts as args[0]
      uint32 len(args[0])
      ...
      uint32 len(args[N-1])
      bytes  args[0]
      ...
      bytes  args[N-1]

    Matches the reader at pkg/util/fips/acvp/wrapper.go::readRequest. The
    test/decode side is symmetric with wrapper_test.go::decodeResponse.
    """
    payload = [command_name.encode("utf-8")] + list(args)
    out = struct.pack("<I", len(payload))
    for chunk in payload:
        out += struct.pack("<I", len(chunk))
    for chunk in payload:
        out += chunk
    return out


def _parse_response(blob):
    """Decode the symmetric response framing. Returns a list of byte slices."""
    if len(blob) < 4:
        raise ValueError(f"response too short: {len(blob)} bytes")
    (count,) = struct.unpack("<I", blob[0:4])
    offset = 4
    lengths = []
    for _ in range(count):
        if offset + 4 > len(blob):
            raise ValueError("truncated length header")
        (n,) = struct.unpack("<I", blob[offset : offset + 4])
        lengths.append(n)
        offset += 4
    args = []
    for n in lengths:
        if offset + n > len(blob):
            raise ValueError(f"truncated payload (want {n} bytes, have {len(blob)-offset})")
        args.append(blob[offset : offset + n])
        offset += n
    return args


def _build_acvp_binary(cmd_path, binary_name):
    """Compile <cmd_path> with -tags acvp_wrapper and symlink as <binary_name>-acvp.

    Returns the absolute path to the symlink, or None if the build fails (the
    caller skips the scenario in that case so missing toolchain doesn't fail
    the whole suite).
    """
    tmpdir = tempfile.mkdtemp(prefix="acvp-e2e-")
    binary_path = os.path.join(tmpdir, binary_name)
    symlink_path = os.path.join(tmpdir, f"{binary_name}-acvp")

    env = os.environ.copy()
    # GOFIPS140 must be set; the wrapper's Run() refuses to start if
    # crypto/fips140.Enabled() reports false. v1.0.0 matches the build pinned
    # in dev/go_build_config.sh.
    env.setdefault("GOFIPS140", "v1.0.0")
    env.setdefault("GODEBUG", "fips140=only")
    env.setdefault("CGO_ENABLED", "0")

    result = subprocess.run(
        [
            "go",
            "build",
            "-tags",
            "acvp_wrapper",
            "-o",
            binary_path,
            cmd_path,
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != 0:
        return None, f"go build failed: {result.stderr.strip()}"

    # Symlink-based argv0 dispatch — the responder fires only when
    # filepath.Base(os.Args[0]) ends with "-acvp" (see
    # cmd/<binary>/app/acvp_dispatch_on.go).
    try:
        os.symlink(binary_path, symlink_path)
    except OSError as exc:
        return None, f"symlink failed: {exc}"

    return symlink_path, None


def _invoke_responder(binary_path, request_blob, timeout=15):
    """Run the responder once, sending request_blob on stdin and returning stdout."""
    env = os.environ.copy()
    env["GODEBUG"] = "fips140=only"
    proc = subprocess.run(
        [binary_path],
        input=request_blob,
        env=env,
        capture_output=True,
        timeout=timeout,
    )
    return proc


def _acvp_smoke(binary_name, cmd_path):
    """Shared body for both binaries. Exercises:

      1. getConfig — round-trip the capability JSON and check it advertises
         FIPS-approved primitives + excludes the deliberately-omitted ML-KEM
         and ML-DSA (which require Go-internal APIs not in this build).
      2. SHA2-256 AFT — hash a known input, compare to hashlib.sha256.
    """
    if shutil.which("go") is None:
        skip("go toolchain unavailable; ACVP build requires Go 1.26+")
        return

    with Given(f"Build {binary_name} with -tags acvp_wrapper"):
        binary_path, build_err = _build_acvp_binary(cmd_path, binary_name)
        if binary_path is None:
            # Build failure is the scenario's failure mode — surface it so the
            # CI workflow's acvp_test.yaml job catches the same regression.
            assert False, error(f"ACVP-tagged build of {binary_name} failed: {build_err}")

    with When("Round-trip a getConfig request"):
        proc = _invoke_responder(binary_path, _frame_request("getConfig"))
        assert proc.returncode == 0, error(
            f"responder exited {proc.returncode}; stderr={proc.stderr.decode('utf-8', 'replace')}"
        )
        responses = _parse_response(proc.stdout)
        assert len(responses) == 1, error(f"want 1 response arg, got {len(responses)}")
        config_text = responses[0].decode("utf-8")

    with Then("Capability JSON advertises FIPS-approved primitives"):
        # The wrapper is documented to expose SHA2 / AES-GCM and to exclude
        # ML-KEM / ML-DSA (those need Go-internal crypto APIs). Pin the
        # invariant in both directions — a future commit dropping AES-GCM or
        # silently re-enabling ML-KEM trips this assertion.
        assert "SHA2-256" in config_text, error("getConfig must advertise SHA2-256")
        assert "ACVP-AES-GCM" in config_text, error("getConfig must advertise ACVP-AES-GCM")
        assert "ML-KEM" not in config_text, error(
            "getConfig must NOT advertise ML-KEM (uses Go-internal API)"
        )
        assert "ML-DSA" not in config_text, error(
            "getConfig must NOT advertise ML-DSA (uses Go-internal API)"
        )
        # Sanity-check the bytes parse as JSON; a malformed config would make
        # acvptool reject the entire run.
        try:
            json.loads(config_text)
        except json.JSONDecodeError as exc:
            assert False, error(f"getConfig payload is not valid JSON: {exc}")

    with When("Round-trip a SHA2-256 AFT request"):
        # Algorithm Functional Test: send a message, expect SHA2-256 digest.
        # `abc` is the canonical short-input test vector and matches the
        # wrapper_test.go::TestSHA256AFT case so the e2e and unit assertions
        # are pinned to the same fixture.
        message = b"abc"
        proc = _invoke_responder(binary_path, _frame_request("SHA2-256", message))
        assert proc.returncode == 0, error(
            f"SHA2-256 responder exited {proc.returncode}; "
            f"stderr={proc.stderr.decode('utf-8', 'replace')}"
        )
        responses = _parse_response(proc.stdout)
        assert len(responses) == 1, error(f"want 1 response arg, got {len(responses)}")

    with Then("Hash output matches hashlib.sha256"):
        want = hashlib.sha256(message).digest()
        got = responses[0]
        assert got == want, error(
            f"SHA2-256 hash mismatch: want {want.hex()}, got {got.hex()}"
        )

    # Cleanup is best-effort — leftover /tmp/acvp-e2e-* dirs get reaped by the
    # OS or the next runner invocation; failing to clean up here must not mask
    # the assertion outcomes above.
    try:
        shutil.rmtree(os.path.dirname(binary_path), ignore_errors=True)
    except Exception:
        pass


@TestScenario
@Name("test_acvp_operator. ACVP responder smoke test: clickhouse-operator binary")
def test_acvp_operator(self):
    """Build operator with -tags acvp_wrapper and verify the embedded ACVP
    responder answers getConfig + SHA2-256 AFT correctly.

    Companion to test_acvp_metrics_exporter — both binaries embed the same
    pkg/util/fips/acvp package under the build tag, so we run each independently
    to confirm the argv0 dispatch fires in each binary's main path.
    """
    _acvp_smoke("clickhouse-operator", "./cmd/operator")


@TestScenario
@Name("test_acvp_metrics_exporter. ACVP responder smoke test: metrics-exporter binary")
def test_acvp_metrics_exporter(self):
    """Mirror of test_acvp_operator for the metrics-exporter binary. Both
    binaries ship the same FIPS module statically linked; this scenario
    confirms metrics-exporter's argv0 dispatch is wired identically so a
    regression in either binary's main path is caught.
    """
    _acvp_smoke("metrics-exporter", "./cmd/metrics_exporter")


@TestFeature
@Name("e2e.test_acvp")
def test(self):
    """ACVP responder e2e smoke tests."""
    scenarios = [
        test_acvp_operator,
        test_acvp_metrics_exporter,
    ]
    for t in scenarios:
        Scenario(test=t)()


if main():
    test()

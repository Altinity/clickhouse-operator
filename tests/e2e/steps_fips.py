# Copyright 2026 Altinity Ltd and/or its affiliates. All rights reserved.
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

# Python 3.8: postpone annotation evaluation so PEP-604 unions (X | None) and
# PEP-585 builtin generics (list[dict]) in this file don't fail at import time.
from __future__ import annotations

import copy
import json
import os
import re
import select
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

import e2e.util as util

from e2e.steps import create_shell_namespace_clickhouse_template, delete_test_namespace, get_shell
from testflows.asserts import error
from testflows.core import *

import e2e.kubectl as kubectl
import struct
import hashlib

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

FAKE_OPENSSL_SERVER = "fake-openssl-server"
TLS_REJECT_MARKERS = (
    "Cipher is (NONE)",
    "Cipher    : 0000",
    "handshake failure",
    "alert handshake failure",
    "no shared cipher",
    "no protocols available",
    "unsupported protocol",
    "wrong version number",
    "tlsv1 alert protocol version",
    "no peer certificate available",
)

# Server-originated evidence read BACK from the peer. A client-side local refusal
# (OpenSSL 3.x / hardened openssl.cnf MinProtocol floor) never sends a ClientHello
# and cannot produce these.
TLS_SERVER_REJECT_MARKERS = (
    "sslv3 alert",
    "tlsv1 alert",
    "alert handshake failure",
    "alert protocol version",
    "no shared cipher",
    "ssl alert number",
)

approved_tls1_3_ciphers = [
    "TLS_AES_256_GCM_SHA384",
    "TLS_AES_128_GCM_SHA256",
]
# TLS 1.2 suites permitted by Go's native FIPS 140-3 module
# (crypto/tls/defaults_fips140.go `allowedCipherSuitesFIPS`, the set clickhouse-backup's
# GOFIPS140=v1.0.0 build negotiates). OpenSSL names for the six Go suites; a Go-FIPS
# server offers only these at TLS 1.2, so they are ACCEPTED, not rejected. Kept separate
# from approved_tls1_3_ciphers because only clickhouse-backup (Go, MinVersion 1.2) uses
# them -- the OpenSSL-backed ClickHouse/Keeper listeners are pinned to TLS 1.3.
approved_tls1_2_ciphers = [
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES128-SHA256",
    "ECDHE-ECDSA-AES128-SHA256",
]
ciphers_by_protocol = {
    "TLSv1.3": [
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
    ],
    "TLSv1.2": [
        "ECDHE-ECDSA-AES256-GCM-SHA384",
        "ECDHE-RSA-AES256-GCM-SHA384",
        "DHE-DSS-AES256-GCM-SHA384",
        "DHE-RSA-AES256-GCM-SHA384",
        "ECDHE-ECDSA-CHACHA20-POLY1305",
        "ECDHE-RSA-CHACHA20-POLY1305",
        "DHE-RSA-CHACHA20-POLY1305",
        "ECDHE-ECDSA-AES256-CCM",
        "DHE-RSA-AES256-CCM",
        "ECDHE-ECDSA-ARIA256-GCM-SHA384",
        "ECDHE-ARIA256-GCM-SHA384",
        "DHE-DSS-ARIA256-GCM-SHA384",
        "DHE-RSA-ARIA256-GCM-SHA384",
        "ADH-AES256-GCM-SHA384",
        "ECDHE-ECDSA-AES128-GCM-SHA256",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "DHE-DSS-AES128-GCM-SHA256",
        "DHE-RSA-AES128-GCM-SHA256",
        "ECDHE-ECDSA-AES128-CCM",
        "DHE-RSA-AES128-CCM",
        "ECDHE-ECDSA-ARIA128-GCM-SHA256",
        "ECDHE-ARIA128-GCM-SHA256",
        "DHE-DSS-ARIA128-GCM-SHA256",
        "DHE-RSA-ARIA128-GCM-SHA256",
        "ADH-AES128-GCM-SHA256",
        "ECDHE-ECDSA-AES256-CCM8",
        "ECDHE-ECDSA-AES128-CCM8",
        "DHE-RSA-AES256-CCM8",
        "DHE-RSA-AES128-CCM8",
        "ECDHE-ECDSA-AES256-SHA384",
        "ECDHE-RSA-AES256-SHA384",
        "DHE-RSA-AES256-SHA256",
        "DHE-DSS-AES256-SHA256",
        "ECDHE-ECDSA-CAMELLIA256-SHA384",
        "ECDHE-RSA-CAMELLIA256-SHA384",
        "DHE-RSA-CAMELLIA256-SHA256",
        "DHE-DSS-CAMELLIA256-SHA256",
        "ADH-AES256-SHA256",
        "ADH-CAMELLIA256-SHA256",
        "ECDHE-ECDSA-AES128-SHA256",
        "ECDHE-RSA-AES128-SHA256",
        "DHE-RSA-AES128-SHA256",
        "DHE-DSS-AES128-SHA256",
        "ECDHE-ECDSA-CAMELLIA128-SHA256",
        "ECDHE-RSA-CAMELLIA128-SHA256",
        "DHE-RSA-CAMELLIA128-SHA256",
        "DHE-DSS-CAMELLIA128-SHA256",
        "ADH-AES128-SHA256",
        "ADH-CAMELLIA128-SHA256",
        "RSA-PSK-AES256-GCM-SHA384",
        "DHE-PSK-AES256-GCM-SHA384",
        "RSA-PSK-CHACHA20-POLY1305",
        "DHE-PSK-CHACHA20-POLY1305",
        "ECDHE-PSK-CHACHA20-POLY1305",
        "DHE-PSK-AES256-CCM",
        "RSA-PSK-ARIA256-GCM-SHA384",
        "DHE-PSK-ARIA256-GCM-SHA384",
        "AES256-GCM-SHA384",
        "AES256-CCM",
        "ARIA256-GCM-SHA384",
        "PSK-AES256-GCM-SHA384",
        "PSK-CHACHA20-POLY1305",
        "PSK-AES256-CCM",
        "PSK-ARIA256-GCM-SHA384",
        "RSA-PSK-AES128-GCM-SHA256",
        "DHE-PSK-AES128-GCM-SHA256",
        "DHE-PSK-AES128-CCM",
        "RSA-PSK-ARIA128-GCM-SHA256",
        "DHE-PSK-ARIA128-GCM-SHA256",
        "AES128-GCM-SHA256",
        "AES128-CCM",
        "ARIA128-GCM-SHA256",
        "PSK-AES128-GCM-SHA256",
        "PSK-AES128-CCM",
        "PSK-ARIA128-GCM-SHA256",
        "DHE-PSK-AES256-CCM8",
        "DHE-PSK-AES128-CCM8",
        "AES256-CCM8",
        "AES128-CCM8",
        "PSK-AES256-CCM8",
        "PSK-AES128-CCM8",
        "AES256-SHA256",
        "CAMELLIA256-SHA256",
        "AES128-SHA256",
        "CAMELLIA128-SHA256",
    ],
    "TLSv1": [
        "ECDHE-ECDSA-AES256-SHA",
        "ECDHE-RSA-AES256-SHA",
        "AECDH-AES256-SHA",
        "ECDHE-ECDSA-AES128-SHA",
        "ECDHE-RSA-AES128-SHA",
        "AECDH-AES128-SHA",
        "ECDHE-PSK-AES256-CBC-SHA384",
        "ECDHE-PSK-AES256-CBC-SHA",
        "RSA-PSK-AES256-CBC-SHA384",
        "DHE-PSK-AES256-CBC-SHA384",
        "ECDHE-PSK-CAMELLIA256-SHA384",
        "RSA-PSK-CAMELLIA256-SHA384",
        "DHE-PSK-CAMELLIA256-SHA384",
        "PSK-AES256-CBC-SHA384",
        "PSK-CAMELLIA256-SHA384",
        "ECDHE-PSK-AES128-CBC-SHA256",
        "ECDHE-PSK-AES128-CBC-SHA",
        "RSA-PSK-AES128-CBC-SHA256",
        "DHE-PSK-AES128-CBC-SHA256",
        "ECDHE-PSK-CAMELLIA128-SHA256",
        "RSA-PSK-CAMELLIA128-SHA256",
        "DHE-PSK-CAMELLIA128-SHA256",
        "PSK-AES128-CBC-SHA256",
        "PSK-CAMELLIA128-SHA256",
    ],
}

_OPENSSL_NEGOTIATED_CIPHER = re.compile(
    r"(?:^|\n)Cipher is (?!\(NONE\))(?P<cipher>\S+)",
    re.IGNORECASE,
)

CIPHERS_PROTOCOL_TLS_VERSION = {
    "TLSv1.3": "1.3",
    "TLSv1.2": "1.2",
    "TLSv1": "1.0",
}

FIPS_REJECTED_PROTOCOL_CASES = (
    {"name": "TLS 1.0 protocol", "tls_version": "1.0", "cipher_suite": None},
    {"name": "TLS 1.1 protocol", "tls_version": "1.1", "cipher_suite": None},
    {"name": "TLS 1.2 protocol", "tls_version": "1.2", "cipher_suite": None},
)


def fips_rejected_cipher_cases_from_ciphers_by_protocol():
    """Every cipher in ciphers_by_protocol except approved TLS 1.3 suites."""
    cases = []
    for protocol, tls_version in CIPHERS_PROTOCOL_TLS_VERSION.items():
        for cipher in ciphers_by_protocol[protocol]:
            if protocol == "TLSv1.3" and cipher in approved_tls1_3_ciphers:
                continue
            cases.append({
                "name": f"TLS {tls_version} {cipher}",
                "tls_version": tls_version,
                "cipher_suite": cipher,
            })
    return tuple(cases)


FIPS_LISTENER_REJECTED_TLS_CASES = (
    *FIPS_REJECTED_PROTOCOL_CASES,
    *fips_rejected_cipher_cases_from_ciphers_by_protocol(),
)

# clickhouse-backup's Go FIPS runtime pins ciphers but keeps stdlib default
# MinVersion (TLS 1.2), so at TLS 1.2 it ACCEPTS any FIPS-approved suite it can
# negotiate: bare `-tls1_2` (default → an approved suite) and any explicit cipher
# in approved_tls1_2_ciphers (the RSA server cert negotiates the ECDHE-RSA ones;
# the ECDHE-ECDSA ones fail as "no shared cipher" but are excluded too so the sweep
# stays cert-agnostic). Every non-approved 1.2 cipher and all legacy protocols
# (1.0/1.1) stay in the rejected set.
FIPS_BACKUP_LISTENER_REJECTED_TLS_CASES = tuple(
    case for case in FIPS_LISTENER_REJECTED_TLS_CASES
    if not (
        case["tls_version"] == "1.2"
        and (case["cipher_suite"] is None or case["cipher_suite"] in approved_tls1_2_ciphers)
    )
)

FIPS_APPROVED_TLS13_CIPHER_CASES = tuple(
    {
        "name": f"TLS 1.3 {cipher}",
        "tls_version": "1.3",
        "cipher_suite": cipher,
    }
    for cipher in approved_tls1_3_ciphers
)

FIPS_OPERATOR_APPROVED_TLS13_CIPHER = "TLS_AES_256_GCM_SHA384"
FIPS_OPERATOR_APPROVED_TLS13_CIPHER_SUITES = ":".join(approved_tls1_3_ciphers)

OPERATOR_CONTAINER_TLS_FAILURE_NEEDLES = (
    "handshake failure",
    "no shared cipher",
    "alert handshake failure",
    "TLS connect error",
    "HTTP:000",
)

# ---------------------------------------------------------------------------
# Build verification
# ---------------------------------------------------------------------------

@TestStep(Finally)
def cleanup_fips_extract_dir(self):
    extract_dir = self.context.fips_extract_dir
    if extract_dir and os.path.isdir(extract_dir):
        shutil.rmtree(extract_dir, ignore_errors=True)


@TestStep(Given)
def fips_extract_shipped_binaries(self):
    """Extract operator binaries from shipped container images (distroless-safe).

    Follows docs/fips_evidence_verification.md §5: ``docker create`` +
    ``docker cp`` to the host, then ``go version -m`` / ``--fips-info`` locally.
    """
    operator_image = (
        f"{self.context.operator_docker_repo}:"
        f"{self.context.operator_version}"
    )
    metrics_exporter_image = (
        f"{self.context.metrics_exporter_docker_repo}:"
        f"{self.context.operator_version}"
    )

    # Concurrent FIPS tests all extract via the host docker daemon at once; under that
    # contention the docker create/cp calls intermittently fail mid-extraction (seen as
    # a transient IndexError under POOL_SIZE=25). Retry the whole extraction — each
    # attempt uses a fresh tempdir + uuid-suffixed containers, so it is idempotent.
    attempts = 4
    for attempt in range(1, attempts + 1):
        extract_dir = tempfile.mkdtemp(prefix="fips-shipped-bin-")
        op_bin = os.path.join(extract_dir, "clickhouse-operator")
        me_bin = os.path.join(extract_dir, "metrics-exporter")
        suffix = uuid.uuid1().hex[:8]
        try:
            for image, image_path, dest, label in (
                (operator_image, "/clickhouse-operator", op_bin, f"cho-verify-{suffix}"),
                (
                    metrics_exporter_image,
                    "/metrics-exporter",
                    me_bin,
                    f"me-verify-{suffix}",
                ),
            ):
                container_name = shlex.quote(label)
                # create is inside the try so a create that fails or times out still
                # gets a rm: the daemon may hold the container even when we gave up on
                # it, and the next attempt uses a fresh suffix that would never reclaim
                # it. -f because rm of a merely-created container still needs it gone.
                try:
                    kubectl.run_host_cmd(f"docker create --name {container_name} {shlex.quote(image)}")
                    kubectl.run_host_cmd(
                        f"docker cp {container_name}:{shlex.quote(image_path)} {shlex.quote(dest)}"
                    )
                finally:
                    kubectl.run_host_cmd(f"docker rm -f {container_name}", ok_to_fail=True)
                os.chmod(dest, 0o755)
            break
        except Exception as exc:
            shutil.rmtree(extract_dir, ignore_errors=True)
            if attempt == attempts:
                raise
            note(f"FIPS binary extraction failed ({type(exc).__name__}: {exc}); retry {attempt}/{attempts - 1}")
            time.sleep(attempt * 3)

    self.context.fips_extract_dir = extract_dir
    self.context.fips_op_bin = op_bin
    self.context.fips_me_bin = me_bin
    self.context.cleanup(cleanup_fips_extract_dir)

    note(f"extracted {operator_image} -> {op_bin}")
    note(f"extracted {metrics_exporter_image} -> {me_bin}")


@TestStep(Then)
def check_binary_go_version(self, binary_path, version):
    """Check binary GOFIPS140 metadata via ``go version -m``."""
    build_info = kubectl.run_shell(f"go version -m {shlex.quote(binary_path)}")
    assert version in build_info, error(
        f"{binary_path}: {version} not found in go version -m output"
    )


@TestStep(Given)
def run_fips_info(self, binary_path, env=None):
    """Run binary --fips-info and parse YAML output."""
    env = env or {}
    env_prefix = " ".join(f'{k}="{v}"' for k, v in env.items())
    cmd = f"{env_prefix} {shlex.quote(binary_path)} --fips-info".strip()
    return yaml.safe_load(kubectl.run_shell(cmd))


@TestStep(Then)
def check_fips_info_values(
    self,
    binary_path,
    godebug_runtime=None,
    binary="clickhouse-operator",
    version=None,
    gofips_version="v1.0.0",
    godebug_default="fips140=on",
):
    """Check parsed --fips-info output."""
    # The --fips-info "version" is the build-baked release version, not the
    # image tag. Default to the release-file value resolved in set_settings so
    # this never compares against a stale hardcode or the OPERATOR_VERSION tag.
    if version is None:
        version = self.context.release_version
    env = {}
    if godebug_runtime is not None:
        env["GODEBUG"] = "" if godebug_runtime == "" else f"fips140={godebug_runtime}"

    fips_info = run_fips_info(binary_path=binary_path, env=env)

    godebug_fips_expected = {
        None: (True, False),
        "": (True, False),
        "off": (False, False),
        "on": (True, False),
        "only": (True, True),
    }
    if godebug_runtime not in godebug_fips_expected:
        raise ValueError(f"unsupported GODEBUG fips140 mode: {godebug_runtime}")

    expected_enabled, expected_enforced = godebug_fips_expected[godebug_runtime]

    assert fips_info["binary"] == binary, error()
    assert fips_info["version"] == version, error()
    assert fips_info["git_sha"], error()
    assert fips_info["built_at"], error()
    assert fips_info["go_version"].startswith("go"), error()
    assert fips_info["goos"] == "linux", error()
    assert fips_info["goarch"] == "amd64", error()
    assert "fips_module" in fips_info, error()
    assert fips_info["fips_module"]["enabled"] is expected_enabled, error()
    assert fips_info["fips_module"]["enforced"] is expected_enforced, error()
    assert fips_info["fips_module"]["version"] == gofips_version, error()
    assert "GOFIPS140=" in fips_info["fips_module"]["build_setting"], error()
    assert "godebug" in fips_info, error()

    expected_runtime_env = (
        "" if godebug_runtime in (None, "") else f"fips140={godebug_runtime}"
    )
    assert fips_info["godebug"]["runtime_env"] == expected_runtime_env, error()
    assert fips_info["godebug"]["default"] == godebug_default, error()


@TestStep(Then)
def check_fips_runtime_modes(
    self,
    binary_path,
    binary,
    version,
    gofips_version="v1.0.0",
    godebug_default="fips140=on",
):
    """Check all supported runtime GODEBUG FIPS modes."""
    runtime_cases = (
        ("unset", None),
        ("empty", ""),
        ("off", "off"),
        ("on", "on"),
        ("only", "only"),
    )

    for name, mode in runtime_cases:
        with By(f"GODEBUG mode is {name}"):
            check_fips_info_values(
                binary_path=binary_path,
                binary=binary,
                godebug_runtime=mode,
                version=version,
                gofips_version=gofips_version,
                godebug_default=godebug_default,
            )


# ---------------------------------------------------------------------------
# Operator startup logs
# ---------------------------------------------------------------------------

@TestStep(When)
def get_container_logs(self, pod, container, ns):
    """Return the tail logs of the given container."""
    return kubectl.launch(f"logs {pod} -c {container} --tail=4000", ns=ns)


@TestStep(Then)
def fips_startup_banner_ok(self, container, logs, chopconf_enforced="true"):
    """Assert the FIPS startup banner is present in logs."""
    fips140_mode = current().context.fips140_mode
    module_active = "false" if fips140_mode == "off" else "true"
    runtime_enforced = "true" if fips140_mode == "only" else "false"

    expected_banner = (
        f"FIPS: "
        f"chopconf.fips.enforced={chopconf_enforced} "
        f"build.linked=true "
        f"module.active={module_active} "
        f"runtime.enforced={runtime_enforced} "
        f"module=v1.0.0"
    )

    if expected_banner not in logs:
        fips_lines = "\n".join(line for line in logs.splitlines() if "FIPS" in line)
        assert False, error(
            f"{container}: expected FIPS banner not found:\n{expected_banner}\n\n"
            f"grep FIPS:\n{fips_lines or '(no matching lines)'}"
        )


@TestStep(Then)
def fips_assert_minversion13_coerced_in_logs(self, logs):
    """Assert chopconf minVersion 1.2 was coerced to 1.3 for all TLS clients."""

    coercion_lines = (
        "FIPS strict: coerced security.clickhouse.tls.minVersion: 1.2 → 1.3",
        "FIPS strict: coerced security.zookeeper.tls.minVersion: 1.2 → 1.3",
        "FIPS strict: coerced security.kubernetes.tls.minVersion: 1.2 → 1.3",
    )

    for line in coercion_lines:
        assert line in logs, error(
            f"expected FIPS minVersion coercion log line not found:\n{line}"
        )


@TestStep(Then)
def fips_assert_coercion_lines_in_logs(self, logs, expected_lines):
    """Assert operator logs contain each expected FIPS coercion line."""
    for line in expected_lines:
        assert line in logs, error(
            f"expected FIPS coercion log line not found:\n{line}"
        )


@TestStep(Then)
def fips_assert_fips_enforced_coercion_in_logs(self, logs):
    """Assert fips.enforced coerces verify, minVersion, and IPC mode in operator logs."""
    fips_assert_minversion13_coerced_in_logs(logs=logs)
    fips_assert_coercion_lines_in_logs(
        logs=logs,
        expected_lines=(
            "FIPS strict: coerced security.clickhouse.tls.verify: None → Strict",
            "FIPS strict: coerced security.zookeeper.tls.verify: None → Strict",
            "FIPS strict: coerced security.kubernetes.tls.verify: None → Strict",
            "FIPS strict: coerced security.ipc.mode: Plain → Secure",
        ),
    )


@TestStep(When)
def fips_apply_manifest_raw(self, manifest_path):
    """Apply a CHI/CHK manifest without waiting for reconcile."""
    kubectl.apply(util.get_full_path(manifest_path))


@TestStep(Then)
def fips_assert_chi_aborted(
    self,
    chi,
    reason=None,
    expect_no_sts=False,
    expect_reason_leading=False,
):
    """Wait for CHI Aborted status and optionally assert reason and no StatefulSet."""
    kubectl.wait_chi_status(chi, "Aborted")
    errors = kubectl.get_field("chi", chi, ".status.errors")
    if reason:
        assert reason in errors, error(
            f"expected [{reason}] in status.errors, got {errors}"
        )
        if expect_reason_leading:
            stripped = errors.strip().lstrip("[").lstrip()
            assert errors.strip().startswith(f"[{reason}]") or stripped.startswith(
                f"{reason}]"
            ), error(
                f"errors must start with [{reason}] prefix, got {errors!r}"
            )
    if expect_no_sts:
        sts_count = kubectl.get_count(
            "sts", label=f"clickhouse.altinity.com/chi={chi}"
        )
        assert sts_count == 0, error(
            f"expected no StatefulSet for aborted CHI {chi}, got {sts_count}"
        )


@TestStep(Then)
def fips_assert_chk_aborted(self, chk, reason=None, expect_no_sts=False):
    """Wait for CHK Aborted status and optionally assert reason and no StatefulSet."""
    kubectl.wait_chk_status(chk, "Aborted")
    errors = kubectl.get_field("chk", chk, ".status.errors")
    if reason:
        assert reason in errors, error(
            f"expected [{reason}] in status.errors, got {errors}"
        )
    if expect_no_sts:
        sts_count = kubectl.get_count(
            "sts", label=f"clickhouse-keeper.altinity.com/chk={chk}"
        )
        assert sts_count == 0, error(
            f"expected no StatefulSet for aborted CHK {chk}, got {sts_count}"
        )


@TestStep(Then)
def fips_assert_chi_admitted(self, chi, reason="FIPSImagePolicyViolation"):
    """Wait for at least one StatefulSet and assert no image-policy violation.

    Used for acceptance tests where the image may not be pullable but the
    operator must not abort the CHI with ``reason``.
    """
    kubectl.wait_object(
        "sts",
        "",
        label=f"-l clickhouse.altinity.com/chi={chi}",
        count=1,
    )
    errors = kubectl.get_field("chi", chi, ".status.errors")
    assert reason not in errors, error(
        f"unexpected {reason} in status.errors, got {errors}"
    )


# ---------------------------------------------------------------------------
# TLS secrets
# ---------------------------------------------------------------------------

@TestStep(Given)
def create_tls_secret_for_fips_hosts(
    self,
    chi=None,
    chk=None,
    secret_name="clickhouse-certs",
    replicas=2,
    pod_hostnames=None,
    extra_dns_names=None,
):
    """Create a TLS secret whose SANs match this test namespace's pod DNS names."""
    ns = self.context.test_namespace
    certs_dir = tempfile.mkdtemp(prefix=f"{secret_name}-{ns}-")

    ca_key = os.path.join(certs_dir, "ca.key")
    ca_crt = os.path.join(certs_dir, "ca.crt")
    server_key = os.path.join(certs_dir, "server.key")
    server_csr = os.path.join(certs_dir, "server.csr")
    server_crt = os.path.join(certs_dir, "server.crt")
    openssl_cnf = os.path.join(certs_dir, "openssl.cnf")
    dhparam = os.path.join(certs_dir, "dhparam.pem")

    with open(util.get_full_path("manifests/secret/clickhouse-certs.yaml"), encoding="utf-8") as f:
        dhparam_content = yaml.safe_load(f)["stringData"]["dhparam.pem"]
    with open(dhparam, "w", encoding="utf-8") as f:
        f.write(dhparam_content)

    dns_suffixes = ("", f".{ns}", f".{ns}.svc", f".{ns}.svc.cluster.local")
    dns_names = ["localhost", "clickhouse", "clickhouse1", f"*.{ns}.svc.cluster.local"]

    if pod_hostnames:
        for host in pod_hostnames:
            for suffix in dns_suffixes:
                dns_names.append(f"{host}{suffix}")
    else:
        assert chi and chk, error(
            "create_tls_secret_for_fips_hosts requires chi and chk "
            "when pod_hostnames is not set"
        )
        for replica in range(replicas):
            for host in (
                f"chi-{chi}-default-0-{replica}",
                f"chk-{chk}-keeper-0-{replica}",
            ):
                for suffix in dns_suffixes:
                    dns_names.append(f"{host}{suffix}")

    if extra_dns_names:
        dns_names.extend(extra_dns_names)

    san_entries = ["IP.1 = 127.0.0.1"]
    san_entries.extend(
        f"DNS.{i} = {name}" for i, name in enumerate(dns_names, start=1)
    )

    with open(openssl_cnf, "w", encoding="utf-8") as f:
        f.write(
            "\n".join(
                [
                    "[req]",
                    "distinguished_name = dn",
                    "req_extensions = v3_req",
                    "prompt = no",
                    "",
                    "[dn]",
                    "CN = clickhouse-fips-test",
                    "",
                    "[v3_req]",
                    "basicConstraints = CA:FALSE",
                    "keyUsage = digitalSignature, keyEncipherment",
                    "extendedKeyUsage = serverAuth, clientAuth",
                    "subjectAltName = @alt_names",
                    "",
                    "[alt_names]",
                    *san_entries,
                    "",
                ]
            )
        )

    commands = (
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-days", "365", "-subj", "/CN=clickhouse-fips-test-ca",
            "-keyout", ca_key, "-out", ca_crt,
        ],
        [
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
            "-keyout", server_key, "-out", server_csr,
            "-config", openssl_cnf,
        ],
        [
            "openssl", "x509", "-req", "-in", server_csr,
            "-CA", ca_crt, "-CAkey", ca_key, "-CAcreateserial",
            "-out", server_crt, "-days", "365",
            "-extensions", "v3_req", "-extfile", openssl_cnf,
        ],
    )

    for command in commands:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, error(
            "failed to generate FIPS TLS test certificate\n"
            f"command: {' '.join(shlex.quote(part) for part in command)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    kubectl.launch(
        f"create secret generic {secret_name} "
        f"--from-file=server.crt={shlex.quote(server_crt)} "
        f"--from-file=server.key={shlex.quote(server_key)} "
        f"--from-file=ca.crt={shlex.quote(ca_crt)} "
        f"--from-file=dhparam.pem={shlex.quote(dhparam)}",
        ns=ns,
    )

    self.context.tls = {
        "secret_name": secret_name,
        "ca_crt": ca_crt,
        "server_crt": server_crt,
        "server_key": server_key,
        "dhparam": dhparam,
        "certs_dir": certs_dir,
        "dns_names": dns_names,
    }


# ---------------------------------------------------------------------------
# External ClickHouse client (in-cluster pod, same pattern as test-034 / test-058)
# ---------------------------------------------------------------------------

_FIPS_CLIENT_MANIFEST = "manifests/chi/fips-client.yaml"
_FIPS_CLIENT_POD = "fips-client"
_FIPS_CLIENT_CONFIGMAP = "fips-client-config"

_FIPS_OPENSSL_MANIFEST = "manifests/chi/fips-openssl.yaml"
_FIPS_OPENSSL_POD = "fips-openssl"
_FIPS_OPENSSL_CA_FILE = "/certs/ca.crt"


def _chi_service_host_from_pod(pod):
    """Map CHI/CHK pod name to its headless Service host (strip STS ordinal)."""
    return re.sub(r"-\d+$", "", pod)


def ensure_fips_openssl_pod(ns=None, secret_name=None):
    """Ensure the in-cluster OpenSSL probe Pod is Running (idempotent).

    CHI/CHK images do not ship an openssl CLI; host openssl is often LibreSSL.
    Namespace teardown removes the pod — no separate Finally required.
    """
    ctx = current().context
    ns = ns or ctx.test_namespace
    secret_name = (
        secret_name
        or getattr(ctx, "tls", {}).get("secret_name")
        or "clickhouse-certs"
    )

    existing = kubectl.get("pod", _FIPS_OPENSSL_POD, ns=ns, ok_to_fail=True)
    if existing and (existing.get("status") or {}).get("phase") == "Running":
        return _FIPS_OPENSSL_POD

    manifest_host = util.get_full_path(_FIPS_OPENSSL_MANIFEST)
    apply_path = util.get_full_path(_FIPS_OPENSSL_MANIFEST, lookup_in_host=False)
    tmp_path = None

    if secret_name != "clickhouse-certs":
        doc = yaml.safe_load(open(manifest_host, encoding="utf-8"))
        for vol in doc["spec"]["volumes"]:
            if vol.get("name") == "cert" and "secret" in vol:
                vol["secret"]["secretName"] = secret_name
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)
            tmp_path = f.name
            apply_path = tmp_path

    try:
        kubectl.launch(
            f"delete pod {_FIPS_OPENSSL_POD} --ignore-not-found",
            ns=ns,
            ok_to_fail=True,
        )
        kubectl.apply(apply_path, ns=ns)
    finally:
        if tmp_path:
            os.unlink(tmp_path)

    kubectl.wait_pod_status(_FIPS_OPENSSL_POD, "Running", ns=ns)
    note(f"openssl probe pod ready: {_FIPS_OPENSSL_POD}")
    return _FIPS_OPENSSL_POD


@TestStep(Given)
def start_external_ch_container(self, ns=None, cipher_suites=None, secret_name=None):
    """Start the in-cluster FIPS clickhouse-client Pod from ``fips-client.yaml``.

    The step name is kept for call-site compatibility; this applies a Pod + ConfigMap
    (test-034 / test-058 style), not a host Docker container.
    """
    ns = ns or self.context.test_namespace
    secret_name = (
        secret_name
        or getattr(self.context, "tls", {}).get("secret_name")
        or "clickhouse-certs"
    )
    manifest_host = util.get_full_path(_FIPS_CLIENT_MANIFEST)
    apply_path = util.get_full_path(_FIPS_CLIENT_MANIFEST, lookup_in_host=False)
    tmp_path = None

    if cipher_suites or secret_name != "clickhouse-certs":
        docs = list(yaml.safe_load_all(open(manifest_host, encoding="utf-8")))
        for doc in docs:
            if not doc:
                continue
            if doc.get("kind") == "ConfigMap" and cipher_suites:
                xml = doc["data"]["config.xml"]
                needle = "<verificationMode>strict</verificationMode>"
                insert = (
                    f"{needle}\n"
                    f"                <cipherSuites>{':'.join(cipher_suites)}</cipherSuites>"
                )
                doc["data"]["config.xml"] = xml.replace(needle, insert, 1)
            if doc.get("kind") == "Pod" and secret_name != "clickhouse-certs":
                for vol in doc["spec"]["volumes"]:
                    if vol.get("name") == "cert" and "secret" in vol:
                        vol["secret"]["secretName"] = secret_name
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            yaml.dump_all(docs, f, default_flow_style=False, sort_keys=False)
            tmp_path = f.name
            apply_path = tmp_path

    try:
        kubectl.launch(f"delete pod {_FIPS_CLIENT_POD} --ignore-not-found", ns=ns, ok_to_fail=True)
        kubectl.launch(
            f"delete configmap {_FIPS_CLIENT_CONFIGMAP} --ignore-not-found",
            ns=ns,
            ok_to_fail=True,
        )
        kubectl.apply(apply_path, ns=ns)
    finally:
        if tmp_path:
            os.unlink(tmp_path)

    kubectl.wait_pod_status(_FIPS_CLIENT_POD, "Running", ns=ns)
    note(f"external ClickHouse client pod started: {_FIPS_CLIENT_POD}")
    self.context.external_chi_container = _FIPS_CLIENT_POD
    self.context.external_chi_client_config = _FIPS_CLIENT_CONFIGMAP

    yield

    with Finally("stop external ClickHouse client pod"):
        stop_external_ch_container()


@TestStep(Finally)
def stop_external_ch_container(self):
    """Remove the FIPS clickhouse-client Pod and ConfigMap from ``fips-client.yaml``."""
    ns = getattr(self.context, "test_namespace", None)
    if not ns:
        return
    kubectl.launch(f"delete pod {_FIPS_CLIENT_POD} --ignore-not-found", ns=ns, ok_to_fail=True)
    kubectl.launch(
        f"delete configmap {_FIPS_CLIENT_CONFIGMAP} --ignore-not-found",
        ns=ns,
        ok_to_fail=True,
    )


@TestStep(When)
def fips_ch_external_secure_query(self, pod, sql, ns=None):
    """Run a ClickHouse query from the FIPS client Pod over in-cluster TLS.

    Requires ``start_external_ch_container`` at scenario start. Connects to the
    CHI/CHK headless Service derived from ``pod`` (same pattern as test-034 / test-058).
    """
    ns = ns or self.context.test_namespace
    client_pod = self.context.external_chi_container
    host = _chi_service_host_from_pod(pod)

    # Use run_shell (not launch) so shlex-quoted SQL with spaces survives.
    out = kubectl.run_shell(
        f"{current().context.kubectl_cmd} -n {shlex.quote(ns)} "
        f"exec {shlex.quote(client_pod)} -c clickhouse-client -- "
        f"clickhouse-client --secure --host {shlex.quote(host)} --port 9440 "
        f"--query {shlex.quote(sql)}"
    )
    return out.strip()


# ---------------------------------------------------------------------------
# Manifest edit / apply
# ---------------------------------------------------------------------------

@TestStep(Given)
def fips_edit_manifest(self, source_manifest, replicas_count=None,  cipher_suites=None, kind="chi"):
    """Load a CHI/CHK manifest, patch ``replicasCount``, write a temp copy."""
    source_path = util.get_full_path(source_manifest)
    with open(source_path, encoding="utf-8") as f:
        manifest = yaml.safe_load(f)

    if replicas_count is not None:
        manifest["spec"]["configuration"]["clusters"][0]["layout"]["replicasCount"] = (
            replicas_count
        )
    if cipher_suites is not None:
        xml = manifest["spec"]["configuration"]["files"]["openssl.xml"]

        old = (
            "TLS_AES_128_GCM_SHA256:"
            "TLS_AES_256_GCM_SHA384"
        )

        manifest["spec"]["configuration"]["files"]["openssl.xml"] = (
            xml.replace(
                old,
                ":".join(cipher_suites),
            )
        )

    fd, temp_path = tempfile.mkstemp(suffix=".yaml", prefix=f"fips-{kind}-")
    os.close(fd)
    with open(temp_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)

    note(f"edited manifest written to {temp_path}")
    if replicas_count is not None:
        note(f"  replicasCount={replicas_count}")


    return temp_path


@TestStep(When)
def fips_apply_manifest(
    self,
    manifest_path,
    replica_count=None,
    kind="chi",
    apply_templates=None,
    timeout=None,
    expected_status=None,
):
    """Apply a CHI/CHK manifest and wait for reconcile plus pod readiness."""
    if kind == "chi":
        default_timeout = 900
    elif kind == "chk":
        default_timeout = 600
    else:
        raise ValueError(f"unsupported manifest kind: {kind}")

    check = {
        "do_not_delete": 1,
    }
    if replica_count is not None:
        check["pod_count"] = replica_count
    if expected_status is not None:
        if kind == "chi":
            check["chi_status"] = expected_status
        elif kind == "chk":
            check["chk_status"] = expected_status
    if apply_templates:
        check["apply_templates"] = apply_templates

    kubectl.create_and_check(
        manifest=manifest_path,
        kind=kind,
        check=check,
        timeout=timeout or default_timeout,
    )


# ---------------------------------------------------------------------------
# Dataplane health and replication
# ---------------------------------------------------------------------------

@TestStep(Given)
def get_binary_version(self, pod, binary, container=None, ns=None):
    """Run ``<binary> --version`` inside a pod and return the output."""
    ns = ns or self.context.test_namespace
    container_arg = f" -c {container}" if container else ""
    return kubectl.launch(
        f"exec {pod}{container_arg} -- {binary} --version",
        ns=ns,
    )

@TestStep(Then)
def check_fips_binary_version(self, pod, binary, container=None, ns=None):
    """Run ``<binary> --version`` inside a pod and check it contains altinityfips tag."""

    version = get_binary_version(pod=pod, binary=binary, container=container, ns=ns)

    assert "altinityfips" in version, error(
        f"{pod}: expected altinityfips in {binary} version, got {version!r}"
    )

def translate_tcp_port_output(raw):
    """Translates raw output to a readable set of ports"""
    ports = set()

    for line in raw.splitlines():
        cols = line.split()
        if len(cols) < 4 or cols[0] == "sl" or cols[3] != "0A":
            continue
        try:
            ports.add(int(cols[1].split(":")[1], 16))
        except (IndexError, ValueError):
            continue

    return ports

@TestStep(When)
def fips_read_listening_ports(
    self,
    pod,
    container,
    ns=None,
    debug=False,
    target=None,
):
    """Return TCP ports in LISTEN state from /proc/net/tcp and /proc/net/tcp6."""

    ns = ns or self.context.test_namespace

    if debug:
        target = target or container
        raw = kubectl.launch(
            f"debug {pod} "
            f"--image=busybox:1.36 "
            f"--target={target} "
            f"--attach "
            f"-- sh -c 'cat /proc/1/net/tcp /proc/1/net/tcp6'",
            ns=ns,
        )
    else:
        raw = kubectl.launch(
            f"exec {pod} -c {container} -- "
            f"sh -c 'cat /proc/net/tcp /proc/net/tcp6'",
            ns=ns,
        )

    return translate_tcp_port_output(raw=raw)


@TestStep(Then)
def fips_assert_only_expected_ports(
    self,
    pod,
    expected,
    container="clickhouse",
    ns=None,
    max_iters=1,
    sleep_s=2,
    debug=False
):
    """Assert the container listens on expected required ports."""

    ports = set()

    for attempt in range(max_iters):
        ports = fips_read_listening_ports(
            pod=pod,
            container=container,
            ns=ns,
            debug=debug
        )

        note(f"listening ports on {pod}: {sorted(ports)}")

        missing = expected - ports
        unexpected = ports - expected

        if not missing and not unexpected:
            return

        if attempt + 1 < max_iters:
            note(
                f"{pod}: waiting for TLS listeners "
                f"(missing={sorted(missing)}, unexpected={sorted(unexpected)}), "
                f"retry in {sleep_s}s"
            )
            time.sleep(sleep_s)

    missing = expected - ports
    assert not missing, error(
        f"{pod}: required {container} TLS ports missing: {sorted(missing)}"
    )

    unexpected = ports - expected
    assert not unexpected, error(
        f"{pod}: unexpected {container} ports listening "
        f"(approved={sorted(expected)}): {sorted(unexpected)}"
    )


@TestStep(When)
def fips_poll_secure_scalar(self, pod, sql, expected, max_iters=30, sleep_s=2):
    """Poll an external secure query until the scalar result matches ``expected``."""
    last = None

    for _ in range(max_iters):
        out = fips_ch_external_secure_query(pod=pod, sql=sql)
        try:
            last = int(out)
        except ValueError:
            last = out

        if last == expected:
            return last

        time.sleep(sleep_s)

    assert last == expected, error(
        f"{pod}: query never returned {expected!r} (last value: {last!r}, "
        f"polled {max_iters} times)"
    )


@TestStep(Then)
def fips_wait_cluster_topology(
    self,
    pod,
    cluster_name,
    replica_count,
    max_iters=30,
    sleep_s=2,
):
    """Poll ``system.clusters`` through an external TLS client until topology converges."""
    fips_poll_secure_scalar(
        pod=pod,
        sql=(
            f"SELECT count() FROM system.clusters "
            f"WHERE cluster = '{cluster_name}'"
        ),
        expected=replica_count,
        max_iters=max_iters,
        sleep_s=sleep_s,
    )
    note(f"{pod} sees {replica_count} hosts in cluster {cluster_name!r}")


def openssl_tls_version_args(tls_version):
    if tls_version == "1.3":
        return ["-tls1_3"]
    if tls_version == "1.2":
        return ["-tls1_2"]
    if tls_version == "1.1":
        return ["-tls1_1"]
    if tls_version == "1.0":
        return ["-tls1"]
    if tls_version == "ssl3":
        return ["-ssl3"]
    if tls_version == "ssl2":
        return ["-ssl2"]

    raise ValueError(f"unsupported TLS/SSL version: {tls_version}")


def openssl_cipher_args(tls_version, cipher_suite):
    # TLS 1.0/1.1 cipher suites are all SHA1/RSA-based and sit below the host
    # openssl's default SECLEVEL=2 floor, so s_server has no cipher it is allowed
    # to offer and aborts the handshake with an internal_error alert (SSL alert 80)
    # instead of the expected protocol_version alert (SSL alert 70). A min-1.3
    # client (the Go FIPS client under test) then sees `remote error: tls: internal
    # error`, which is NOT a rejection marker -> false FAIL. Drop to SECLEVEL=0 for
    # the legacy protocols so the server genuinely offers TLS 1.0/1.1: the min-1.3
    # client now gets the clean `protocol version not supported` alert (already a
    # marker), while a peer that wrongly ACCEPTED the legacy protocol would complete
    # the handshake -- so this preserves the rejection assertion with no false pass.
    if tls_version in ("1.0", "1.1"):
        return ["-cipher", f"{cipher_suite or 'DEFAULT'}@SECLEVEL=0"]

    if not cipher_suite:
        return []

    if tls_version == "1.3":
        return ["-ciphersuites", cipher_suite]

    return ["-cipher", cipher_suite]



def openssl_s_client_negotiated_cipher(output):
    """Return the negotiated cipher name when s_client completed a handshake."""
    match = _OPENSSL_NEGOTIATED_CIPHER.search(output)
    if match:
        return match.group("cipher")
    return None


@TestStep(When)
def fips_run_openssl_s_client_on_pod_port(
    self,
    pod,
    port,
    cipher_suite="TLS_AES_128_GCM_SHA256",
    tls_version="1.3",
    ok_to_fail=False,
    ns=None,
    container=None,
):
    """Run ``openssl s_client`` from the ``fips-openssl`` probe Pod.

    Target is the CHI/CHK Service DNS name for ``pod`` (not host openssl and not
    the workload image — those lack a usable OpenSSL 3 ``s_client``).
    ``container`` is unused (kept for call-site compatibility).
    """
    del container  # probes always run in fips-openssl
    ns = ns or self.context.test_namespace
    target_host = _chi_service_host_from_pod(pod)

    with Given("openssl probe pod is available"):
        openssl_pod = ensure_fips_openssl_pod(ns=ns)

    with When(
        f"openssl s_client from {openssl_pod} to {target_host}:{port}"
    ):
        tls_args = " ".join(shlex.quote(a) for a in openssl_tls_version_args(tls_version))
        cipher_args = " ".join(
            shlex.quote(a) for a in openssl_cipher_args(tls_version, cipher_suite)
        )
        # Feed "Q\n" so s_client exits after the handshake.
        cmd = (
            f"printf 'Q\\n' | {current().context.kubectl_cmd} -n {shlex.quote(ns)} "
            f"exec -i {shlex.quote(openssl_pod)} -- "
            f"openssl s_client "
            f"-connect {shlex.quote(target_host)}:{int(port)} "
            f"-servername localhost "
            f"-CAfile {_FIPS_OPENSSL_CA_FILE} "
            f"-verify_return_error "
            f"{tls_args} {cipher_args}"
        )
        shell = current().context.shell
        result = shell(cmd, timeout=60)
        output = result.output or ""

    if not ok_to_fail:
        with Then("openssl s_client handshake succeeds"):
            assert result.exitcode == 0, error(
                f"{pod}:{port}: openssl s_client failed for "
                f"tls={tls_version}, cipher={cipher_suite}\n"
                f"exit code: {result.exitcode}\n"
                f"output:\n{output}"
            )

    return output

@TestStep(Check)
def fips_assert_rejected_tls_cases_on_endpoint(
    self,
    label,
    pod,
    port,
    rejected_cases,
    ns=None,
):
    """Assert rejected TLS protocol/cipher cases fail on one endpoint."""
    ns = ns or self.context.test_namespace

    for case in rejected_cases:
        with Check(f"{label} {pod}:{port} rejects {case['name']}"):
            output = fips_run_openssl_s_client_on_pod_port(
                pod=pod,
                port=port,
                tls_version=case["tls_version"],
                cipher_suite=case["cipher_suite"],
                ok_to_fail=True,
                ns=ns,
            )

            output_lower = output.lower()

            negotiated_cipher = openssl_s_client_negotiated_cipher(output)
            assert negotiated_cipher is None, error(
                f"{label} {pod}:{port}: server negotiated disallowed {case['name']}\n"
                f"negotiated cipher: {negotiated_cipher}\n"
                f"tls_version={case['tls_version']}\n"
                f"cipher_suite={case['cipher_suite']}\n"
                f"output:\n{output}"
            )

            if case["tls_version"] in ("1.0", "1.1"):
                # Downgrade cases: in-pod openssl may refuse the legacy protocol
                # LOCALLY and never contact the server -> a client-side refusal
                # must NOT count as a pass. Require a SERVER-originated alert;
                # else skip (an actually-accepting server is already caught above
                # by the negotiated_cipher assert).
                if not any(m in output_lower for m in TLS_SERVER_REJECT_MARKERS):
                    skip(
                        f"{label} {pod}:{port}: no server-side rejection alert for "
                        f"{case['name']} - pod openssl likely refused locally "
                        f"(no ClientHello reached the server)\noutput:\n{output}"
                    )
            else:
                assert any(
                    marker.lower() in output_lower
                    for marker in TLS_REJECT_MARKERS
                ), error(
                    f"{label} {pod}:{port}: expected TLS rejection for {case['name']}\n"
                    f"tls_version={case['tls_version']}\n"
                    f"cipher_suite={case['cipher_suite']}\n"
                    f"output:\n{output}"
                )


@TestStep(Check)
def fips_assert_all_rejected_tls_cases_on_all_endpoints(
    self,
    chi_pods,
    chk_pods,
    ns=None,
):
    """Assert all rejected TLS cases fail on every FIPS TLS endpoint."""
    ns = ns or self.context.test_namespace

    endpoints = (
        ("ClickHouse HTTPS", chi_pods[0], 8443, FIPS_LISTENER_REJECTED_TLS_CASES),
        ("ClickHouse native TLS", chi_pods[0], 9440, FIPS_LISTENER_REJECTED_TLS_CASES),
        ("ClickHouse interserver HTTPS", chi_pods[0], 9010, FIPS_LISTENER_REJECTED_TLS_CASES),
        ("Keeper secure client", chk_pods[0], 2281, FIPS_LISTENER_REJECTED_TLS_CASES),
        ("Backup API HTTPS", chi_pods[0], 7171, FIPS_BACKUP_LISTENER_REJECTED_TLS_CASES),
    )

    for label, pod, port, endpoint_rejected_cases in endpoints:
        fips_assert_rejected_tls_cases_on_endpoint(
            label=label,
            pod=pod,
            port=port,
            rejected_cases=endpoint_rejected_cases,
            ns=ns,
        )


@TestStep(Then)
def fips_assert_aes256_tls13_probes(
    self,
    chi_pods,
    chk_pods,
    ns=None,
):
    """Assert approved TLS 1.3 AES-256-GCM cipher negotiates on FIPS TLS listeners."""
    ns = ns or self.context.test_namespace
    approved_cipher = "TLS_AES_256_GCM_SHA384"

    endpoints = (
        ("ClickHouse HTTPS", chi_pods[0], 8443),
        ("ClickHouse native TLS", chi_pods[0], 9440),
        ("ClickHouse interserver HTTPS", chi_pods[0], 9010),
        ("Keeper secure client", chk_pods[0], 2281),
        ("Backup API HTTPS", chi_pods[0], 7171),
    )

    for label, pod, port in endpoints:
        with Then(f"{label} {pod}:{port} accepts approved AES-256 TLS 1.3 cipher"):
            output = fips_run_openssl_s_client_on_pod_port(
                pod=pod,
                port=port,
                tls_version="1.3",
                cipher_suite=approved_cipher,
                ok_to_fail=True,
                ns=ns,
            )

            assert f"Cipher is {approved_cipher}" in output, error(
                f"{label} {pod}:{port}: expected {approved_cipher} to negotiate\n"
                f"output:\n{output}"
            )

@TestStep(When)
def fips_curl_pod_port(self, pod, port, path="/", ns=None):
    """Return the HTTP status code from a plain ``curl`` to a pod listener via port-forward."""
    ns = ns or self.context.test_namespace
    local_port = _free_local_port()

    pf = subprocess.Popen(
        [
            "kubectl",
            *self.context.kubectl_context_args,
            "-n", ns,
            "port-forward",
            f"pod/{pod}",
            f"{local_port}:{port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            if pf.poll() is not None:
                out, err = pf.communicate()
                assert False, error(
                    "kubectl port-forward exited early\n"
                    f"stdout:\n{out}\n"
                    f"stderr:\n{err}"
                )

            try:
                socket.create_connection(
                    ("127.0.0.1", int(local_port)), timeout=0.5
                ).close()
                break
            except OSError:
                time.sleep(0.2)
        else:
            assert False, error(
                f"kubectl port-forward to {pod}:{port} "
                f"did not become ready on 127.0.0.1:{local_port}"
            )

        result = subprocess.run(
            [
                "curl", "-sS",
                "-o", "/dev/null",
                "-w", "%{http_code}",
                f"http://127.0.0.1:{local_port}{path}",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, error(
            f"{pod}:{port}{path}: curl failed\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        return result.stdout.strip()

    finally:
        pf.terminate()
        try:
            pf.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pf.kill()


@TestStep(Then)
def check_chi_ports(self, pod, ns=None):
    """TLS positive/negative probes for ClickHouse HTTPS and native ports."""
    ns = ns or self.context.test_namespace
    approved_cipher = "TLS_AES_128_GCM_SHA256"

    for port in (8443, 9010):
        with Then(f"{pod}:{port} accepts approved TLS 1.3 cipher"):
            output = fips_run_openssl_s_client_on_pod_port(
                pod=pod, port=port, cipher_suite=approved_cipher, ns=ns,
            )
            assert f"Cipher is {approved_cipher}" in output, error(
                f"{pod}:{port}: expected approved cipher negotiation\n{output}"
            )

    with And(f"{pod}:9440 accepts approved native TLS query"):
        out = fips_ch_external_secure_query(pod=pod, sql="SELECT 1")
        assert out == "1", error(
            f"{pod}:9440: expected SELECT 1 over native TLS, got {out!r}"
        )



@TestStep(Then)
def check_chk_ports(self, pod, ns=None):
    """TLS and readiness HTTP probes for ClickHouse Keeper listeners."""
    ns = ns or self.context.test_namespace
    approved_cipher = "TLS_AES_128_GCM_SHA256"
    port = 2281

    # raft 9444 doesnt communicate over TLS endpoint
    with Then(f"{pod}:{port} accepts approved TLS 1.3 cipher"):
        output = fips_run_openssl_s_client_on_pod_port(
            pod=pod, port=port, cipher_suite=approved_cipher, ns=ns,
        )
        assert f"Cipher is {approved_cipher}" in output, error(
            f"{pod}:{port}: expected approved cipher negotiation\n{output}"
        )

    with And(f"{pod}:9182/ready accepts plain HTTP"):
        code = fips_curl_pod_port(pod=pod, port=9182, path="/ready", ns=ns)
        assert code == "200", error(
            f"{pod}:9182/ready: expected HTTP 200, got {code!r}"
        )


@TestStep(Then)
def check_backup_ports(self, pod, ns=None):
    """TLS positive/negative probes for the clickhouse-backup HTTPS API."""
    ns = ns or self.context.test_namespace
    approved_cipher = "TLS_AES_128_GCM_SHA256"

    with Then(f"{pod}:7171 accepts approved TLS 1.3 cipher"):
        out = kubectl.launch(
            f"exec {pod} -c clickhouse-backup -- "
            f"sh -c 'curl -sS -o /dev/null -w HTTP:%{{http_code}} "
            f"--cacert /etc/clickhouse-backup/tls/ca.crt "
            f"--tlsv1.3 --tls13-ciphers {approved_cipher} "
            f"https://127.0.0.1:7171/backup/tables'",
            ns=ns,
        )
        assert out == "HTTP:200", error(
            f"{pod}:7171: expected HTTP 200 with approved cipher, got {out!r}"
        )

    with Then(f"{pod}:7171 rejects plaintext requests"):
        out = kubectl.launch(
            f"exec {pod} -c clickhouse-backup -- "
            f"sh -c 'curl -s -o /dev/null -w %{{http_code}} "
            f"http://127.0.0.1:7171/backup/tables'",
            ns=ns,
            ok_to_fail=True,
        )
        assert out != "200", error(
            f"{pod}:7171: expected plaintext HTTP request to be rejected, got {out!r}"
        )

@TestStep(Then)
def check_k8s_api_requires_tls_from_operator_pod(self, ns=None):
    """Assert Kubernetes API :443 rejects plaintext HTTP from operator pod containers."""
    ns = ns or current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)

    for container in ("clickhouse-operator", "metrics-exporter"):
        with Then(f"{container} cannot use plaintext HTTP to Kubernetes API :443"):
            out = kubectl.launch(
                f"exec {pod} -c {container} -- "
                "curl -skv http://kubernetes.default.svc:443",
                ns=ns,
                ok_to_fail=True,
            )

            assert "Client sent an HTTP request to an HTTPS server" in out, error(
                f"{container}: expected Kubernetes API :443 to reject plaintext HTTP\n{out}"
            )

@TestStep(Then)
def check_operator_clickhouse_tls_logs(self, ns=None):
    """Assert operator uses HTTPS/TLS config when communicating with ClickHouse."""
    ns = ns or current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)

    logs = kubectl.launch(
        f"logs {pod} -c clickhouse-operator",
        ns=ns,
    )

    assert "setupTLSAdvanced():TLS setup OK" in logs, error(
        "operator did not log ClickHouse TLS setup"
    )
    assert "verify=Strict minVersion=1.3" in logs, error(
        "operator ClickHouse TLS config is not Strict / TLS 1.3"
    )
    assert "Ping(https://clickhouse_operator:" in logs, error(
        "operator did not log HTTPS ClickHouse ping"
    )
    assert ":8443?tls_config=" in logs, error(
        "operator ClickHouse ping did not use HTTPS port 8443 with TLS config"
    )

@TestStep(Then)
def check_metrics_exporter_discovers_clickhouse_https(self, ns=None):
    """Assert metrics-exporter discovers ClickHouse hosts using HTTPS :8443."""

    ns = ns or current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)

    logs = kubectl.launch(
        f"logs {pod} -c metrics-exporter --tail=4000",
        ns=ns,
    )

    assert '"httpsPort":8443' in logs, error(
        "metrics-exporter did not discover ClickHouse hosts with httpsPort=8443\n"
        f"{logs}"
    )

@TestStep(Then)
def check_clickhouse_uses_secure_keeper_port(self, chi, ns=None):
    """Assert ClickHouse replicas use Keeper secure client port 2281 with secure=yes."""

    ns = ns or current().context.test_namespace
    pods = sorted(kubectl.get_pod_names(chi))

    for pod in pods:
        with Then(f"{pod} connects to Keeper on secure port 2281"):
            logs = kubectl.launch(
                f"logs {pod} -c clickhouse --tail=5000",
                ns=ns,
            )

            assert re.search(r"Connected to ZooKeeper at .+:2281\b", logs), error(
                f"{pod}: ClickHouse did not connect to Keeper on port 2281\n{logs}"
            )


@TestStep(Then)
def check_operator_skips_plaintext_keeper_dial(self, ns=None):
    """Assert operator skips plaintext ZK helper when Keeper ensemble is TLS-only."""
    ns = ns or current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)

    logs = kubectl.launch(
        f"logs {pod} -c clickhouse-operator",
        ns=ns,
    )

    assert 'Port:&2281,Secure:&"yes"' in logs, error(
        "operator logs do not show Keeper configured as secure port 2281"
    )
    assert "Skip ZK root-path ensure" in logs, error(
        "operator did not log skipping ZK root-path ensure"
    )
    assert "ensemble is TLS-only and the operator dial is plaintext" in logs, error(
        "operator did not log that plaintext Keeper dial was skipped for TLS-only ensemble"
    )

@TestStep(Then)
def check_operator_ports(self, ns=None):
    """Plain HTTP probes for operator Prometheus listener ports."""
    ns = ns or current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)

    for port in (9999, 8888):
        with Then(f"operator pod:{port}/metrics accepts plain HTTP"):
            code = fips_curl_pod_port(pod=pod, port=port, path="/metrics", ns=ns)
            assert code == "200", error(
                f"operator pod:{port}/metrics: expected HTTP 200, got {code!r}"
            )


@TestStep(Then)
def run_operator_fips_checks(self):
    """
    Run FIPS validation checks against the operator pod:

    * verify the pod network namespace exposes only expected Prometheus ports
    * verify clickhouse-operator and metrics-exporter emit FIPS startup banners
    * verify metrics ports accept HTTP and reject disapproved TLS handshakes
    """
    ns = current().context.operator_namespace
    pod = kubectl.get_operator_pod(ns=ns)
    expected_ports = {8888, 9999}

    with Then("operator pod exposes only expected listener ports"):
        fips_assert_only_expected_ports(
            pod=pod,
            container="clickhouse-operator",
            ns=ns,
            expected=expected_ports,
            debug=True,
        )

    with And("both containers report the FIPS startup banner"):
        op_logs = get_container_logs(
            pod=pod,
            container="clickhouse-operator",
            ns=ns,
        )
        me_logs = get_container_logs(
            pod=pod,
            container="metrics-exporter",
            ns=ns,
        )
        fips_startup_banner_ok(container="clickhouse-operator", logs=op_logs)
        fips_startup_banner_ok(container="metrics-exporter", logs=me_logs)

    with Then("operator metrics ports accept HTTP and reject disapproved TLS"):
        check_operator_ports(ns=ns)

    with Then("Kubernetes API port 443 requires TLS from operator pod containers"):
        check_k8s_api_requires_tls_from_operator_pod(ns=ns)

@TestStep(Then)
def run_operator_reconcile_fips_checks(self):
    """Run the fips checks after operator already reconciled CHK and CHI."""

    ns = current().context.operator_namespace

    with Then("operator communicates with ClickHouse over HTTPS/TLS"):
        check_operator_clickhouse_tls_logs(ns=ns)

    with And("operator skips plaintext Keeper helper against TLS-only Keeper"):
        check_operator_skips_plaintext_keeper_dial(ns=ns)


@TestStep(Then)
def run_chi_fips_checks(self, workload, replica_count, cluster_name="default"):
    """
    Run FIPS and TLS validation checks against the ClickHouse cluster:

    * wait for the expected cluster topology to become available
    * verify ClickHouse binaries report an Altinity FIPS build
    * verify only approved secure listener ports are exposed
    * verify external TLS connectivity to ClickHouse succeeds
    * verify the server reports a FIPS version string
    * verify operator-generated configuration removes plaintext listeners
    * verify each listener accepts approved TLS and rejects disapproved TLS
    """
    pods = sorted(kubectl.get_pod_names(workload))
    binary = "clickhouse"
    container = "clickhouse"
    expected_ports = {8443, 9440, 9010, 7171}
    pod0 = pods[0]

    note(f"CHI pods: {pods}")
    assert len(pods) == replica_count, error(
        f"expected {replica_count} CHI pods, got {len(pods)}: {pods}"
    )

    with When("I wait for full cluster deployment"):
        fips_wait_cluster_topology(
            pod=pod0,
            cluster_name=cluster_name,
            replica_count=replica_count,
        )

    for pod in pods:
        with Then("check the binary version contains altinityfips tag"):
            check_fips_binary_version(pod=pod, binary=binary, container=container)

        with And("check the container only listens on expected ports"):
            fips_assert_only_expected_ports(
                pod=pod,
                expected=expected_ports,
                container=container,
                max_iters=30,
                sleep_s=2,
            )

        with And("check TLS port behavior on each replica"):
            check_chi_ports(pod=pod)

        with And("operator-generated ClickHouse config removes plaintext ports"):
            check_ports_in_chi_settings(pod=pod)

    with Then("check connection via external secure query"):
        check_external_clickhouse_reports_fips_version(pod=pod0)

    return pods


@TestStep(Then)
def run_chk_fips_checks(self, workload, replica_count):
    """
    Run FIPS and TLS validation checks against the ClickHouse Keeper cluster:

    * verify the expected number of Keeper pods are running
    * verify Keeper binaries report an Altinity FIPS build
    * verify only approved secure listener ports are exposed
    * verify operator-generated configuration removes plaintext listeners
    * verify Raft inter-node communication is configured for TLS
    * verify each listener accepts approved TLS and rejects disapproved TLS
    """
    pods = sorted(kubectl.get_chk_pod_names(workload))
    binary = "clickhouse-keeper"
    container = "clickhouse-keeper"
    expected_ports = {2281, 9444, 9182}

    note(f"CHK pods: {pods}")
    assert len(pods) == replica_count, error(
        f"expected {replica_count} CHK pods, got {len(pods)}: {pods}"
    )

    for pod in pods:
        with Then("check the binary version contains altinityfips tag"):
            check_fips_binary_version(pod=pod, binary=binary, container=container)

        with And("check the container only listens on expected ports"):
            fips_assert_only_expected_ports(
                pod=pod,
                expected=expected_ports,
                container=container,
                max_iters=30,
                sleep_s=2,
            )

        with And("check TLS port behavior on each Keeper node"):
            check_chk_ports(pod=pod)

        with And("operator-generated Keeper config removes plaintext listeners"):
            check_ports_in_chk_settings(pod=pod)

    return pods


@TestStep(Then)
def run_backup_fips_checks(self, workload, replica_count):
    """
    Run FIPS and TLS validation checks against clickhouse-backup sidecars:

    * verify the expected number of CHI pods with backup sidecars are running
    * verify clickhouse-backup binaries report a FIPS build
    * verify only approved secure listener ports are exposed
    * verify each sidecar binary embeds GOFIPS metadata
    * verify the HTTPS API accepts approved TLS and rejects disapproved TLS
    """
    pods = sorted(kubectl.get_pod_names(workload))
    container = "clickhouse-backup"
    expected_ports = {8443, 9440, 9010, 7171}

    note(f"CHI pods with backup sidecar: {pods}")
    assert len(pods) == replica_count, error(
        f"expected {replica_count} CHI pods, got {len(pods)}: {pods}"
    )

    for pod in pods:
        with Then("check the backup binary version contains fips tag"):
            check_backup_fips_binary_version(pod=pod)

        with And("check the sidecar only listens on expected ports"):
            fips_assert_only_expected_ports(
                pod=pod,
                expected=expected_ports,
                container=container,
                max_iters=30,
                sleep_s=2,
            )

        with Then("check TLS port behavior on each backup sidecar"):
            check_backup_ports(pod=pod)

        with And("each sidecar binary embeds GOFIPS metadata"):
            check_clickhouse_backup_embeds_gofips(pod=pod)

        with And("clickhouse-backup TLS config is secure"):
            check_clickhouse_backup_clickhouse_tls_config(pod=pod)

    return pods


@TestStep(Then)
def fips_check_replication_across_replicas(self, chi_pods, table="repl_test"):
    """Verify ReplicatedMergeTree data converges to every replica over TLS."""
    if len(chi_pods) < 2:
        note(f"skipping replication check with {len(chi_pods)} replica(s)")
        return

    pod0 = chi_pods[0]

    with When("a replicated table is created on the cluster"):
        fips_ch_external_secure_query(
            pod=pod0,
            sql=(
                f"CREATE TABLE IF NOT EXISTS {table} ON CLUSTER '{{cluster}}' "
                "(a UInt32) "
                "ENGINE = ReplicatedMergeTree("
                f"'/clickhouse/{{installation}}/{{cluster}}/tables/{{shard}}/{table}', "
                "'{replica}') ORDER BY a"
            ),
        )

    with And("rows are inserted on replica 0"):
        fips_ch_external_secure_query(
            pod=pod0,
            sql=f"INSERT INTO {table} SELECT number FROM numbers(10)",
        )

    with Then("rows are replicated to every other replica over interserver TLS"):
        target = 10
        for pod in chi_pods[1:]:
            fips_poll_secure_scalar(
                pod=pod,
                sql=f"SELECT count() FROM {table}",
                expected=target,
            )


@TestStep(When)
def fips_read_chop_generated_chi_settings(self, pod, container="clickhouse", ns=None):
    """Return the operator-generated ``chop-generated-settings.xml`` from ``pod``."""
    ns = ns or self.context.test_namespace
    return kubectl.launch(
        f"exec {pod} -c {container} -- "
        f"cat /etc/clickhouse-server/config.d/chop-generated-settings.xml",
        ns=ns,
    )


@TestStep(Then)
def check_ports_in_chi_settings(self, pod):
    """Check approved TLS ports and removed plaintext ports in CHI settings."""

    settings_xml = fips_read_chop_generated_chi_settings(pod=pod)
    note(f"chop-generated-settings.xml:\n{settings_xml}")

    assert "<https_port>8443</https_port>" in settings_xml, error(
        "https_port 8443 missing from operator-generated settings"
    )
    assert "<tcp_port_secure>9440</tcp_port_secure>" in settings_xml, error(
        "tcp_port_secure 9440 missing from operator-generated settings"
    )
    assert "<interserver_https_port>9010</interserver_https_port>" in settings_xml, error(
        "interserver_https_port 9010 missing from operator-generated settings"
    )

    for port in (
        "http_port",
        "tcp_port",
        "mysql_port",
        "postgresql_port",
        "interserver_http_port",
    ):
        assert f'{port} remove="1"' in settings_xml, error(
            f"{port} not marked removed in operator-generated settings"
        )


@TestStep(When)
def fips_read_chop_generated_chk_settings(self, pod, container="clickhouse-keeper", ns=None):
    """Return operator-generated Keeper listener and Raft XML from ``pod``."""
    ns = ns or self.context.test_namespace
    common_listeners_xml = kubectl.launch(
        f"exec {pod} -c {container} -- "
        "cat /etc/clickhouse-keeper/keeper_config.d/chop-generated-common-listeners.xml",
        ns=ns,
    )
    raft_xml = kubectl.launch(
        f"exec {pod} -c {container} -- "
        "cat /etc/clickhouse-keeper/keeper_config.d/chop-generated-raft.xml",
        ns=ns,
    )
    return common_listeners_xml, raft_xml


@TestStep(Then)
def check_ports_in_chk_settings(self, pod):
    """Check plaintext listener removal and Raft TLS in CHK settings."""

    common_listeners_xml, raft_xml = fips_read_chop_generated_chk_settings(pod=pod)
    note(f"chop-generated-common-listeners.xml:\n{common_listeners_xml}")
    note(f"chop-generated-raft.xml:\n{raft_xml}")

    assert '<tcp_port remove="1"/>' in common_listeners_xml, error(
        "tcp_port not marked removed in operator-generated Keeper settings"
    )
    assert "<secure>1</secure>" in raft_xml, error(
        "expected <secure>1</secure> in operator-generated Raft config"
    )

@TestStep(Then)
def check_backup_fips_binary_version(self, pod, ns=None):
    """Run ``clickhouse-backup --version`` and check it contains a fips tag."""
    version = get_binary_version(
        pod=pod,
        binary="/bin/clickhouse-backup",
        container="clickhouse-backup",
        ns=ns,
    )
    note(f"{pod} clickhouse-backup --version: {version}")
    assert "fips" in version.lower(), error(
        f"{pod}: expected fips in clickhouse-backup version, got {version!r}"
    )


@TestStep(Then)
def check_clickhouse_backup_embeds_gofips(
    self,
    pod,
    gofips_version="v1.0.0",
    ns=None,
):
    """Verify each clickhouse-backup sidecar binary embeds GOFIPS140 metadata."""
    ns = ns or self.context.test_namespace
    expected = f"GOFIPS140={gofips_version}"

    backup_bin = f"/tmp/{pod}-clickhouse-backup"
    kubectl.launch(
        f"cp {pod}:/bin/clickhouse-backup {backup_bin} "
        f"-c clickhouse-backup",
        ns=ns,
    )
    build_info = kubectl.run_shell(f"go version -m {backup_bin}")
    assert expected in build_info, error(
        f"{pod}: expected {expected} in clickhouse-backup binary"
    )
    note(f"{pod} clickhouse-backup embeds {expected}")


@TestStep(Then)
def check_external_clickhouse_reports_fips_version(self, pod):
    """Verify an in-cluster FIPS client Pod sees a FIPS ClickHouse server over TLS."""
    version = fips_ch_external_secure_query(pod=pod, sql="SELECT version()")
    note(f"external SELECT version(): {version}")
    assert "fips" in version.lower(), error(
        f"expected FIPS in ClickHouse version(), got {version!r}"
    )

# The operator stamps the enclosing Go function name into every announcer line, so a marker like
# "connect():FAILED" silently stops matching the moment that function is renamed - the assertion
# then fails while the operator is behaving perfectly. That is exactly what happened when
# Connection.connect() became Connection.openPools(): the rejection was still logged, 82 times,
# and the test polled for 57 minutes for a string the binary could no longer emit.
#
# Match the OPERATION instead of the call stack. These cover the dial-time failures
# (Open/Open2/Ping/Ping2) without the trailing "(" so the numbered variants match too. Kept
# deliberately narrow: the caller still ANDs this against the specific rejection text, so widening
# it to any "tls:" line - which would turn the assertion vacuous - is not on the table.
_CH_CONNECT_FAILURE_MARKERS = (
    "FAILED Ping",
    "FAILED Open",
    "FAILED connect(",
)


def _is_ch_connect_failure_line(line):
    """Return True when the line is an operator connection-establishment failure, whatever the
    enclosing Go function happens to be called."""
    return any(marker in line for marker in _CH_CONNECT_FAILURE_MARKERS)


def _fips_tls_rejection_present_in_logs(logs, min_version, rejection):
    """Return True when logs contain coerced TLS setup and a connect rejection."""
    expected_setup_parts = (
        "setupTLSAdvanced():TLS setup OK",
        f"minVersion={min_version}",
    )
    setup_found = any(
        all(part in line for part in expected_setup_parts)
        for line in logs.splitlines()
    )
    rejection_found = any(
        _is_ch_connect_failure_line(line) and rejection in line
        for line in logs.splitlines()
    )
    return setup_found and rejection_found


def _fips_tls_rejection_log_excerpt(logs):
    return "\n".join(
        line for line in logs.splitlines()
        if (
            "setupTLSAdvanced()" in line
            or _is_ch_connect_failure_line(line)
            or "tls:" in line
            or "minVersion" in line
        )
    )


# Distroless operator/exporter images ship sh/curl only (no cat/base64). Read the
# IPC token with POSIX shell builtins — same file both containers mount.
_IPC_TOKEN_READ_SHELL = (
    'TOKEN=""; '
    'while IFS= read -r line || [ -n "$line" ]; do TOKEN="${TOKEN}${line}"; done '
    "< /etc/clickhouse-operator-ipc/token"
)


def _kubectl_pod_exec_stdin(ns, pod, container, shell_script, stdin=None, timeout=120):
    """kubectl exec -i … sh -c <script>, optionally piping stdin to the container."""
    cmd = shlex.split(current().context.kubectl_cmd) + [
        "exec",
        "-i",
        f"--namespace={ns}",
        pod,
        "-c",
        container,
        "--",
        "sh",
        "-c",
        shell_script,
    ]
    result = subprocess.run(
        cmd,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        print(f"kubectl exec failed, command:\n{' '.join(cmd)}")
        print(f"exit code: {result.returncode}")
        print(f"stdout:\n{result.stdout}")
        print(f"stderr:\n{result.stderr}")
        assert result.returncode == 0, error()
    return result.stdout


def _chi_service_name_from_pod(pod):
    """Return per-host StatefulSet Service name (operator FQDN host part, not pod name)."""
    # Pod chi-{chi}-default-{shard}-{replica}-0 → Service chi-{chi}-default-{shard}-{replica}
    if pod.endswith("-0"):
        return pod[:-2]
    return pod


def _chi_host_fqdn_from_pod(pod, ns):
    """Return host FQDN as the operator sets host.Runtime.Address.FQDN."""
    return f"{_chi_service_name_from_pod(pod)}.{ns}.svc.cluster.local."


def _chi_host_name_from_pod(pod, chi):
    """Return shard-replica host label from a CHI pod name."""
    prefix = f"chi-{chi}-default-"
    if pod.startswith(prefix) and pod.endswith("-0"):
        return pod[len(prefix):-2]
    # str.removeprefix is 3.9+; this repo runs Python 3.8.
    return pod[len(prefix):] if pod.startswith(prefix) else pod


def _build_metrics_exporter_chi_payload(chi, ns, pods, https_port=8443):
    hosts = []
    for pod in pods:
        hosts.append({
            "name": _chi_host_name_from_pod(pod, chi),
            "hostname": _chi_host_fqdn_from_pod(pod, ns),
            "httpsPort": https_port,
        })
    return {
        "type": "cr",
        "cr": {
            "namespace": ns,
            "name": chi,
            "labels": {},
            "annotations": {},
            "clusters": [{"name": "default", "hosts": hosts}],
        },
    }


@TestStep(When)
def register_chi_hosts_with_metrics_exporter(
    self,
    chi,
    ns=None,
    operator_namespace=None,
    metrics_port=8888,
    https_port=8443,
):
    """POST /chi so metrics-exporter knows HTTPS hosts when operator IPC does not.

    Injects a WatchedCR with httpsPort only. FIPS-enforced config uses Secure IPC,
    so the request must include X-CHOP-Token from the shared volume.
    """
    ns = ns or self.context.test_namespace
    operator_namespace = operator_namespace or current().context.operator_namespace
    operator_pod = kubectl.get_operator_pod(ns=operator_namespace)
    pods = sorted(kubectl.get_pod_names(chi, ns=ns))
    assert pods, error(f"no pods found for CHI {chi} in namespace {ns}")

    payload = _build_metrics_exporter_chi_payload(
        chi=chi,
        ns=ns,
        pods=pods,
        https_port=https_port,
    )
    body = json.dumps(payload, separators=(",", ":"))

    with When(f"register {chi} HTTPS hosts with metrics-exporter via POST /chi"):
        post_script = (
            f"{_IPC_TOKEN_READ_SHELL}; "
            f"curl -sS -o /dev/null -w '%{{http_code}}' "
            f"-X POST http://127.0.0.1:{metrics_port}/chi "
            f"-H 'Content-Type: application/json' "
            f"-H \"X-CHOP-Token: ${{TOKEN}}\" "
            f"-d @-"
        )
        code = _kubectl_pod_exec_stdin(
            ns=operator_namespace,
            pod=operator_pod,
            container="metrics-exporter",
            shell_script=post_script,
            stdin=body,
        ).strip()
        assert code == "200", error(
            f"metrics-exporter POST /chi failed for {chi}: HTTP {code!r}\n"
            f"payload hosts: {pods}\nbody: {body}"
        )


@TestStep(When)
def trigger_metrics_exporter_collect(self, operator_namespace=None, metrics_port=8888):
    """Scrape /metrics so metrics-exporter dials registered ClickHouse HTTPS hosts."""
    operator_namespace = operator_namespace or current().context.operator_namespace
    operator_pod = kubectl.get_operator_pod(ns=operator_namespace)

    with When(f"scrape metrics-exporter /metrics on 127.0.0.1:{metrics_port}"):
        code = kubectl.launch(
            f"exec {operator_pod} -c metrics-exporter -- "
            f"curl -sS -o /dev/null -w %{{http_code}} "
            f"http://127.0.0.1:{metrics_port}/metrics",
            ns=operator_namespace,
        )
        assert code == "200", error(
            f"metrics-exporter /metrics scrape failed: HTTP {code!r}"
        )


@TestStep(Then)
def fips_poll_tls_rejection_in_logs(
    self,
    workload,
    containers,
    min_version="1.3",
    rejection="remote error: tls: protocol version not supported",
    operator_namespace=None,
    trigger_metrics_exporter=False,
    chi=None,
    max_iters=60,
    sleep_s=5,
):
    """Poll selected operator-pod containers until TLS version rejection appears."""
    operator_namespace = operator_namespace or current().context.operator_namespace
    expected_setup_parts = (
        "setupTLSAdvanced():TLS setup OK",
        f"minVersion={min_version}",
    )
    last_logs_by_container = {container: "" for container in containers}

    for attempt in range(max_iters):
        if trigger_metrics_exporter:
            with When("metrics-exporter collect is triggered via /metrics scrape"):
                if chi:
                    with When(f"metrics-exporter is registered with CHI {chi} HTTPS hosts"):
                        register_chi_hosts_with_metrics_exporter(chi=chi)
                trigger_metrics_exporter_collect(
                    operator_namespace=operator_namespace,
                )

        operator_pod = kubectl.get_operator_pod(ns=operator_namespace)
        container_results = {}

        for container in containers:
            with Then(f"tail logs from {container}"):
                logs = get_container_logs(
                    pod=operator_pod,
                    container=container,
                    ns=operator_namespace,
                )
                last_logs_by_container[container] = logs
                container_results[container] = _fips_tls_rejection_present_in_logs(
                    logs,
                    min_version=min_version,
                    rejection=rejection,
                )

        if all(container_results.values()):
            note(
                f"{workload}: observed TLS version rejection in "
                f"{', '.join(containers)} after attempt {attempt + 1}/{max_iters}"
            )
            return

        if attempt + 1 < max_iters:
            time.sleep(sleep_s)

    matching_sections = [
        f"{container}:\n{_fips_tls_rejection_log_excerpt(last_logs_by_container[container]) or '(none)'}"
        for container in containers
    ]

    assert False, error(
        f"{workload}: expected TLS version rejection not found in all containers\n"
        f"containers: {', '.join(containers)}\n"
        f"expected setup line parts: {expected_setup_parts}\n"
        f"expected rejection: {rejection}\n\n"
        f"matching log lines:\n" + "\n\n".join(matching_sections)
    )


@TestStep(Then)
def fips_assert_operator_tls_rejection_in_logs(
    self,
    workload,
    min_version="1.3",
    rejection="remote error: tls: protocol version not supported",
    operator_namespace=None,
    containers=("clickhouse-operator",),
    trigger_metrics_exporter=False,
    chi=None,
    max_iters=60,
    sleep_s=5,
):
    """Assert operator pod containers log TLS min-version rejection."""
    with Given(f"workload {workload} must reject TLS below {min_version}"):
        fips_poll_tls_rejection_in_logs(
            workload=workload,
            containers=containers,
            min_version=min_version,
            rejection=rejection,
            operator_namespace=operator_namespace,
            trigger_metrics_exporter=trigger_metrics_exporter,
            chi=chi,
            max_iters=max_iters,
            sleep_s=sleep_s,
        )

@TestStep(Then)
def fips_assert_chi_tls_rejected(
    self,
    chi,
    expected_status="InProgress",
    min_version="1.3",
):
    """Assert CHI remains unfinished because operator TLS client rejects server TLS policy."""
    with When(f"CHI {chi} pod is running"):
        kubectl.wait_object(
            "pod",
            "",
            label=f"-l clickhouse.altinity.com/chi={chi}",
            count=1,
        )

    with Then(f"CHI {chi} does not complete reconciliation"):
        status = kubectl.get_chi_status(chi)
        assert status != "Completed", error(
            f"CHI {chi} reached Completed; expected operator TLS client to reject the server"
        )
        if status != expected_status:
            kubectl.wait_chi_status(chi, expected_status)

    with Then("operator and metrics-exporter reject TLS 1.2-only ClickHouse"):
        fips_assert_operator_tls_rejection_in_logs(
            workload=f"chi/{chi}",
            min_version=min_version,
            containers=("clickhouse-operator", "metrics-exporter"),
            trigger_metrics_exporter=True,
            chi=chi,
        )


@TestStep(Then)
def fips_assert_chk_tls_rejected(
    self,
    chk,
    expected_status="InProgress",
    min_version="1.3",
):
    """Assert CHK remains unfinished because operator TLS client rejects server TLS policy."""
    with When(f"CHK {chk} pod is running"):
        kubectl.wait_object(
            "pod",
            "",
            label=f"-l clickhouse-keeper.altinity.com/chk={chk}",
            count=1,
        )

    with Then(f"CHK {chk} does not complete reconciliation"):
        status = kubectl.get_field("chk", chk, ".status.status")
        assert status != "Completed", error(
            f"CHK {chk} reached Completed; expected operator TLS client to reject the server"
        )
        if status != expected_status:
            kubectl.wait_chk_status(chk, expected_status)

    with Then("operator rejects TLS 1.2-only Keeper"):
        fips_assert_operator_tls_rejection_in_logs(
            workload=f"chk/{chk}",
            min_version=min_version,
            containers=("clickhouse-operator",),
        )

@TestStep(Then)
def check_clickhouse_backup_clickhouse_tls_config(self, pod, ns=None):
    """Verify clickhouse-backup container is configured to reach ClickHouse via TLS native port."""
    ns = ns or self.context.test_namespace

    out = kubectl.launch(
        f"exec {pod} -c clickhouse-backup -- "
        "sh -c '"
        "echo PORT:$CLICKHOUSE_PORT; "
        "echo SECURE:$CLICKHOUSE_SECURE; "
        "echo TLS_CA:$CLICKHOUSE_TLS_CA; "
        "echo SKIP_VERIFY:$CLICKHOUSE_SKIP_VERIFY"
        "'",
        ns=ns,
    )

    assert "PORT:9440" in out, error(out)
    assert "SECURE:true" in out, error(out)
    assert "TLS_CA:/etc/clickhouse-backup/tls/ca.crt" in out, error(out)
    assert "SKIP_VERIFY:false" in out, error(out)

@TestStep(Then)
def check_clickhouse_backup_restore_roundtrip_https(
    self,
    pod,
    table="fips_backup_roundtrip",
    ns=None,
):
    """Create data, backup via HTTPS API, drop data, restore via HTTPS API, verify data."""
    ns = ns or self.context.test_namespace
    backup_name = f"{table}_backup"

    def api(method, path):
        return kubectl.launch(
            f"exec {pod} -c clickhouse-backup -- "
            "sh -c "
            f"\"curl -sS -X {method} "
            "--cacert /etc/clickhouse-backup/tls/ca.crt "
            "-o /tmp/backup_api.out "
            "-w 'HTTP:%{http_code}' "
            f"'https://127.0.0.1:7171{path}'\"",
            ns=ns,
        ).strip()

    with Given("test table exists with data"):
        fips_ch_external_secure_query(
            pod=pod,
            sql=f"DROP TABLE IF EXISTS {table} SYNC",
        )
        fips_wait_table_removed_from_dropped_tables(pod=pod, table=table)

        fips_ch_external_secure_query(
            pod=pod,
            sql=f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n",
        )
        fips_ch_external_secure_query(
            pod=pod,
            sql=f"INSERT INTO {table} SELECT number FROM numbers(10)",
        )

    with When("backup is created through HTTPS API"):
        out = api(
            "POST",
            f"/backup/create?name={backup_name}&table=default.{table}",
        )
        assert out.startswith("HTTP:2"), error(out)

    with And("table is dropped"):
        fips_ch_external_secure_query(
            pod=pod,
            sql=f"DROP TABLE IF EXISTS {table} SYNC",
        )
        fips_wait_table_removed_from_dropped_tables(pod=pod, table=table)

    with And("backup is restored through HTTPS API"):
        out = api(
            "POST",
            f"/backup/restore/{backup_name}?table=default.{table}",
        )
        assert out.startswith("HTTP:2"), error(out)

    with Then("restored data is visible through strict TLS ClickHouse client"):
        fips_poll_secure_scalar(
            pod=pod,
            sql=f"SELECT count() FROM {table}",
            expected=10,
        )

@TestStep(Then)
def fips_wait_table_removed_from_dropped_tables(
    self,
    pod,
    table,
    database="default",
    timeout=60,
):
    """Wait until Atomic database dropped-table metadata no longer reserves the table UUID."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        out = fips_ch_external_secure_query(
            pod=pod,
            sql=(
                "SELECT count() "
                "FROM system.dropped_tables "
                f"WHERE database = '{database}' AND table = '{table}'"
            ),
        )

        if out.strip() == "0":
            return

        time.sleep(1)

    assert False, error(
        f"{database}.{table} still present in system.dropped_tables after {timeout}s"
    )

@TestStep(When)
def run_binary_with_forced_fips_cast_failure(
    self,
    binary_path,
    cast_name,
):
    """Run binary with GODEBUG forcing the named FIPS CAST/PCT to fail."""
    result = subprocess.run(
        [
            "env",
            f"GODEBUG=fips140=only,failfipscast={cast_name}",
            binary_path,
            "--version",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    return result


@TestStep(Check)
def check_fips_cast_failure_result(
    self,
    result,
    binary,
    cast_name,
    failure_kind="CAST",
):
    """Assert binary exits non-zero with the expected forced FIPS CAST/PCT failure."""
    output = f"{result.stdout}\n{result.stderr}"

    assert result.returncode != 0, error(
        f"{binary}: expected {failure_kind} failure exit, got {result.returncode}\n{output}"
    )

    assert f"FIPS 140-3 self-test failed: {cast_name}" in output, error(
        f"{binary}: expected {failure_kind} failure for {cast_name}\n{output}"
    )

    expected_simulated_message = (
        "simulated PCT failure"
        if "PCT" in cast_name or failure_kind == "PCT"
        else "simulated CAST failure"
    )

    assert expected_simulated_message in output, error(
        f"{binary}: expected {expected_simulated_message!r}\n{output}"
    )

def _free_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return str(s.getsockname()[1])


def _wait_for_listening_port(host, port, process=None, timeout=15, label="service"):
    """Poll until host:port accepts TCP connections or timeout."""
    deadline = time.time() + timeout
    port = int(port)
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            return False, "exited"
        try:
            socket.create_connection((host, port), timeout=0.5).close()
            return True, ""
        except OSError:
            time.sleep(0.2)
    if process is not None and process.poll() is None:
        process.kill()
    return False, "timeout"


def _fips_hostrun_env(kubeconfig_path, namespace=None, operator_pod_name=None):
    env = os.environ.copy()
    env["GODEBUG"] = "fips140=only"
    env["KUBECONFIG"] = kubeconfig_path
    if namespace is not None:
        env["OPERATOR_POD_NAMESPACE"] = namespace
    else:
        env.pop("OPERATOR_POD_NAMESPACE", None)
    if operator_pod_name is not None:
        env["OPERATOR_POD_NAME"] = operator_pod_name
    else:
        env.pop("OPERATOR_POD_NAME", None)
    return env


@TestStep(Then)
def check_fips_integrity_failure(self, binary_path, binary_label):
    """Assert binary panics when the .go.fipsinfo HMAC is tampered with."""

    cmd = f"readelf -S -W {shlex.quote(binary_path)}"
    readelf_out = kubectl.run_shell(cmd)

    match = re.search(r"\.go\.fipsinfo\s+\w+\s+\w+\s+([0-9a-fA-F]+)", readelf_out)
    assert match, error(f"{binary_label}: .go.fipsinfo section not found in ELF headers")

    with Given("I edit the binary to corrupt the .go.fipsinfo HMAC"):
        section_offset = int(match.group(1), 16)
        hmac_byte_offset = section_offset + 16

        corrupted_bin = f"{binary_path}.corrupted"
        shutil.copy2(binary_path, corrupted_bin)

        with open(corrupted_bin, "rb+") as f:
            f.seek(hmac_byte_offset)
            original_byte = f.read(1)
            corrupted_byte = bytes([original_byte[0] ^ 0xFF])
            f.seek(hmac_byte_offset)
            f.write(corrupted_byte)

    with When("I execute --version on a corrupted bin file"):
        result = subprocess.run(
            [corrupted_bin, "--version"],
            env={"GODEBUG": "fips140=on"},
            capture_output=True,
            text=True,
            check=False
        )

        output = f"{result.stdout}\n{result.stderr}"
        note(output)

    with Then("the process must terminate with a verification mismatch panic"):
        assert result.returncode != 0, error(
            f"{binary_label}: tampered binary did not exit with error"
        )
        assert "fips140: verification mismatch" in output, error(
            f"{binary_label}: expected integrity panic not found in output:\n{output}"
        )

    note(f"{binary_label}: integrity check successfully detected tampering")

@TestStep(Then)
def check_tls13_cipher_fails(
    self,
    pod,
    port,
    cipher,
    target_host=None,
    container=None,
    ns=None,
):
    """Verify a TLS 1.3 cipher cannot be negotiated (via ``fips-openssl`` pod)."""
    del container  # probes always run in fips-openssl
    ns = ns or self.context.test_namespace
    target_host = target_host or _chi_service_host_from_pod(pod)
    openssl_pod = ensure_fips_openssl_pod(ns=ns)

    out = kubectl.launch(
        f"exec {openssl_pod} -- "
        "openssl s_client "
        f"-connect {target_host}:{port} "
        "-servername localhost "
        f"-CAfile {_FIPS_OPENSSL_CA_FILE} "
        "-tls1_3 "
        f"-ciphersuites {cipher}",
        ns=ns,
        ok_to_fail=True,
    )

    assert (
        "Cipher is (NONE)" in out
        or "handshake failure" in out
        or "alert handshake failure" in out
        or "no shared cipher" in out
    ), error(
        f"{target_host}:{port}: expected {cipher} to be rejected\n{out}"
    )

@TestStep(Finally)
def cleanup_admission_only_chi(self, chi):
    """Cleanup chi"""
    kubectl.launch(
        f"delete chi {chi} --ignore-not-found=true --wait=false",
        ns=current().context.test_namespace,
        timeout=600,
        ok_to_fail=True,
    )
    kubectl.launch(
        f"delete sts,pod,svc,pvc,cm,secret "
        f"-l clickhouse.altinity.com/chi={chi} "
        f"--ignore-not-found=true --wait=false",
        ns=current().context.test_namespace,
        timeout=600,
        ok_to_fail=True,
    )

FAKE_K8S_TLS_REJECT_ERRORS = (
    "remote error: tls: handshake failure",
    "remote error: tls: protocol version not supported",
)


def _write_fake_kubeconfig(path, port, ca_cert_path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"""apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:{port}
    certificate-authority: {ca_cert_path}
  name: fake
contexts:
- context:
    cluster: fake
    user: fake
  name: fake
current-context: fake
users:
- name: fake
  user:
    token: fake-token
""")


def _fake_k8s_probe_env(work_dir, port, ca_cert_path):
    kubeconfig_path = os.path.join(work_dir, "fake-kubeconfig")
    _write_fake_kubeconfig(kubeconfig_path, port, ca_cert_path)
    return _fips_hostrun_env(
        kubeconfig_path,
        namespace="default",
        operator_pod_name="fips-local-tls-probe",
    )


_FAKE_K8S_PROBE_NOTE_MARKERS = (
    "Starting clickhouse-operator",
    "Starting metrics exporter",
    "kubeconfig auth source:",
    "FIPS:",
    "FIPS env:",
    "CIPHER is ",
    *FAKE_K8S_TLS_REJECT_ERRORS,
)


def _format_fake_k8s_probe_note(output, max_lines=50):
    """Return a compact note for local fake-k8s probe runs.

    Keeps only TLS/FIPS-relevant lines and trims noisy config dumps.
    """
    selected = []
    for line in output.splitlines():
        if any(marker in line for marker in _FAKE_K8S_PROBE_NOTE_MARKERS):
            selected.append(line)

    if not selected:
        tail = output.splitlines()[-20:]
        return "\n".join(tail)

    if len(selected) > max_lines:
        selected = [f"... truncated, showing last {max_lines} relevant lines ..."] + selected[-max_lines:]
    return "\n".join(selected)


def _read_available_stdout(pipe, timeout=0):
    readable, _, _ = select.select([pipe], [], [], timeout)
    if not readable:
        return ""
    data = os.read(pipe.fileno(), 65536)
    if not data:
        return ""
    return data.decode("utf-8", "replace")


def _drain_stdout(pipe, output_parts):
    while True:
        chunk = _read_available_stdout(pipe)
        if not chunk:
            break
        output_parts.append(chunk)


def _stop_process(process, timeout=5):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def _fake_k8s_tls_probe_complete(output, server_log_path, expectation, cipher_suite):
    if expectation == "approved":
        with open(server_log_path, encoding="utf-8", errors="replace") as f:
            server_log = f.read()
        return bool(cipher_suite) and f"CIPHER is {cipher_suite}" in server_log
    return any(err in output for err in FAKE_K8S_TLS_REJECT_ERRORS)


def _run_fips_binary_until_tls_probe(
    binary_path,
    config_path,
    env,
    server_log_path,
    expectation,
    cipher_suite=None,
    max_wait_sec=45,
):
    process = subprocess.Popen(
        [
            binary_path,
            "-logtostderr=true",
            "-v=2",
            f"--config={config_path}",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output_parts = []
    deadline = time.time() + max_wait_sec

    try:
        while time.time() < deadline:
            chunk = _read_available_stdout(process.stdout, timeout=0.5)
            if chunk:
                output_parts.append(chunk)

            output = "".join(output_parts)
            if _fake_k8s_tls_probe_complete(
                output, server_log_path, expectation, cipher_suite
            ):
                break

            if process.poll() is not None:
                _drain_stdout(process.stdout, output_parts)
                break
    finally:
        _stop_process(process)
        _drain_stdout(process.stdout, output_parts)

    return process, "".join(output_parts)


@TestStep(Given)
def prepare_local_strict_operator_config(self):
    """Write strict FIPS config (Enforced, fips.enforced=true) under the TLS work dir."""
    base_config_path = util.get_full_path(
        "../../config/config.yaml", lookup_in_host=True
    )
    with open(base_config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    work_dir = self.context.fips_local_openssl_tls_dir

    security = config.setdefault("security", {})
    security["policy"] = "Enforced"
    security.setdefault("fips", {})["enforced"] = True
    ipc_dir = os.path.join(work_dir, "ipc")
    os.makedirs(ipc_dir, exist_ok=True)
    token_path = os.path.join(ipc_dir, "token")
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(uuid.uuid4().hex)
    security.setdefault("ipc", {})["tokenPath"] = token_path
    config_path = os.path.join(work_dir, "strict-operator-config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False)

    self.context.fips_local_strict_config_path = config_path
    self.context.fips_local_ipc_token_path = token_path
    note(f"strict FIPS operator config -> {config_path}")
    return config_path


@TestStep(Given)
def prepare_local_openssl_tls_material(self):
    """Create temp dir with self-signed cert and key for local openssl s_server."""
    work_dir = tempfile.mkdtemp(prefix="fips-local-openssl-tls-")
    cert_path = os.path.join(work_dir, "server.crt")
    key_path = os.path.join(work_dir, "server.key")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", key_path,
            "-out", cert_path,
            "-days", "1",
            "-nodes",
            "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    self.context.fips_local_openssl_tls_dir = work_dir
    self.context.fips_local_openssl_cert = cert_path
    self.context.fips_local_openssl_key = key_path

    yield

    cleanup_local_openssl_tls_material()

@TestStep(Finally)
def cleanup_local_openssl_tls_material(self):
    work_dir = getattr(self.context, "fips_local_openssl_tls_dir", None)
    if work_dir and os.path.isdir(work_dir):
        shutil.rmtree(work_dir, ignore_errors=True)


@TestStep(When)
def start_local_openssl_server(self, cipher_suite=None, tls_version="1.3", port=None):
    """Start openssl s_server on localhost (fake Kubernetes API or fake ClickHouse HTTPS)."""
    port = port or _free_local_port()
    log_path = os.path.join(self.context.fips_local_openssl_tls_dir, "s_server.log")

    with Given(f"openssl s_server on 127.0.0.1:{port} (TLS {tls_version})"):
        command = [
            "openssl", "s_server",
            "-accept", str(port),
            "-cert", self.context.fips_local_openssl_cert,
            "-key", self.context.fips_local_openssl_key,
        ]
        command.extend(openssl_tls_version_args(tls_version))
        command.extend(openssl_cipher_args(tls_version, cipher_suite))

        # TLS 1.0/1.1: -www keeps s_server alive after the readiness TCP probe; without
        # it the process exits before the binary connects.
        if tls_version in ("1.0", "1.1"):
            command.append("-www")
        command.append("-state")

        # Line-buffer stdout so handshake / CIPHER lines flush to s_server.log while polling.
        if shutil.which("stdbuf"):
            command = ["stdbuf", "-oL", "-eL", *command]
        log_file = open(log_path, "w", encoding="utf-8")

        process = subprocess.Popen(
            command,
            # Open stdin pipe: without -www, EOF on stdin makes s_server exit early.
            stdin=subprocess.PIPE,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )

    with When("s_server is listening on localhost"):
        ok, reason = _wait_for_listening_port(
            "127.0.0.1", port, process=process, timeout=10, label="openssl s_server"
        )
        if not ok:
            log_file.close()
            if reason == "exited":
                with open(log_path, encoding="utf-8") as f:
                    server_log = f.read()
                assert False, error(
                    f"local openssl s_server exited early on port {port}\n{server_log}"
                )
            assert False, error(f"local openssl s_server not listening on 127.0.0.1:{port}")

    self.context.fips_local_openssl_process = process
    self.context.fips_local_openssl_log_file = log_file
    self.context.fips_local_openssl_log_path = log_path
    self.context.fips_local_openssl_port = port

    return port

@TestStep(Finally)
def stop_local_openssl_server(self):
    process = getattr(self.context, "fips_local_openssl_process", None)
    log_file = getattr(self.context, "fips_local_openssl_log_file", None)
    if process and process.stdin and not process.stdin.closed:
        process.stdin.close()
    _stop_process(process)
    if log_file and not log_file.closed:
        log_file.close()
    self.context.fips_local_openssl_process = None
    self.context.fips_local_openssl_log_file = None


@TestStep(When)
def run_binary_against_local_fake_k8s(
    self,
    binary_path,
    config_path,
    expectation="approved",
    cipher_suite=None,
    max_wait_sec=45,
):
    """Run binary with fake kubeconfig; poll output until TLS probe evidence appears."""
    with Given("fake kubeconfig targeting local s_server"):
        env = _fake_k8s_probe_env(
            self.context.fips_local_openssl_tls_dir,
            self.context.fips_local_openssl_port,
            self.context.fips_local_openssl_cert,
        )

    with When("binary runs against fake Kubernetes API"):
        _, output = _run_fips_binary_until_tls_probe(
            binary_path=binary_path,
            config_path=config_path,
            env=env,
            server_log_path=self.context.fips_local_openssl_log_path,
            expectation=expectation,
            cipher_suite=cipher_suite,
            max_wait_sec=max_wait_sec,
        )
        note(_format_fake_k8s_probe_note(output))
    return output


@TestStep(Then)
def assert_local_fake_k8s_tls_probe(
    self,
    binary_label,
    binary_path,
    config_path,
    expectation,
    cipher_suite=None,
    tls_version="1.3",
    case_name=None,
):
    """Start s_server, run binary once, assert negotiated cipher or TLS rejection."""

    label = case_name or cipher_suite or f"TLS {tls_version} protocol"

    with Given(f"fake Kubernetes API ({label})"):
        start_local_openssl_server(
            cipher_suite=cipher_suite,
            tls_version=tls_version,
        )
    try:
        with When(f"{binary_label} connects via fake kubeconfig"):
            output = run_binary_against_local_fake_k8s(
                binary_path=binary_path,
                config_path=config_path,
                expectation=expectation,
                cipher_suite=cipher_suite,
            )

        with open(self.context.fips_local_openssl_log_path, encoding="utf-8", errors="replace") as f:
            server_log = f.read()

        if expectation == "approved":
            with Then(f"check {binary_label} negotiates {cipher_suite}"):
                cipher_line = f"CIPHER is {cipher_suite}"
                assert cipher_suite and cipher_line in server_log, error(
                    f"{binary_label} {label}: expected {cipher_suite!r} in server log, but none found"
                )
                note(f"cipher proof: {cipher_line} - found in logs")
        else:
            with Then(f"{binary_label} rejects TLS handshake"):
                assert any(err in output for err in FAKE_K8S_TLS_REJECT_ERRORS), error(
                    f"{binary_label} {label}: expected TLS rejection in binary output, but none found"
                )
                reject_line = next(
                    (
                        line for line in output.splitlines()
                        if any(err in line for err in FAKE_K8S_TLS_REJECT_ERRORS)
                    ),
                    None,
                )
                if reject_line:
                    note(f"TLS rejection proof: {reject_line.strip()}")
    finally:
        with Finally("stop local openssl server"):
            stop_local_openssl_server()


@TestStep(Check)
def assert_local_fake_k8s_approved_tls_cases(
    self,
    binary_label,
    binary_path,
    config_path,
):
    """Run all FIPS_APPROVED_TLS13_CIPHER_CASES against the local fake API."""

    for case in FIPS_APPROVED_TLS13_CIPHER_CASES:
        with Check(f"{binary_label} accepts {case['name']} against local fake k8s API"):
            assert_local_fake_k8s_tls_probe(
                binary_label=binary_label,
                binary_path=binary_path,
                config_path=config_path,
                tls_version=case["tls_version"],
                cipher_suite=case["cipher_suite"],
                expectation="approved",
                case_name=case["name"],
            )


@TestStep(Check)
def assert_local_fake_k8s_rejected_tls_cases(
    self,
    binary_label,
    binary_path,
    config_path,
):
    """Run all FIPS_LISTENER_REJECTED_TLS_CASES against the local fake API."""

    for case in FIPS_LISTENER_REJECTED_TLS_CASES:
        # Operator K8s client min TLS is 1.3 (PR #2020). Skip 1.2 *cipher* cases,
        # but keep the single 1.2 *protocol* case as a live negative probe.
        if case["tls_version"] == "1.2" and case["cipher_suite"] is not None:
            continue
        with Check(f"{binary_label} rejects {case['name']} against local fake k8s API"):
            assert_local_fake_k8s_tls_probe(
                binary_label=binary_label,
                binary_path=binary_path,
                config_path=config_path,
                tls_version=case["tls_version"],
                cipher_suite=case["cipher_suite"],
                expectation="rejected",
                case_name=case["name"],
            )


def _write_unreachable_kubeconfig(path):
    """Kubeconfig pointing at a closed port — metrics-exporter discovery fails open."""
    with open(path, "w", encoding="utf-8") as f:
        f.write("""apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:1
    insecure-skip-tls-verify: true
  name: noop
contexts:
- context:
    cluster: noop
    user: noop
  name: noop
current-context: noop
users:
- name: noop
  user:
    token: noop
""")


def _hostrun_metrics_exporter_env(work_dir):
    """Host-run metrics-exporter without real or fake k8s (POST /chi triggers CH scrape)."""
    kubeconfig_path = os.path.join(work_dir, "hostrun-kubeconfig-noop")
    _write_unreachable_kubeconfig(kubeconfig_path)
    return _fips_hostrun_env(kubeconfig_path), kubeconfig_path


LOCAL_FAKE_CLICKHOUSE_CR = "fake-openssl-clickhouse"
LOCAL_OPERATOR_OPENSSL_TLS_PORT = 8443
LOCAL_OPERATOR_OPENSSL_CHI = "test-030017-chi"
LOCAL_OPERATOR_OPENSSL_CHI_MANIFEST = "manifests/chi/test-030017-chi.yaml"
LOCAL_FAKE_K8S_NS = "fake-op"

_FAKE_K8S_CHI_GROUP = "clickhouse.altinity.com"
_FAKE_K8S_CHI_VERSION = "v1"
_FAKE_K8S_CHI_PLURAL = "clickhouseinstallations"
_FAKE_K8S_CHK_GROUP = "clickhouse-keeper.altinity.com"
_FAKE_K8S_CHK_VERSION = "v1"
_FAKE_K8S_CHK_PLURAL = "clickhousekeeperinstallations"

_HOSTRUN_CLICKHOUSE_ACCESS = {
    "username": "clickhouse_operator",
    "password": "clickhouse_operator_password",
    "scheme": "https",
    "port": LOCAL_OPERATOR_OPENSSL_TLS_PORT,
}

_HOSTRUN_PERMISSIVE_CH_TLS = {
    "minVersion": "1.3",
    "verify": "None",
}


def _fake_k8s_uid() -> str:
    return str(uuid.uuid4())


def _fake_k8s_resource_version(store: "_FakeK8sStore") -> str:
    store.resource_version += 1
    return str(store.resource_version)


def _fake_k8s_meta(name: str, namespace: str, labels: dict | None = None, rv: str = "1") -> dict:
    meta = {
        "name": name,
        "namespace": namespace,
        "uid": _fake_k8s_uid(),
        "resourceVersion": rv,
        "generation": 1,
    }
    if labels:
        meta["labels"] = labels
    return meta


class _FakeK8sStore:
    def __init__(self, namespace: str, chi: dict, chi_manifest: str = ""):
        self.namespace = namespace
        self.chi_manifest = chi_manifest
        self.resource_version = 100
        self.chi = chi
        self.chit_list: list[dict] = []
        self.chopconf_list: list[dict] = []
        self.statefulsets: dict[str, dict] = {}
        self.pods: dict[str, dict] = {}
        self.services: dict[str, dict] = {}
        self.configmaps: dict[str, dict] = {}
        self.secrets: dict[str, dict] = {}
        self.pvcs: dict[str, dict] = {}
        self.endpoint_slices: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.watchers: list[tuple[threading.Condition, dict]] = []

    def reset_for_reconcile(self):
        """Clear reconcile artifacts so the next operator run re-Pings ClickHouse."""
        with self.lock:
            if self.chi_manifest:
                self.chi = _load_fake_k8s_chi(self.chi_manifest, self.namespace)
            else:
                self.chi.setdefault("status", {})
                self.chi["status"] = {}
                self.chi["metadata"]["generation"] = 1
                self.chi["metadata"]["resourceVersion"] = "1"
            self.statefulsets.clear()
            self.pods.clear()
            self.services.clear()
            self.configmaps.clear()
            self.secrets.clear()
            self.pvcs.clear()
            self.endpoint_slices.clear()
            self.resource_version = 100
            chi_event = {"type": "MODIFIED", "object": copy.deepcopy(self.chi)}
        self.notify_watchers(chi_event)

    def notify_watchers(self, event: dict):
        with self.lock:
            dead = []
            for cond, state in self.watchers:
                if state.get("closed"):
                    dead.append((cond, state))
                    continue
                state.setdefault("events", []).append(event)
                cond.notify_all()
            for item in dead:
                self.watchers.remove(item)


def _fake_k8s_pod_from_sts(sts: dict) -> dict:
    ns = sts["metadata"]["namespace"]
    name = sts["metadata"]["name"] + "-0"
    labels = copy.deepcopy(sts["spec"].get("template", {}).get("metadata", {}).get("labels", {}))
    labels.setdefault(
        "clickhouse.altinity.com/chi",
        sts["metadata"]["labels"].get("clickhouse.altinity.com/chi", ""),
    )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            **_fake_k8s_meta(name, ns, labels),
            "ownerReferences": [{
                "apiVersion": sts["apiVersion"],
                "kind": sts["kind"],
                "name": sts["metadata"]["name"],
                "uid": sts["metadata"]["uid"],
                "controller": True,
                "blockOwnerDeletion": True,
            }],
        },
        "spec": copy.deepcopy(sts["spec"].get("template", {}).get("spec", {})),
        "status": {
            "phase": "Running",
            "podIP": "127.0.0.1",
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "FakeK8s",
            }],
        },
    }


class _FakeK8sHandler(BaseHTTPRequestHandler):
    store: _FakeK8sStore
    log_path: str | None = None

    def log_message(self, fmt, *args):
        line = f"{self.command} {self.path} -> {fmt % args}\n"
        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line)
        sys.stderr.write(line)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _json(self, code: int, payload: Any):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _status(self, code: int, message: str = ""):
        self.send_response(code, message)
        self.end_headers()

    def _list(self, kind: str, api_version: str, items: list[dict]) -> dict:
        return {
            "apiVersion": api_version,
            "kind": f"{kind}List",
            "metadata": {"resourceVersion": str(self.store.resource_version)},
            "items": items,
        }

    def _watch(self, events: list[dict]):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        try:
            for event in events:
                self.wfile.write((json.dumps(event) + "\n").encode("utf-8"))
                self.wfile.flush()
            while True:
                time.sleep(3600)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _is_watch(self) -> bool:
        return parse_qs(urlparse(self.path).query).get("watch", ["false"])[0] == "true"

    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        watch = self._is_watch()

        if path == "/version":
            return self._json(200, {
                "major": "1",
                "minor": "30",
                "gitVersion": "v1.30.0-fake",
            })

        if path == "/api":
            return self._json(200, {
                "kind": "APIVersions",
                "versions": ["v1"],
                "serverAddressByClientCIDRs": [],
            })

        if path == "/apis":
            return self._json(200, {
                "kind": "APIGroupList",
                "groups": [
                    {"name": "", "versions": [{"groupVersion": "v1", "version": "v1"}], "preferredVersion": {"groupVersion": "v1", "version": "v1"}},
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}], "preferredVersion": {"groupVersion": "apps/v1", "version": "v1"}},
                    {"name": "discovery.k8s.io", "versions": [{"groupVersion": "discovery.k8s.io/v1", "version": "v1"}], "preferredVersion": {"groupVersion": "discovery.k8s.io/v1", "version": "v1"}},
                    {"name": _FAKE_K8S_CHI_GROUP, "versions": [{"groupVersion": f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}", "version": _FAKE_K8S_CHI_VERSION}], "preferredVersion": {"groupVersion": f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}", "version": _FAKE_K8S_CHI_VERSION}},
                    {"name": _FAKE_K8S_CHK_GROUP, "versions": [{"groupVersion": f"{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}", "version": _FAKE_K8S_CHK_VERSION}], "preferredVersion": {"groupVersion": f"{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}", "version": _FAKE_K8S_CHK_VERSION}},
                    {"name": "apiextensions.k8s.io", "versions": [{"groupVersion": "apiextensions.k8s.io/v1", "version": "v1"}], "preferredVersion": {"groupVersion": "apiextensions.k8s.io/v1", "version": "v1"}},
                ],
            })

        if path == "/apis/apps/v1":
            return self._json(200, {
                "kind": "APIResourceList",
                "groupVersion": "apps/v1",
                "resources": [
                    {"name": "statefulsets", "namespaced": True, "kind": "StatefulSet", "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                ],
            })

        if path == "/apis/discovery.k8s.io/v1":
            return self._json(200, {
                "kind": "APIResourceList",
                "groupVersion": "discovery.k8s.io/v1",
                "resources": [
                    {"name": "endpointslices", "namespaced": True, "kind": "EndpointSlice", "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                ],
            })

        if path == f"/apis/{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}":
            return self._json(200, {
                "kind": "APIResourceList",
                "groupVersion": f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}",
                "resources": [
                    {"name": _FAKE_K8S_CHI_PLURAL, "namespaced": True, "kind": "ClickHouseInstallation", "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                    {"name": "clickhouseinstallationtemplates", "namespaced": True, "kind": "ClickHouseInstallationTemplate", "verbs": ["get", "list", "watch"]},
                    {"name": "clickhouseoperatorconfigurations", "namespaced": True, "kind": "ClickHouseOperatorConfiguration", "verbs": ["get", "list", "watch"]},
                ],
            })

        if path == f"/apis/{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}":
            return self._json(200, {
                "kind": "APIResourceList",
                "groupVersion": f"{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}",
                "resources": [
                    {"name": _FAKE_K8S_CHK_PLURAL, "namespaced": True, "kind": "ClickHouseKeeperInstallation", "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                ],
            })

        ns = self.store.namespace
        chi_path = f"/apis/{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}/namespaces/{ns}/{_FAKE_K8S_CHI_PLURAL}"
        if path == chi_path:
            if watch:
                return self._watch([{"type": "ADDED", "object": self.store.chi}])
            return self._json(200, self._list("ClickHouseInstallation", f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}", [self.store.chi]))

        m = re.match(
            rf"/apis/{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}/namespaces/{re.escape(ns)}/{_FAKE_K8S_CHI_PLURAL}/([^/]+)$",
            path,
        )
        if m:
            if m.group(1) == self.store.chi["metadata"]["name"]:
                return self._json(200, self.store.chi)
            return self._status(404)

        for plural, kind, items in (
            ("clickhouseinstallationtemplates", "ClickHouseInstallationTemplate", self.store.chit_list),
            ("clickhouseoperatorconfigurations", "ClickHouseOperatorConfiguration", self.store.chopconf_list),
        ):
            p = f"/apis/{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}/namespaces/{ns}/{plural}"
            if path == p:
                if watch:
                    return self._watch([])
                return self._json(200, self._list(kind, f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}", items))

        chk_path = f"/apis/{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}/namespaces/{ns}/{_FAKE_K8S_CHK_PLURAL}"
        if path == chk_path:
            if watch:
                return self._watch([])
            return self._json(200, self._list("ClickHouseKeeperInstallation", f"{_FAKE_K8S_CHK_GROUP}/{_FAKE_K8S_CHK_VERSION}", []))

        es_path = f"/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices"
        if path == es_path:
            items = list(self.store.endpoint_slices.values())
            if watch:
                return self._watch([{"type": "ADDED", "object": o} for o in items])
            return self._json(200, self._list("EndpointSlice", "discovery.k8s.io/v1", items))

        if path == f"/api/v1/namespaces/{ns}":
            return self._json(200, {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": _fake_k8s_meta(ns, ns),
                "status": {"phase": "Active"},
            })

        for collection, kind, api_ver in (
            (self.store.statefulsets, "StatefulSet", "apps/v1"),
            (self.store.pods, "Pod", "v1"),
            (self.store.services, "Service", "v1"),
            (self.store.configmaps, "ConfigMap", "v1"),
            (self.store.secrets, "Secret", "v1"),
            (self.store.pvcs, "PersistentVolumeClaim", "v1"),
        ):
            base = "/apis/apps/v1" if kind == "StatefulSet" else "/api/v1"
            if kind == "StatefulSet":
                list_path = f"{base}/namespaces/{ns}/statefulsets"
            else:
                list_path = f"{base}/namespaces/{ns}/{kind.lower()}s" if kind != "Pod" else f"{base}/namespaces/{ns}/pods"
            if path == list_path:
                items = list(collection.values())
                if watch:
                    events = [{"type": "ADDED", "object": o} for o in items]
                    return self._watch(events)
                return self._json(200, self._list(kind, api_ver, items))

            m = re.match(rf"{re.escape(list_path)}/([^/]+)$", path)
            if m and m.group(1) in collection:
                return self._json(200, collection[m.group(1)])

        crd = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/clickhouseinstallations.clickhouse.altinity.com"
        if path == crd:
            return self._json(200, {
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": {"name": "clickhouseinstallations.clickhouse.altinity.com"},
                "spec": {"group": _FAKE_K8S_CHI_GROUP, "names": {"plural": _FAKE_K8S_CHI_PLURAL, "kind": "ClickHouseInstallation"}, "scope": "Namespaced", "versions": [{"name": _FAKE_K8S_CHI_VERSION, "served": True, "storage": True}]},
            })

        chk_crd = f"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{_FAKE_K8S_CHK_PLURAL}.{_FAKE_K8S_CHK_GROUP}"
        if path == chk_crd:
            return self._json(200, {
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": {"name": f"{_FAKE_K8S_CHK_PLURAL}.{_FAKE_K8S_CHK_GROUP}"},
                "spec": {"group": _FAKE_K8S_CHK_GROUP, "names": {"plural": _FAKE_K8S_CHK_PLURAL, "kind": "ClickHouseKeeperInstallation"}, "scope": "Namespaced", "versions": [{"name": _FAKE_K8S_CHK_VERSION, "served": True, "storage": True}]},
            })

        self.log_message("unhandled GET %s", path)
        return self._status(404)

    def do_POST(self):  # noqa: N802
        path = urlparse(self.path).path
        if path == "/debug/reset-chi-reconcile":
            self.store.reset_for_reconcile()
            return self._json(200, {"status": "ok"})

        body = json.loads(self._read_body() or b"{}")
        ns = self.store.namespace
        rv = _fake_k8s_resource_version(self.store)

        events_path = f"/api/v1/namespaces/{ns}/events"
        if path == events_path:
            event = body if body else {
                "apiVersion": "v1",
                "kind": "Event",
                "metadata": {"name": f"fake-event-{_fake_k8s_uid()}", "namespace": ns},
            }
            event.setdefault("metadata", {})["resourceVersion"] = rv
            return self._json(201, event)

        sts_path = f"/apis/apps/v1/namespaces/{ns}/statefulsets"
        if path == sts_path:
            obj = body
            obj["metadata"]["resourceVersion"] = rv
            obj["metadata"].setdefault("uid", _fake_k8s_uid())
            self.store.statefulsets[obj["metadata"]["name"]] = obj
            pod = _fake_k8s_pod_from_sts(obj)
            pod["metadata"]["resourceVersion"] = rv
            self.store.pods[pod["metadata"]["name"]] = pod
            self.store.notify_watchers({"type": "ADDED", "object": copy.deepcopy(obj)})
            self.store.notify_watchers({"type": "ADDED", "object": copy.deepcopy(pod)})
            return self._json(201, obj)

        for collection, kind in (
            (self.store.services, "services"),
            (self.store.configmaps, "configmaps"),
            (self.store.secrets, "secrets"),
            (self.store.pvcs, "persistentvolumeclaims"),
            (self.store.endpoint_slices, "endpointslices"),
        ):
            if kind == "endpointslices":
                base = f"/apis/discovery.k8s.io/v1/namespaces/{ns}/{kind}"
            else:
                base = f"/api/v1/namespaces/{ns}/{kind}"
            if path == base:
                obj = body
                obj["metadata"]["resourceVersion"] = rv
                obj["metadata"].setdefault("uid", _fake_k8s_uid())
                collection[obj["metadata"]["name"]] = obj
                self.store.notify_watchers({"type": "ADDED", "object": copy.deepcopy(obj)})
                return self._json(201, obj)

        self.log_message("unhandled POST %s", path)
        return self._status(404)

    def do_PUT(self):  # noqa: N802
        return self._upsert()

    def do_PATCH(self):  # noqa: N802
        return self._upsert(patch=True)

    def _upsert(self, patch: bool = False):
        path = urlparse(self.path).path
        body = json.loads(self._read_body() or b"{}")
        ns = self.store.namespace
        rv = _fake_k8s_resource_version(self.store)

        chi_name = self.store.chi["metadata"]["name"]
        chi_base = f"/apis/{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}/namespaces/{ns}/{_FAKE_K8S_CHI_PLURAL}/{chi_name}"
        if path in (chi_base, chi_base + "/status"):
            if patch:
                if "metadata" in body and "finalizers" in body.get("metadata", {}):
                    self.store.chi.setdefault("metadata", {})["finalizers"] = body["metadata"]["finalizers"]
                if "status" in body:
                    self.store.chi.setdefault("status", {}).update(body["status"])
            else:
                self.store.chi.update(body)
            self.store.chi["metadata"]["resourceVersion"] = rv
            self.store.notify_watchers({"type": "MODIFIED", "object": copy.deepcopy(self.store.chi)})
            return self._json(200, self.store.chi)

        for collection in (self.store.statefulsets, self.store.services, self.store.configmaps):
            for name, obj in list(collection.items()):
                prefixes = [
                    f"/apis/apps/v1/namespaces/{ns}/statefulsets/{name}",
                    f"/api/v1/namespaces/{ns}/services/{name}",
                    f"/api/v1/namespaces/{ns}/configmaps/{name}",
                ]
                if path in prefixes:
                    if patch:
                        for k, v in body.items():
                            if k == "metadata":
                                obj["metadata"].update(v)
                            else:
                                obj[k] = v
                    else:
                        obj.update(body)
                    obj["metadata"]["resourceVersion"] = rv
                    collection[name] = obj
                    return self._json(200, obj)

        self.log_message("unhandled PATCH/PUT %s", path)
        return self._status(404)

    def do_DELETE(self):  # noqa: N802
        path = urlparse(self.path).path
        ns = self.store.namespace
        for collection, segment in (
            (self.store.statefulsets, "statefulsets"),
            (self.store.pods, "pods"),
            (self.store.services, "services"),
            (self.store.configmaps, "configmaps"),
        ):
            m = re.match(rf"/apis/apps/v1/namespaces/{re.escape(ns)}/{segment}/([^/]+)$", path)
            if not m:
                m = re.match(rf"/api/v1/namespaces/{re.escape(ns)}/{segment}/([^/]+)$", path)
            if m and m.group(1) in collection:
                obj = collection.pop(m.group(1))
                return self._json(200, obj)
        return self._status(404)


def _load_fake_k8s_chi(path: str, namespace: str) -> dict:
    with open(path, encoding="utf-8") as f:
        chi = yaml.safe_load(f)
    chi.setdefault("metadata", {})
    chi["metadata"]["namespace"] = namespace
    chi["metadata"].setdefault("name", "test-030017-chi")
    chi["metadata"].setdefault("uid", _fake_k8s_uid())
    chi["metadata"].setdefault("resourceVersion", "1")
    chi["metadata"].setdefault("generation", 1)
    chi["metadata"].setdefault(
        "finalizers",
        ["finalizer.clickhouseinstallation.altinity.com"],
    )
    chi.setdefault("status", {})
    chi.setdefault("apiVersion", f"{_FAKE_K8S_CHI_GROUP}/{_FAKE_K8S_CHI_VERSION}")
    chi.setdefault("kind", "ClickHouseInstallation")
    return chi


class _FakeK8sAPIServer:
    """In-process minimal Kubernetes API for host-run operator reconcile probes."""

    def __init__(self, namespace, chi_manifest_path, cert_path, key_path, log_path=""):
        self._namespace = namespace
        self._chi_manifest_path = chi_manifest_path
        self._cert_path = cert_path
        self._key_path = key_path
        self._log_path = log_path
        self._store = _FakeK8sStore(
            namespace,
            _load_fake_k8s_chi(chi_manifest_path, namespace),
            chi_manifest_path,
        )
        self._httpd = None
        self._thread = None
        self.port = None

    def start(self, port):
        _FakeK8sHandler.store = self._store
        _FakeK8sHandler.log_path = self._log_path or None
        self._httpd = ThreadingHTTPServer(("127.0.0.1", int(port)), _FakeK8sHandler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(self._cert_path, self._key_path)
        self._httpd.socket = ctx.wrap_socket(self._httpd.socket, server_side=True)
        self.port = int(port)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def reset_reconcile(self):
        self._store.reset_for_reconcile()


def _write_hostrun_clickhouse_tls_config(work_dir):
    """Minimal chopconf for host-run metrics-exporter → fake ClickHouse HTTPS probes."""
    config_path = os.path.join(work_dir, "hostrun-clickhouse-tls-config.yaml")
    config = {
        "security": {
            "policy": "Permissive",
            "clickhouse": {"tls": dict(_HOSTRUN_PERMISSIVE_CH_TLS)},
            "ipc": {"mode": "Plain"},
        },
        "clickhouse": {"access": dict(_HOSTRUN_CLICKHOUSE_ACCESS)},
    }
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False)
    return config_path


def _write_hostrun_operator_openssl_tls_config(work_dir, namespace=LOCAL_FAKE_K8S_NS):
    """Host-run operator chopconf: fake_k8s reconcile + Ping(https://host:8443)."""
    base_config_path = util.get_full_path(
        "../../config/config.yaml", lookup_in_host=True
    )
    config_folder = os.path.dirname(base_config_path)
    with open(base_config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    security = config.setdefault("security", {})
    security["policy"] = "Permissive"
    security.setdefault("clickhouse", {}).setdefault("tls", {}).update(
        dict(_HOSTRUN_PERMISSIVE_CH_TLS)
    )
    security.setdefault("images", {})["policy"] = "FIPSRequired"
    security.setdefault("ipc", {})["mode"] = "Plain"
    config.setdefault("watch", {}).setdefault("namespaces", {})["include"] = [namespace]
    config.setdefault("reconcile", {}).setdefault("host", {}).setdefault("wait", {}).update({
        "exclude": "no",
        "queries": "no",
        "probes": {
            "startup": "no",
            "readiness": "no",
        },
    })
    config.setdefault("clickhouse", {}).setdefault("access", {}).update(
        dict(_HOSTRUN_CLICKHOUSE_ACCESS)
    )
    for section in ("clickhouse", "keeper"):
        paths = config.get(section, {}).get("configuration", {}).get("file", {}).get("path", {})
        if isinstance(paths, dict):
            for key, value in list(paths.items()):
                if value and not os.path.isabs(value):
                    paths[key] = os.path.join(config_folder, value)

    config_path = os.path.join(work_dir, "hostrun-operator-openssl-tls-config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False)
    return config_path


def _hostrun_operator_env(namespace, kubeconfig_path):
    return _fips_hostrun_env(kubeconfig_path, namespace=namespace)


def _start_fake_k8s_server(work_dir, ca_cert_path, key_path, namespace=LOCAL_FAKE_K8S_NS):
    """Start in-process fake Kubernetes API for operator host-run probes."""
    port = int(_free_local_port())
    log_path = os.path.join(work_dir, "fake-k8s.log")
    chi_manifest = util.get_full_path(LOCAL_OPERATOR_OPENSSL_CHI_MANIFEST)
    server = _FakeK8sAPIServer(namespace, chi_manifest, ca_cert_path, key_path, log_path)
    server.start(port)
    ok, reason = _wait_for_listening_port(
        "127.0.0.1", port, timeout=15, label="fake_k8s"
    )
    if not ok:
        server.stop()
        assert False, error(f"fake_k8s not listening on 127.0.0.1:{port} ({reason})")
    return port, server, log_path


def _stop_fake_k8s_server(server):
    if server is not None:
        server.stop()


def _require_hostrun_operator_fake_k8s_session(context):
    session = getattr(context, "hostrun_operator_fake_k8s_session", None)
    if not session:
        assert False, error(
            "operator openssl s_server probe requires an active fake_k8s session; "
            "call start_hostrun_operator_fake_k8s_session first"
        )
    return session


@TestStep(Given)
def start_hostrun_operator_fake_k8s_session(self):
    """Start fake_k8s once for all operator openssl s_server cipher probes."""
    if getattr(self.context, "hostrun_operator_fake_k8s_session", None):
        note("fake_k8s session already active")
        return

    work_dir = self.context.fips_local_openssl_tls_dir
    config_path = _write_hostrun_operator_openssl_tls_config(work_dir)
    fake_k8s_port, fake_k8s_server, fake_k8s_log = _start_fake_k8s_server(
        work_dir,
        self.context.fips_local_openssl_cert,
        self.context.fips_local_openssl_key,
        namespace=LOCAL_FAKE_K8S_NS,
    )
    kubeconfig_path = os.path.join(work_dir, "operator-fake-k8s-kubeconfig")
    _write_fake_kubeconfig(
        kubeconfig_path,
        fake_k8s_port,
        self.context.fips_local_openssl_cert,
    )
    self.context.hostrun_operator_fake_k8s_session = {
        "config_path": config_path,
        "env": _hostrun_operator_env(LOCAL_FAKE_K8S_NS, kubeconfig_path),
        "server": fake_k8s_server,
        "port": fake_k8s_port,
        "log_path": fake_k8s_log,
    }
    note(f"fake_k8s -> https://127.0.0.1:{fake_k8s_port}")
    note(f"chopconf -> {config_path}")


@TestStep(Finally)
def stop_hostrun_operator_fake_k8s_session(self):
    """Stop fake_k8s started for operator openssl s_server cipher probes."""
    session = getattr(self.context, "hostrun_operator_fake_k8s_session", None)
    if not session:
        return
    _stop_fake_k8s_server(session["server"])
    self.context.hostrun_operator_fake_k8s_session = None


def _reset_hostrun_operator_fake_k8s_session(session):
    """Reset fake_k8s CHI + objects so the next operator run re-reconciles and re-Pings."""
    session["server"].reset_reconcile()


def _read_log_tail(log_path, max_lines=40):
    if not log_path or not os.path.isfile(log_path):
        return "(no log file)"
    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return "".join(lines[-max_lines:])


def _tail_text(text, max_lines=15):
    if not text:
        return "(empty)"
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(
        [f"... truncated, showing last {max_lines} lines ...", *lines[-max_lines:]]
    )


def _fips_assert(condition, message):
    """Raise AssertionError without testflows AssertEval (avoids IndexError on failure)."""
    if not condition:
        raise AssertionError(message)


_HOSTRUN_CH_TLS_FAILURE_MARKERS = (
    "setupTLSAdvanced",
    # NB function-name-stamped markers rot on rename - see _CH_CONNECT_FAILURE_MARKERS. This is an
    # any-match list that already carries the operation-shaped "FAILED Ping(", so it kept working
    # when connect() became openPools(); the stale entry is dropped rather than updated.
    "QueryContext():FAILED",
    "FAILED Ping(",
    "FAILED Open",
    "FAILED connect(",
    "remote error: tls",
    "handshake failure",
    "protocol version not supported",
    "no cipher suite",
    "no supported versions",
)


def _hostrun_ch_tls_connection_failure_in_logs(logs):
    """Match client-side TLS failure in operator/exporter ClickHouse dial logs."""
    return any(
        any(marker in line for marker in _HOSTRUN_CH_TLS_FAILURE_MARKERS)
        for line in logs.splitlines()
    )


def _hostrun_ch_tls_rejection_note(output, max_lines=30):
    """Compact TLS-rejection proof for host-run operator/exporter ClickHouse probes."""
    markers = _HOSTRUN_CH_TLS_FAILURE_MARKERS
    selected = [
        line for line in output.splitlines()
        if any(marker in line for marker in markers)
    ]
    if not selected:
        return ""
    if len(selected) > max_lines:
        selected = (
            [f"... truncated, showing last {max_lines} TLS-relevant lines ..."]
            + selected[-max_lines:]
        )
    return "\n".join(selected)


def _build_hostrun_metrics_exporter_chi_payload(chi, hostname, https_port):
    return {
        "type": "cr",
        "cr": {
            "namespace": "default",
            "name": chi,
            "labels": {},
            "annotations": {},
            "clusters": [{
                "name": "default",
                "hosts": [{
                    "name": "0-0",
                    "hostname": hostname,
                    "httpsPort": int(https_port),
                }],
            }],
        },
    }


def _wait_local_http_ready(url, timeout=10, process=None, log_path=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            _fips_assert(
                False,
                f"metrics-exporter exited with code {process.returncode} "
                f"before {url} became ready\n"
                f"log tail:\n{_read_log_tail(log_path)}",
            )
        result = subprocess.run(
            ["curl", "-sf", "-o", "/dev/null", url],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.5)
    log_tail = _read_log_tail(log_path)
    hint = ""
    if "address already in use" in log_tail:
        hint = (
            "\nHint: metrics-exporter HTTP port is already bound; "
            "use --metrics-endpoint/--chi-list-endpoint on a free port."
        )
    _fips_assert(
        False,
        f"HTTP endpoint not ready: {url}\nlog tail:\n{log_tail}{hint}",
    )


def _post_hostrun_metrics_exporter_chi(body, metrics_port=8888, token_path=None):
    cmd = [
        "curl",
        "-sS",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "-X",
        "POST",
        f"http://127.0.0.1:{metrics_port}/chi",
        "-H",
        "Content-Type: application/json",
    ]
    if token_path:
        with open(token_path, encoding="utf-8") as f:
            token = f.read().strip()
        cmd.extend(["-H", f"X-CHOP-Token: {token}"])
    cmd.extend(["-d", body])
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=False,
    )
    _fips_assert(
        result.returncode == 0,
        f"POST /chi failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
    )
    return result.stdout.strip()


def _scrape_hostrun_metrics_exporter(metrics_port=8888):
    result = subprocess.run(
        [
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            f"http://127.0.0.1:{metrics_port}/metrics",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    _fips_assert(
        result.returncode == 0,
        f"GET /metrics failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
    )
    return result.stdout.strip()


def _openssl_s_server_rejected_handshake(server_log):
    """Handshake failure lines from openssl s_server when client rejects or cannot negotiate."""
    if not server_log:
        return False
    lowered = server_log.lower()
    markers = (
        "ssl_accept:error",
        "no protocols available",
        "no shared cipher",
        "alert handshake failure",
        "wrong version number",
        "tlsv1 alert protocol version",
    )
    return any(marker in lowered for marker in markers)


def _fake_clickhouse_tls_probe_complete(output, server_log_path, expectation, cipher_suite):
    with open(server_log_path, encoding="utf-8", errors="replace") as f:
        server_log = f.read()
    if expectation == "approved":
        return bool(cipher_suite) and f"CIPHER is {cipher_suite}" in server_log
    return (
        any(err in output for err in FAKE_K8S_TLS_REJECT_ERRORS)
        or _hostrun_ch_tls_connection_failure_in_logs(output)
        or _openssl_s_server_rejected_handshake(server_log)
    )


def _wait_log_file_tls_probe(
    log_path,
    process,
    server_log_path,
    expectation,
    cipher_suite=None,
    max_wait_sec=120,
):
    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            with open(log_path, encoding="utf-8", errors="replace") as f:
                output = f.read()
            if _fake_clickhouse_tls_probe_complete(
                output, server_log_path, expectation, cipher_suite
            ):
                return output
            _fips_assert(
                False,
                f"process exited with code {process.returncode} before TLS probe completed\n"
                f"log tail:\n{_read_log_tail(log_path)}",
            )

        with open(log_path, encoding="utf-8", errors="replace") as f:
            output = f.read()
        if _fake_clickhouse_tls_probe_complete(
            output, server_log_path, expectation, cipher_suite
        ):
            return output
        time.sleep(0.5)

    with open(log_path, encoding="utf-8", errors="replace") as f:
        output = f.read()
    server_tail = _read_log_tail(server_log_path)
    _fips_assert(
        False,
        "TLS probe did not complete\n"
        f"expectation={expectation!r} cipher={cipher_suite!r}\n"
        f"binary log tail:\n{_read_log_tail(log_path)}\n"
        f"s_server log tail:\n{server_tail}",
    )


def _run_metrics_exporter_against_local_fake_clickhouse(
    binary_path,
    config_path,
    ca_cert_path,
    server_port,
    server_log_path,
    expectation,
    cipher_suite=None,
    metrics_port=None,
    max_wait_sec=60,
):
    work_dir = os.path.dirname(config_path)
    log_path = os.path.join(work_dir, "fake-clickhouse-metrics-exporter.log")
    metrics_port = metrics_port or _free_local_port()
    metrics_endpoint = f":{metrics_port}"
    env, kubeconfig_path = _hostrun_metrics_exporter_env(work_dir)
    process = None
    log_file = None
    probe_action = (
        "negotiates approved cipher with fake ClickHouse"
        if expectation == "approved"
        else "rejects TLS handshake with fake ClickHouse"
    )
    try:
        with Given(f"metrics-exporter listening on 127.0.0.1:{metrics_port}"):
            log_file = open(log_path, "w", encoding="utf-8")
            process = subprocess.Popen(
                [
                    binary_path,
                    "-logtostderr=true",
                    "-v=1",
                    f"--kubeconfig={kubeconfig_path}",
                    f"--config={config_path}",
                    f"--metrics-endpoint={metrics_endpoint}",
                    f"--chi-list-endpoint={metrics_endpoint}",
                ],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            _wait_local_http_ready(
                f"http://127.0.0.1:{metrics_port}/metrics",
                process=process,
                log_path=log_path,
            )

        with When(f"POST /chi registers 127.0.0.1:{server_port} as fake ClickHouse"):
            payload = _build_hostrun_metrics_exporter_chi_payload(
                chi=LOCAL_FAKE_CLICKHOUSE_CR,
                hostname="127.0.0.1",
                https_port=server_port,
            )
            body = json.dumps(payload, separators=(",", ":"))
            code = _post_hostrun_metrics_exporter_chi(
                body=body, metrics_port=metrics_port
            )
            _fips_assert(
                code == "200",
                f"POST /chi failed: HTTP {code!r}\nbody: {body}\n"
                f"log tail:\n{_read_log_tail(log_path)}",
            )

        with And("GET /metrics triggers ClickHouse collect"):
            metrics_code = _scrape_hostrun_metrics_exporter(
                metrics_port=metrics_port
            )
            _fips_assert(
                metrics_code == "200",
                f"GET /metrics failed: HTTP {metrics_code!r}\n"
                f"log tail:\n{_read_log_tail(log_path)}",
            )

        with When(f"metrics-exporter {probe_action}"):
            return _wait_log_file_tls_probe(
                log_path,
                process,
                server_log_path,
                expectation,
                cipher_suite=cipher_suite,
                max_wait_sec=max_wait_sec,
            )
    finally:
        if log_file is not None:
            log_file.close()
        with Finally("stop metrics-exporter"):
            _stop_process(process)


def _run_operator_against_openssl_s_server(
    binary_path,
    operator_session,
    server_log_path,
    expectation,
    cipher_suite=None,
    chi_port=None,
    max_wait_sec=180,
):
    """Run clickhouse-operator until TLS probe completes; fake_k8s must already be up."""
    config_path = operator_session["config_path"]
    env = operator_session["env"]
    chi_port = chi_port or LOCAL_OPERATOR_OPENSSL_TLS_PORT
    work_dir = os.path.dirname(config_path)
    log_path = os.path.join(work_dir, "hostrun-operator-openssl.log")
    process = None
    log_file = None
    try:
        _reset_hostrun_operator_fake_k8s_session(operator_session)

        with When("clickhouse-operator starts"):
            log_file = open(log_path, "w", encoding="utf-8")
            process = subprocess.Popen(
                [
                    binary_path,
                    "-logtostderr=true",
                    "-v=2",
                    f"--config={config_path}",
                ],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )

        with When(f"reconcile reaches Ping(https://127.0.0.1:{chi_port})"):
            return _wait_log_file_tls_probe(
                log_path,
                process,
                server_log_path,
                expectation,
                cipher_suite=cipher_suite,
                max_wait_sec=max_wait_sec,
            )
    finally:
        if log_file is not None:
            log_file.close()
        with Finally("stop clickhouse-operator"):
            _stop_process(process)


@TestStep(Then)
def assert_local_fake_clickhouse_tls_probe(
    self,
    binary_label,
    binary_path,
    expectation,
    probe_target="metrics-exporter",
    cipher_suite=None,
    tls_version="1.3",
    case_name=None,
):
    """Run openssl s_server on :8443 (operator) or random port (metrics-exporter)."""

    label = case_name or cipher_suite or f"TLS {tls_version} protocol"
    work_dir = self.context.fips_local_openssl_tls_dir
    server_port = (
        LOCAL_OPERATOR_OPENSSL_TLS_PORT
        if probe_target == "operator"
        else None
    )

    operator_session = None
    if probe_target == "operator":
        operator_session = _require_hostrun_operator_fake_k8s_session(self.context)
    else:
        with Given(f"host-run chopconf for {probe_target}"):
            if probe_target == "metrics-exporter":
                config_path = _write_hostrun_clickhouse_tls_config(work_dir)
            else:
                assert False, error(f"unknown probe_target: {probe_target!r}")
            note(f"chopconf -> {config_path}")

    with Given(f"openssl s_server on 127.0.0.1:{server_port or 'ephemeral'} ({label})"):
        start_local_openssl_server(
            cipher_suite=cipher_suite,
            tls_version=tls_version,
            port=server_port,
        )
    try:
        chi_port = self.context.fips_local_openssl_port
        server_log_path = self.context.fips_local_openssl_log_path

        if probe_target == "operator":
            output = _run_operator_against_openssl_s_server(
                binary_path=binary_path,
                operator_session=operator_session,
                server_log_path=server_log_path,
                expectation=expectation,
                cipher_suite=cipher_suite,
                chi_port=chi_port,
            )
        else:
            output = _run_metrics_exporter_against_local_fake_clickhouse(
                binary_path=binary_path,
                config_path=config_path,
                ca_cert_path=self.context.fips_local_openssl_cert,
                server_port=chi_port,
                server_log_path=server_log_path,
                expectation=expectation,
                cipher_suite=cipher_suite,
            )

        with open(server_log_path, encoding="utf-8", errors="replace") as f:
            server_log = f.read()

        if expectation == "approved":
            with Then(f"check {binary_label} negotiates {cipher_suite}"):
                cipher_line = f"CIPHER is {cipher_suite}"
                assert cipher_suite and cipher_line in server_log, error(
                    f"{binary_label} {label}: expected {cipher_suite!r} in server log, "
                    f"but none found"
                )
                note(f"cipher proof: {cipher_line} - found in logs")
        else:
            with Then(f"{binary_label} rejects TLS handshake"):
                _fips_assert(
                    _fake_clickhouse_tls_probe_complete(
                        output, server_log_path, expectation, cipher_suite
                    ),
                    f"{binary_label} {label}: expected TLS rejection in binary output, "
                    f"but none found\nbinary log tail:\n{_tail_text(output)}\n"
                    f"s_server log tail:\n{_read_log_tail(server_log_path)}",
                )
                rejection_note = _hostrun_ch_tls_rejection_note(output)
                if not rejection_note and _openssl_s_server_rejected_handshake(server_log):
                    rejection_note = (
                        "TLS rejection proof (openssl s_server log):\n"
                        f"{_tail_text(server_log)}"
                    )
                note(
                    rejection_note
                    or f"TLS rejection proof ({binary_label}):\n{_tail_text(output)}"
                )
    finally:
        with Finally("stop local openssl server"):
            stop_local_openssl_server()


@TestStep(Check)
def assert_local_fake_clickhouse_approved_tls_cases(
    self,
    binary_label,
    binary_path,
    probe_target="metrics-exporter",
):
    """Run all FIPS_APPROVED_TLS13_CIPHER_CASES against openssl s_server."""

    for case in FIPS_APPROVED_TLS13_CIPHER_CASES:
        with Check(f"{binary_label} accepts {case['name']} against openssl s_server"):
            assert_local_fake_clickhouse_tls_probe(
                binary_label=binary_label,
                binary_path=binary_path,
                probe_target=probe_target,
                tls_version=case["tls_version"],
                cipher_suite=case["cipher_suite"],
                expectation="approved",
                case_name=case["name"],
            )


@TestStep(Check)
def assert_local_fake_clickhouse_rejected_tls_cases(
    self,
    binary_label,
    binary_path,
    probe_target="metrics-exporter",
):
    """Run all FIPS_LISTENER_REJECTED_TLS_CASES against openssl s_server."""

    for case in FIPS_LISTENER_REJECTED_TLS_CASES:
        with Check(f"{binary_label} rejects {case['name']} against openssl s_server"):
            assert_local_fake_clickhouse_tls_probe(
                binary_label=binary_label,
                binary_path=binary_path,
                probe_target=probe_target,
                tls_version=case["tls_version"],
                cipher_suite=case["cipher_suite"],
                expectation="rejected",
                case_name=case["name"],
            )


@TestStep(Given)
def create_kubernetes_namespace_without_operator(self):
    """Create an isolated test namespace without installing the operator."""
    with Given("I create shell"):
        shell = get_shell()
        self.context.shell = shell

    match = re.search(r"test_\d+(?:_\d+)?", current().name)
    assert match, error(
        f"cannot derive namespace prefix from test name: {current().name!r}"
    )
    random_namespace = f"{match.group(0).replace('_', '-')}-{uuid.uuid1()}"
    self.context.test_namespace = random_namespace
    self.context.operator_namespace = random_namespace
    util.create_namespace(self.context.test_namespace)
    current().context.cleanup(delete_test_namespace)


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


def acvp_smoke(binary_name, cmd_path):
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
            # local pkg/util/fips/acvp/run.sh reproducer catches the same regression.
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


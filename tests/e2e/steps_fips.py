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

import os
import re
import shlex
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

import yaml

import e2e.util as util

from e2e.steps import create_shell_namespace_clickhouse_template
from testflows.asserts import error
from testflows.core import *

import e2e.kubectl as kubectl


FAKE_OPENSSL_SERVER = "fake-openssl-server"

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
                kubectl.run_shell(f"docker create --name {container_name} {shlex.quote(image)}")
                try:
                    kubectl.run_shell(
                        f"docker cp {container_name}:{shlex.quote(image_path)} {shlex.quote(dest)}"
                    )
                finally:
                    kubectl.run_shell(f"docker rm {container_name}", ok_to_fail=True)
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
    chi,
    chk,
    secret_name="clickhouse-certs",
    replicas=2,
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

    for replica in range(replicas):
        for host in (
            f"chi-{chi}-default-0-{replica}",
            f"chk-{chk}-keeper-0-{replica}",
        ):
            for suffix in dns_suffixes:
                dns_names.append(f"{host}{suffix}")

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
        "ca_crt": ca_crt,
        "server_crt": server_crt,
        "server_key": server_key,
        "dhparam": dhparam,
        "certs_dir": certs_dir,
        "dns_names": dns_names,
    }


# ---------------------------------------------------------------------------
# External ClickHouse client
# ---------------------------------------------------------------------------

@TestStep(Given)
def start_external_ch_container(self, ns=None, cipher_suites=None):
    ns = ns or self.context.test_namespace
    ca_crt = self.context.tls["ca_crt"]
    container = f"fips-ch-ext-{ns}"[:63]
    fips_image = "altinity/clickhouse-server:25.3.8.30001.altinityfips"

    cipher_suites_xml = ""
    if cipher_suites:
        cipher_suites_xml = (
            f"\n      <cipherSuites>{':'.join(cipher_suites)}</cipherSuites>"
        )

    openssl_xml = f"""
<clickhouse>
  <openSSL>
    <client>
      <caConfig>/tmp/ca.crt</caConfig>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <verificationMode>strict</verificationMode>{cipher_suites_xml}
    </client>
  </openSSL>
</clickhouse>
""".strip()

    with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False) as f:
        client_config = f.name
        f.write(openssl_xml)

    subprocess.run(
        ["docker", "rm", "-f", container],
        capture_output=True,
        text=True,
        check=False,
    )

    result = subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container,
            "--network", "host",
            "-v", f"{ca_crt}:/tmp/ca.crt:ro",
            "-v", f"{client_config}:/tmp/client.xml:ro",
            fips_image,
            "sleep", "infinity",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, error(
        f"failed to start external ClickHouse client container\n"
        f"image: {fips_image}\n"
        f"exit code: {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )

    note(f"external ClickHouse client container started: {container}")
    self.context.external_chi_container = container

    yield

    with Finally("stop external ClickHouse client container"):
        stop_external_ch_container()

@TestStep(Finally)
def stop_external_ch_container(self):
    """Remove the scenario-scoped external ClickHouse Docker client container."""
    container = self.context.external_chi_container
    subprocess.run(
        ["docker", "rm", "-f", container],
        capture_output=True,
        text=True,
        check=False,
    )


@TestStep(When)
def fips_ch_external_secure_query(self, pod, sql, ns=None):
    """Run a ClickHouse query from the scenario Docker client over port-forward TLS.

    Requires ``start_external_ch_container`` at scenario start.
    """
    ns = ns or self.context.test_namespace
    container = self.context.external_chi_container
    local_port = _free_local_port()

    pf = subprocess.Popen(
        [
            "kubectl",
            *self.context.kubectl_context_args,
            "-n", ns,
            "port-forward",
            f"pod/{pod}",
            f"{local_port}:9440",
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
                raise RuntimeError(
                    f"kubectl port-forward exited early\n"
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
            raise RuntimeError(
                f"kubectl port-forward to {pod}:9440 "
                f"did not become ready on 127.0.0.1:{local_port}"
            )

        query_result = subprocess.run(
            [
                "docker", "exec", container,
                "clickhouse-client",
                "--config-file", "/tmp/client.xml",
                "--secure",
                "--host", "127.0.0.1",
                "--port", local_port,
                "--query", sql,
            ],
            text=True,
            capture_output=True,
            check=False,
        )

        assert query_result.returncode == 0, error(
            f"external Docker clickhouse-client failed\n"
            f"container: {container}\n"
            f"exit code: {query_result.returncode}\n"
            f"stdout:\n{query_result.stdout}\n"
            f"stderr:\n{query_result.stderr}"
        )

        return query_result.stdout.strip()
    finally:
        pf.terminate()
        try:
            pf.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pf.kill()


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


@TestStep(When)
def fips_run_openssl_s_client_on_pod_port(
    self,
    pod,
    port,
    cipher_suite="TLS_AES_128_GCM_SHA256",
    tls_version="1.3",
    ok_to_fail=False,
    ns=None,
):
    """Run ``openssl s_client`` against a pod listener through ``kubectl port-forward``."""
    ns = ns or self.context.test_namespace
    ca_crt = self.context.tls["ca_crt"]
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

        command = [
            "openssl", "s_client",
            "-connect", f"127.0.0.1:{local_port}",
            "-servername", "localhost",
            "-CAfile", ca_crt,
            "-verify_return_error",
        ]

        if tls_version == "1.3":
            command.append("-tls1_3")
            if cipher_suite:
                command.extend(["-ciphersuites", cipher_suite])
        elif tls_version == "1.2":
            command.append("-tls1_2")
            if cipher_suite:
                command.extend(["-cipher", cipher_suite])
        elif tls_version == "1.1":
            command.append("-tls1_1")
            if cipher_suite:
                command.extend(["-cipher", cipher_suite])
        else:
            raise ValueError(f"unsupported TLS version: {tls_version}")

        result = subprocess.run(
            command,
            input="Q\n",
            text=True,
            capture_output=True,
            check=False,
        )

        output = f"{result.stdout}\n{result.stderr}"

        if not ok_to_fail:
            assert result.returncode == 0, error(
                f"{pod}:{port}: openssl s_client failed for "
                f"tls={tls_version}, cipher={cipher_suite}\n"
                f"exit code: {result.returncode}\n"
                f"output:\n{output}"
            )

        return output

    finally:
        pf.terminate()
        try:
            pf.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pf.kill()

@TestStep(Then)
def fips_assert_rejected_tls_probes(
    self,
    chi_pods,
    chk_pods,
    ns=None,
):
    """Assert rejected TLS protocol/cipher combinations fail handshake."""
    ns = ns or self.context.test_namespace

    rejected_cases = (
        {
            "name": "TLS 1.3 ChaCha20-Poly1305",
            "tls_version": "1.3",
            "cipher_suite": "TLS_CHACHA20_POLY1305_SHA256",
        },
        {
            "name": "TLS 1.1 protocol",
            "tls_version": "1.1",
            "cipher_suite": None,
        },
    )

    endpoints = (
        ("ClickHouse HTTPS", chi_pods[0], 8443),
        ("ClickHouse native TLS", chi_pods[0], 9440),
        ("ClickHouse interserver HTTPS", chi_pods[0], 9010),
        ("Keeper secure client", chk_pods[0], 2281),
        ("Backup API HTTPS", chi_pods[0], 7171),
    )

    rejected_markers = (
        "Cipher is (NONE)",
        "handshake failure",
        "no protocols available",
        "no shared cipher",
    )

    for case in rejected_cases:
        for label, pod, port in endpoints:
            with Then(f"{label} {pod}:{port} rejects {case['name']}"):
                output = fips_run_openssl_s_client_on_pod_port(
                    pod=pod,
                    port=port,
                    tls_version=case["tls_version"],
                    cipher_suite=case["cipher_suite"],
                    ok_to_fail=True,
                    ns=ns,
                )

                assert any(
                    marker.lower() in output.lower()
                    for marker in rejected_markers
                ), error(
                    f"{label} {pod}:{port}: expected rejected TLS probe to fail "
                    f"for {case['name']}\n"
                    f"output:\n{output}"
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
    """Verify an external strict-TLS client sees a FIPS ClickHouse server."""
    version = fips_ch_external_secure_query(pod=pod, sql="SELECT version()")
    note(f"external SELECT version(): {version}")
    assert "fips" in version.lower(), error(
        f"expected FIPS in ClickHouse version(), got {version!r}"
    )

@TestStep(Then)
def fips_assert_operator_tls_rejection_in_logs(
    self,
    workload,
    min_version="1.3",
    rejection="remote error: tls: protocol version not supported",
    operator_namespace=None,
    max_iters=60,
    sleep_s=5,
):
    """Poll operator logs until the expected TLS version rejection is observed."""

    operator_namespace = operator_namespace or current().context.operator_namespace

    expected_setup_parts = (
        "setupTLSAdvanced():TLS setup OK",
        f"minVersion={min_version}",
    )

    last_logs = ""

    for attempt in range(max_iters):
        operator_pod = kubectl.get_operator_pod(ns=operator_namespace)
        last_logs = get_container_logs(
            pod=operator_pod,
            container="clickhouse-operator",
            ns=operator_namespace,
        )

        setup_found = any(
            all(part in line for part in expected_setup_parts)
            for line in last_logs.splitlines()
        )

        rejection_found = any(
            "connect():FAILED" in line and rejection in line
            for line in last_logs.splitlines()
        )

        if setup_found and rejection_found:
            note(
                f"{workload}: observed TLS version rejection "
                f"after attempt {attempt + 1}/{max_iters}"
            )
            return

        if attempt + 1 < max_iters:
            time.sleep(sleep_s)

    matching_lines = "\n".join(
        line for line in last_logs.splitlines()
        if (
            "setupTLSAdvanced()" in line
            or "connect():FAILED" in line
            or "tls:" in line
            or "minVersion" in line
        )
    )

    assert False, error(
        f"{workload}: expected TLS version rejection not found\n"
        f"expected setup line parts: {expected_setup_parts}\n"
        f"expected rejection: {rejection}\n\n"
        f"matching log lines:\n{matching_lines or '(none)'}"
    )

@TestStep(Then)
def fips_assert_chi_tls_rejected(
    self,
    chi,
    expected_status="InProgress",
    min_version="1.3",
):
    """Assert CHI remains unfinished because operator TLS client rejects server TLS policy."""
    kubectl.wait_object(
        "pod",
        "",
        label=f"-l clickhouse.altinity.com/chi={chi}",
        count=1,
    )

    status = kubectl.get_chi_status(chi)
    assert status != "Completed", error(
        f"CHI {chi} reached Completed; expected operator TLS client to reject the server"
    )
    if status != expected_status:
        kubectl.wait_chi_status(chi, expected_status)

    fips_assert_operator_tls_rejection_in_logs(
        workload=f"chi/{chi}",
        min_version=min_version,
    )


@TestStep(Then)
def fips_assert_chk_tls_rejected(
    self,
    chk,
    expected_status="InProgress",
    min_version="1.3",
):
    """Assert CHK remains unfinished because operator TLS client rejects server TLS policy."""
    kubectl.wait_object(
        "pod",
        "",
        label=f"-l clickhouse-keeper.altinity.com/chk={chk}",
        count=1,
    )

    status = kubectl.get_field("chk", chk, ".status.status")
    assert status != "Completed", error(
        f"CHK {chk} reached Completed; expected operator TLS client to reject the server"
    )
    if status != expected_status:
        kubectl.wait_chk_status(chk, expected_status)

    fips_assert_operator_tls_rejection_in_logs(
        workload=f"chk/{chk}",
        min_version=min_version,
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

@TestStep(Then)
def check_fips_cast_failure(self, binary_path, binary, cast_name="HMAC-SHA2-256"):
    """Assert binary exits non-zero when FIPS CAST is forced to fail."""
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

    output = f"{result.stdout}\n{result.stderr}"

    assert result.returncode != 0, error(
        f"{binary}: expected CAST failure exit, got {result.returncode}\n{output}"
    )
    assert f"FIPS 140-3 self-test failed: {cast_name}" in output, error(
        f"{binary}: expected CAST failure for {cast_name}\n{output}"
    )
    assert "simulated CAST failure" in output, error(
        f"{binary}: expected simulated CAST failure message\n{output}"
    )

def _free_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return str(s.getsockname()[1])


@TestStep(Then)
def check_fips_integrity_failure(self, binary_path, binary_label):
    """Assert binary panics when the .go.fipsinfo HMAC is tampered with."""

    cmd = f"readelf -S -W {shlex.quote(binary_path)}"
    readelf_out = kubectl.run_shell(cmd)

    match = re.search(r"\.go\.fipsinfo\s+\w+\s+\w+\s+([0-9a-fA-F]+)", readelf_out)
    assert match, error(f"{binary_label}: .go.fipsinfo section not found in ELF headers")

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
    target_host="localhost",
    container=None,
    ns=None,
):
    """Verify a TLS 1.3 cipher cannot be negotiated."""

    ns = ns or self.context.test_namespace

    container_arg = f"-c {container} " if container else ""

    out = kubectl.launch(
        f"exec {pod} {container_arg}-- "
        "openssl s_client "
        f"-connect {target_host}:{port} "
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
def fips_cleanup_admission_only_chi(self, chi):
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

@TestStep(Then)
def check_synthetic_tls13_smoke_from_operator_pod(self, chi_pod, ns=None):
    """Synthetic TLS 1.3 AES-256 smoke from operator pod containers.

    Uses real endpoints:
    * Kubernetes API: kubernetes.default.svc:443
    * ClickHouse HTTPS: CHI pod IP:8443

    Kubernetes is checked in two parts:
    * verbose unauthenticated /version request proves TLS version/cipher;
    * non-verbose authenticated pod API request proves service-account API access
      without leaking the bearer token into logs.

    CHK is intentionally excluded because the operator does not normally
    establish a runtime TLS client session to ClickHouse Keeper.
    """
    test_ns = ns or current().context.test_namespace
    operator_ns = current().context.operator_namespace
    operator_pod = kubectl.get_operator_pod(ns=operator_ns)

    chi_ip = kubectl.get_field(
        "pod",
        chi_pod,
        ".status.podIP",
        ns=test_ns,
    )

    approved_cipher = "TLS_AES_256_GCM_SHA384"
    k8s_auth_url = (
        "https://kubernetes.default.svc:443"
        f"/api/v1/namespaces/{operator_ns}/pods/{operator_pod}"
    )

    for container in ("clickhouse-operator", "metrics-exporter"):
        with Then(f"{container} negotiates AES-256 TLS 1.3 to Kubernetes API"):
            tls_out = kubectl.launch(
                f"exec {operator_pod} -c {container} -- "
                "sh -c '"
                "curl -sS -v "
                "--tlsv1.3 "
                f"--tls13-ciphers {approved_cipher} "
                "--cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt "
                "-o /dev/null "
                "-w \"HTTP:%{http_code}\" "
                "https://kubernetes.default.svc:443/version "
                "2>&1"
                "'",
                ns=operator_ns,
                ok_to_fail=True,
            )

            assert "TLSv1.3" in tls_out, error(
                f"{container}: expected TLSv1.3 to Kubernetes API\n{tls_out}"
            )
            assert approved_cipher in tls_out, error(
                f"{container}: expected {approved_cipher} to Kubernetes API\n{tls_out}"
            )
            assert "HTTP:200" in tls_out, error(
                f"{container}: expected Kubernetes /version HTTP 200\n{tls_out}"
            )

            auth_out = kubectl.launch(
                f"exec {operator_pod} -c {container} -- "
                "sh -c '"
                "IFS= read -r TOKEN < /var/run/secrets/kubernetes.io/serviceaccount/token; "
                "curl -sS "
                "--tlsv1.3 "
                f"--tls13-ciphers {approved_cipher} "
                "--cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt "
                "-H \"Authorization: Bearer ${TOKEN}\" "
                "-o /dev/null "
                "-w \"HTTP:%{http_code}\" "
                f"{k8s_auth_url}"
                "'",
                ns=operator_ns,
                ok_to_fail=True,
            )

            assert "HTTP:200" in auth_out, error(
                f"{container}: expected authenticated Kubernetes API HTTP 200\n{auth_out}"
            )

        with And(f"{container} negotiates AES-256 TLS 1.3 to ClickHouse HTTPS"):
            out = kubectl.launch(
                f"exec {operator_pod} -c {container} -- "
                "sh -c '"
                "curl -sS -k -v "
                "--tlsv1.3 "
                f"--tls13-ciphers {approved_cipher} "
                "-o /dev/null "
                "-w \"HTTP:%{http_code}\" "
                f"https://{chi_ip}:8443/ping "
                "2>&1"
                "'",
                ns=operator_ns,
                ok_to_fail=True,
            )

            assert "TLSv1.3" in out, error(
                f"{container}: expected TLSv1.3 to ClickHouse HTTPS\n{out}"
            )
            assert approved_cipher in out, error(
                f"{container}: expected {approved_cipher} to ClickHouse HTTPS\n{out}"
            )
            assert "HTTP:200" in out, error(
                f"{container}: expected ClickHouse /ping HTTP 200\n{out}"
            )



@TestStep(Finally)
def fips_delete_fake_openssl_server(self, ns=None):
    """Delete fake OpenSSL TLS server pod/service."""
    ns = ns or current().context.test_namespace

    kubectl.launch(
        f"delete svc {FAKE_OPENSSL_SERVER} --ignore-not-found",
        ns=ns,
        ok_to_fail=True,
    )
    kubectl.launch(
        f"delete pod {FAKE_OPENSSL_SERVER} --ignore-not-found",
        ns=ns,
        ok_to_fail=True,
    )


@TestStep(Given)
def fips_create_fake_openssl_server(self, cipher_suite, ns=None):
    """Create fake TLS 1.3 server restricted to one cipher suite."""
    ns = ns or current().context.test_namespace

    fips_delete_fake_openssl_server(ns=ns)

    manifest = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {FAKE_OPENSSL_SERVER}
  labels:
    app: {FAKE_OPENSSL_SERVER}
spec:
  restartPolicy: Always
  containers:
  - name: openssl
    image: altinity/clickhouse-server:25.3.8.30001.altinityfips
    command: ["sh", "-lc"]
    args:
    - |
      openssl s_server \\
        -accept 18443 \\
        -cert /tls/server.crt \\
        -key /tls/server.key \\
        -tls1_3 \\
        -ciphersuites {cipher_suite} \\
        -www \\
        -state
    ports:
    - containerPort: 18443
    volumeMounts:
    - name: tls
      mountPath: /tls
      readOnly: true
  volumes:
  - name: tls
    secret:
      secretName: clickhouse-certs
"""

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix="-fake-openssl-server.yaml",
            delete=False,
        ) as f:
            f.write(manifest)
            tmp_path = f.name

        kubectl.launch(f"apply -f {shlex.quote(tmp_path)}", ns=ns)

        kubectl.launch(
            f"expose pod {FAKE_OPENSSL_SERVER} "
            f"--name={FAKE_OPENSSL_SERVER} "
            "--port=8443 "
            "--target-port=18443",
            ns=ns,
        )

        kubectl.launch(
            f"wait pod {FAKE_OPENSSL_SERVER} "
            "--for=condition=Ready "
            "--timeout=120s",
            ns=ns,
        )
    finally:
        if tmp_path:
            os.unlink(tmp_path)


@TestStep(When)
def fips_curl_tls13_from_operator_container(
    self,
    container,
    url,
    cipher_suite,
    ns=None,
):
    """Run curl from one operator pod container with forced TLS 1.3 cipher."""
    ns = ns or current().context.operator_namespace
    operator_pod = kubectl.get_operator_pod(ns=ns)

    return kubectl.launch(
        f"exec {operator_pod} -c {container} -- "
        "sh -c '"
        "curl -k -sS -v "
        "--tlsv1.3 "
        f"--tls13-ciphers {cipher_suite} "
        f"{url} "
        "-o /dev/null "
        "-w \"HTTP:%{http_code}\" "
        "2>&1"
        "'",
        ns=ns,
        ok_to_fail=True,
    )


@TestStep(Then)
def fips_assert_operator_containers_tls13_fails(
    self,
    url,
    cipher_suite,
    ns=None,
):
    """Assert both operator pod containers fail with the requested TLS 1.3 cipher."""
    ns = ns or current().context.operator_namespace

    failure_needles = (
        "handshake failure",
        "no shared cipher",
        "alert handshake failure",
        "TLS connect error",
        "HTTP:000",
    )

    for container in ("clickhouse-operator", "metrics-exporter"):
        with Then(f"{container} fails to negotiate {cipher_suite} to {url}"):
            out = fips_curl_tls13_from_operator_container(
                container=container,
                url=url,
                cipher_suite=cipher_suite,
                ns=ns,
            )

            assert any(needle in out for needle in failure_needles), error(
                f"{container}: expected TLS handshake failure for {cipher_suite}\n{out}"
            )


@TestStep(Then)
def fips_assert_connection_rejected_on_non_approved_cipher(
    self,
    ns=None,
):
    """Assert fake ChaCha-only TLS server rejects approved AES-only clients.

    This is the synthetic negative control for rejected cipher behavior.
    """
    ns = ns or current().context.test_namespace

    approved_cipher = "TLS_AES_256_GCM_SHA384"
    rejected_cipher = "TLS_CHACHA20_POLY1305_SHA256"
    fake_url = f"https://{FAKE_OPENSSL_SERVER}:8443/ping"

    with Given("fake OpenSSL server offers only non-approved ChaCha TLS 1.3 cipher"):
        fips_create_fake_openssl_server(
            cipher_suite=rejected_cipher,
            ns=ns,
        )

    with Then("operator pod containers restricted to approved AES-256 fail the handshake"):
        fips_assert_operator_containers_tls13_fails(
            url=fake_url,
            cipher_suite=approved_cipher,
            ns=ns,
        )

    with Finally("delete fake OpenSSL server"):
        fips_delete_fake_openssl_server(ns=ns)
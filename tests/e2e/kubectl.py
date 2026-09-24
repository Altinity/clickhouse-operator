import json
import os
import subprocess
import time
import threading

from testflows.core import *
from testflows.asserts import error
# from testflows.connect import Shell

from e2e.retry_sleep import retry_sleep

import e2e.settings as settings
import e2e.yaml_manifest as yaml_manifest
import e2e.util as util

current_dir = os.path.dirname(os.path.abspath(__file__))
max_retries = 20

# Reconcile statuses that mean the operator has STOPPED working on the CR. Polling for a
# different value from here just burns the whole retry budget — 495s at retries=20/backoff=5 —
# so a genuine regression shows up as a timeout instead of a failure. Mirrors StatusAborted in
# pkg/apis/clickhouse.altinity.com/v1/type_status.go. StatusTerminating is deliberately absent:
# DeleteStart() sets it and the object then disappears, so get_field returns "" anyway.
_STATUS_COMPLETED = "Completed"
_STATUS_ABORTED = "Aborted"
_STATUS_IN_PROGRESS = "InProgress"
_STATUS_TERMINATING = "Terminating"
_TERMINAL_CR_STATUSES = frozenset({_STATUS_COMPLETED, _STATUS_ABORTED})

# Aborted is NOT reliably terminal: reconcile.recovery.onStatus.aborted.onPodReady ships as
# "retry", so a pod flipping NotReady -> Ready re-enqueues the CR and it transits
# Aborted -> InProgress -> Completed on its own (test_010035). Two independent guards keep that
# from turning into a flaky failure:
#   - ARMED: a terminal status is only counted once a NON-terminal one has been observed first.
#     Without this, the status left over from the PREVIOUS apply — before the operator has picked
#     up the new spec — reads as a fresh failure.
#   - HELD: it must then persist across this many consecutive polls AND this many seconds.
# An Aborted CR additionally has to be provably unrecoverable (see _abort_needs_spec_edit) before
# it is failed at all. Everything else runs the full budget: the operator's own recovery windows -
# a 300s statefulSet.update.timeout, kubelet's 300s CrashLoop backoff cap - are the same order as
# that budget, so there is no hold long enough to be safe and short enough to be worth having.
_terminal_confirm_polls = 3
# 120s of continuous hold, on top of arming. Poll times are 0,5,15,30,50,75,105,135, so the
# earliest give-up is ~135s against a 495s budget.
_terminal_confirm_sec = 120

# Statuses the operator actually writes (type_status.go:35-38). Arming requires one of these:
# get_field returns "" for a missing CR or a transient kubectl failure, and treating that as
# "the operator is making progress" would arm the watch on noise.
_KNOWN_CR_STATUSES = _TERMINAL_CR_STATUSES | {_STATUS_IN_PROGRESS, _STATUS_TERMINATING}

# An Aborted CR is only unrecoverable when the SPEC itself must change. Mirror the operator
# rather than guessing: normalizeTimeAbortReasons (pkg/controller/chi/worker-pod-retry.go:44-50)
# is the authoritative list, and shouldTriggerAutoRecovery re-enqueues every OTHER Aborted CR on
# any pod NotReady->Ready flip (default on). Crucially the commonest abort - a plain
# statefulSet.update.timeout - carries NO reason tag at all and IS recoverable, so the test must
# keep waiting for it. Whitelisting "self-clearing" reasons instead would have this backwards and
# would fast-fail the recoverable majority.
_SPEC_EDIT_ABORT_REASONS = (
    "FIPSValidationFailed",
    "RootCAConflict",
    "RootCASecretUnresolved",
    "FIPSImagePolicyViolation",
    "RemovedSecretRefSyntax",
)


# container .state.waiting.reason values kubelet will never leave on its own: the image cannot be
# pulled, or the container cannot be built from the given spec. Each needs a spec edit to clear,
# so a readiness wait sitting on one is unsatisfiable and is pure dead time.
#
# CrashLoopBackOff is DELIBERATELY ABSENT. kubelet's restart backoff caps at 300s, so a pod that
# boots on its sixth try - ClickHouse racing a not-yet-elected Keeper, or the test_010031 ConfigMap
# mount race - shows this reason continuously for longer than any hold worth having against a 495s
# budget. test_operator.py:1163 already hand-checks it where it genuinely is terminal.
_TERMINAL_POD_WAITING_REASONS = frozenset({
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
})

# Readings proving kubelet is actively working the pod, so the watch may arm. "<none>" is what
# custom-columns prints for a container that is not waiting at all. "" means an absent pod or a
# kubectl blip and must NOT arm - the same rule as _KNOWN_CR_STATUSES.
_KNOWN_POD_WAITING_REASONS = _TERMINAL_POD_WAITING_REASONS | {
    "<none>",
    "ContainerCreating",
    "PodInitializing",
}


def _is_unset(value):
    """kubectl -o=custom-columns renders an absent field as the literal <none>, not ''."""
    return (value is None) or (value.strip() in ("", "<none>"))


def _terminal_pod_fail_on(kind, field):
    """Terminal waiting reasons for a pod READINESS wait.

    Scoped to waits for `.ready`. A test that waits FOR a waiting.reason on purpose - polling for
    ErrImagePull or CrashLoopBackOff to appear - is watching a different field and derives the
    empty set, so it keeps today's behaviour with no per-test edit.
    """
    if kind not in ("pod", "pods"):
        return frozenset()
    if not field.endswith(".ready"):
        return frozenset()
    return _TERMINAL_POD_WAITING_REASONS


def _pod_waiting_reason(kind, name, field, ns, shell, fail_on):
    """Read the sibling waiting.reason column and reduce it to one value.

    A `[*]` field yields one comma-joined token per container, so a terminal token anywhere wins;
    failing that, report any known token so the watch can arm.
    """
    raw = get_field(kind, name, field[: -len(".ready")] + ".state.waiting.reason", ns, shell=shell)
    tokens = [t.strip() for t in raw.split(",")] if raw else []
    return next(
        (t for t in tokens if t in fail_on),
        next((t for t in tokens if t in _KNOWN_POD_WAITING_REASONS), ""),
    )


def _abort_needs_spec_edit(newest_error):
    """True when the CURRENT abort carries a reason the operator itself refuses to auto-recover.

    ReconcileAbortWithReason emits "[Reason] message" and PushError prepends, so entry [0] is the
    abort we are looking at. Only that entry is inspected: .status.errors is append-only and never
    cleared, so a tag pushed earlier in the same test would otherwise make every later abort -
    including a perfectly recoverable one - look permanent, and fail the wait early.
    """
    if _is_unset(newest_error):
        return False
    return any(newest_error.startswith(f"[{reason}]") for reason in _SPEC_EDIT_ABORT_REASONS)


def _terminal_fail_on(kind, field, accepted):
    """Terminal statuses that make this particular wait unsatisfiable.

    Scoped deliberately. A wait for InProgress legitimately STARTS from a terminal status (a new
    spec applied over an Aborted CR), and a wait that already accepts Aborted is opting out by
    construction — so both derive the empty set and keep today's behaviour with no per-test edit.
    Pod, PVC and StatefulSet waits never acquire CR-status vocabulary.
    """
    if kind not in ("chi", "chk"):
        return frozenset()
    if field != ".status.status":
        return frozenset()
    # Only a wait whose every acceptable value is itself terminal can conclude anything from
    # seeing the OTHER terminal value. A wait for InProgress legitimately starts from a terminal
    # status - a fresh spec applied over a Completed or Aborted CR - so it derives the empty set.
    if not accepted or not accepted <= _TERMINAL_CR_STATUSES:
        return frozenset()
    return _TERMINAL_CR_STATUSES - accepted


class _TerminalStatusWatch:
    """Decides when a terminal-and-wrong status has been seen long enough to be believed.

    Pure and side-effect free so the guard is testable without a cluster; `wait_field` owns the
    polling and the reporting. `observe` returns True once the wait is provably unsatisfiable.
    """

    def __init__(self, fail_on, confirm_polls=None, confirm_sec=None, known=None):
        self.fail_on = fail_on
        self.known = _KNOWN_CR_STATUSES if known is None else known
        self.confirm_polls = _terminal_confirm_polls if confirm_polls is None else confirm_polls
        self.confirm_sec = _terminal_confirm_sec if confirm_sec is None else confirm_sec
        self.armed = False
        self.polls = 0
        self.since = None

    def observe(self, value, now):
        if value not in self.fail_on:
            # A reading the operator actually wrote, and not the wrong terminal one, proves it is
            # still moving this CR: arm, and invalidate whatever streak preceded it. An unknown
            # value ("" from a missing CR or a kubectl blip) proves nothing and is ignored.
            if value in self.known:
                self.armed = True
                self.polls = 0
                self.since = None
            return False
        if not self.armed:
            # Terminal from the very first look: this is the previous apply's status, not a
            # verdict on ours. Wait it out exactly as before.
            return False
        self.polls += 1
        if self.since is None:
            self.since = now
        return (self.polls >= self.confirm_polls) and ((now - self.since) >= self.confirm_sec)


def _terminal_status_report(kind, name, field, cur_value, desc, polls, held, ns, shell):
    """Failure text naming the status reached and whatever the CR recorded about why."""
    lines = [
        f"{kind} {name} {field} reached terminal status {cur_value!r} and held it for "
        f"{int(held)}s across {polls} consecutive polls while waiting for {desc}. "
        f"The operator has finished reconciling this CR, so the remaining retries are dead "
        f"time - failing now instead of timing out."
    ]
    status_error = get_field(kind, name, ".status.error", ns, shell=shell)
    errors = get_field(kind, name, ".status.errors", ns, shell=shell)
    if not _is_unset(status_error):
        lines.append(f".status.error: {status_error}")
    if not _is_unset(errors):
        lines.append(f".status.errors: {errors}")
    if _is_unset(status_error) and _is_unset(errors):
        lines.append(".status.error / .status.errors: <empty>")
    return "\n".join(lines)

# A transient kube-apiserver/network outage (operator restart, etcd hiccup, brief
# network loss, a minikube control-plane bounce) makes kubectl exit non-zero with
# one of these connectivity signatures rather than a real command result. Matched
# case-insensitively (values are lowercase) against the merged stdout+stderr stream.
# Deliberately conservative: only client-side transport/discovery phrases that can
# never be a legitimate command outcome -- NOT NotFound/AlreadyExists/Invalid/
# Forbidden, which are deterministic results that must keep failing fast.
_TRANSIENT_APISERVER_ERRORS = (
    "no route to host",
    "connection refused",
    "unexpected eof",
    "couldn't get current server api group list",
    "the connection to the server",  # e.g. "...localhost:8080 was refused"
    "was refused - did you specify the right host",
    "unable to connect to the server",
    "i/o timeout",
    "tls handshake timeout",
    "etcdserver: request timed out",
    "etcdserver: leader changed",
    "the server is currently unable to handle the request",
    "transport is closing",
    # Aggregated discovery has not caught up with a CRD that was just (re)installed. Reads like a
    # deterministic error but is genuinely retryable: test_090099 deletes the CHI CRD, reinstalls
    # the operator and immediately applies a CHI. Bounded by _transient_max_retries, not 500s.
    "no matches for kind",
    "could not find the requested resource",
)
# Bounded retry budget for transient kube-apiserver errors (see run_shell).
_transient_max_retries = 5


def launch(command, ok_to_fail=False, ns=None, timeout=600, shell=None):
    # Build commanddef launch

    if ns is None:
        if hasattr(current().context, "test_namespace"):
            ns = current().context.test_namespace

    cmd = f"{current().context.kubectl_cmd} "
    cmd_args = command.split(" ")
    if ns is not None and ns != "" and ns != "--all-namespaces":
        cmd += f"{cmd_args[0]} --namespace={ns} "
    elif ns == "--all-namespaces":
        cmd += f"{cmd_args[0]} {ns} "
    else:
        cmd += f"{cmd_args[0]} "

    if len(cmd_args) > 1:
        cmd += " ".join(cmd_args[1:])

    # save command for debug purposes
    # command = cmd
    # print(f"run command: {cmd}")

    # retry_transient: kubectl-only resilience (launch builds a kubectl command),
    # so a momentary apiserver blip is retried rather than hard-failing the test.
    # Direct run_shell() callers keep fail-fast: host tooling in steps_fips.py
    # (`go version -m`, `--fips-info`, readelf) and the piped-manifest path in
    # delete(). apply()'s piped branch opts in explicitly, being a kubectl call.
    return run_shell(cmd, timeout, ok_to_fail, shell=shell, retry_transient=True)


def run_shell(cmd, timeout=600, ok_to_fail=False, shell=None, retry_transient=False):
    # Run command

    attempt = 0
    while True:
        if shell is None:
            res_cmd = current().context.shell(cmd, timeout=timeout)
        else:
            res_cmd = shell(cmd, timeout=timeout)

        # Check command failure
        code = res_cmd.exitcode
        if code == 0 or ok_to_fail:
            # Command test result
            return res_cmd.output if (code == 0) or ok_to_fail else ""

        # code != 0 and ok_to_fail is False. Retry ONLY a transient kube-apiserver
        # connectivity blip (retry_transient is set by launch() for kubectl calls),
        # bounded. A genuine failure (NotFound, validation, ...) matches nothing here
        # and falls straight through to the original print+assert, unchanged.
        output = res_cmd.output or ""
        if (
            retry_transient
            and attempt < _transient_max_retries
            and any(sig in output.lower() for sig in _TRANSIENT_APISERVER_ERRORS)
        ):
            attempt += 1
            retry_sleep(
                attempt,
                5,
                reason=f"Transient kube-apiserver error (attempt {attempt}/{_transient_max_retries})",
            )
            continue

        print(f"command failed, command:\n{cmd}")
        print(f"command failed, exit code:\n{code}")
        print(f"command failed, output :\n{output}")
        assert code == 0, error()


def run_host_cmd(cmd, timeout=60, ok_to_fail=False):
    """Run a command on the test host, with a timeout that is actually enforced."""
    # Not run_shell(): that hands the command to a testflows Shell whose expect()
    # budget is refreshed by every newline and is never given an absolute deadline, so a
    # command that keeps emitting output never times out, and one that hangs outright is
    # not killed -- it just pollutes the session. subprocess kills the child, so a
    # caller's retry loop sees a failure instead of hanging the whole suite. That is
    # also why the default is small: 60s total wall clock, where run_shell's 600s is a
    # per-line budget. Use for host tooling; the docker calls in steps_fips.py are the
    # first. kubectl calls belong in launch(), which adds kube-apiserver retry handling.
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        if not ok_to_fail:
            raise
        note(f"timed out after {timeout}s, tolerated: {cmd}")
        # Hand back whatever the child wrote before the kill. TimeoutExpired carries it
        # as bytes even under text=True.
        return "".join(
            stream.decode(errors="replace") if isinstance(stream, bytes) else stream
            for stream in (exc.stdout, exc.stderr) if stream
        )
    output = f"{result.stdout}{result.stderr}"
    assert ok_to_fail or result.returncode == 0, error(
        f"host command failed with exit code {result.returncode}: {cmd}\n{output}"
    )
    return output


def delete_kind(kind, name, ns=None, ok_to_fail=False, shell=None, wait=True):
    with When(f"Delete {kind} {name}"):
        wait_flag = "" if wait else "--wait=false"
        launch(
            f"delete {kind} {name} -v 5 --now --timeout=600s {wait_flag}".strip(),
            ns=ns,
            timeout=600,
            ok_to_fail=ok_to_fail,
            shell=shell
        )


def force_clear_finalizers(kind, name, ns=None, shell=None):
    """Remove a stuck CR's finalizers so the apiserver can reap it.

    Used when the operator was killed or is mid-restart (e.g. chopconf
    onChange=restart) and never removed the finalizer. Logs a WARNING so a
    genuine operator-cleanup bug stays visible rather than being silently masked.
    """
    print(f"WARNING: {kind}/{name} still present; force-clearing finalizers (operator may be killed/restarting)")
    launch(
        f"patch {kind} {name} --type=merge -p '{{\"metadata\":{{\"finalizers\":null}}}}'",
        ns=ns, ok_to_fail=True, shell=shell,
    )


def delete_chi(chi, ns=None, wait=True, ok_undeleted = False, ok_to_fail=False, shell=None):
    if settings.no_cleanup:
        print(f"NO_CLEANUP is set, skipping delete_chi: {chi}")
        return

    delete_kind("chi", chi, ns=ns, ok_to_fail=ok_to_fail, shell=shell)

    if wait:
        wait_object("chi", chi, count=0, ns=ns, shell=shell)

        with Then(f"All {chi} objects should be deleted"):
            chi_objects     = get_obj_names(chi, "pod,service,sts,pvc,cm,pdb,secret", kind = 'chi', ns=ns, shell=shell)
            chi_objects_ext = get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=chi, ns=ns, shell=shell)
            name_collision = get_count("chk", chk=chi, ns=ns, shell=shell)

            if len(chi_objects_ext)>0 and not name_collision:
                print("WARNING: some objects were not deleted:")
                print(*chi_objects_ext, sep='\n')
                assert ok_undeleted or len(chi_objects_ext)==0
            elif len(chi_objects) > 0:
                print("WARNING: some objects were not deleted:")
                print(chi_objects, sep='\n')
                assert ok_undeleted or len(chi_objects)==0


def delete_chk(chk, ns=None, wait=True, ok_to_fail=False, shell=None):
    if settings.no_cleanup:
        print(f"NO_CLEANUP is set, skipping delete_chk: {chk}")
        return

    delete_kind("chk", chk, ns=ns, ok_to_fail=ok_to_fail, shell=shell)

    if wait:
        # Canonical watch-driven readiness: kubectl wait --for=delete returns the
        # instant the apiserver removes the CR from etcd, sidestepping the poll
        # window in wait_object. If the CHK is already gone this returns OK fast.
        target_ns = ns if ns is not None else current().context.test_namespace
        launch(
            f"wait --for=delete chk/{chk} --timeout=300s",
            ns=target_ns, ok_to_fail=True, shell=shell,
        )
        # Defensive: if the operator pod was killed mid-finalizer-removal (e.g.
        # because operator runs in the same namespace as the CHK and the
        # namespace is mid-tear-down), the CHK CR stays stuck with finalizer.
        # Force-clear so namespace deletion can proceed instead of hanging the
        # whole suite on a downstream timeout.
        if get_count("chk", name=chk, ns=ns, shell=shell):
            force_clear_finalizers("chk", chk, ns=target_ns, shell=shell)

        # def wait_object(kind, name, names=[], label="", count=1, ns=None, retries=max_retries, backoff=5, shell=None):
        wait_object("chk", chk, count=0, ns=ns, shell=shell)

        with Then(f"All {chk} objects should be deleted"):
            # Pod/PVC termination is async after STS deletion — poll until clean
            chk_objects = []
            chk_objects_ext = []
            name_collision = 0
            for i in range(1, max_retries):
                chk_objects = get_obj_names(chk, "pod,service,sts,pvc,cm,pdb,secret", kind='chk', ns=ns, shell=shell)
                chk_objects_ext = get_obj_names_grepped("pod,service,sts,pvc,cm,pdb,secret", grep=chk, ns=ns, shell=shell)
                name_collision = get_count("chi", chi=chk, ns=ns, shell=shell)
                if (len(chk_objects_ext) == 0 or name_collision) and len(chk_objects) == 0:
                    break
                retry_sleep(i, 5, "Not ready")

            if len(chk_objects_ext) > 0 and not name_collision:
                print("WARNING: some objects were not deleted:")
                print(*chk_objects_ext, sep='\n')
                assert len(chk_objects_ext) == 0
            elif len(chk_objects) > 0:
                print("WARNING: some objects were not deleted:")
                print(chk_objects, sep='\n')
                assert len(chk_objects) == 0


def delete_all_chi(ns=None):
    delete_all("chi", ns=ns)


def delete_all_chk(ns=None):
    delete_all("chk", ns=ns)


def delete_all(kind, ns=None):
    crds = launch("get crds -o=custom-columns=name:.spec.names.shortNames[0]", ns=ns).splitlines()
    if kind in crds:
        try:
            to_delete = get(kind, "", ns=ns, ok_to_fail=True) or {}
        except Exception:
            to_delete = {}
        if "items" in to_delete:
            for i in to_delete["items"]:
                name = i["metadata"]["name"]
                # Initial delete. ok_to_fail: a stuck finalizer (operator killed
                # OR mid-restart — e.g. chopconf onChange=restart in test_030008)
                # makes `kubectl delete --timeout` exit non-zero; we recover via
                # the force-clear loop below, so this must not raise here.
                delete_kind(kind, name, ns=ns, ok_to_fail=True, wait=False)
                # Stuck/re-attached finalizer recovery. The operator can RE-ATTACH
                # a finalizer after a clear while it is restarting, so a single
                # wait_object would race the restart and raise. Re-clear + re-delete
                # up to max_retries until the CR is actually reaped. We WARN on each
                # pass so a genuine operator-cleanup bug stays visible in the logs
                # instead of being silently masked.
                for attempt in range(1, max_retries):
                    if get_count(kind, name=name, ns=ns) == 0:
                        break
                    force_clear_finalizers(kind, name, ns=ns)
                    delete_kind(kind, name, ns=ns, ok_to_fail=True, wait=False)
                    # Only sleep if another re-check follows; skip on the last
                    # attempt so a genuinely-stuck CR hits the final wait_object
                    # (the authoritative leak assertion) without an extra wait.
                    if attempt < max_retries - 1:
                        retry_sleep(attempt, 5, f"{kind}/{name} still terminating")
                # Final assertion: if the CR survived every force-clear, this
                # raises — surfacing a real cleanup leak rather than hiding it.
                # wait_object(kind, name, ns=ns, count=0)


def delete_all_keeper(ns=None):
    for keeper_type in ("zookeeper-operator", "zookeeper", "clickhouse-keeper"):
        expected_resource_types = (
            ("zookeepercluster",) if keeper_type == "zookeeper-operator" else ("sts", "pvc", "cm", "svc")
        )
        for resource_type in expected_resource_types:
            try:
                item_list = get(
                    resource_type,
                    "",
                    label=f"-l app={keeper_type}",
                    ns=ns,
                    ok_to_fail=True,
                ) or {}
            except Exception as e:
                item_list = {}
            if "items" in item_list:
                for item in item_list["items"]:
                    name = item["metadata"]["name"]
                    launch(f"delete {resource_type} -n {current().context.test_namespace} {name}", ok_to_fail=True)


def create_and_check(manifest, check, kind="chi", ns=None, shell=None, timeout=1800):
    chi_name = yaml_manifest.get_name(util.get_full_path(manifest))

    if kind == "chi":
        label = f"-l clickhouse.altinity.com/chi={chi_name}"
    elif kind == "chk":
        label = f"-l clickhouse-keeper.altinity.com/chk={chi_name}"
    else:
        assert False, error(f"Unknown kind {kind}")

    if "apply_templates" in check:
        debug("Need to apply additional templates")
        for t in check["apply_templates"]:
            debug(f"Applying template: {util.get_full_path(t, False)} \n{t}")
            apply(util.get_full_path(t, False), ns=ns, shell=shell)
        time.sleep(5)

    apply_chi(util.get_full_path(manifest, False), ns=ns, timeout=timeout, shell=shell)

    if "chi_status" in check:
        wait_chi_status(chi_name, check["chi_status"], ns=ns, shell=shell)
    elif "chk_status" in check:
        wait_chk_status(chi_name, check["chk_status"], ns=ns, shell=shell)
    else:
        # Wait for reconcile to start before performing other checks. In some cases it does not start, so we can pass
        # wait_field_changed("chi", chi_name, state_field, prev_state, ns)
        wait_field(kind=kind, name=chi_name, field=".status.status", value="InProgress"
                   , ns=ns, retries=3, throw_error=False, shell=shell)
        actionPlan = get_actionPlan(kind, chi_name, ns, shell)
        print(actionPlan)

        wait_field(kind=kind, name=chi_name, field=".status.status", value="Completed"
                   , ns=ns, shell=shell)

    if "object_counts" in check:
        wait_objects(chi_name, check["object_counts"], ns=ns, shell=shell)

    if "pod_count" in check:
        wait_object(
            "pod",
            "",
            label=label,
            count=check["pod_count"],
            ns=ns,
            shell=shell
        )

    if "pod_image" in check:
        check_pod_image(chi_name, check["pod_image"], ns=ns, shell=shell)

    if "pod_volumes" in check:
        check_pod_volumes(chi_name, check["pod_volumes"], ns=ns, shell=shell)

    if "pod_podAntiAffinity" in check:
        check_pod_antiaffinity(chi_name, ns=ns, shell=shell)

    if "pod_ports" in check:
        check_pod_ports(chi_name, check["pod_ports"], ns=ns, shell=shell)

    if "service" in check:
        check_service(check["service"][0], check["service"][1], ns=ns, shell=shell)

    if "configmaps" in check:
        check_configmaps(chi_name, ns=ns, shell=shell)

    if "pdb" in check:
        check_pdb(chi_name, kind, check["pdb"], ns=ns, shell=shell)

    if "do_not_delete" not in check:
        delete_chi(chi_name, ns=ns, shell=shell)


def get(kind, name, label="", ns=None, ok_to_fail=False, shell=None):
    out = launch(f"get {kind} {name} {label} -o json", ns=ns, ok_to_fail=ok_to_fail, shell=shell)
    stripped = out.strip()

    if not stripped or stripped.startswith("Error"):
        if ok_to_fail:
            return None
        raise ValueError(f"kubectl returned error: {stripped}")

    try:
        return json.loads(stripped)
    except json.JSONDecodeError as e:
        if ok_to_fail:
            return None
        raise ValueError(f"Failed to parse JSON from: {stripped}") from e


def get_container_restart_count(pod_name, container=None, ns=None, shell=None):
    """restartCount of a single named container in a pod; None if pod/container absent.

    Name-scoped on purpose: callers detecting a SPECIFIC container's in-place
    restart must not use the pod-total sum (which moves whenever any sibling
    container restarts). Parses the pod JSON in Python to avoid the jsonpath
    quoting hazard of an inline `[?(@.name=="...")]` filter.
    """
    pod = get("pod", pod_name, ns=ns, ok_to_fail=True, shell=shell)
    if not pod:
        return None
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    if container != None:
        for cs in statuses:
            if cs.get("name") == container:
                return int(cs.get("restartCount") or 0)
    else:
        return sum(int(cs.get("restartCount") or 0) for cs in statuses)
    return None


def get_chi_normalizedCompleted(chi, ns=None, shell=None):
    chi_storage = get("configmap", f"chi-storage-{chi}", ns=ns)
    return json.loads(chi_storage["data"]["status-normalizedCompleted"])

def get_actionPlan(kind, name, ns=None, shell=None):
    if kind == 'chi':
        storage = get("configmap", f"chi-storage-{name}", ok_to_fail=True, ns=ns)
        if storage != None:
            return storage["data"].get("status-actionPlan", "")
    return ""


def create_ns(ns):
    if ns is None:
        launch(f"create ns {current().context.test_namespace}", ns=None)
        launch(f"get ns {current().context.test_namespace}", ns=None)
    else:
        launch(f"create ns {ns}", ns=None)
        launch(f"get ns {ns}", ns=None)


def delete_ns(ns = None, delete_chi=False, ok_to_fail=False, timeout=1000):
    if ns is None:
        ns = current().context.test_namespace
    if delete_chi:
        delete_all_chi(ns)
        delete_all_chk(ns)
    launch(
        f"delete ns {ns} -v 5 --now --timeout={timeout}s",
        ns=None,
        ok_to_fail=ok_to_fail,
        timeout=timeout,
    )
    for attempt in retries(timeout=300, delay=10):
        with attempt:
            out = launch(f"get namespace {ns}", ok_to_fail=True)
            assert "Error" in out


def get_count(kind, name="", label="", chi="", chk ="", ns=None, shell=None):
    if chi != "" and label == "":
        label = f"-l clickhouse.altinity.com/chi={chi}"
    if chk != "" and label == "":
        label = f"-l clickhouse-keeper.altinity.com/chk={chk}"

    if ns is None:
        ns = current().context.test_namespace

    if kind == "pv":
        # pv is not namespaced so need to search namespace in claimRef
        if name:
            out = launch(f"get pv {name} --no-headers", ok_to_fail=True, shell=shell)
            if (out is None) or (len(out) == 0):
                return 0
            return len(out.splitlines())
        else:
            out = launch(f'get pv {label} -o yaml | grep "namespace: {ns}"', ok_to_fail=True, shell=shell)
    else:
        out = launch(
            f"get {kind} {name} -o=custom-columns=kind:kind,name:.metadata.name {label}",
            ns=ns,
            ok_to_fail=True,
            shell=shell
        )

    if (out is None) or (len(out) == 0):
        return 0
    else:
        return len(out.splitlines()) - 1


def count_objects(label="", ns=None, shell=None):
    return {
        "statefulset": get_count("sts", ns=ns, label=label, shell=shell),
        "pod": get_count("pod", ns=ns, label=label, shell=shell),
        "service": get_count("service", ns=ns, label=label, shell=shell),
    }


def apply(manifest, ns=None, validate=True, timeout=600, shell=None):
    # No blanket retry loop. Both branches below already retry the transient apiserver signatures
    # in _TRANSIENT_APISERVER_ERRORS, bounded to _transient_max_retries. Everything else a kubectl
    # apply can fail with - CRD validation, a pruned unknown field, a malformed manifest - is
    # deterministic: retrying it ~250 times cost 500s and buried the real error under the last
    # attempt's identical copy of it.
    with When(f"{manifest} is applied"):
        if " | " not in manifest:
            manifest = f'"{manifest}"'
            launch(f"apply --validate={validate} -f {manifest}", ns=ns, timeout=timeout, shell=shell)
        else:
            # This piped server-side branch bypasses launch(), so it has to ask for the transient
            # retry itself - it is the operator-install path that every test runs.
            run_shell(
                f"set -o pipefail && {manifest} | {current().context.kubectl_cmd} apply --server-side --force-conflicts --namespace={current().context.test_namespace} --validate={validate} -f -",
                timeout=timeout,
                shell=shell,
                retry_transient=True,
            )


def apply_chi(manifest, ns=None, validate=True, timeout=600, shell=None):
    if ns is None:
        ns = current().context.test_namespace
    chi_name = yaml_manifest.get_name(manifest)
    with When(f"CHI {chi_name} is applied"):
        if current().context.kubectl_mode == "replace":
            if get_count("chi", chi_name, ns=ns) == 0:
                create(manifest, ns=ns, validate=validate, timeout=timeout)
            else:
                replace(manifest, ns=ns, validate=validate, timeout=timeout)
        else:
            apply(manifest, ns=ns, validate=validate, timeout=timeout, shell=shell)


def create(manifest, ns=None, validate=True, timeout=600):
    with When(f"{manifest} is created"):
        if "<(" not in manifest:
            manifest = f'"{manifest}"'
        launch(f"create --validate={validate} -f {manifest}", ns=ns, timeout=timeout)


def replace(manifest, ns=None, validate=True, timeout=600):
    with When(f"{manifest} is replaced"):
        if "<(" not in manifest:
            manifest = f'"{manifest}"'
        launch(f"replace --validate={validate} -f {manifest}", ns=ns, timeout=timeout)


def delete(manifest, ns=None, timeout=600, ok_to_fail=False):
    with When(f"{manifest} is deleted"):
        if " | " not in manifest:
            manifest = f'"{manifest}"'
            return launch(f"delete -f {manifest}", ns=ns, timeout=timeout, ok_to_fail=ok_to_fail)
        else:
            run_shell(f"{manifest} | {current().context.kubectl_cmd} delete -f -", timeout=timeout, ok_to_fail=ok_to_fail)


def wait_objects(chi, object_counts, ns=None, shell=None, retries=max_retries):
    with Then(
        f"Waiting for: "
        f"{object_counts['statefulset']} statefulsets, "
        f"{object_counts['pod']} pods and "
        f"{object_counts['service']} services "
        f"to be available"
    ):
        for i in range(1, retries):
            cur_object_counts = count_objects(label=f"-l clickhouse.altinity.com/chi={chi}", ns=ns, shell=shell)
            if cur_object_counts == object_counts:
                break
            retry_sleep(i, 5,
                "Not ready. [ "
                f"statefulset: {cur_object_counts['statefulset']} "
                f"pod: {cur_object_counts['pod']} "
                f"service: {cur_object_counts['service']} ]",
            )
        assert cur_object_counts == object_counts, error()


def wait_object(kind, name, names=[], label="", count=1, ns=None, retries=max_retries, backoff=5, shell=None):
    with Then(f"{count} {kind}(s) {name} should be created"):
        for i in range(1, retries):
            cur_count = get_count(kind, ns=ns, name=name, label=label, shell=shell)
            if cur_count == count:
                break
            retry_sleep(i, backoff, f"Not ready ({cur_count}/{count})")
        assert cur_count == count, error()


def wait_command(command, result, count=1, ns=None, retries=max_retries):
    with Then(f"{command} should return {result}"):
        for i in range(1, retries):
            res = launch(command, ok_to_fail=True, ns=ns)
            if res == result:
                break
            retry_sleep(i, 5, f"Not ready ({res})")
        assert res == result, error()


def wait_chi_status(chi, status, ns=None, retries=max_retries, throw_error=True, shell=None):
    wait_field("chi", chi, ".status.status", status, ns, retries, throw_error=throw_error, shell=shell)


def wait_chk_status(chk, status, ns=None, retries=max_retries, throw_error=True, shell=None):
    wait_field("chk", chk, ".status.status", status, ns, retries, throw_error=throw_error, shell=shell)


def get_chi_status(chi, ns=None):
    return get_field("chi", chi, ".status.status", ns)


def wait_pod_status(pod, status, shell=None, ns=None):
    wait_field("pod", pod, ".status.phase", status, ns, shell=shell)


def get_pod_status(pod, shell=None, ns=None):
    return get_field("pod", pod, ".status.phase", ns, shell=shell)

def wait_container_status(pod, status, shell=None, ns=None):
    wait_field("pod", pod, ".status.containerStatuses[0].ready", status, ns, shell=shell)

def get_container_status(pod, container_index=0, shell=None, ns=None):
    return get_field("pod", pod, f".status.containerStatuses[{container_index}].ready", ns, shell=shell)


def get_condition_status(pod_name, condition_type, shell=None, ns=None):
    pod = get("pod", pod_name, ns=ns, ok_to_fail=True, shell=shell)
    if not pod:
        return None
    conditions = (pod.get("status") or {}).get("conditions") or []
    for condition in conditions:
        if condition.get("type") == condition_type:
            return condition.get("status")
    return None

def wait_field(
    kind,
    name,
    field,
    value,
    ns=None,
    retries=max_retries,
    backoff=5,
    throw_error=True,
    shell=None,
):
    # `value` may be a single scalar (str/int/bool) — match by equality — or
    # a collection (list/tuple/set/frozenset) — match if the field equals ANY
    # element. The collection form lets callers accept multiple acceptable
    # states for racy K8s transitions (e.g. ErrImagePull → ImagePullBackOff
    # within seconds).
    if isinstance(value, (list, tuple, set, frozenset)):
        accepted = set(value)
        if not accepted:
            raise ValueError("wait_field: collection value must be non-empty")
        match = lambda v: v in accepted
        # sort the *string representations* so mixed-type collections (e.g.
        # ["x", None] or [1, "1"]) don't raise TypeError before polling begins.
        desc = f"one of {sorted(map(repr, accepted))}"
    else:
        accepted = {value}
        match = lambda v: v == value
        desc = repr(value)

    fail_on = _terminal_fail_on(kind, field, accepted)
    pod_fail_on = _terminal_pod_fail_on(kind, field)

    with Then(f"{kind} {name} {field} should be {desc}"):
        cur_value = get_field(kind, name, field, ns, shell=shell)
        watch = _TerminalStatusWatch(fail_on)
        pod_watch = _TerminalStatusWatch(pod_fail_on, known=_KNOWN_POD_WAITING_REASONS)
        for i in range(1, retries):
            if match(cur_value):
                break
            if pod_fail_on:
                # Costs one extra read, and only on an iteration that was about to sleep anyway.
                reason = _pod_waiting_reason(kind, name, field, ns, shell, pod_fail_on)
                if pod_watch.observe(reason, time.time()):
                    if throw_error is False:
                        break
                    assert False, error(
                        f"pod {name} is waiting with reason {reason!r}, held for "
                        f"{int(time.time() - pod_watch.since)}s across {pod_watch.polls} polls "
                        f"while waiting for {field} to be {desc}. kubelet cannot clear this "
                        f"without a spec change - failing now instead of timing out."
                    )
            if watch.observe(cur_value, time.time()):
                held = time.time() - watch.since
                # An Aborted CR is only failed early when its CURRENT abort is one the operator
                # itself will not auto-recover. Everything else - above all the untagged
                # update-timeout abort, the commonest of the lot - runs the full budget. The
                # mirror case (waiting FOR Aborted, seeing Completed) has no reason to inspect.
                unrecoverable = (cur_value != _STATUS_ABORTED) or _abort_needs_spec_edit(
                    get_field(kind, name, ".status.errors[0]", ns, shell=shell)
                )
                if not unrecoverable:
                    # Recoverable: drop the streak so the next verdict costs one read per confirm
                    # window rather than one per poll, while still re-arming if it later aborts
                    # with a spec-edit reason.
                    watch.polls = 0
                    watch.since = None
                elif throw_error is False:
                    break
                else:
                    assert False, error(
                        _terminal_status_report(
                            kind, name, field, cur_value, desc, watch.polls, held, ns, shell
                        )
                    )
            retry_sleep(i, backoff, f"Not ready ({cur_value})")
            cur_value = get_field(kind, name, field, ns, shell=shell)
        assert match(cur_value) or throw_error is False, error()


def wait_field_changed(
    kind,
    name,
    field,
    prev_value,
    ns=None,
    retries=max_retries,
    backoff=5,
    throw_error=True,
):
    with Then(f"{kind} {name} {field} should be different from {prev_value}"):
        for i in range(1, retries):
            cur_value = get_field(kind, name, field, ns)
            if cur_value != "" and cur_value != prev_value:
                break
            retry_sleep(i, backoff, "Not ready")
        assert cur_value != "" and cur_value != prev_value or throw_error == False, error()


def wait_jsonpath(kind, name, field, value, ns=None, retries=max_retries):
    with Then(f"{kind} {name} -o jsonpath={field} should be {value}"):
        for i in range(1, retries):
            cur_value = get_jsonpath(kind, name, field, ns)
            if cur_value == value:
                break
            retry_sleep(i, 5, f"Not ready ({cur_value})")
        assert cur_value == value, error()


def get_field(kind, name, field, ns=None, shell=None):
    out = launch(f"get {kind} {name} -o=custom-columns=field:\"{field}\"", ns=ns, ok_to_fail=True, shell=shell).splitlines()
    if len(out) > 1:
        return out[1]
    else:
        return ""


def get_jsonpath(kind, name, field, ns=None):
    out = launch(f'get {kind} {name} -o jsonpath="{field}"', ns=ns).splitlines()
    return out[0]


def get_default_storage_class(ns=None):
    out = launch(
        f"get storageclass "
        f"-o=custom-columns="
        r'DEFAULT:".metadata.annotations.storageclass\.kubernetes\.io/is-default-class",NAME:.metadata.name',
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()
    out = launch(
        f"get storageclass "
        f"-o=custom-columns="
        r'DEFAULT:".metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class",NAME:.metadata.name',
        ns=ns,
    ).splitlines()
    for line in out[1:]:
        if line.startswith("true"):
            parts = line.split(maxsplit=1)
            return parts[1].strip()


def get_pod_spec(chi_name, pod_name="", ns=None, shell=None):
    label = f"-l clickhouse.altinity.com/chi={chi_name}"
    if pod_name == "":
        pod = get("pod", "", ns=ns, label=label, shell=shell)["items"][0]
    else:
        pod = get("pod", pod_name, ns=ns, shell=shell)
    return pod["spec"]

def get_chk_pod_spec(chk_name, pod_name="", ns=None, shell=None):
    label = f"-l clickhouse-keeper.altinity.com/chk={chk_name}"
    if pod_name == "":
        pod = get("pod", "", ns=ns, label=label, shell=shell)["items"][0]
    else:
        pod = get("pod", pod_name, ns=ns, shell=shell)
    return pod["spec"]

def get_pod_status_full(chi_name, pod_name="", ns=None, shell=None):
    label = f"-l clickhouse.altinity.com/chi={chi_name}"
    if pod_name == "":
        pod = get("pod", "", ns=ns, label=label, shell=shell)["items"][0]
    else:
        pod = get("pod", pod_name, ns=ns, shell=shell)
    return pod["status"]


def get_clickhouse_start(chi_name, ns=None, shell=None):
    pod_name = get_pod_names(chi_name, ns=ns, shell=shell)[0]
    return get_field("pod", pod_name, ".status.containerStatuses[0].state.running.startedAt")


def get_pod_image(chi_name, pod_name="", ns=None, shell=None):
    pod_image = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["image"]
    return pod_image


def get_pod_names(chi_name, ns=None, shell=None):
    return get_obj_names(chi_name, "pods", kind="chi", ns=ns, shell=shell)


def get_chk_pod_names(chk_name, ns=None, shell=None):
    return get_obj_names(chk_name, "pods", kind="chk", ns=ns, shell=shell)


def get_obj_names(chi_name, obj_type="pods", kind = "chi", ns=None, shell=None):
    label = ""
    if kind == "chi":
        label = f"-l clickhouse.altinity.com/chi={chi_name}"
    elif kind == "chk":
        label = f"-l clickhouse-keeper.altinity.com/chk={chi_name}"
    obj_names = launch(
        f"get {obj_type} -o=custom-columns=name:.metadata.name {label}",
        ns=ns,
    ).splitlines()
    return obj_names[1:]


def get_obj_names_grepped(obj_type="pods", grep = '', ns=None, shell=None):
    obj_names = launch(
        f"get {obj_type} -o=custom-columns=type:.kind,name:.metadata.name",
        ns=ns,
    ).splitlines()[1:]
    return sorted(filter(lambda o: grep in o, obj_names))


def get_pod_volumes(chi_name, pod_name="", ns=None, shell=None):
    volume_mounts = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["volumeMounts"]
    return volume_mounts


def get_pod_ports(chi_name, pod_name="", ns=None, shell=None):
    port_specs = get_pod_spec(chi_name, pod_name, ns, shell=shell)["containers"][0]["ports"]
    ports = []
    for p in port_specs:
        ports.append(p["containerPort"])
    return ports

def get_operator_pod(ns=None, shell=None):
    out = launch(f"get pod -l app=clickhouse-operator -o=custom-columns=field:.metadata.name", ns=ns, ok_to_fail=True, shell=shell).splitlines()
    if len(out) > 1:
        return out[1]
    else:
        return ""

def check_pod_ports(chi_name, ports, ns=None, shell=None):
    pod_ports = get_pod_ports(chi_name, ns=ns, shell=shell)
    with Then(f"Expect pod ports {pod_ports} to match {ports}"):
        assert sorted(pod_ports) == sorted(ports)


def check_pod_image(chi_name, image, ns=None, shell=None):
    pod_image = get_pod_image(chi_name, ns=ns, shell=shell)
    with Then(f"Expect pod image {pod_image} to match {image}"):
        assert pod_image == image


def check_pod_volumes(chi_name, volumes, ns=None, shell=None):
    pod_volumes = get_pod_volumes(chi_name, ns=ns, shell=shell)
    for v in volumes:
        with Then(f"Expect pod has volume mount {v}"):
            found = 0
            for vm in pod_volumes:
                if vm["mountPath"] == v:
                    found = 1
                    break
            assert found == 1


def get_pvc_size(pvc_name, ns=None):
    return get_field("pvc", pvc_name, ".spec.resources.requests.storage", ns)


def get_pv_name(pvc_name, ns=None):
    return get_field("pvc", pvc_name, ".spec.volumeName", ns)


def get_pv_size(pvc_name, ns=None):
    return get_field("pv", get_pv_name(pvc_name, ns), ".spec.capacity.storage", ns)


def check_pod_antiaffinity(
    chi_name,
    pod_name="",
    match_labels={},
    topologyKey="kubernetes.io/hostname",
    ns=None,
    shell=None
):
    pod_spec = get_pod_spec(chi_name, pod_name, ns, shell=shell)
    if match_labels == {}:
        match_labels = {
            "clickhouse.altinity.com/app": "chop",
            "clickhouse.altinity.com/chi": f"{chi_name}",
            "clickhouse.altinity.com/namespace": f"{current().context.test_namespace}",
        }
    expected = {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {
                "labelSelector": {
                    "matchLabels": match_labels,
                },
                "topologyKey": f"{topologyKey}",
            },
        ],
    }
    with Then(f"Expect podAntiAffinity to exist and match {expected}"):
        assert "affinity" in pod_spec
        assert "podAntiAffinity" in pod_spec["affinity"]
        assert pod_spec["affinity"]["podAntiAffinity"] == expected


def check_service(service_name, service_type, headless = False, ns=None, shell=None):
    with When(f"{service_name} is available"):
        service = get("service", service_name, ns=ns, shell=shell)

        with Then(f"Service type is {service_type}"):
            assert service["spec"]["type"] == service_type

        if service_type == "ClusterIP":
            clusterIP = service["spec"]["clusterIP"]
            if headless:
                with Then("clusterIP should be None"):
                    if clusterIP != "None":
                        print(f"ERROR: clusterIP should be None but it is: {clusterIP}")
                    assert clusterIP == "None"
            else:
                with Then("clusterIP should be set"):
                    assert clusterIP != "None"


def check_configmaps(chi_name, ns=None, shell=None):
    check_configmap(
        f"chi-{chi_name}-common-configd",
        [
            "01-clickhouse-01-listen.xml",
            "01-clickhouse-02-logger.xml",
            "01-clickhouse-03-query_log.xml",
        ],
        ns=ns,
        shell=shell
    )

    check_configmap(
        f"chi-{chi_name}-common-usersd",
        [
            "01-clickhouse-operator-profile.xml",
            "02-clickhouse-default-profile.xml",
        ],
        ns=ns,
        shell=shell
    )


def check_configmap(cfg_name, values, ns=None, shell=None):
    cfm = get("configmap", cfg_name, ns=ns, shell=shell)
    for v in values:
        with Then(f"{cfg_name} should contain {v}"):
            assert v in cfm["data"], error()


def check_pdb(chi, kind, clusters, ns=None, shell=None):
    if kind == "chi":
        label = "clickhouse.altinity.com"
    elif kind == "chk":
        label = "clickhouse-keeper.altinity.com"
    else:
        error("Unknown kind:" + kind)

    for c in clusters.keys():
        with Then(f"PDB is configured for cluster {c}"):
            is_managed = True
            if isinstance(clusters[c], dict):
                is_managed = clusters[c].get("is_managed", True)
                max_unavailable = clusters[c].get("max_unavailable", 1)
            else:
                # Treat simple integer as maxUnavailable to ensure backward compatibility.
                max_unavailable = clusters[c]

            pdb = get("pdb", kind + "-" + chi + "-" + c, ok_to_fail=is_managed is False, shell=shell)

            if not is_managed:
                assert pdb is None
                continue

            labels = pdb["spec"]["selector"]["matchLabels"]
            assert labels[f"{label}/app"] == "chop"
            if kind == "chi":
                assert labels[f"{label}/chi"] == chi
            else:
                assert labels[f"{label}/chk"] == chi
            assert labels[f"{label}/cluster"] == c
            assert labels[f"{label}/namespace"] == current().context.test_namespace
            assert pdb["spec"]["maxUnavailable"] == max_unavailable

def force_chi_reconcile(chi, taskID="reconcile", status="Completed", ns=None, shell=None):
    force_reconcile(chi, "chi", taskID, status, ns, shell)

def force_chk_reconcile(chk, taskID="reconcile", status="Completed", ns=None, shell=None):
    force_reconcile(chk, "chk", taskID, status, ns, shell)


def force_reconcile(name, kind, taskID, status="Completed", ns=None, shell=None):
    with Then(f"Trigger {kind} reconcile with taskID:\"{taskID}\""):
        cmd = f'patch {kind} {name} --type=\'json\' --patch=\'[{{"op":"add","path":"/spec/taskID","value":"{taskID}"}}]\''
        launch(cmd, ns=ns, shell=shell)
        # CHI waits STRICTLY for InProgress first. That wait is not cosmetic - it is the only
        # thing proving the operator has observed this taskID. The CR is already Completed when
        # force_reconcile is called, so accepting Completed here returns before the patch has
        # been picked up, and the caller then reads pre-patch status as if the reconcile had run.
        # A taskID patch edits .spec, so a CHI always produces a real reconcile and this wait
        # always resolves.
        #
        # CHK takes an ACCEPT-SET because its reconciler genuinely may not transition: it returns
        # "No reconcile work" when only taskID changed, leaving status Completed throughout, and a
        # strict poll would burn its whole retry budget - 495s - before asserting.
        if kind == "chi":
            wait_chi_status(name, "InProgress", ns=ns, shell=shell)
            wait_chi_status(name, status, ns=ns, shell=shell)
        elif kind == "chk":
            wait_chk_status(name, ["InProgress", status], ns=ns, shell=shell)
            wait_chk_status(name, status, ns=ns, shell=shell)
        else:
            assert kind == "chi" or kind == "chk"

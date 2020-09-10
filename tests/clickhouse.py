from kubectl import *

from settings import test_namespace


def clickhouse_query(chi_name, query, with_error=False, pod="", host="127.0.0.1",  port="9000", user="default", pwd="", ns=test_namespace, timeout=60, advanced_params=''):
    pod_names = kube_get_pod_names(chi_name, ns)
    pod_name = pod_names[0]
    for p in pod_names:
        if host in p or p == pod:
            pod_name = p
            break

    if pwd != "":
        pwd_str = f"--password={pwd}"
    else:
        pwd_str = ""

    if with_error:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -mn -h {host} --port={port} -u {user} {pwd_str} {advanced_params} --query=\"{query}\" 2>&1", ok_to_fail=True, timeout=timeout)
    else:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -mn -h {host} --port={port} -u {user} {pwd_str} {advanced_params} --query=\"{query}\"", timeout=timeout)


def clickhouse_query_with_error(chi_name, query, pod="", host="127.0.0.1", port="9000", user="default", pwd="", ns=test_namespace, timeout=60, advanced_params=''):
    return clickhouse_query(chi_name, query, True, pod, host, port, user, pwd, ns, timeout, advanced_params)

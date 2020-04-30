from kubectl import *


def clickhouse_query(chi_name, query, with_error=False, host="127.0.0.1", port="9000", user="default", pwd="", ns="test"):
    pod_names = kube_get_pod_names(chi_name, ns)
    pod_name = pod_names[0]
    for p in pod_names:
        if host in p:
            pod_name = p

    if pwd != "":
        pwd_str = f"--password={pwd}"
    else:
        pwd_str = ""

    if with_error:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -mn -h {host} --port={port} -u {user} {pwd_str} --query=\"{query}\" 2>&1", ok_to_fail=True)
    else:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -mn -h {host} --port={port} -u {user} {pwd_str} --query=\"{query}\"")


def clickhouse_query_with_error(chi_name, query, host="127.0.0.1", port="9000", user="default", pwd="", ns="test"):
    return clickhouse_query(chi_name, query, True, host, port, user, pwd, ns)

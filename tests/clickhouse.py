from kubectl import *

def clickhouse_query(chi_name, query, with_error = False, host = "127.0.0.1", ns="test"):
    pod_name = kube_get_pod_names(chi_name, ns)[0]
    if (with_error):
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -h {host} --query=\"{query}\" 2>&1", True)
    else:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client -h {host} --query=\"{query}\"")

def clickhouse_query_with_error(chi_name, query, host = "127.0.0.1", ns="test"):
    return clickhouse_query(chi_name, query, True, host, ns)


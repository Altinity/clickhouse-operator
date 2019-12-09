from kubectl import *

def clickhouse_query(chi_name, ns, query, with_error = False):
    pod_name = kube_get_pod_name(chi_name, ns)
    if (with_error):
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client --query=\"{query}\" 2>&1", True)
    else:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client --query=\"{query}\"")

def clickhouse_query_with_error(chi_name, ns, query):
    return clickhouse_query(chi_name, ns, query, True)

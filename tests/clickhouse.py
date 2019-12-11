from kubectl import *

def clickhouse_query(chi_name, query, with_error = False, ns="test"):
    pod_name = kube_get_pod_names(chi_name, ns)[0]
    if (with_error):
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client --query=\"{query}\" 2>&1", True)
    else:
        return kubectl(f"exec {pod_name} -n {ns} -- clickhouse-client --query=\"{query}\"")

def clickhouse_query_with_error(chi_name, query, ns="test"):
    return clickhouse_query(chi_name, query, True, ns)

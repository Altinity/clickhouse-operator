import e2e.kubectl as kubectl
import e2e.settings as settings
from testflows.core import *


def query(
    chi_name,
    sql,
    with_error=False,
    host="127.0.0.1",
    port="9000",
    user="",
    pwd="",
    ns=None,
    timeout=60,
    advanced_params="",
    pod="",
    container="clickhouse-pod",
):
    pod_names = kubectl.get_pod_names(chi_name, ns)
    pod_name = pod_names[0]
    for p in pod_names:
        if host in p or p == pod:
            pod_name = p
            break

    pwd_str = "" if pwd == "" else f"--password={pwd}"
    user_str = "" if user == "" else f"--user={user}"

    if with_error:
        return kubectl.launch(
            f"exec {pod_name} -n {current().context.test_namespace} -c {container}"
            f" --"
            f" clickhouse-client -mn --receive_timeout={timeout} -h {host} --port={port} {user_str} {pwd_str} {advanced_params}"
            f' --query="{sql}"'
            f" 2>&1",
            timeout=timeout,
            ns=current().context.test_namespace,
            ok_to_fail=True,
        )
    else:
        return kubectl.launch(
            f"exec {pod_name} -n {current().context.test_namespace} -c {container}"
            f" -- "
            f"clickhouse-client -mn --receive_timeout={timeout} -h {host} --port={port} {user_str} {pwd_str} {advanced_params}"
            f'--query="{sql}"',
            timeout=timeout,
            ns=current().context.test_namespace,
        )


def query_with_error(
    chi_name,
    sql,
    host="127.0.0.1",
    port="9000",
    user="",
    pwd="",
    ns=None,
    timeout=60,
    advanced_params="",
    pod="",
    container="clickhouse-pod",
):
    return query(
        chi_name=chi_name,
        sql=sql,
        with_error=True,
        host=host,
        port=port,
        user=user,
        pwd=pwd,
        ns=current().context.test_namespace,
        timeout=timeout,
        advanced_params=advanced_params,
        pod=pod,
        container=container,
    )


def drop_table_on_cluster(chi, cluster_name="all-sharded", table="default.test"):
    drop_local_sql = f"DROP TABLE {table} ON CLUSTER '{cluster_name}' SYNC"
    query(chi["metadata"]["name"], drop_local_sql, timeout=240)


def create_table_on_cluster(
    chi,
    cluster_name="all-sharded",
    table="default.test",
    create_definition="(event_time DateTime, test UInt64) ENGINE MergeTree() ORDER BY tuple()",
    if_not_exists=False,
):
    create_sql = "CREATE TABLE"
    if if_not_exists:
        create_sql += " IF NOT EXISTS"
    create_sql = f"{create_sql} {table} ON CLUSTER '{cluster_name}' {create_definition}"
    query(chi["metadata"]["name"], create_sql, timeout=240)


def drop_distributed_table_on_cluster(
    chi,
    cluster_name="all-sharded",
    distr_table="default.test_distr",
    local_table="default.test",
):
    drop_distr_sql = f"DROP TABLE {distr_table} ON CLUSTER '{cluster_name}'"
    query(chi["metadata"]["name"], drop_distr_sql, timeout=240)
    drop_table_on_cluster(chi, cluster_name, local_table)


def create_distributed_table_on_cluster(
    chi,
    cluster_name="all-sharded",
    distr_table="default.test_distr",
    local_table="default.test",
    fields_definition="(event_time DateTime, test UInt64)",
    local_engine="ENGINE MergeTree() ORDER BY tuple()",
    distr_engine="ENGINE Distributed('all-sharded',default, test, test)",
):
    create_table_on_cluster(chi, cluster_name, local_table, fields_definition + " " + local_engine)
    create_distr_sql = f"CREATE TABLE {distr_table} ON CLUSTER '{cluster_name}' {fields_definition} {distr_engine}"
    query(chi["metadata"]["name"], create_distr_sql, timeout=240)

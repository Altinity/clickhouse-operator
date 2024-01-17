# ClickHouse operator hardening guide

## Overview

This section provides an overview of the **clickhouse-operator** security model.

With the default settings, the ClickHouse operator deploys ClickHouse with two users protected by network restriction rules to block unauthorized access.

### The 'default' user

The '**default**' user is used to connect to ClickHouse instance from a pod where it is running, and also for distributed queries. It is deployed with an **empty password** that was a long-time default for ClickHouse out-of-the-box installation.

To secure it, the operator applies network security rules that restrict connections to the pods running the ClickHouse cluster, and nothing else.

Before version **0.19.0**  `hostRegexp` was applied that captured pod names. This did not work correctly in some Kubernetes distributions, such as GKE. In later versions, the operator additionally applies a restrictive set of pod IP addresses and rebuilds this set if the IP address of a pod changes for whatever reason.

The following `users.xml` is set up by operator for a cluster that has two nodes. In this configuration, a '**default**' user can not connect from outside of the cluster.

```xml
<users>
  <default>
    <networks>
      <host_regexp>(chi-my-cluster-[^.]+\d+-\d+|clickhouse\-my-cluster\.test\.svc\.cluster\.local$</host_regexp>
      <ip>::1</ip>
      <ip>127.0.0.1</ip>
      <ip>172.17.0.4</ip>
      <ip>172.17.0.12</ip>
    </networks>
    <profile>default</profile>
    <quota>default</quota>
  </default>
</users>
```

### The 'clickhouse_operator' user

The '**clickhouse_operator**' user is used by the operator itself to perform DMLs when adding or removing ClickHouse replicas and shards, and also for collecting monitoring data. The **user** and **password** values are stored in a secret.

The following example shows how **secret** is referenced in the **clickhouse_operator** configuration:

```yaml
clickhouse:
  access:
    secret:
      # Empty `namespace` means that k8s secret would be looked
      # in the same namespace where the operator's pod is running.
      namespace: ""
      name: "clickhouse-operator"
```

The following example shows a **secret**:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-operator
type: Opaque
stringData:
  username: clickhouse_operator
  password: chpassword
```

We recommend that you do not include the **user** and **password** within the operator configuration without a **secret**, though it is also supported.

To change '**clickhouse_operator**' user password you can modify `etc-clickhouse-operator-files` configmap or create `ClickHouseOperatorConfiguration` object.

See [operator configuration](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_configuration.md) for more information about operator configuration files.

The operator also protects access for the '**clickhouse\_operator**' user using an IP mask. When deploying a user into a ClickHouse server, access is restricted to the IP address of the pod where the operator is running, and nothing else. Therefore, the '**clickhouse_operator**' user can not be used outside of this pod.

## Securing ClickHouse users

More ClickHouse users can be created using SQL `CREATE USER` statement or in a dedicated section of `ClickHouseInstallation`.

To make sure passwords are not exposed, for the `ClickHouseInstallation` the operator provides the following:

### Using hashed passwords

User passwords in `ClickHouseInstallation` can be specified in plain, as sha256 and double sha1 hashes.

When a password is specified in plaintext, the operator hashes it when deploying to ClickHouse, but that is still left in unsecure plaintext format in the `ClickHouseInstallation`.

Altinity recommends providing hashes explicitly as follows:

```yaml
spec:
  useTemplates:
    - name: clickhouse-version
  configuration:
    users:
      user1/password: pwduser1  # This will be hashed in ClickHouse config files, but this NOT RECOMMENDED
      user2/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
      user3/password_double_sha1_hex: cbe205a7351dd15397bf423957559512bd4be395
```

### Using secrets

The operator also allows user to specify passwords and password hashes in a Kubernetes secret as follows:

```yaml
spec:
  configuration:
    users:
      user1/password:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser1
      user2/password_sha256_hex:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser2          
      user3/password_double_sha1_hex:
        valueFrom:
          secretKeyRef:
            name: clickhouse_secret
            key: pwduser3                
```

The following example refers to the secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-secret
type: Opaque
stringData:
  pwduser1: pwduser1
  pwduser2: e106728c3541ec3694822a29d4b8b5f1f473508adc148fcb58a60c42bcf3234c
  pwduser3: cbe205a7351dd15397bf423957559512bd4be395

```

**DEPRECATED**: Since version 0.23.x the syntax to read passwords and password hashes from a secret using special 'k8s\_secret\_' and 'k8s\_secret\_env\_' prefixes is deprecated:

```yaml
spec:
  configuration:
    users:
      user1/k8s_secret_password: clickhouse-secret/pwduser1
      user2/k8s_secret_password_sha256_hex: clickhouse-secret/pwduser2
      user3/k8s_secret_password_double_sha1_hex: clickhouse-secret/pwduser3
```

```yaml
spec:
  configuration:
    users:
      user1/k8s_secret_env_password: clickhouse-secret/pwduser1
      user2/k8s_secret_env_password_sha256_hex: clickhouse-secret/pwduser2
      user3/k8s_secret_env_password_double_sha1_hex: clickhouse-secret/pwduser3
```

### Securing the 'default' user

While the '**default**' user is protected by network rules, passwordless operation is often not allowed by infosec teams. The password for the '**default**' user can be changed the same way as for other users. However, the '**default**' user is also used by ClickHouse to run distributed queries. If the password changes, distributed queries may stop working.

To keep distributed queries running without exposing the password, configure ClickHouse to use a secret token for inter-cluster communications instead of 'default' user credentials.


The operator supports the following:

#### 'auto' token

The following example shows how to let ClickHouse generate the secret token automatically. This is the simplest and recommended way.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: default
        secret:
          auto: "true"
```

#### Custom token

The following example shows how to define a token.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: "default"
        secret:
          value: "my_secret"
```

#### Custom token from Kubernetes secret

The following example shows how to define a token within a secret.

```
spec:
  configuration:
    users:
      default/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
    clusters:
      - name: "default"
        secret:
          valueFrom:
            secretKeyRef:
              name: "secure-inter-cluster-communications"
              key: "secret"
```

## Securing ClickHouse server settings

Some ClickHouse server settings may contain sensitive data, for example, passwords or keys to access external systems. ClickHouse allows a user to keep connection information for external systems in [Named Collections](https://clickhouse.com/docs/en/operations/named-collections) defined by DDL, but sometimes it is more convenient to store keys in server configuration files. In order to do it securely, sensitive information needs to be stored in secrets.

For example, in order to access S3 bucket one may define the following secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-credentials
type: Opaque
stringData:
  AWS_SECRET_ACCESS_KEY: *****
  AWS_ACCESS_KEY_ID: *****
```

Secret can be referred in ```ClickHouseInstllation``` as follows:

```yaml
spec:
  configuration:
    settings:
      s3/my_bucket/endpoint: "https://my-bucket.s3.amazonaws.com/sample/"
      s3/my_bucket/secret_access_key:
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: AWS_SECRET_ACCESS_KEY
      s3/my_bucket/access_key:
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: AWS_ACCESS_KEY_ID
```

Under the hood, secrets can be mapped to environment variables and referred in XML configuration files using ```from_env``` syntax. So the snippet above is equivalent to the following:

```yaml
spec:
  templates:
    podTemplates:
      - name: default
        spec:
          containers:
          - name: clickhouse
            image: altinity/clickhouse-server:23.3.8.22.altinitystable
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: s3-credentials
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: s3-credentials
                  key: AWS_SECRET_ACCESS_KEY
  configuration:
    files:
      config.d/s3.xml: |
        <clickhouse>
          <s3>
            <my_bucket>
               <endpoint>https://my-bucket.s3.amazonaws.com/sample/</endpoint>
               <access_key_id from_env="AWS_ACCESS_KEY_ID"></access_key_id>
               <secret_access_key from_env="AWS_SECRET_ACCESS_KEY"></secret_access_key>
            </my_bucket>
          </s3>
        </clickhouse>
```

## Securing the network

This section covers how to secure your network.

### Network Overview

With default settings, the operator deploys pods and services that expose 3 ports:

* 8123 -- HTTP interface
* 9000 -- TCP interface
* 9009 -- used for replication protocol between cluster nodes (HTTP)

For every pod, there is one service created, and also load balancer service is created to access the cluster. Additional load balancers and custom services may be created using service templates.

### Enabling secure connections to clickhouse-server

[ClickHouse Network Hardening Guide](https://docs.altinity.com/operationsguide/security/clickhouse-hardening-guide/network-hardening/) describes steps required to secure ClickHouse server. Some of them are manual, others are outomated by operator.

The ClickHouse HTTPS/TLS configuration requires the following steps:

* Generate the following certificate files:

  * `server.crt`
  * `server.key`
  * `dhparam.pem`

* Add generated files into the `files` section of `ClickHouseInstallation`
* Add **openSSL configuration** for the server (and client, if ClickHouse needs to connect to other nodes by SSL)
* Enabling **secure ports** in a ClickHouse configuration
* Defining a custom **podTemplate** with secure ports
* Defining a custom **serviceTemplate** if needed

The **podTemplate** is automated by the operator using a '**secure**' flag in the cluster definition rather than the user doing it.

The following example shows a typical secure configuration:

```yaml
spec:
  configuration:
    clusters:
    - name: default
      secure: "yes"
    settings:
      tcp_port: 9000 # keep for localhost
      tcp_port_secure: 9440
      https_port: 8443
    files:
      openssl.xml: |
        <clickhouse>
          <openSSL>
            <server>
              <certificateFile>/etc/clickhouse-server/config.d/server.crt</certificateFile>
              <privateKeyFile>/etc/clickhouse-server/config.d/server.key</privateKeyFile>
              <dhParamsFile>/etc/clickhouse-server/config.d/dhparam.pem</dhParamsFile>
              <verificationMode>none</verificationMode>
              <loadDefaultCAFile>true</loadDefaultCAFile>
              <cacheSessions>true</cacheSessions>
              <disableProtocols>sslv2,sslv3</disableProtocols>
              <preferServerCiphers>true</preferServerCiphers>
            </server>
          </openSSL>
        </clickhouse>
      config.d/server.crt: |
        ***

      config.d/server.key: |
        ***

      config.d/dhparam.pem: |
        ***

```

Certificate files can also be stored in secrets: 

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-certs
type: Opaque
stringData:
  server.crt: |
        ***

  server.key: |
        ***

  dhparam.pem: |
        ***
```

and referred as below:


```yaml
spec:
  configuration:
    files:
      openssl.xml: |
        <clickhouse>
          <openSSL>
            <server>
              <certificateFile>/etc/clickhouse-server/secret-files.d/server.crt</certificateFile>
              <privateKeyFile>/etc/clickhouse-server/secret-files.d/server.key</privateKeyFile>
              <dhParamsFile>/etc/clickhouse-server/secret-files.d/dhparam.pem</dhParamsFile>
              <verificationMode>none</verificationMode>
              <loadDefaultCAFile>true</loadDefaultCAFile>
              <cacheSessions>true</cacheSessions>
              <disableProtocols>sslv2,sslv3</disableProtocols>
              <preferServerCiphers>true</preferServerCiphers>
            </server>
          </openSSL>
        </clickhouse>
      server.crt:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: server.crt
      server.key:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: server.key
      dhparam.pem:
        valueFrom:
          secretKeyRef:
            name: clickhouse-certs
            key: dhparam.pem

```

**Note**: secret files are mapped into `secret-files.d` configuration folder.


### Disabling insecure connections

The operator automatically adjusts services used to access individual pods when '**secure**' flag is used. Additionally, '**inscure: "no"**' flag can be added as of version 0.21.x in order to disable insecure ports:

```
spec:
  configuration:
    clusters:
    - name: default
      secure: "yes"
      insecure: "no"
```

That adjusts pod services properly, but it does not adjust load balancer service. In real environments, the load balancer is managed by a separate **serviceTemplate**. User must define the available ports explicitly.

The following example shows how to define ports for load balancer service:

```yaml
spec:
  templates:
    serviceTemplates:
      - generateName: clickhouse-{chi}
        metadata:
          annotations:
           # cloud specific annotations to configure load balancer
        name: default-service-template
        spec:
          ports:
            - name: https
              port: 8443
            - name: secureclient
              port: 9440
          type: LoadBalancer
```

### Using secure connections for distributed queries

If ClickHouse is configured using the '**secure**' flag as described above, secure connections for distributed installations are already enabled:

```yaml
spec:
  configuration:
    clusters:
      - name: "default"
        secure: "yes"
        secret:
          auto: "yes"
```

Under the hood operator changes **remote_servers** configuration automatically, providing a secure port and flag:

```xml
<shard>
   <internal_replication>False</internal_replication>
      <replica>
          <host>***</host>
          <port>9440</port>
          <secure>1</secure>
      </replica>
   </shard>
```

It may also require an openSSL client configuration:

```yaml
spec:
  configuration:
    files:
      openssl_client.xml: |
        <clickhouse>
          <openSSL>
            <client>
                <loadDefaultCAFile>true</loadDefaultCAFile>
                <cacheSessions>true</cacheSessions>
                <disableProtocols>sslv2,sslv3</disableProtocols>
                <preferServerCiphers>true</preferServerCiphers>
                <verificationMode>none</verificationMode>
                <invalidCertificateHandler>
                    <name>AcceptCertificateHandler</name>
                </invalidCertificateHandler>
            </client>
          </openSSL>
        </clickhouse>
```

**Note:** To secure connections for external users only, but keep inter-cluster communications insecure, instead of using the '**secure**' flag, specify the **podTemplate** explicitly and open the proper ports:

```yaml
spec:
  templates:
    podTemplates:
    - name: default
      containers:
      - name: clickhouse-pod
        image: clickhouse/clickhouse-server:22.8
        ports:
        - name: http
          containerPort: 8123
        - name: https
          containerPort: 8443
        - name: client
          containerPort: 9000
        - name: secureclient
          containerPort: 9440
        - name: interserver
          containerPort: 9009
```

### Forcing HTTPS for operator connections

To have ClickHouse use HTTPS, use following YAML example to set the operator configuration for the '**clickhouse_operator**' user. It switches all interactions with ClickHouse to HTTPS, including health checks.

```yaml
configuration:
  access:
    scheme: https
    port: 8443
```

### Forcing HTTPS for replication

To force ClickHouse replication to use HTTPS on a securerly configured `ClickHouseInstallation`, set the required ClickHouse ports as follows:

```yaml
spec:
  configuration:
    settings:
      interserver_http_port: _removed_
      interserver_https_port: 9009
```

### Forcing HTTPS for ZooKeeper

**TODO**:

- Coming soon

# ClickHouse operator hardening guide

## Overview of clickhouse-operator security model

Wtih default settings, ClickHouse operator deploys ClickHouse with two users protected by network restriction rules in order to block unauthorized access. There are many security controls available. 

### 'default' user

'default' user is used in order to connect to ClickHouse instance from a pod where it is running, and also for distributed queries. It is deployed with **empty password** that was a long time default for ClickHouse out-of-the box installation. In order to make it secure, operator applies network security rules that restrict connections to the pods running ClickHouse cluster, and nothing else. In versions before 0.19.0 it applied hostRegexp that captured pod names. It did not work correctly in some Kubernetes distributions, however, including GKE. Therefore in later versions operator additionally applies a restrictive set of pod IP addresses, and rebuilds this set if IP address of a pod changes for whatever reason. This is how it looks in generated users.xml for cluster with two nodes:

```
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

With this approach 'default' user can not connect from outside of the cluster.

### 'clickhouse_operator' user 

'clickhouse_operator' user is used by operator itself in perform DMLs when adding or removing ClickHouse replicas and shards, and also for collecting monitoring data. User and password is stored in a secret. The name of the secret is configured in operator configuration as follows:

```
clickhouse:
  access:
    secret:
      # Empty `namespace` means that k8s secret would be looked in the same namespace where operator's pod is running.
      namespace: ""
      name: "clickhouse-operator"
```

Here is an example of the secret:

```
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-operator
type: Opaque
stringData:
  username: clickhouse_operator
  password: chpassword
```

It is possible to specify user and password explicitly in operator configuration without a secret, but it is not recommended.

In order to change 'clickhouse_operator' user password you can modify `etc-clickhouse-operator-files` configmap or create `ClickHouseOperatorConfiguration` object. See [operator configuration](https://github.com/Altinity/clickhouse-operator/blob/master/docs/operator_configuration.md) for details.

Operator protects access for 'clickhouse\_operator' user using an IP mask. When deploying user into ClickHouse server access is restricted to the IP address of the pod where operator is running, and nothing else. So 'clickhouse_operator' user can not be used outside of this pod.


## Securing ClickHouse users

More ClickHouse users can be created using SQL (`CREATE USER`) or specified in a dedicated section of `ClickHouseInstallation`. In the latter case operator provides following functionality in order to make sure passwords are not exposed.

### Using hashed passwords

User passwords in `ClickHouseInstallation` can be specified in plain, as sha256 and double sha1 hashes. When password is specified in plain, operator hashes it when deploying to ClickHouse, but it is still left plain in `ClickHouseInstallation` that is not secure. Therefore is recommended to provide hashes explicitly, as follows:

```
spec:
  useTemplates:
    - name: clickhouse-version
  configuration:
    users:
      user1/password: pwduser1 # will be hashed in ClickHouse config files, not recommended
      user2/password_sha256_hex: 716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448
      user3/password_double_sha1_hex: cbe205a7351dd15397bf423957559512bd4be395
```

### Using secrets

Operator provides a special syntax in order to read passwords and password hashes from a secret.

```
spec:
  configuration:
    users:
      user1/k8s_secret_password: clickhouse-secret/pwduser1
      user2/k8s_secret_password_sha256_hex: clickhouse-secret/pwduser2
      user3/k8s_secret_password_double_sha1_hex: clickhouse-secret/pwduser3
```

It refers to the secret, that may look like this:

```
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

**Note**: while passwords are retrieved from secrets, and do not appear in `ClickHouseInstallation` anymore, they are still deployed to ClickHouse users.xml configuration (hashed). The alternate approach is to map secrets to environment variables and use ClickHouse 'from_env' feature that reads parts of configuration from the environment variables, so even hashes are not exposed. However, that would require to recreate ClickHouse podTemplates when adding new users.

### Securing 'default' user

While 'default' user is protected by network rules, the passwordless operatation is often not allowed by infosec teams. The password for 'default' user can be changed the same way as for other users. However, 'default' user is also used by ClickHouse in order to run distributed queries. If password changes, distributed queries may stop working. In order keep distributed queries running without exposing the password, ClickHouse allows to use a secret token for inter-cluster communications instead of 'default' user credentials. Operator supports it as follows:

#### 'auto' secret

ClickHouse can generate this secret token automatically. This is simplest and recommended way.

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

Token is explicitly defined.

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

Token is defined in a secret.

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

## Securing the network

### Network overview

With default settings, operator deploys pods and services that expose 3 ports:

* 8123 -- HTTP interface
* 9000 -- TCP interface
* 9009 -- used for replication protocol between cluster nodes (HTTP)

For every pod there is one service created, and also load balancer service is created to access the cluster. Additional load balancers and custom services may be created using service templates. See [link] for more information.

### Enabling secure connections to clickhouse-server

ClickHouse HTTPS/TLS configuration requeries several steps.

* generate certificate files: server.crt, server.key and dhparam.pem. See details in [Network Hardening Guide](https://docs.altinity.com/operationsguide/security/clickhouse-hardening-guide/network-hardening/)
* add generated files into `files` section of `ClickHouseInstallation`
* add openSSL configuration for server (and client, if ClickHouse needs to connect to other nodes by SSL)
* enable secure ports in ClickHouse configuration
* define a custom podTemplate with secure ports
* define a custom serviceTemplate if needed

The podTemplate is automated by operator with a use of 'secure' flag in the cluster definition, so user does not need to do it. The secure configuration typically looks like this:

```
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
      server.crt: |
        ***

      server.key: |
        ***

      dhparam.pem: |
        ***

```      
Operator automatically adjusts services used to access individual pods when 'secure' is used, but it does not adjust load balancer service. In real environments, load balancer is managed by a separate serviceTemplate, so it is a user responsibility to define available ports. Here is a typical example:

```
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
**TODO**: We plan to add a special flag in order to disable insecure ports on a pod and pod service level automatically as well.

### Using secure connections for distributed queries

If ClickHouse is configured using 'secure' flag as described above, secure connections for distributed are already enabled:

```
spec:
  configuration:
    clusters:
      - name: "default"
        secure: "yes"
        secret:
          auto: "yes"
```
Under the hood operator changes remote_servers configuration automatically, providing a secure port and flag:

```
<shard>
   <internal_replication>False</internal_replication>
      <replica>
          <host>***</host>
          <port>9440</port>
          <secure>1</secure>
      </replica>
   </shard>

```

It may also require openSSL client configuration:

```
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
        </clickhouse>
      </openSSL>
```            
Note, if you want secure connections for external users only, but keep inter-cluster communications insecure, instead of 'secure' flag you'll have to specify podTemplate explicitly and open proper ports:

```
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

HTTPS for 'clickhouse_operator' user can be enabled in operator configuration as follows:

```
configuration:
  access:
    scheme: https
    port: 8443
```
It switches all interactions with ClickHouse to HTTPS, including health checks.

### Forcing HTTPS for replication

In order to enable ClickHouse replication other HTTPS on a securerly configured `ClickHouseInstallation`, port needs to be switched in ClickHouse configuration as follows: 

```
spec:
  configuration:
    clusters:
      - name: single
    settings:
      interserver_http_port: _removed_
      interserver_https_port: 9009
```

### Forcing HTTPS for ZooKeeper

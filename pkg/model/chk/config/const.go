// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	DirPathConfigRoot = "/etc/clickhouse-keeper"

	// DirPathConfigCommon specifies full path to folder,
	// where generated common XML files for the following sections would be placed:
	// 1. remote servers
	// 2. operator-provided additional config files
	DirPathConfigCommon = DirPathConfigRoot + "/" + api.CommonConfigDir + "/"

	// DirPathConfigUsers specifies full path to folder, where generated users XML files would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. operator-provided additional config files
	DirPathConfigUsers = DirPathConfigRoot + "/" + api.UsersConfigDir + "/"

	// DirPathConfigHost specifies full path to folder, where generated host XML files would be placed
	// for the following sections:
	// 1. macros
	// 2. zookeeper
	// 3. settings
	// 4. files
	// 5. operator-provided additional config files
	DirPathConfigHost = DirPathConfigRoot + "/" + api.HostConfigDir + "/"

	// DirPathClickHouseKeeperData specifies full path of data folder where ClickHouse would place its data storage
	DirPathClickHouseKeeperData = "/var/lib/clickhouse-keeper"

	// DirPathClickHouseLog  specifies full path of data folder where ClickHouse would place its log files
	DirPathClickHouseKeeperLog = "/var/log/clickhouse-keeper-log"
)

const (
	// DefaultKeeperDockerImage specifies default ClickHouse docker image to be used
	DefaultKeeperDockerImage = "clickhouse/clickhouse-keeper:latest"

	// KeeperContainerName specifies name of the clickhouse container in the pod
	KeeperContainerName = "clickhouse-keeper"
)

const (
	xmlTagYandex = "yandex"
)

const (
	configSettings = "settings"
)

const (
	configMain = "main"
)

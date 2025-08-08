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

package zookeeper

import (
	"context"
	"strings"

	"github.com/go-zookeeper/zk"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

type PathManager struct {
	*Connection
}

func NewPathManager(connection *Connection) *PathManager {
	return &PathManager{
		Connection: connection,
	}
}

func (p *PathManager) Ensure(path string) {
	// Sanity check
	path = strings.TrimSpace(path)
	if len(path) == 0 {
		return
	}
	if path == "/" {
		return
	}

	// Params if the zk node to be created on each folder
	ctx := context.TODO()
	value := []byte{}
	flags := int32(0)
	acl := []zk.ACL{
		{
			Perms:  zk.PermAll,
			Scheme: "world",
			ID:     "anyone",
		},
	}

	// Create path step-by-step
	log.Info("zk path to be verified: %s", path)
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	subPath := ""
	for _, folder := range pathParts {
		subPath += "/" + folder
		if ok, err := p.Connection.Exists(ctx, subPath); !ok {
			if err != nil {
				log.Warning("received error while checking zk path: %s err: %v", subPath, err)
			}
		} else {
			log.Info("zk path already exists: %s", subPath)
			continue // for
		}

		log.Info("zk path does not exist, need to create: %s", subPath)

		created, err := p.Connection.Create(ctx, subPath, value, flags, acl)
		if err == nil {
			log.Info("zk path created: %s", created)
		} else {
			log.Warning("zk path FAILED to create: %s err: %v", subPath, err)
		}
	}
}

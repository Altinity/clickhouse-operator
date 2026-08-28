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
	"errors"
	"fmt"
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

func (p *PathManager) Ensure(ctx context.Context, path string) error {
	// Sanity check
	path = strings.TrimSpace(path)
	if len(path) == 0 {
		return nil
	}
	if path == "/" {
		return nil
	}

	// Params if the zk node to be created on each folder
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
	var errs []error
	log.Info("zk path to be verified: %s", path)
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	subPath := ""
	for _, folder := range pathParts {
		if err := ctx.Err(); err != nil {
			return err
		}
		subPath += "/" + folder

		exists, existsErr := p.Connection.Exists(ctx, subPath)
		if err := ctx.Err(); err != nil {
			return err
		}
		// existsErr has to gate this: the client reports exists=true alongside most
		// errors, so trusting exists alone would skip a component that is not there.
		// Falling through costs nothing - Create treats ErrNodeExists as success.
		if exists && (existsErr == nil) {
			log.Info("zk path already exists: %s", subPath)
			continue // for
		}

		log.Info("zk path does not exist, need to create: %s", subPath)

		created, err := p.Connection.Create(ctx, subPath, value, flags, acl)
		switch {
		case err == nil:
			log.Info("zk path created: %s", created)
		case errors.Is(err, zk.ErrNodeExists):
			// Created concurrently - by a peer operator or by ClickHouse itself. The
			// component is present, which is all this function promises.
			log.Info("zk path created concurrently: %s", subPath)
		default:
			if err := ctx.Err(); err != nil {
				return err
			}
			// Per-attempt failures are already logged in Connection.retry.
			log.Warning("zk path ensure failed for %s after retries", subPath)
			// Keyed on the Create failure alone; existsErr rides along as context. An
			// Exists that failed before a successful Create leaves the component present
			// and must not be reported.
			errs = append(errs, fmt.Errorf("zk: ensure %q: %w", subPath, errors.Join(existsErr, err)))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("zk path ensure %q failed: %w", path, errors.Join(errs...))
	}
	return nil
}

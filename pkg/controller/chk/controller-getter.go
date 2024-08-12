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

package chk

import (
	"fmt"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// getPodsIPs gets all pod IPs
func (c *Controller) getPodsIPs(obj interface{}) (ips []string) {
	log.V(3).M(obj).F().S().Info("looking for pods IPs")
	defer log.V(3).M(obj).F().E().Info("looking for pods IPs")

	for _, pod := range c.kube.Pod().GetAll(obj) {
		if ip := pod.Status.PodIP; ip == "" {
			log.V(3).M(pod).F().Warning("Pod NO IP address found. Pod: %s/%s", pod.Namespace, pod.Name)
		} else {
			ips = append(ips, ip)
			log.V(3).M(pod).F().Info("Pod IP address found. Pod: %s/%s IP: %s", pod.Namespace, pod.Name, ip)
		}
	}
	return ips
}

// GetCHIByObjectMeta gets CHI by namespaced name
func (c *Controller) GetCHIByObjectMeta(meta meta.Object, isCR bool) (*apiChk.ClickHouseKeeperInstallation, error) {
	var crName string
	if isCR {
		crName = meta.GetName()
	} else {
		var err error
		crName, err = commonLabeler.GetCRNameFromObjectMeta(meta)
		if err != nil {
			return nil, fmt.Errorf("unable to find CR by name: '%s'. More info: %v", meta.GetName(), err)
		}
	}

	cr, err := c.kube.CR().Get(controller.NewContext(), meta.GetNamespace(), crName)
	if cr == nil {
		return nil, err
	}
	return cr.(*apiChk.ClickHouseKeeperInstallation), err
}

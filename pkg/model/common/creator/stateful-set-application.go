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

package creator

import (
	"github.com/altinity/clickhouse-operator/pkg/util"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

// stsSetupApplication performs PodTemplate setup of StatefulSet
func (c *Creator) stsSetupApplication(statefulSet *apps.StatefulSet, host *api.Host) {
	// Post-process StatefulSet
	// Setup application container
	c.stsSetupAppContainer(statefulSet, host)
	// Setup dedicated log container
	c.stsSetupLogContainer(statefulSet, host)
	// Setup additional host alias(es)
	c.stsSetupHostAliases(statefulSet, host)
}

func (c *Creator) stsSetupAppContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	// We need to be sure app container is healthy
	c.stsEnsureAppContainerSpecified(statefulSet, host)
	c.stsEnsureAppContainerProbesSpecified(statefulSet, host)
	c.stsEnsureAppContainerNamedPortsSpecified(statefulSet, host)
	// Setup ENV vars for the app
	c.stsAppContainerSetupEnvVars(statefulSet, host)
	// Setup app according to troubleshoot mode (if any)
	c.stsAppContainerSetupTroubleshootingMode(statefulSet, host)
}

// stsEnsureAppContainerSpecified is a unification wrapper.
// Ensures main application container is specified
func (c *Creator) stsEnsureAppContainerSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	c.cm.EnsureAppContainer(statefulSet, host)
}

// stsEnsureLogContainerSpecified is a unification wrapper
// Ensures log container is in place, if required
func (c *Creator) stsEnsureLogContainerSpecified(statefulSet *apps.StatefulSet) {
	c.cm.EnsureLogContainer(statefulSet)
}

// stsAppContainerSetupEnvVars setup ENV vars for main application container
func (c *Creator) stsAppContainerSetupEnvVars(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = util.MergeEnvVars(container.Env, host.GetCR().GetRuntime().GetAttributes().GetAdditionalEnvVars()...)
}

// stsEnsureAppContainerProbesSpecified
func (c *Creator) stsEnsureAppContainerProbesSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}

	if container.LivenessProbe == nil {
		container.LivenessProbe = c.pm.CreateProbe(interfaces.ProbeDefaultLiveness, host)
	}
	if container.ReadinessProbe == nil {
		container.ReadinessProbe = c.pm.CreateProbe(interfaces.ProbeDefaultReadiness, host)
	}
}

// stsSetupHostAliases
func (c *Creator) stsSetupHostAliases(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []core.HostAlias{
		{
			IP: "127.0.0.1",
			Hostnames: []string{
				c.nm.Name(interfaces.NamePodHostname, host),
			},
		},
	}
	// Add hostAliases from PodTemplate if any
	if podTemplate, ok := host.GetPodTemplate(); ok {
		statefulSet.Spec.Template.Spec.HostAliases = append(
			statefulSet.Spec.Template.Spec.HostAliases,
			podTemplate.Spec.HostAliases...,
		)
	}
}

// stsAppContainerSetupTroubleshootingMode
func (c *Creator) stsAppContainerSetupTroubleshootingMode(statefulSet *apps.StatefulSet, host *api.Host) {
	if !host.GetCR().IsTroubleshoot() {
		// We are not troubleshooting
		return
	}

	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		// Unable to locate ClickHouse container
		return
	}

	// Let's setup troubleshooting in ClickHouse container

	sleep := " || sleep 1800"
	if len(container.Command) > 0 {
		// In case we have user-specified command, let's
		// append troubleshooting-capable tail and hope for the best
		container.Command[len(container.Command)-1] += sleep
	} else {
		// Assume standard ClickHouse container is used
		// Substitute entrypoint with troubleshooting-capable command
		container.Command = []string{
			"/bin/sh",
			"-c",
			"/entrypoint.sh" + sleep,
		}
	}
	// Appended `sleep` command makes Pod unable to respond to probes and probes would fail all the time,
	// causing repeated restarts of the Pod by k8s. Restart is triggered by probes failures.
	// Thus we need to disable all probes in troubleshooting mode.
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
}

// stsSetupLogContainer
func (c *Creator) stsSetupLogContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		c.stsEnsureLogContainerSpecified(statefulSet)
		c.a.V(1).F().Info("add log container for host: %s", host.Runtime.Address.HostName)
	}
}

// stsGetAppContainer is a unification wrapper
func (c *Creator) stsGetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return c.cm.GetAppContainer(statefulSet)
}

// stsEnsureAppContainerNamedPortsSpecified
func (c *Creator) stsEnsureAppContainerNamedPortsSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}
	// Walk over all assigned ports of the host and ensure each port in container
	host.WalkSpecifiedPorts(
		func(name string, port *types.Int32, protocol core.Protocol) bool {
			k8s.ContainerEnsurePortByName(container, name, port.Value())
			// Do not abort, continue iterating
			return false
		},
	)
}

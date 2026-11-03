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

package v1

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func names(cs []core.Container) (r []string) {
	for _, c := range cs {
		r = append(r, c.Name+"="+c.Image)
	}
	return
}

func TestMergeStrategicPodSpec(t *testing.T) {
	t.Run("sidecar-only template does not swallow clickhouse", func(t *testing.T) {
		to := core.PodSpec{Containers: []core.Container{{Name: "sidecar", Image: "side"}}}
		from := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "ch"}}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeOverrideByNonEmptyValues))
		require.Equal(t, []string{"sidecar=side", "clickhouse=ch"}, names(to.Containers))
	})
	t.Run("same-named container: winner fields win, loser-only fields kept, env by name", func(t *testing.T) {
		to := core.PodSpec{Containers: []core.Container{
			{Name: "clickhouse", Image: "chit", ImagePullPolicy: core.PullAlways, Env: []core.EnvVar{{Name: "A", Value: "chit"}, {Name: "B", Value: "chit"}}},
			{Name: "sidecar", Image: "side"},
		}}
		from := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "chi", Env: []core.EnvVar{{Name: "A", Value: "chi"}}}}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeOverrideByNonEmptyValues))
		require.Equal(t, []string{"clickhouse=chi", "sidecar=side"}, names(to.Containers))
		require.Equal(t, core.PullAlways, to.Containers[0].ImagePullPolicy)
		require.Equal(t, []core.EnvVar{{Name: "A", Value: "chi"}, {Name: "B", Value: "chit"}}, to.Containers[0].Env)
	})
	t.Run("empty resources on the winner do not wipe loser limits", func(t *testing.T) {
		to := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "chit", Resources: core.ResourceRequirements{
			Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("1")},
		}}}}
		from := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "chi"}}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeOverrideByNonEmptyValues))
		require.Equal(t, "chi", to.Containers[0].Image)
		require.True(t, to.Containers[0].Resources.Limits.Cpu().Equal(resource.MustParse("1")))
	})
	t.Run("winner with nil containers keeps loser containers (null is not delete)", func(t *testing.T) {
		to := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "chit"}}}
		from := core.PodSpec{NodeSelector: map[string]string{"zone": "a"}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeOverrideByNonEmptyValues))
		require.Equal(t, []string{"clickhouse=chit"}, names(to.Containers))
		require.Equal(t, "a", to.NodeSelector["zone"])
	})
	t.Run("fill-empty keeps receiver", func(t *testing.T) {
		to := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "mine"}}}
		from := core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Image: "parent", ImagePullPolicy: core.PullNever}, {Name: "extra", Image: "x"}}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeFillEmptyValues))
		require.Equal(t, "mine", to.Containers[0].Image)
		require.Equal(t, core.PullNever, to.Containers[0].ImagePullPolicy)
		require.Len(t, to.Containers, 2)
	})
	t.Run("volume source is replaced, not unioned", func(t *testing.T) {
		to := core.PodSpec{Volumes: []core.Volume{{Name: "data", VolumeSource: core.VolumeSource{ConfigMap: &core.ConfigMapVolumeSource{LocalObjectReference: core.LocalObjectReference{Name: "cm"}}}}}}
		from := core.PodSpec{Volumes: []core.Volume{{Name: "data", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}}}
		require.NoError(t, mergeStrategic(&to, &from, MergeTypeOverrideByNonEmptyValues))
		require.Len(t, to.Volumes, 1)
		require.NotNil(t, to.Volumes[0].EmptyDir)
		require.Nil(t, to.Volumes[0].ConfigMap, "retainKeys must drop the losing volume source")
	})
}

// Keyed lists keep the order the template stack accumulated them in. Position is not a merge
// input anywhere - the clickhouse container is looked up by name - but env order is an existing
// contract the e2e scenarios pin, and a template's sidecar staying ahead of the installation's
// additions is what an operator reading the pod expects.
func TestMergeStrategicKeepsStackOrder(t *testing.T) {
	t.Run("env: template entries first, installation's new ones appended, shared name overridden in place", func(t *testing.T) {
		template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{
			Name: "clickhouse", Env: []core.EnvVar{{Name: "FROM_TPL", Value: "tpl"}, {Name: "SHARED", Value: "tpl"}},
		}}}}
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{
			Name: "clickhouse", Env: []core.EnvVar{{Name: "SHARED", Value: "chi"}, {Name: "FROM_CHI", Value: "chi"}},
		}}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		var envs []string
		for _, e := range got.Spec.Containers[0].Env {
			envs = append(envs, e.Name+"="+e.Value)
		}
		require.Equal(t, []string{"FROM_TPL=tpl", "SHARED=chi", "FROM_CHI=chi"}, envs)
	})

	t.Run("containers: template sidecar stays first, installation's clickhouse appended intact", func(t *testing.T) {
		template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{
			{Name: "log-shipper", Image: "shipper:1", Args: []string{"--tail"}},
		}}}
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{
			{Name: "clickhouse", Image: "clickhouse:2"},
		}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		require.Len(t, got.Spec.Containers, 2)
		require.Equal(t, "log-shipper", got.Spec.Containers[0].Name)
		require.Equal(t, "clickhouse", got.Spec.Containers[1].Name)
		require.Equal(t, "clickhouse:2", got.Spec.Containers[1].Image)
		// The by-index merge folded the installation's clickhouse container into the template's
		// sidecar: the sidecar's name and image survived, the clickhouse image was lost, and there
		// was no second container at all. Flipping that merge to override would have leaked the
		// sidecar's fields into clickhouse instead.
		require.Empty(t, got.Spec.Containers[1].Args, "the sidecar's args must stay on the sidecar")
	})
}

// Service ports pair by port number. The previous merge walked ports by index and never appended,
// so an installation declaring more ports than its template lost the extra ones.
func TestServiceTemplatePortsSurviveMerge(t *testing.T) {
	template := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{{Name: "http", Port: 8123}}}}
	installation := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{
		{Name: "http", Port: 8123, TargetPort: intstr.FromInt32(9123)},
		{Name: "tcp", Port: 9000},
	}}}

	got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

	require.Len(t, got.Spec.Ports, 2, "the installation's extra port was dropped")
	require.Equal(t, int32(9123), got.Spec.Ports[0].TargetPort.IntVal, "installation's value for the shared port must win")
	require.Equal(t, int32(9000), got.Spec.Ports[1].Port)

	t.Run("a port served on two protocols", func(t *testing.T) {
		template := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{
			{Name: "dns-tcp", Port: 53, Protocol: core.ProtocolTCP},
			{Name: "dns-udp", Port: 53, Protocol: core.ProtocolUDP},
		}}}
		installation := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{
			{Name: "dns-udp", Port: 53, Protocol: core.ProtocolUDP, NodePort: 30053},
		}}}
		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)
		require.Len(t, got.Spec.Ports, 2, "one protocol folded into the other")
		require.Equal(t, core.ProtocolTCP, got.Spec.Ports[0].Protocol)
		require.Equal(t, int32(30053), got.Spec.Ports[1].NodePort)
	})
}

// Strategic merge keys port lists on the number alone, while k8s declares a port's identity as
// number and protocol: the same port on TCP and on UDP is two ports. Pairing on the number would
// fold one protocol into the other and leave two ports under one name, which the API server
// rejects - in either direction, since whichever side wins supplies the elements that get paired.
func TestMergeStrategicPairsPortsOnProtocol(t *testing.T) {
	ports := func(pt *PodTemplate) (r []string) {
		for _, p := range pt.Spec.Containers[0].Ports {
			r = append(r, fmt.Sprintf("%s:%d/%s:h%d", p.Name, p.ContainerPort, p.Protocol, p.HostPort))
		}
		return
	}
	dns := func(extra ...core.ContainerPort) *PodTemplate {
		return &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "dns", Ports: append([]core.ContainerPort{
			{Name: "dns-tcp", ContainerPort: 53, Protocol: core.ProtocolTCP},
			{Name: "dns-udp", ContainerPort: 53, Protocol: core.ProtocolUDP},
		}, extra...)}}}}
	}

	t.Run("override of one protocol leaves the other alone", func(t *testing.T) {
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "dns", Ports: []core.ContainerPort{
			{Name: "dns-udp", ContainerPort: 53, Protocol: core.ProtocolUDP, HostPort: 5353},
		}}}}}
		got := dns().MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, []string{"dns-tcp:53/TCP:h0", "dns-udp:53/UDP:h5353"}, ports(got))
	})

	t.Run("a winning side's two protocols both survive", func(t *testing.T) {
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "dns", Ports: []core.ContainerPort{
			{Name: "http", ContainerPort: 8123},
		}}}}}
		// fill-empty makes the receiver the winner, so its own ports are what k8s pairs
		got := dns().MergeFrom(installation, MergeTypeFillEmptyValues)
		require.Equal(t, []string{"dns-tcp:53/TCP:h0", "dns-udp:53/UDP:h0", "http:8123/:h0"}, ports(got))
	})

	t.Run("an omitted protocol is TCP", func(t *testing.T) {
		template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "dns", Ports: []core.ContainerPort{
			{Name: "http", ContainerPort: 8123},
		}}}}}
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "dns", Ports: []core.ContainerPort{
			{Name: "http", ContainerPort: 8123, Protocol: core.ProtocolTCP, HostPort: 18123},
		}}}}}
		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, []string{"http:8123/TCP:h18123"}, ports(got), "the same port, not a second one")
	})
}

// The losing side's elements all survive the merge, repeated keys included; restoring the stack
// order must not collapse them either, which a rebuild by lookup did.
func TestMergeStrategicKeepsDuplicateMergeKeys(t *testing.T) {
	t.Run("env declared twice under one name", func(t *testing.T) {
		template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: []core.EnvVar{
			{Name: "X", Value: "1"}, {Name: "X", Value: "2"}, {Name: "Y", Value: "y"},
		}}}}}
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: []core.EnvVar{
			{Name: "Z", Value: "z"},
		}}}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		var envs []string
		for _, e := range got.Spec.Containers[0].Env {
			envs = append(envs, e.Name+"="+e.Value)
		}
		require.Equal(t, []string{"X=1", "X=2", "Y=y", "Z=z"}, envs)
	})

	t.Run("a winning side's repeated env name keeps its effective value", func(t *testing.T) {
		// k8s may or may not merge a winning side's repeats, depending on whether the losing side has
		// the list at all; either way kubelet uses the last one, which must stay the winner's last
		for _, losing := range [][]core.EnvVar{nil, {{Name: "W", Value: "w"}}} {
			template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: losing}}}}
			installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: []core.EnvVar{
				{Name: "X", Value: "1"}, {Name: "X", Value: "2"},
			}}}}}

			got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

			effective := ""
			for _, e := range got.Spec.Containers[0].Env {
				if e.Name == "X" {
					effective = e.Value
				}
			}
			require.Equal(t, "2", effective, "losing side %v", losing)
		}
	})
}

// Host settings and files marshal to maps under user-defined keys, for which strategic merge has
// no schema; a vector under the same key on both sides used to fail the whole merge, and the
// fallback took the winner whole - dropping every port only the loser had set.
func TestHostTemplateSettingsMergeOutsideStrategicPatch(t *testing.T) {
	networks := func(cidrs ...string) *Settings {
		s := NewSettings()
		s.Set("networks/ip", NewSettingVector(cidrs))
		return s
	}
	template := &HostTemplate{Name: "h", Spec: Host{
		HostPorts:    HostPorts{TCPPort: types.NewInt32(9001)},
		HostSettings: HostSettings{Settings: networks("10.0.0.0/8", "::1")},
	}}
	installation := &HostTemplate{Name: "h", Spec: Host{
		HostPorts:    HostPorts{HTTPPort: types.NewInt32(8124)},
		HostSettings: HostSettings{Settings: networks("192.168.0.0/16")},
	}}

	got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

	require.Equal(t, int32(9001), got.Spec.TCPPort.Value(), "template-only port must survive")
	require.Equal(t, int32(8124), got.Spec.HTTPPort.Value(), "installation's port must land")
	require.Equal(t, NewSettingVector([]string{"192.168.0.0/16"}).String(), got.Spec.Settings.Get("networks/ip").String(), "installation wins the shared settings key")
}

// Go's omitempty never omits a struct, so an unset intstr.IntOrString or resource.Quantity used to
// travel as a zero value and override the other side's - a service port restated only to add a
// nodePort lost the template's targetPort, and the Service defaulted it back to the port number.
func TestMergeStrategicKeepsUnsetStructFields(t *testing.T) {
	t.Run("service port targetPort", func(t *testing.T) {
		template := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{
			{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8123)},
		}}}
		installation := &ServiceTemplate{Spec: core.ServiceSpec{Ports: []core.ServicePort{
			{Name: "http", Port: 80, NodePort: 30080},
		}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		require.Equal(t, intstr.FromInt32(8123), got.Spec.Ports[0].TargetPort, "the template's targetPort must survive")
		require.Equal(t, int32(30080), got.Spec.Ports[0].NodePort, "the installation's nodePort must land")
	})

	t.Run("resource field divisor", func(t *testing.T) {
		env := func(divisor *resource.Quantity) []core.EnvVar {
			ref := &core.ResourceFieldSelector{Resource: "limits.memory"}
			if divisor != nil {
				ref.Divisor = *divisor
			}
			return []core.EnvVar{{Name: "MEM", ValueFrom: &core.EnvVarSource{ResourceFieldRef: ref}}}
		}
		mi := resource.MustParse("1Mi")
		template := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: env(&mi)}}}}
		installation := &PodTemplate{Spec: core.PodSpec{Containers: []core.Container{{Name: "clickhouse", Env: env(nil)}}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		require.Equal(t, "1Mi", got.Spec.Containers[0].Env[0].ValueFrom.ResourceFieldRef.Divisor.String())
	})

	t.Run("a retain-keys element listed without its struct field", func(t *testing.T) {
		// resourceClaims keep only the keys the winner lists, so an unset struct field the winner
		// drops outright would strip the loser's too - and a claim without a source is invalid
		source := "gpu-template"
		template := &PodTemplate{Spec: core.PodSpec{ResourceClaims: []core.PodResourceClaim{
			{Name: "gpu", Source: core.ClaimSource{ResourceClaimTemplateName: &source}},
		}}}
		installation := &PodTemplate{Spec: core.PodSpec{ResourceClaims: []core.PodResourceClaim{{Name: "gpu"}}}}

		got := template.MergeFrom(installation, MergeTypeOverrideByNonEmptyValues)

		require.Len(t, got.Spec.ResourceClaims, 1)
		require.NotNil(t, got.Spec.ResourceClaims[0].Source.ResourceClaimTemplateName, "the claim lost its source")
		require.Equal(t, source, *got.Spec.ResourceClaims[0].Source.ResourceClaimTemplateName)
	})
}

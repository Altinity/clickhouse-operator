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

package chi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	clickhouseAltinityCom "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

const (
	remoteServersLegacyFilename = "chop-generated-remote_servers.xml"
	remoteServersHashAnnotation = clickhouseAltinityCom.APIGroupName + "/remote-servers-hash"
)

func (w *worker) createRemoteServersFragments(
	cr *api.ClickHouseInstallation,
	opts *config.FilesGeneratorOptions,
) ([]interfaces.RemoteServersFragment, error) {
	generator := managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, cr, configGeneratorOptions(cr))
	fragments, err := generator.CreateRemoteServersFragments(opts)
	if err != nil {
		return nil, err
	}
	return fragments, nil
}

func (w *worker) isLegacyRemoteServersMode(fragments []interfaces.RemoteServersFragment) bool {
	if len(fragments) == 0 {
		return true
	}
	if len(fragments) != 1 {
		return false
	}
	return fragments[0].TotalBytes <= chop.Config().ClickHouse.Config.SplitThresholdBytes()
}

func (w *worker) remoteServersHash(fragments []interfaces.RemoteServersFragment) string {
	b := &strings.Builder{}
	b.WriteString(strconv.Itoa(len(fragments)))
	b.WriteString("\n")
	for _, fragment := range fragments {
		b.WriteString(fragment.Cluster)
		b.WriteString(":")
		b.WriteString(strconv.Itoa(fragment.ShardStart))
		b.WriteString(":")
		b.WriteString(strconv.Itoa(fragment.ShardEnd))
		b.WriteString(":")
		b.WriteString(strconv.Itoa(fragment.PayloadBytes))
		b.WriteString(":")
		b.WriteString(fragment.XML)
		b.WriteString("\n")
	}
	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

func (w *worker) applyRemoteServersRuntimeAttributes(
	cr *api.ClickHouseInstallation,
	fragments []interfaces.RemoteServersFragment,
	legacyMode bool,
) {
	attributes := cr.GetRuntime().GetAttributes()

	mounts := make([]api.RemoteServersMount, 0)
	if !legacyMode {
		for _, fragment := range fragments {
			mounts = append(mounts, api.RemoteServersMount{
				ConfigMapName: w.c.namer.Name(interfaces.NameConfigMapRemoteServers, cr, fragment.Cluster, fragment.ShardStart),
				FileName:      chiCreator.FragmentFilenameByClusterAndShardStart(fragment.Cluster, fragment.ShardStart),
			})
		}
	}
	attributes.SetRemoteServersMounts(mounts)
	attributes.SetAdditionalPodTemplateAnnotation(remoteServersHashAnnotation, w.remoteServersHash(fragments))
}

func (w *worker) desiredRemoteServersFragmentConfigMapNames(
	cr api.ICustomResource,
	fragments []interfaces.RemoteServersFragment,
	legacyMode bool,
) map[string]struct{} {
	desired := make(map[string]struct{})
	if legacyMode {
		return desired
	}
	for _, fragment := range fragments {
		name := w.c.namer.Name(interfaces.NameConfigMapRemoteServers, cr, fragment.Cluster, fragment.ShardStart)
		desired[name] = struct{}{}
	}
	return desired
}

func (w *worker) reconcileConfigMapRemoteServers(
	ctx context.Context,
	cr api.ICustomResource,
	fragments []interfaces.RemoteServersFragment,
) error {
	for _, fragment := range fragments {
		configMap := w.task.Creator().CreateConfigMap(interfaces.ConfigMapRemoteServers, fragment)
		err := w.reconcileConfigMap(ctx, cr, configMap)
		if err != nil {
			w.task.RegistryFailed().RegisterConfigMap(configMap.GetObjectMeta())
			return err
		}
		w.task.RegistryReconciled().RegisterConfigMap(configMap.GetObjectMeta())
	}
	return nil
}

func (w *worker) deleteStaleRemoteServersFragmentConfigMaps(
	ctx context.Context,
	cr api.ICustomResource,
	desired map[string]struct{},
) error {
	opts := controller.NewListOptions(map[string]string{
		clickhouseAltinityCom.APIGroupName + "/chi": cr.GetName(),
	})
	configMaps, err := w.c.kube.ConfigMap().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		return err
	}

	for i := range configMaps {
		configMap := &configMaps[i]
		if _, ok := configMap.GetLabels()[clickhouseAltinityCom.APIGroupName+"/remote-servers-shard"]; !ok {
			continue
		}
		if _, ok := desired[configMap.GetName()]; ok {
			continue
		}
		if err := w.c.kube.ConfigMap().Delete(ctx, configMap.GetNamespace(), configMap.GetName()); err != nil && !apiErrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete remote_servers fragment configmap %s/%s: %w", configMap.GetNamespace(), configMap.GetName(), err)
		}
	}

	return nil
}

func (w *worker) deleteRemoteServersFragmentConfigMapsPostSTS(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if w.task.RegistryFailed().NumStatefulSet() > 0 {
		w.a.V(1).M(cr).F().Info("Skip remote_servers fragment GC because StatefulSet reconcile has failures")
		return nil
	}

	fragments, err := w.createRemoteServersFragments(cr, nil)
	if err != nil {
		return err
	}
	legacyMode := w.isLegacyRemoteServersMode(fragments)
	desiredHash := w.remoteServersHash(fragments)
	hashApplied, err := w.isRemoteServersHashAppliedToAllStatefulSets(ctx, cr, desiredHash)
	if err != nil {
		return err
	}
	if !hashApplied {
		w.a.V(1).M(cr).F().Info("Skip remote_servers fragment GC because StatefulSet hash is not yet applied to all hosts")
		return nil
	}
	desired := w.desiredRemoteServersFragmentConfigMapNames(cr, fragments, legacyMode)

	return w.deleteStaleRemoteServersFragmentConfigMaps(ctx, cr, desired)
}

func (w *worker) isRemoteServersHashAppliedToAllStatefulSets(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	hash string,
) (bool, error) {
	applied := true
	errs := cr.WalkHosts(func(host *api.Host) error {
		sts, getErr := w.c.kube.STS().Get(ctx, host)
		if apiErrors.IsNotFound(getErr) {
			return nil
		}
		if getErr != nil {
			return getErr
		}

		if sts == nil {
			applied = false
			return nil
		}

		annotations := sts.Spec.Template.GetAnnotations()
		if annotations == nil {
			applied = false
			return nil
		}

		if annotations[remoteServersHashAnnotation] != hash {
			applied = false
		}

		return nil
	})

	if len(errs) > 0 {
		return false, errs[0]
	}

	return applied, nil
}

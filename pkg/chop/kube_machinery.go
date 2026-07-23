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

package chop

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	kube "k8s.io/client-go/kubernetes"
	kuberest "k8s.io/client-go/rest"
	kubeclientcmd "k8s.io/client-go/tools/clientcmd"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/util/tlsutil"
)

// lastKubeConfigInsecure records whether the most recently loaded kubeconfig
// had TLSClientConfig.Insecure=true. Captured here at load time so the
// post-file-load gate inside ConfigManager.Init can decide whether to fail
// fast based on security.kubernetes.tls.verify.
var lastKubeConfigInsecure bool

// captureInsecure records the Insecure flag on conf (if non-nil). Returns
// conf+err unchanged so callers can chain it onto BuildConfigFromFlags/
// InClusterConfig results.
func captureInsecure(conf *kuberest.Config, err error) (*kuberest.Config, error) {
	if (err == nil) && (conf != nil) {
		lastKubeConfigInsecure = conf.TLSClientConfig.Insecure
	}
	return conf, err
}

// getKubeConfig creates kuberest.Config object based on current environment
func getKubeConfig(kubeConfigFile, masterURL string) (*kuberest.Config, error) {
	if len(kubeConfigFile) > 0 {
		log.F().Info("kubeconfig auth source: --kubeconfig flag (%s)", kubeConfigFile)
		return captureInsecure(kubeclientcmd.BuildConfigFromFlags(masterURL, kubeConfigFile))
	}

	if len(os.Getenv("KUBECONFIG")) > 0 {
		log.F().Info("kubeconfig auth source: KUBECONFIG env (%s)", os.Getenv("KUBECONFIG"))
		return captureInsecure(kubeclientcmd.BuildConfigFromFlags(masterURL, os.Getenv("KUBECONFIG")))
	}

	if conf, err := kuberest.InClusterConfig(); err == nil {
		log.F().Info("kubeconfig auth source: in-cluster ServiceAccount")
		return captureInsecure(conf, nil)
	}

	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("user not found")
	}

	homeConfig := filepath.Join(usr.HomeDir, ".kube", "config")
	conf, err := kubeclientcmd.BuildConfigFromFlags("", homeConfig)
	if err != nil {
		return nil, fmt.Errorf("~/.kube/config not found")
	}

	log.F().Info("kubeconfig auth source: %s", homeConfig)
	return captureInsecure(conf, nil)
}

// GetClientset gets k8s API clients - both kube native client and our custom client.
// chopConfigFile supplies the file-based chopconf path used to resolve the K8s-API
// TLS minVersion floor before the first network call (same timing as the
// insecure-kubeconfig gate in ConfigManager.Init).
func GetClientset(kubeConfigFile, masterURL, chopConfigFile string) (
	*kube.Clientset,
	*apiextensions.Clientset,
	*chopclientset.Clientset,
	dynamic.Interface,
) {
	kubeConfig, err := getKubeConfig(kubeConfigFile, masterURL)
	if err != nil {
		log.F().Fatal("Unable to build kubeconf: %s", err.Error())
		os.Exit(1)
	}

	minVerStr, hardened := resolveK8sTLSMinVersion(chopConfigFile)
	if minVer := tlsutil.VersionUint16(minVerStr); minVer != 0 {
		applyK8sClientTLSMinVersion(kubeConfig, minVer, hardened)
	}

	// Layer on k8s client rate limiting overrides if specified in CHOP config.
	if maybeQps := os.Getenv(deployment.OPERATOR_K8S_CLIENT_QPS_LIMIT); maybeQps != "" {
		parsedQps, err := strconv.ParseFloat(maybeQps, 32)
		if err != nil || parsedQps <= 0 {
			log.F().Fatal(
				"Invalid value set for %s, expecting a nonzero float32, got %s",
				deployment.OPERATOR_K8S_CLIENT_QPS_LIMIT,
				maybeQps,
			)
		}
		kubeConfig.QPS = float32(parsedQps)
	}
	if maybeBurst := os.Getenv(deployment.OPERATOR_K8S_CLIENT_BURST_LIMIT); maybeBurst != "" {
		parsedBurst, err := strconv.ParseInt(maybeBurst, 10, 64)
		if err != nil || parsedBurst <= 0 {
			log.F().Fatal(
				"Invalid value set for %s, expecting a nonzero integer, got %s",
				deployment.OPERATOR_K8S_CLIENT_BURST_LIMIT,
				maybeBurst,
			)
		}
		kubeConfig.Burst = int(parsedBurst)
	}

	kubeClientset, err := kube.NewForConfig(kubeConfig)
	if err != nil {
		log.F().Fatal("Unable to initialize kubernetes API clientset: %s", err.Error())
	}

	apiextensionsClientset, err := apiextensions.NewForConfig(kubeConfig)
	if err != nil {
		log.F().Fatal("Unable to initialize kubernetes API extensions clientset: %s", err.Error())
	}

	chopClientset, err := chopclientset.NewForConfig(kubeConfig)
	if err != nil {
		log.F().Fatal("Unable to initialize clickhouse-operator API clientset: %s", err.Error())
	}

	dynamicClientset, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		log.F().Fatal("Unable to initialize kubernetes dynamic clientset: %s", err.Error())
	}

	return kubeClientset, apiextensionsClientset, chopClientset, dynamicClientset
}

// resolveK8sTLSMinVersion reads the file-based chopconf and returns the effective
// K8s-API TLS floor ("1.2"|"1.3"|"") plus whether a hardened (FIPS/Enforced) posture
// requires it. Uses a nil-client ConfigManager because file loading never touches the
// API. Errors yield ("", false) - no floor.
//
// File-only by design, mirroring the insecure-kubeconfig gate (RequiresStrictK8sTLS):
// this runs before the first secure API call, so hardening declared ONLY in a CR-based
// ClickHouseOperatorConfiguration (merged later, after the API client exists) is not
// visible here and will not floor the K8s transport. Declare fips.enforced / policy in
// the file-based chopconf for the K8s-API floor to apply.
func resolveK8sTLSMinVersion(chopConfigFile string) (minVersion string, hardened bool) {
	cm := newConfigManager(nil, nil, chopConfigFile)
	fileConfig, err := cm.getFileBasedConfig(chopConfigFile)
	if err != nil || fileConfig == nil {
		return "", false
	}
	return string(fileConfig.ResolveK8sTLSMinVersion()), fileConfig.Security.RequiresHardening()
}

// applyK8sClientTLSMinVersion stamps MinVersion onto the rest.Config transport via
// rest.Config.Wrap, preserving client-go's TLS/proxy/HTTP2 setup. client-go invokes the
// wrapper on the freshly-built *http.Transport before any request, and crypto/tls reads
// MinVersion at handshake, so the floor takes effect on the actual ClientHello (h1 and h2).
//
// If the built RoundTripper is not *http.Transport the floor cannot be enforced. Under a
// hardened (FIPS/Enforced) posture that is fatal - the operator must not silently negotiate
// below the required floor - mirroring the fail-closed insecure-kubeconfig gate. For a
// user-chosen floor without hardening, it degrades to a warning (best-effort).
func applyK8sClientTLSMinVersion(cfg *kuberest.Config, minVer uint16, hardened bool) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		out, err := floorTransportTLSMinVersion(rt, minVer)
		if err != nil {
			if hardened {
				log.F().Fatal("k8s client TLS minVersion floor (0x%04x) unenforceable under hardened posture: %v", minVer, err)
				os.Exit(1)
			}
			log.F().Warning("k8s client TLS minVersion floor (0x%04x) not applied: %v", minVer, err)
			return rt
		}
		log.F().Info("k8s client TLS minVersion floor applied: 0x%04x - K8s API servers below this version will be refused", minVer)
		return out
	})
}

// floorTransportTLSMinVersion sets MinVersion on the transport's TLS config. It clones the
// TLS config before mutating because client-go caches *http.Transport keyed on TLS options
// (transport/cache.go), so the config may be shared across clientsets built from the same
// rest.Config - all want the same floor, but cloning avoids mutating shared state in place.
// Returns an error (floor cannot be enforced) if rt is not an *http.Transport.
func floorTransportTLSMinVersion(rt http.RoundTripper, minVer uint16) (http.RoundTripper, error) {
	t, ok := rt.(*http.Transport)
	if !ok {
		return rt, fmt.Errorf("transport is %T, not *http.Transport", rt)
	}
	tlsConfig := &tls.Config{}
	if t.TLSClientConfig != nil {
		tlsConfig = t.TLSClientConfig.Clone()
	}
	tlsConfig.MinVersion = minVer
	t.TLSClientConfig = tlsConfig
	return t, nil
}

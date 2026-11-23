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

package clickhouse

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"sync"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
)

// clientCertLoaders holds one loader per (certificate file, key file) pair, so every
// registered tls.Config, and every connection pool cloned from one, shares a single
// last good certificate.
var clientCertLoaders sync.Map // map[clientCertPair]*clientCertLoader

type clientCertPair struct {
	certFile string
	keyFile  string
}

// clientCertLoader serves a client certificate read from a PEM certificate file and a PEM key
// file. The pair is read on every handshake, and a pair that fails to load never replaces one
// that loaded.
type clientCertLoader struct {
	pair clientCertPair

	mu   sync.Mutex
	cert *tls.Certificate
	// reported is the last failure logged, so a failure that persists is logged once rather than
	// on every handshake. Cleared by a good load, so the next failure is logged again.
	reported string

	// warnings counts logged reload failures. Read by tests.
	warnings int
}

// clientCertificateFor returns a tls.Config.GetClientCertificate callback that presents the
// certificate in certFile with the key in keyFile.
//
// The files are not read here but on every new TLS handshake, with tls.LoadX509KeyPair. That:
//   - lets the files appear after the operator starts (for example, written by a sidecar);
//   - picks up a rotated certificate without a restart, whatever the rotation looks like: an
//     in-place rewrite, or the kubelet's atomic swap of a projected volume's ..data link.
//
// The cost is one read and parse per handshake. The operator's connections are pooled, so
// handshakes are rare and the cost is negligible.
//
// A pair that fails to load (for example, a half-written pair caught mid-rotation, or files that
// have gone missing) is logged as a warning with the file paths, once per distinct failure, and
// the last good certificate is kept. Before any certificate has loaded the callback returns an
// error, so the handshake fails visibly instead of proceeding without a client certificate. A
// lone path (one of the two files unset) fails every handshake the same way.
func clientCertificateFor(certFile, keyFile string, l log.Announcer) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	loader := getClientCertLoader(certFile, keyFile)
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return loader.get(l)
	}
}

// getClientCertLoader returns the shared loader for the pair, creating it on first use.
func getClientCertLoader(certFile, keyFile string) *clientCertLoader {
	pair := clientCertPair{certFile: certFile, keyFile: keyFile}
	loader, _ := clientCertLoaders.LoadOrStore(pair, &clientCertLoader{pair: pair})
	return loader.(*clientCertLoader)
}

// get reads the pair and returns it, or the last good certificate when the read fails.
func (c *clientCertLoader) get(l log.Announcer) (*tls.Certificate, error) {
	if (c.pair.certFile == "") || (c.pair.keyFile == "") {
		return nil, fmt.Errorf("unable to load client certificate: clickhouse.access.clientCertFile (%q) and "+
			"clickhouse.access.clientKeyFile (%q) must be set together", c.pair.certFile, c.pair.keyFile)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cert, err := tls.LoadX509KeyPair(c.pair.certFile, c.pair.keyFile)
	if err != nil {
		if c.cert == nil {
			return nil, fmt.Errorf("unable to load client certificate from %s (key %s): %w",
				c.pair.certFile, c.pair.keyFile, err)
		}
		if err.Error() != c.reported {
			// Logs the file paths and the error only, never file contents.
			l.F().Warning("unable to reload client certificate from %s (key %s), keeping the previous one: %v",
				c.pair.certFile, c.pair.keyFile, err)
			c.reported = err.Error()
			c.warnings++
		}
		return c.cert, nil
	}

	c.reported = ""
	if (c.cert == nil) || !bytes.Equal(cert.Certificate[0], c.cert.Certificate[0]) {
		c.cert = &cert
		l.V(1).F().Info("loaded client certificate from %s (key %s)", c.pair.certFile, c.pair.keyFile)
	}
	return c.cert, nil
}

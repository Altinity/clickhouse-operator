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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// mtlsTestCA is a CA generated in the test. It signs the server certificate and every client
// certificate, so no PEM fixture is committed to the tree.
type mtlsTestCA struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	pem   []byte
	pool  *x509.CertPool
	clock time.Time
}

func newMTLSTestCA(t *testing.T) *mtlsTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mtls-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &mtlsTestCA{
		cert:  cert,
		key:   key,
		pem:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pool:  pool,
		clock: now,
	}
}

// issue returns a PEM certificate and PEM key signed by the CA.
func (ca *mtlsTestCA) issue(t *testing.T, serial int64, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "mtls-test-" + strconv.FormatInt(serial, 10)},
		NotBefore:    ca.clock.Add(-time.Hour),
		NotAfter:     ca.clock.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/clickhouse-operator"}},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// clientPairFiles is a client certificate and key on disk, as a mounted Secret would provide.
type clientPairFiles struct {
	certFile string
	keyFile  string
}

func newClientPairFiles(t *testing.T) *clientPairFiles {
	dir := t.TempDir()
	return &clientPairFiles{
		certFile: filepath.Join(dir, "tls.crt"),
		keyFile:  filepath.Join(dir, "tls.key"),
	}
}

func (f *clientPairFiles) write(t *testing.T, certPEM, keyPEM []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(f.certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(f.keyFile, keyPEM, 0o600))
}

// mtlsTestServer answers the driver's ping ("select 1") only for clients presenting a certificate
// signed by the CA, and records the serial number of each certificate it was shown.
type mtlsTestServer struct {
	*httptest.Server
	mu      sync.Mutex
	serials []int64
}

func newMTLSTestServer(t *testing.T, ca *mtlsTestCA) *mtlsTestServer {
	t.Helper()
	certPEM, keyPEM := ca.issue(t, 1000, x509.ExtKeyUsageServerAuth)
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	s := &mtlsTestServer{}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.serials = append(s.serials, r.TLS.PeerCertificates[0].SerialNumber.Int64())
		s.mu.Unlock()
		_, _ = io.WriteString(w, "1\n")
	}))
	s.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.pool,
		MinVersion:   tls.VersionTLS12,
	}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

func (s *mtlsTestServer) port(t *testing.T) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(s.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return port
}

// lastSerial is the serial number of the client certificate the server saw most recently.
func (s *mtlsTestServer) lastSerial(t *testing.T) int64 {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(t, s.serials, "the server saw no client certificate")
	return s.serials[len(s.serials)-1]
}

// connect opens a fresh pair of pools through the real go-clickhouse driver, which means a new
// TLS handshake, and pings over it. It returns the ping error.
func connect(t *testing.T, s *mtlsTestServer, ca *mtlsTestCA, files *clientPairFiles) error {
	t.Helper()
	params := NewEndpointConnectionParams(api.ChSchemeHTTPS, "127.0.0.1", "", "", string(ca.pem), s.port(t))
	params.SetTLSSecurity(api.TLSVerifyStrict, "", "")
	return connectWith(t, params, files)
}

// connectWith is connect with caller-built params, for configurations other than verify=Strict.
func connectWith(t *testing.T, params *EndpointConnectionParams, files *clientPairFiles) error {
	t.Helper()
	if files != nil {
		params.SetClientCertificate(files.certFile, files.keyFile)
	}
	params.SetTimeouts(NewTimeouts(5*time.Second, 5*time.Second))

	conn := NewConnection(params).SetLog(log.New())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	primary, secondary, err := conn.openPools(ctx)
	if err == nil {
		closePools(primary, secondary)
	}
	return err
}

// TestClientCertificate_MutualTLSHandshake drives setupTLSAdvanced and the go-clickhouse driver
// against a server that requires a client certificate, as ClickHouse does with
// verificationMode=strict.
func TestClientCertificate_MutualTLSHandshake(t *testing.T) {
	ca := newMTLSTestCA(t)
	s := newMTLSTestServer(t, ca)

	t.Run("without a client certificate the server refuses the handshake", func(t *testing.T) {
		require.Error(t, connect(t, s, ca, nil))
	})

	t.Run("before the files exist the handshake fails rather than going out without a certificate", func(t *testing.T) {
		files := newClientPairFiles(t)
		require.Error(t, connect(t, s, ca, files))
	})

	t.Run("with the files the certificate is presented", func(t *testing.T) {
		files := newClientPairFiles(t)
		certPEM, keyPEM := ca.issue(t, 11, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)

		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(11), s.lastSerial(t))
	})

	t.Run("a client certificate alone, with no other TLS setting, leaves the legacy registration", func(t *testing.T) {
		files := newClientPairFiles(t)
		certPEM, keyPEM := ca.issue(t, 15, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)

		// No verify, minVersion, serverName or rootCA: without the client certificate this endpoint
		// would use the legacy registration, which presents none.
		params := NewEndpointConnectionParams(api.ChSchemeHTTPS, "127.0.0.1", "", "", "", s.port(t))
		require.NoError(t, connectWith(t, params, files))
		require.Equal(t, int64(15), s.lastSerial(t))
	})

	t.Run("a rotated certificate is presented on the next new connection, with no restart", func(t *testing.T) {
		files := newClientPairFiles(t)
		certPEM, keyPEM := ca.issue(t, 21, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(21), s.lastSerial(t))

		certPEM, keyPEM = ca.issue(t, 22, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(22), s.lastSerial(t), "the new connection must present the rotated certificate")
	})

	t.Run("a corrupt rewrite keeps the last good certificate", func(t *testing.T) {
		files := newClientPairFiles(t)
		certPEM, keyPEM := ca.issue(t, 31, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))

		// A half-written rotation: the new certificate has landed, its key has not.
		newCertPEM, _ := ca.issue(t, 32, x509.ExtKeyUsageClientAuth)
		files.write(t, newCertPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(31), s.lastSerial(t), "a pair that does not load must not replace the last good one")

		files.write(t, []byte("not a certificate"), []byte("not a key"))
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(31), s.lastSerial(t))
	})

	t.Run("deleted files keep the last good certificate until the operator restarts", func(t *testing.T) {
		files := newClientPairFiles(t)
		certPEM, keyPEM := ca.issue(t, 35, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))

		// Removing the mounted files, for example by deleting the Secret, does not withdraw the
		// certificate already loaded: revoking its access is the server's decision.
		require.NoError(t, os.Remove(files.certFile))
		require.NoError(t, os.Remove(files.keyFile))
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(35), s.lastSerial(t), "missing files must not drop the last good certificate")

		// Files that come back are read again.
		certPEM, keyPEM = ca.issue(t, 36, x509.ExtKeyUsageClientAuth)
		files.write(t, certPEM, keyPEM)
		require.NoError(t, connect(t, s, ca, files))
		require.Equal(t, int64(36), s.lastSerial(t))
	})
}

// TestClientCertificate_LoaderIsShared: one loader per pair, so every registration of the pair (two
// clusters with different TLS overlays make two) presents the same certificate and keeps the same
// last good one.
func TestClientCertificate_LoaderIsShared(t *testing.T) {
	ca := newMTLSTestCA(t)
	files := newClientPairFiles(t)
	certPEM, keyPEM := ca.issue(t, 41, x509.ExtKeyUsageClientAuth)
	files.write(t, certPEM, keyPEM)

	loader := getClientCertLoader(files.certFile, files.keyFile)
	require.Same(t, loader, getClientCertLoader(files.certFile, files.keyFile), "one loader per pair")

	first := clientCertificateFor(files.certFile, files.keyFile, log.New())
	second := clientCertificateFor(files.certFile, files.keyFile, log.New())
	a, err := first(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	b, err := second(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	require.Same(t, a, b, "both registrations present the same certificate")

	// A failed reload through one registration is the other's last good certificate too.
	files.write(t, []byte("not a certificate"), []byte("not a key"))
	c, err := second(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	require.Same(t, a, c)

	other := newClientPairFiles(t)
	require.NotSame(t, loader, getClientCertLoader(other.certFile, other.keyFile), "a different pair gets its own loader")
}

// TestClientCertificate_FollowsSymlinkSwap covers the kubelet's projected-volume rotation: the
// files are symlinks through a ..data link that is swapped atomically to a new directory. The two
// generations are written back to back and are the same size, as the kubelet's would be for a
// certificate of the same shape, and no timestamp is set by hand.
func TestClientCertificate_FollowsSymlinkSwap(t *testing.T) {
	ca := newMTLSTestCA(t)
	root := t.TempDir()
	vol := newProjectedVolume(t, root)

	gen1Cert, gen1Key := ca.issue(t, 51, x509.ExtKeyUsageClientAuth)
	vol.writeGeneration(t, "..2026_11_23_00_00_01.1", gen1Cert, gen1Key)
	get := clientCertificateFor(vol.certFile, vol.keyFile, log.New())
	require.Equal(t, int64(51), serialOf(t, get))

	gen2Cert, gen2Key := issueSameSize(t, ca, 52, len(gen1Cert), len(gen1Key))
	vol.writeGeneration(t, "..2026_11_23_00_00_01.2", gen2Cert, gen2Key)
	require.Equal(t, int64(52), serialOf(t, get), "the swapped ..data link must be followed")
}

// TestClientCertificate_InPlaceRewriteIsSeen is the rewrite no file stamp can see: the same files
// (same inode) rewritten with a new certificate of exactly the old size, and the old mtime put back.
// The pair is read on every handshake, so the new certificate is presented anyway.
func TestClientCertificate_InPlaceRewriteIsSeen(t *testing.T) {
	ca := newMTLSTestCA(t)
	files := newClientPairFiles(t)
	tick := time.Now().Truncate(time.Second)

	gen1Cert, gen1Key := ca.issue(t, 61, x509.ExtKeyUsageClientAuth)
	files.write(t, gen1Cert, gen1Key)
	require.NoError(t, os.Chtimes(files.certFile, tick, tick))
	require.NoError(t, os.Chtimes(files.keyFile, tick, tick))
	get := clientCertificateFor(files.certFile, files.keyFile, log.New())
	require.Equal(t, int64(61), serialOf(t, get))
	before, err := os.Stat(files.certFile)
	require.NoError(t, err)

	gen2Cert, gen2Key := issueSameSize(t, ca, 62, len(gen1Cert), len(gen1Key))
	files.write(t, gen2Cert, gen2Key)
	require.NoError(t, os.Chtimes(files.certFile, tick, tick))
	require.NoError(t, os.Chtimes(files.keyFile, tick, tick))
	after, err := os.Stat(files.certFile)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after) && before.ModTime().Equal(after.ModTime()) && (before.Size() == after.Size()),
		"precondition: the rewrite is indistinguishable by inode, mtime and size")

	require.Equal(t, int64(62), serialOf(t, get), "an in-place rewrite must be presented on the next handshake")
}

// TestClientCertificate_MismatchedPairNeverLoads: a certificate with another certificate's key is
// refused by the load itself, so a rotation caught between its two files never installs a pair
// that cannot complete a handshake.
func TestClientCertificate_MismatchedPairNeverLoads(t *testing.T) {
	ca := newMTLSTestCA(t)
	files := newClientPairFiles(t)
	certA, keyA := ca.issue(t, 71, x509.ExtKeyUsageClientAuth)
	certB, keyB := ca.issue(t, 72, x509.ExtKeyUsageClientAuth)
	get := clientCertificateFor(files.certFile, files.keyFile, log.New())

	// No good pair yet: the mismatch is an error, and no certificate is presented.
	files.write(t, certA, keyB)
	_, err := get(&tls.CertificateRequestInfo{})
	require.ErrorContains(t, err, "private key does not match public key")

	files.write(t, certA, keyA)
	require.Equal(t, int64(71), serialOf(t, get))

	// The certificate rotated ahead of its key, then the key ahead of its certificate.
	files.write(t, certB, keyA)
	require.Equal(t, int64(71), serialOf(t, get), "a mismatched pair must not replace the last good one")
	files.write(t, certA, keyB)
	require.Equal(t, int64(71), serialOf(t, get))

	files.write(t, certB, keyB)
	require.Equal(t, int64(72), serialOf(t, get), "the matched pair loads once both files have landed")
}

// TestClientCertificate_ReloadFailureIsLoggedOncePerDistinctFailure: while a reload keeps failing
// the same way, the warning is logged once, not on every handshake; a different failure is logged
// again, and so is the same failure after a good load in between.
func TestClientCertificate_ReloadFailureIsLoggedOncePerDistinctFailure(t *testing.T) {
	ca := newMTLSTestCA(t)
	certA, keyA := ca.issue(t, 81, x509.ExtKeyUsageClientAuth)
	certB, keyB := ca.issue(t, 82, x509.ExtKeyUsageClientAuth)
	files := newClientPairFiles(t)
	files.write(t, certA, keyA)
	loader := getClientCertLoader(files.certFile, files.keyFile)
	good, err := loader.get(log.New())
	require.NoError(t, err)

	files.write(t, certB, keyA) // mismatched pair
	for range 5 {
		cert, err := loader.get(log.New())
		require.NoError(t, err)
		require.Same(t, good, cert)
	}
	require.Equal(t, 1, loader.warnings, "a failure that persists is logged once")

	require.NoError(t, os.Remove(files.certFile)) // a different failure
	_, err = loader.get(log.New())
	require.NoError(t, err)
	require.Equal(t, 2, loader.warnings, "a different failure is logged")

	files.write(t, certB, keyB)
	_, err = loader.get(log.New())
	require.NoError(t, err)
	files.write(t, certA, keyB)
	_, err = loader.get(log.New())
	require.NoError(t, err)
	require.Equal(t, 3, loader.warnings, "after a good load the next failure is logged again")
}

// TestClientCertificate_LonePathFailsTheHandshake: one of the two paths set is a misconfiguration,
// treated like missing files: the handshake fails rather than going out without a certificate.
func TestClientCertificate_LonePathFailsTheHandshake(t *testing.T) {
	ca := newMTLSTestCA(t)
	s := newMTLSTestServer(t, ca)
	files := newClientPairFiles(t)
	certPEM, keyPEM := ca.issue(t, 91, x509.ExtKeyUsageClientAuth)
	files.write(t, certPEM, keyPEM)

	for name, pair := range map[string][2]string{
		"certificate without key": {files.certFile, ""},
		"key without certificate": {"", files.keyFile},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clientCertificateFor(pair[0], pair[1], log.New())(&tls.CertificateRequestInfo{})
			require.ErrorContains(t, err, "must be set together")

			params := NewEndpointConnectionParams(api.ChSchemeHTTPS, "127.0.0.1", "", "", "", s.port(t))
			params.EndpointCredentials.SetTLSSecurity(api.TLSVerifyStrict, "", "")
			params.EndpointCredentials.SetClientCertificate(pair[0], pair[1])
			require.True(t, params.hasClientCertificate(), "a lone path must not fall back to the legacy registration")
		})
	}
}

// projectedVolume lays out a directory the way the kubelet writes a Secret volume: each
// generation in its own directory, a ..data link to the current one, and the visible files as
// links through ..data.
type projectedVolume struct {
	root, certFile, keyFile string
}

func newProjectedVolume(t *testing.T, root string) *projectedVolume {
	t.Helper()
	v := &projectedVolume{root: root, certFile: filepath.Join(root, "tls.crt"), keyFile: filepath.Join(root, "tls.key")}
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.crt"), v.certFile))
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.key"), v.keyFile))
	return v
}

func (v *projectedVolume) writeGeneration(t *testing.T, name string, certPEM, keyPEM []byte) {
	t.Helper()
	dir := filepath.Join(v.root, name)
	require.NoError(t, os.Mkdir(dir, 0o700))
	for file, content := range map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM} {
		path := filepath.Join(dir, file)
		require.NoError(t, os.WriteFile(path, content, 0o600))
	}
	tmp := filepath.Join(v.root, "..data_tmp")
	require.NoError(t, os.Symlink(name, tmp))
	require.NoError(t, os.Rename(tmp, filepath.Join(v.root, "..data")))
}

// issueSameSize issues client certificates until one has exactly the given PEM sizes. An ECDSA
// signature's DER length varies by a byte or two, so a match comes within a few tries.
func issueSameSize(t *testing.T, ca *mtlsTestCA, serial int64, certSize, keySize int) (certPEM, keyPEM []byte) {
	t.Helper()
	for range 200 {
		certPEM, keyPEM = ca.issue(t, serial, x509.ExtKeyUsageClientAuth)
		if (len(certPEM) == certSize) && (len(keyPEM) == keySize) {
			return certPEM, keyPEM
		}
	}
	t.Fatalf("no certificate of %d bytes with a key of %d bytes in 200 tries", certSize, keySize)
	return nil, nil
}

func serialOf(t *testing.T, get func(*tls.CertificateRequestInfo) (*tls.Certificate, error)) int64 {
	t.Helper()
	cert, err := get(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	return leaf.SerialNumber.Int64()
}

// TestClientCertificate_TLSConfigCloneKeepsCallback pins the driver contract rotation relies on:
// go-clickhouse clones the registered tls.Config per connection, and the clone must keep
// GetClientCertificate.
func TestClientCertificate_TLSConfigCloneKeepsCallback(t *testing.T) {
	cfg := &tls.Config{GetClientCertificate: clientCertificateFor("/nonexistent/tls.crt", "/nonexistent/tls.key", log.New())}
	require.NotNil(t, cfg.Clone().GetClientCertificate)
}

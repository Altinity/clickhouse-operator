// Package mergo is a vendored copy of the sunsingerus fork of
// github.com/imdario/mergo.
//
// Why vendored: the operator's exported API packages depend on
// mergo.WithSliceDeepMerge, which exists only in the sunsingerus fork
// (github.com/sunsingerus/mergo). The fork is consumed via a `replace`
// directive, but Go `replace` directives DO NOT propagate to consumers
// importing this repository as a library module — their builds fail with
// "undefined: mergo.WithSliceDeepMerge". Vendoring the ~840 stdlib-only
// lines makes the operator importable as a Go module without any
// consumer-side replaces.
//
// Upstream: https://github.com/sunsingerus/mergo
// Pinned commit: fc6fffa94450 (2023-05-07), fork of imdario/mergo v0.3.15
// License: BSD-3-Clause (this directory, LICENSE) — the same license the
// dependency carried when fetched from the module proxy.
//
// Local changes: none. Do not modify; re-vendor wholesale if the fork
// ever needs updating.
package mergo

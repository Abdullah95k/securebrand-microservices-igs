// Package protosrc embeds the public protobuf contracts (securebrand.fb.v1,
// securebrand.common.v1 and securebrand.ig.v1) so the Schema Registry apply
// commands can register them.
package protosrc

import "embed"

// FS holds the public .proto files at their import paths. The internal task
// envelopes are not part of it.
//
//go:embed securebrand/fb/v1/*.proto securebrand/common/v1/*.proto securebrand/ig/v1/*.proto
var FS embed.FS

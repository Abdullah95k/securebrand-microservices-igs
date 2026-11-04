// Package protosrc embeds the public protobuf contracts
// (securebrand.common.v1 and securebrand.ig.v1) so igsctl schemas apply can
// register them.
package protosrc

import "embed"

// FS holds the public .proto files at their import paths. The internal task
// envelopes are not part of it.
//
//go:embed securebrand/common/v1/*.proto securebrand/ig/v1/*.proto securebrand/content/v1/*.proto
var FS embed.FS

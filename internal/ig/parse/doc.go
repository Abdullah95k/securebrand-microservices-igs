// Package parse turns logged-out Instagram's GraphQL answers and page HTML
// into securebrand.ig.v1 messages. Like fbs's parsers, it finds entities
// wherever they sit (a root field may be renamed, a page may embed the same
// media twice) and reads each field from candidate paths, and it reports
// required fields it could not find as schema drift instead of dropping
// data silently (walk.Report). The field names come from the owner's
// recordings and probe runs (har-findings-2026-10-30.md, build-status.md).
//
// Parsers never compute author keys or apply privacy modes: the emitter
// does, as fbs's does.
package parse

// Parser versions, stamped on each message's Observation. Raise one when
// its parser's output changes for the same input.
const (
	ProfileVersion    = 1
	GridVersion       = 1
	PostVersion       = 1
	CommentsVersion   = 1
	ReelsVersion      = 1
	HighlightsVersion = 1
)

// ParserPost names the post page's parser on raw lines; the others are the
// registry's parser names (configs/ig/queries.seed.yaml).
const ParserPost = "post"

// VersionOf is the version of the parser named (a registry query's parser,
// or ParserPost), stamped on each raw response line; 0 when unknown.
func VersionOf(parser string) int {
	switch parser {
	case "profile":
		return ProfileVersion
	case "grid":
		return GridVersion
	case ParserPost:
		return PostVersion
	case "comments":
		return CommentsVersion
	case "reels":
		return ReelsVersion
	case "highlights":
		return HighlightsVersion
	}
	return 0
}

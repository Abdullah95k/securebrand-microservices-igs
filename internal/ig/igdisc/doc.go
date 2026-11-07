// Package igdisc is Instagram keyword discovery: finding public Instagram
// accounts that talk about Iraq, from the shared keyword list
// (configs/discovery/iraq.yaml, table ig.discovery_keywords), logged out.
// The plan is docs/master/igdisc-plan.md; its status, docs/igdisc/status.md.
//
// Track 1 (this package today) reads nothing from Instagram. A crawler
// hands the Recorder every post and page of comments it emits; the
// Recorder notes the accounts met in a recent post or comment that names a
// keyword (ig.igdisc_evidence) and raises their vet priority in discovery's
// candidates. Discovery's vetter then checks each one with its usual
// profile read and Iraq score, to which the keyword evidence adds a small
// signal (KeywordContext). A keyword match alone never adds an account.
//
// igdisc's tables live in schema ig, named igdisc_*, with migrations of
// their own (migrations/igdisc, MigrateUp). Erasure deletes a person's
// rows (EraseProfile, EraseAuthor).
package igdisc

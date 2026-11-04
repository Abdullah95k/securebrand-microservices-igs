// Package buildinfo names the running build, for events' collector_version
// (the shared contract's envelope).
package buildinfo

import "runtime/debug"

// version is set at build time:
// -ldflags "-X github.com/abdullah95k/securebrand-microservices-igs/internal/platform/buildinfo.version=1.2.3".
var version string

// Version is the build's version: the release tag the image was built
// from, else the VCS revision Go recorded, else "dev".
func Version() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				return s.Value[:12]
			}
		}
	}
	return "dev"
}

// Package iraqtime formats times for the owner, who reads Iraq time
// (UTC+3, no daylight saving; REQ-008 fix 23, gap B7). Data files and logs
// keep UTC; summaries a person reads say Iraq time, labelled.
package iraqtime

import "time"

// Zone is Iraq's time zone.
var Zone = time.FixedZone("Iraq", 3*3600)

// Format is t in Iraq time to the minute, labelled: "2026-11-07 09:15 Iraq
// time". The zero time is "never".
func Format(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return t.In(Zone).Format("2006-01-02 15:04") + " Iraq time"
}

// Span is from and to in Iraq time, the date given once when both fall on
// the same Iraq day: "2026-11-07 09:15 to 09:42 Iraq time".
func Span(from, to time.Time) string {
	if from.IsZero() || to.IsZero() {
		return Format(from) + " to " + Format(to)
	}
	f, t := from.In(Zone), to.In(Zone)
	if f.Format("2006-01-02") == t.Format("2006-01-02") {
		return f.Format("2006-01-02 15:04") + " to " + t.Format("15:04") + " Iraq time"
	}
	return f.Format("2006-01-02 15:04") + " to " + t.Format("2006-01-02 15:04") + " Iraq time"
}

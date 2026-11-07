package log

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"
)

// Account is the attribute "account" that names an account in a log line
// without its username (the owner's rule: user pks only, never usernames):
// "pk:<n>" when the pk is known, else a short hash of the reference
// ("ref#<10 hex>"), stable so the lines about one account still group. A
// reference "id:<pk>" already is a pk.
func Account(pk int64, ref string) slog.Attr {
	return slog.String("account", AccountName(pk, ref))
}

// AccountName is Account's value.
func AccountName(pk int64, ref string) string {
	if pk != 0 {
		return "pk:" + strconv.FormatInt(pk, 10)
	}
	if id, ok := strings.CutPrefix(ref, "id:"); ok {
		if _, err := strconv.ParseInt(id, 10, 64); err == nil {
			return "pk:" + id
		}
	}
	if ref == "" {
		return ""
	}
	h := sha256.Sum256([]byte(strings.ToLower(ref)))
	return "ref#" + hex.EncodeToString(h[:5])
}

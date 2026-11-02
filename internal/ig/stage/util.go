package stage

import (
	"strconv"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
)

func gjsonString(s string) gjson.Result { return gjson.Result{Type: gjson.String, Str: s} }

// timestampFromPK is a media's posting time, which its pk encodes.
func timestampFromPK(pk int64) *timestamppb.Timestamp {
	return timestamppb.New(parse.TakenAtFromPK(strconv.FormatInt(pk, 10)))
}

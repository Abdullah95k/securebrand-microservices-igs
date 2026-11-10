package normalize

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	contracts "github.com/abdullah95k/securebrand-contracts"
)

// The contract's record headers (registry/topics.yaml): every record on a
// shared topic names its message type and the contract version.
const (
	HeaderSBType     = "sb-type"
	HeaderSBContract = "sb-contract"
)

// Headers are a contract message's sb-type and sb-contract headers.
func Headers(m proto.Message) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: HeaderSBType, Value: []byte(proto.MessageName(m))},
		{Key: HeaderSBContract, Value: []byte(contracts.Version)},
	}
}

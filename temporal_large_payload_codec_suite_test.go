package codec_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTemporalLargePayloadCodec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TemporalLargePayloadCodec Suite")
}

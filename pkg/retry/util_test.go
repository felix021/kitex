package retry

import (
	"context"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

func mockRPCInfo(retryTag string) rpcinfo.RPCInfo {
	var tags map[string]string
	if retryTag != "" {
		tags = map[string]string{
			rpcinfo.RetryTag: retryTag,
		}
	}
	to := rpcinfo.NewEndpointInfo("service", "method", nil, tags)
	return rpcinfo.NewRPCInfo(nil, to, nil, nil, nil)
}

func mockContext(retryTag string) context.Context {
	return rpcinfo.NewCtxWithRPCInfo(context.TODO(), mockRPCInfo(retryTag))
}

func TestIsRetryRequest(t *testing.T) {
	t.Run("no-retry-tag", func(t *testing.T) {
		test.Assertf(t, !IsRetryRequest(mockContext(""), nil, nil), "no-retry-tag")
	})
	t.Run("retry-tag=0", func(t *testing.T) {
		test.Assertf(t, !IsRetryRequest(mockContext("0"), nil, nil), "retry-tag=0")
	})
	t.Run("retry-tag=1", func(t *testing.T) {
		test.Assertf(t, IsRetryRequest(mockContext("1"), nil, nil), "retry-tag=1")
	})
}

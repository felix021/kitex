package limitsuite

import (
	"context"
	"fmt"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
)

var _ client.Suite = (*limitSuite)(nil)

// Limiter is used in LimitSuite for limiting requests in client side
type Limiter interface {
	// Allow returns a bool indicating whether this request should be limited
	// ctx can be used to retrieve rpcinfo like methodName for limit of desired granularity
	Allow(ctx context.Context, request, response interface{}) bool
	// Close should release all resource such as allocated memories and goroutines.
	Close()
}

type limitSuite struct {
	limiter Limiter
}

// NewLimitSuite returns a client suite for limiting requests with the given limiter
func NewLimitSuite(limiter Limiter) client.Option {
	return client.WithSuite(&limitSuite{
		limiter: limiter,
	})
}

// Options builds all options needed in limitSuite
func (s *limitSuite) Options() (opts []client.Option) {
	opts = append(opts, client.WithMiddleware(limitMWBuilder(s.limiter)))
	opts = append(opts, client.WithCloseCallbacks(func() error {
		s.limiter.Close()
		return nil
	}))
	return opts
}

// limitMWBuilder will block selected requests which exceeds percentage limit of all requests.
// NOTE:
// 1. limiter.Allow() determines whether the 'limit candidate' should be limited
// 2. limiter.Close() will be called to release related resource when closing the client
func limitMWBuilder(limiter Limiter) endpoint.Middleware {
	return func /* Middleware */ (next endpoint.Endpoint) endpoint.Endpoint {
		return func /* Endpoint */ (ctx context.Context, request interface{}, response interface{}) error {
			if !limiter.Allow(ctx, request, response) {
				err := fmt.Errorf("in limitMWBuilder with %v", limiter)
				return kerrors.ErrCircuitBreak.WithCause(err)
			}
			return next(ctx, request, response)
		}
	}
}

package streamx

import (
	"context"
	"net"
)

func NewServerProvider(ss ServerProvider) ServerProvider {
	return internalServerProvider{ServerProvider: ss}
}

type internalServerProvider struct {
	ServerProvider
}

func (p internalServerProvider) OnStream(ctx context.Context, conn net.Conn) (context.Context, ServerStream, error) {
	ctx, ss, err := p.ServerProvider.OnStream(ctx, conn)
	if err != nil {
		return nil, nil, err
	}
	return ctx, newServerStream(ss), nil
}
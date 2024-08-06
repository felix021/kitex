package streamxclient

import (
	"github.com/cloudwego/kitex/client"
	internal_client "github.com/cloudwego/kitex/internal/client"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/streamx"
)

type ClientOption internal_client.Option
type ClientOptions = internal_client.Options

func WithHostPorts(hostports ...string) ClientOption {
	return convertInternalClientOption(client.WithHostPorts(hostports...))
}

func WithDestService(destService string) ClientOption {
	return convertInternalClientOption(client.WithDestService(destService))
}

func WithClientProvider(pvd streamx.ClientProvider) ClientOption {
	return ClientOption{F: func(o *internal_client.Options, di *utils.Slice) {
		o.RemoteOpt.Provider = pvd
	}}
}

func convertInternalClientOption(o internal_client.Option) ClientOption {
	return ClientOption{F: o.F}
}

func convertClientOption(o ClientOption) internal_client.Option {
	return internal_client.Option{F: o.F}
}

package generic

import (
	"context"
	// "fmt"
	"sync/atomic"

	"github.com/cloudwego/kitex/pkg/generic/proto"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec"
	"github.com/cloudwego/kitex/pkg/remote/codec/perrors"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/jhump/protoreflect/desc"
)

type jsonProtoCodec struct {
	svcDsc   atomic.Value
	provider PbDescriptorProvider
	codec    remote.PayloadCodec
}

func newJsonProtoCodec(p PbDescriptorProvider, codec remote.PayloadCodec) (*jsonProtoCodec, error) {
	if p == nil {
		return nil, perrors.NewProtocolErrorWithMsg("PbDescriptorProvider cannot be nil")
	}
	svc := <-p.Provide()
	c := &jsonProtoCodec{codec: codec, provider: p}
	c.svcDsc.Store(svc)
	go c.update()
	return c, nil
}

func (c *jsonProtoCodec) update() {
	for {
		svc, ok := <-c.provider.Provide()
		if !ok {
			return
		}
		c.svcDsc.Store(svc)
	}
}

func (c *jsonProtoCodec) Marshal(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
	// fmt.Printf("\nLog: jsonproto_codec Marshal called\n")
	method := msg.RPCInfo().Invocation().MethodName()
	if method == "" {
		return perrors.NewProtocolErrorWithMsg("empty methodName in proto Marshal")
	}
	if msg.MessageType() == remote.Exception {
		/*
			if it's an exception, uses the default Protobuf codec to handle the exception.
			Exceptions are not marshaled into JSON.
		*/
		return c.codec.Marshal(ctx, msg, out)
	}
	/*
		Loads the service descriptor for the service that this codec is handling.
		The service descriptor contains the metadata about the service, including the
		definitions of its methods and the types it uses.

		If it fails to load the service descriptor, it returns an error.
	*/
	svcDsc, ok := c.svcDsc.Load().(*desc.ServiceDescriptor)
	if !ok {
		return perrors.NewProtocolErrorWithMsg("get parser ServiceDescriptor failed")
	}
	/*
		Creates a new JSON writer with method descriptor
	*/
	wm, err := proto.NewWriteJSON(svcDsc, method, msg.RPCRole() == remote.Client)
	if err != nil {
		return perrors.NewProtocolErrorWithMsg("NewWriteJSON failed")
	}
	/*
		It sets the codec of the message to be the JSON writer that was just created.
		This prepares the message data to be written out as JSON.
	*/
	msg.Data().(WithCodec).SetCodec(wm)
	/*
		It calls the (original) codec's Marshal function to actually write out the message data.
		Because the message's codec was set to the JSON writer, the data will be written
		out as JSON.
	*/
	return c.codec.Marshal(ctx, msg, out)
}

func (c *jsonProtoCodec) Unmarshal(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
	// fmt.Printf("\nLog: jsonproto_codec Unmarshal called\n")
	if err := codec.NewDataIfNeeded(serviceinfo.GenericMethod, msg); err != nil {
		return err
	}
	svcDsc, ok := c.svcDsc.Load().(*desc.ServiceDescriptor)
	if !ok {
		return perrors.NewProtocolErrorWithMsg("get parser ServiceDescriptor failed")
	}
	rm := proto.NewReadJSON(svcDsc)
	msg.Data().(WithCodec).SetCodec(rm)
	return c.codec.Unmarshal(ctx, msg, in)
}

func (c *jsonProtoCodec) getMethod(req interface{}, method string) (*Method, error) {
	fnSvc := c.svcDsc.Load().(*desc.ServiceDescriptor).FindMethodByName(method)
	if fnSvc == nil {
		return nil, perrors.NewProtocolErrorWithMsg("method not found")
	}

	// Note: In protobuf, there's no direct "oneway" equivalent as in Thrift
	isOneway := false

	return &Method{method, isOneway}, nil
}

func (c *jsonProtoCodec) Name() string {
	return "JSONProto"
}

func (c *jsonProtoCodec) Close() error {
	return c.provider.Close()
}

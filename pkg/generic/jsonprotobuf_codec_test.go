package generic

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cloudwego/kitex/internal/mocks"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/transport"

	"google.golang.org/protobuf/proto"
)

func TestJsonProtobufCodec(t *testing.T) {
	jpc, err := initJsonProtoCodec()
	test.Assert(t, err == nil, err)
	ctx := context.Background()

	// encode // client side
	sendMsg := initSendMsg(transport.TTHeader)
	// fmt.Printf("details: %v\n\n", sendMsg)
	out := remote.NewWriterBuffer(256)
	fmt.Printf("\n\ndata to send %v\n\n", sendMsg.Data())
	err2 := jpc.Marshal(ctx, sendMsg, out)
	test.Assert(t, err2 == nil, err2)
	fmt.Printf("\n\ndetails 2: %v\n", sendMsg.Data())

	// decode server side
	recvMsg := initRecvMsg()
	buf, err := out.Bytes()
	recvMsg.SetPayloadLen(len(buf))
	test.Assert(t, err == nil, err)
	in := remote.NewReaderBuffer(buf)
	err = jpc.Unmarshal(ctx, recvMsg, in)
	test.Assert(t, err == nil, err)
	fmt.Printf("\n\ndata to send %v\n\n", recvMsg.Data())

	// compare Req Arg
	sendReq := (sendMsg.Data()).(*MockReqArgs).Req
	recvReq := (recvMsg.Data()).(*MockReqArgs).Req
	test.Assert(t, sendReq.Msg == recvReq.Msg)
	test.Assert(t, len(sendReq.StrList) == len(recvReq.StrList))
	test.Assert(t, len(sendReq.StrMap) == len(recvReq.StrMap))
	for i, item := range sendReq.StrList {
		fmt.Print(item)
		fmt.Print(recvReq.StrList[i])
		test.Assert(t, item == recvReq.StrList[i])
	}
	for k := range sendReq.StrMap {
		test.Assert(t, sendReq.StrMap[k] == recvReq.StrMap[k])
	}
}

func TestException(t *testing.T) {
	// protoCodec := protobuf.NewProtobufCodec()
	jpc, err := initJsonProtoCodec()
	test.Assert(t, err == nil, err)

	ctx := context.Background()
	ink := rpcinfo.NewInvocation("", "")
	ri := rpcinfo.NewRPCInfo(nil, nil, ink, nil, nil)
	errInfo := "mock exception"
	transErr := remote.NewTransErrorWithMsg(remote.UnknownMethod, errInfo)
	// encode server side
	errMsg := initServerErrorMsg(transport.TTHeader, ri, transErr)
	out := remote.NewWriterBuffer(256)
	err2 := jpc.Marshal(ctx, errMsg, out)
	test.Assert(t, err2.Error() == "empty methodName in proto Marshal")

	// Exception MsgType test
	exceptionMsgTypeInk := rpcinfo.NewInvocation("", "Test")
	exceptionMsgTypeRi := rpcinfo.NewRPCInfo(nil, nil, exceptionMsgTypeInk, nil, nil)
	exceptionMsgTypeMsg := remote.NewMessage(&remote.TransError{}, nil, exceptionMsgTypeRi, remote.Exception, remote.Client)
	// Marshal side
	err = jpc.Marshal(ctx, exceptionMsgTypeMsg, out)
	test.Assert(t, err == nil)
}

var (
	svcInfo = mocks.ServiceInfo()
)

type MockReqArgs struct {
	Req   *MockRequest
	codec interface{} // Add a field to store the codec.
}

func (p *MockReqArgs) Marshal(out []byte) ([]byte, error) {
	if p == nil {
		return nil, errors.New("jsonProtoCodec cannot be nil")
	}
	if !p.IsSetReq() {
		return out, fmt.Errorf("No req in MockReqArgs")
	}
	return proto.Marshal(p.Req)
}

func (p *MockReqArgs) Unmarshal(in []byte) error {
	if p == nil {
		return errors.New("jsonProtoCodec cannot be nil")
	}
	msg := new(MockRequest)
	if err := proto.Unmarshal(in, msg); err != nil {
		return err
	}
	p.Req = msg
	return nil
}

func (p *MockReqArgs) GetReq() *MockRequest {
	return p.Req
}

func (p *MockReqArgs) IsSetReq() bool {
	return p.Req != nil
}
func (m *MockReqArgs) SetCodec(codec interface{}) {
	m.codec = codec
}

func initJsonProtoCodec() (*jsonProtoCodec, error) {
	p, err := NewPbFileProvider("./json_test/idl/mock.proto")
	if err != nil {
		// handle error here
		fmt.Printf("\n\nerror is: %v\n\n", err)
		return nil, err
	}

	jpc, err := newJsonProtoCodec(p, protoCodec)
	if err != nil {
		// handle error here
		fmt.Printf("\n\nerror is: %v\n\n", err)
		return nil, err
	}

	return jpc, nil
}

func initSendMsg(tp transport.Protocol) remote.Message {
	var _args MockReqArgs
	_args.Req = prepareReq()
	ink := rpcinfo.NewInvocation("", "Test")
	ri := rpcinfo.NewRPCInfo(nil, nil, ink, nil, nil)
	msg := remote.NewMessage(&_args, svcInfo, ri, remote.Call, remote.Client)
	msg.SetProtocolInfo(remote.NewProtocolInfo(tp, svcInfo.PayloadCodec))
	return msg
}

func initRecvMsg() remote.Message {
	var _args MockReqArgs
	ink := rpcinfo.NewInvocation("", "Test")
	ri := rpcinfo.NewRPCInfo(nil, nil, ink, nil, nil)
	msg := remote.NewMessage(&_args, svcInfo, ri, remote.Call, remote.Server)
	return msg
}

func initServerErrorMsg(tp transport.Protocol, ri rpcinfo.RPCInfo, transErr *remote.TransError) remote.Message {
	errMsg := remote.NewMessage(transErr, svcInfo, ri, remote.Exception, remote.Server)
	errMsg.SetProtocolInfo(remote.NewProtocolInfo(tp, svcInfo.PayloadCodec))
	return errMsg
}

func prepareReq() *MockRequest {
	strMap := make(map[string]string)
	strMap["key1"] = "val1"
	strMap["key2"] = "val2"
	strList := []string{"str1", "str2"}
	req := &MockRequest{
		Msg:     "MockReq",
		StrMap:  strMap,
		StrList: strList,
	}
	return req
}

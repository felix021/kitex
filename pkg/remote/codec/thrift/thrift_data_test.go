package thrift

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/cloudwego/kitex/internal/mocks/thrift/fast"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/remote"
)

var (
	mockReq = &fast.MockReq{
		Msg: "hello",
	}
	mockReqThrift = []byte{
		11 /* string */, 0, 1 /* id=1 */, 0, 0, 0, 5 /* length=5 */, 104, 101, 108, 108, 111, /* "hello" */
		13 /* map */, 0, 2 /* id=2 */, 11 /* key:string*/, 11 /* value:string */, 0, 0, 0, 0, /* map size=0 */
		15 /* list */, 0, 3 /* id=3 */, 11 /* item:string */, 0, 0, 0, 0, /* list size=0 */
		0, /* end of struct */
	}
)

func TestMarshalBasicThriftData(t *testing.T) {
	t.Run("invalid-data", func(t *testing.T) {
		err := marshalBasicThriftData(context.Background(), nil, 0)
		test.Assert(t, err != nil, err)
	})
	t.Run("valid-data", func(t *testing.T) {
		transport := thrift.NewTMemoryBufferLen(1024)
		tProt := thrift.NewTBinaryProtocol(transport, true, true)
		err := marshalBasicThriftData(context.Background(), tProt, mockReq)
		test.Assert(t, err == nil, err)
		result := transport.Bytes()
		test.Assert(t, reflect.DeepEqual(result, mockReqThrift), result)
	})
}

func TestMarshalThriftData(t *testing.T) {
	t.Run("NoCodec(=FastCodec)", func(t *testing.T) {
		buf, err := MarshalThriftData(context.Background(), nil, mockReq)
		test.Assert(t, err == nil, err)
		test.Assert(t, reflect.DeepEqual(buf, mockReqThrift), buf)
	})
	t.Run("FastCodec", func(t *testing.T) {
		buf, err := MarshalThriftData(context.Background(), NewThriftCodecWithConfig(FastRead|FastWrite), mockReq)
		test.Assert(t, err == nil, err)
		test.Assert(t, reflect.DeepEqual(buf, mockReqThrift), buf)
	})
	t.Run("BasicCodec", func(t *testing.T) {
		buf, err := MarshalThriftData(context.Background(), NewThriftCodecWithConfig(Basic), mockReq)
		test.Assert(t, err == nil, err)
		test.Assert(t, reflect.DeepEqual(buf, mockReqThrift), buf)
	})
	// FrugalCodec: in thrift_frugal_amd64_test.go: TestMarshalThriftDataFrugal
}

func Test_decodeBasicThriftData(t *testing.T) {
	t.Run("empty-input", func(t *testing.T) {
		req := &fast.MockReq{}
		tProt := NewBinaryProtocol(remote.NewReaderBuffer([]byte{}))
		err := decodeBasicThriftData(context.Background(), tProt, req)
		test.Assert(t, err != nil, err)
	})
	t.Run("invalid-input", func(t *testing.T) {
		req := &fast.MockReq{}
		tProt := NewBinaryProtocol(remote.NewReaderBuffer([]byte{0xff}))
		err := decodeBasicThriftData(context.Background(), tProt, req)
		test.Assert(t, err != nil, err)
	})
	t.Run("normal-input", func(t *testing.T) {
		req := &fast.MockReq{}
		tProt := NewBinaryProtocol(remote.NewReaderBuffer(mockReqThrift))
		err := decodeBasicThriftData(context.Background(), tProt, req)
		checkDecodeResult(t, err, req)
	})
}

func checkDecodeResult(t *testing.T, err error, req *fast.MockReq) {
	test.Assert(t, err == nil, err)
	test.Assert(t, req.Msg == mockReq.Msg, req.Msg, mockReq.Msg)
	test.Assert(t, len(req.StrMap) == 0, req.StrMap)
	test.Assert(t, len(req.StrList) == 0, req.StrList)
}

func TestUnmarshalThriftData(t *testing.T) {
	t.Run("NoCodec(=FastCodec)", func(t *testing.T) {
		req := &fast.MockReq{}
		err := UnmarshalThriftData(context.Background(), nil, mockReqThrift, req)
		checkDecodeResult(t, err, req)
	})
	t.Run("FastCodec", func(t *testing.T) {
		req := &fast.MockReq{}
		err := UnmarshalThriftData(context.Background(), NewThriftCodecWithConfig(FastRead|FastWrite), mockReqThrift, req)
		checkDecodeResult(t, err, req)
	})
	t.Run("BasicCodec", func(t *testing.T) {
		req := &fast.MockReq{}
		err := UnmarshalThriftData(context.Background(), NewThriftCodecWithConfig(Basic), mockReqThrift, req)
		checkDecodeResult(t, err, req)
	})
	// FrugalCodec: in thrift_frugal_amd64_test.go: TestUnmarshalThriftDataFrugal
}

func TestUnmarshalThriftException(t *testing.T) {
	// prepare exception thrift binary
	transport := thrift.NewTMemoryBufferLen(marshalThriftBufferSize)
	tProt := thrift.NewTBinaryProtocol(transport, true, true)
	errMessage := "test: invalid protocol"
	exc := thrift.NewTApplicationException(thrift.INVALID_PROTOCOL, errMessage)
	err := exc.Write(tProt)
	test.Assert(t, err == nil, err)

	// unmarshal
	tProtRead := NewBinaryProtocol(remote.NewReaderBuffer(transport.Bytes()))
	err = UnmarshalThriftException(tProtRead)
	transErr, ok := err.(*remote.TransError)
	test.Assert(t, ok, err)
	test.Assert(t, transErr.TypeID() == thrift.INVALID_PROTOCOL, transErr)
	test.Assert(t, transErr.Error() == errMessage, transErr)
}

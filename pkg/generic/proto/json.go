package proto

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

type WriteJSON struct {
	RequestType *desc.MessageDescriptor
	Method      string
}

func NewWriteJSON(svcDsc ServiceDescriptor, method string, isClient bool) (*WriteJSON, error) {
	// fmt.Printf("\nLog: Proto Package's NewWriteJSON called\n")

	methDesc := svcDsc.FindMethodByName(method)
	if methDesc == nil {
		return nil, fmt.Errorf("method %s not found in service descriptor", method)
	}

	RT := methDesc.GetInputType()
	// if caller is not from client, i.e. from RPC server
	if !isClient {
		RT = methDesc.GetOutputType()
	}

	return &WriteJSON{
		// contains information about how the protobuf message should look like
		RequestType: RT,
		Method:      methDesc.GetName(),
	}, nil
}

func (wj *WriteJSON) Write(ctx context.Context, out []byte, msg interface{}) ([]byte, error) {
	// fmt.Printf("\nLog: Proto Package's Write called\n")

	jsonStr, ok := msg.(string)
	if !ok {
		// if not a string means is response from RPC
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("error marshalling msg to JSON: %v", err)
		}
		jsonStr = string(jsonMsg)
	}

	dynMsg := dynamic.NewMessage(wj.RequestType)
	err := dynMsg.UnmarshalJSON([]byte(jsonStr))
	if err != nil {
		return nil, err
	}

	// Marshal dynamic proto message in JSON form
	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return nil, err
	}

	// commented out, to show the string representation is in json form
	// jsonStr := string(jsonBytes)
	// fmt.Println(jsonStr)

	return jsonBytes, nil
}

var _ MessageWriter = (*WriteJSON)(nil)

type ReadJSON struct {
	RequestType MessageDescriptor
	Method      string
}

func NewReadJSON(serviceDesc ServiceDescriptor) *ReadJSON {
	methodDesc := serviceDesc.GetMethods()[0]
	return &ReadJSON{
		RequestType: methodDesc.GetInputType(),
		Method:      methodDesc.GetName(),
	}
}

var _ MessageReader = (*ReadJSON)(nil)

// in []byte should be identical []byte from Write
func (rj *ReadJSON) Read(ctx context.Context, in []byte) (interface{}, error) {
	// fmt.Print("\nLog: Generic/proto Read called\n")
	jsonStr2 := string(in)
	return jsonStr2, nil
}

/*
	Old implementations tried
*/

// Old Write implementation
// func (wj *WriteJSON) Write(ctx context.Context, out []byte, msg interface{}) ([]byte, error) {
// 	// Convert msg to a string
// 	str, ok := msg.(string)
// 	if !ok {
// 		// if not a string means is response from RPC
// 		jsonMsg, err := json.Marshal(msg)
// 		if err != nil {
// 			return nil, fmt.Errorf("error marshalling msg to JSON: %v", err)
// 		}
// 		str = string(jsonMsg)
// 	}

// 	fmt.Print("\n___Generic/proto WRITE CALLED___\n")

// 	// form is in json string which is ok
// 	fmt.Printf("\nstring is: %v\n", str)
// 	fmt.Printf("\nbinary form of string: %v\n", []byte(str))

// 	// Create a map structure to unmarshal the JSON string into
// 	var unmarshalled map[string]interface{}
// 	err := json.Unmarshal([]byte(str), &unmarshalled)
// 	if err != nil {
// 		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
// 	}
// 	// req := &proto.JobCreateRequest{}
// 	// err := protojson.Unmarshal(bytes, req)

// 	// Create a new dynamic message using the message descriptor
// 	dynMsg := dynamic.NewMessage(wj.RequestType)
// 	fmt.Printf("\ndynmsg is: %v\n", dynMsg)

// 	// Set the fields of the dynamic message using the unmarshalled map
// 	for key, value := range unmarshalled {
// 		field := wj.RequestType.FindFieldByName(key)
// 		if field == nil {
// 			return nil, fmt.Errorf("unknown field %q for message type %v", key, wj.RequestType.GetName())
// 		}
// 		fmt.Printf("\nfield is: %v\n", field)
// 		err = dynMsg.TrySetFieldByNumber(int(field.GetNumber()), value)
// 		if err != nil {
// 			return nil, fmt.Errorf("error setting field %q: %v", key, err)
// 		}
// 	}
// 	fmt.Printf("\ndynmsg2 is: %v\n", dynMsg)

// 	// md, err := desc.LoadMessageDescriptorForMessage((*dynamic.Message)(nil))
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// dynMsg2 := dynamic.NewMessage(md)

// 	// // Populate the message
// 	// dynMsg2.SetFieldByName("field1", "value1")
// 	// jsonBytes, err := protojson.Marshal(dynMsg2)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// Marshal the dynamic message into a protobuf byte array
// 	out, err = dynMsg.Marshal()
// 	if err != nil {
// 		return nil, fmt.Errorf("error marshalling message: %v", err)
// 	}
// 	fmt.Printf("\nEncoded form is: %v\n", out)
// 	fmt.Print("\n___Generic/proto WRITE ended___\n")
// 	return out, nil

// 	// Marshal the dynamic message into a JSON byte array
// 	// jsonStr, err := json.Marshal(dynMsg)
// 	// if err != nil {
// 	// 	return nil, fmt.Errorf("error marshalling message: %v", err)
// 	// }
// 	// fmt.Printf("\njsonStr is: %v\n", jsonStr)

// 	// return jsonStr, nil
// }

// Old Read implementation:
// func (rj *ReadJSON) Read(ctx context.Context, in []byte) (interface{}, error) {
// 	// Convert msg to a *dynamic.Message
// 	fmt.Printf("REACHED PUBOR: %v", in)

// 	fmt.Print("\n___Generic/proto READ CALLED___\n")

// 	fmt.Printf("\nbyte is: %v\n", in)
// 	dynMsg := dynamic.NewMessage(rj.RequestType)

// 	// Unmarshal the input byte slice into the dynamic message
// 	if err := dynMsg.Unmarshal(in); err != nil {
// 		return nil, fmt.Errorf("error unmarshalling message: %v", err)
// 	}

// 	fmt.Printf("\nDecoded form is: %v\n", dynMsg)

// 	// Convert the dynamic message to a proto.Message and marshal it into JSON
// 	jsonBytes, err := protojson.Marshal(dynMsg.ProtoMessage())
// 	if err != nil {
// 		return nil, fmt.Errorf("error marshalling message to JSON: %v", err)
// 	}

// 	fmt.Printf("\nJSON form is: %s\n", jsonBytes)
// 	fmt.Print("\n___Generic/proto READ ended___\n")

// 	return string(jsonBytes), nil
// }

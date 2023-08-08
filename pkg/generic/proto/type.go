// /*
//  * Copyright 2021 CloudWeGo Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

package proto

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

type (
	ServiceDescriptor = *desc.ServiceDescriptor
	MessageDescriptor = *desc.MessageDescriptor
)

type Message interface {
	Marshal() ([]byte, error)
	TryGetFieldByNumber(fieldNumber int) (interface{}, error)
	TrySetFieldByNumber(fieldNumber int, val interface{}) error
}

func NewMessage(descriptor MessageDescriptor) Message {
	return dynamic.NewMessage(descriptor)
}

// MessageWriter
type MessageWriter interface {
	Write(ctx context.Context, out []byte, msg interface{}) ([]byte, error)
}

type MessageReader interface {
	// Read(ctx context.Context, in io.Reader, msg proto.Message) error
	Read(ctx context.Context, in []byte) (interface{}, error)
}

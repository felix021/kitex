/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"github.com/cloudwego/kitex/pkg/remote"
	codecutils "github.com/cloudwego/kitex/pkg/remote/codec/utils"
)

const (
	// FrontMask is used in protocol sniffing.
	FrontMask = codecutils.FrontMask
)

// SetOrCheckMethodName is used to set method name to invocation.
func SetOrCheckMethodName(methodName string, message remote.Message) error {
	return codecutils.SetOrCheckMethodName(methodName, message)
}

// SetOrCheckSeqID is used to check the sequence ID.
func SetOrCheckSeqID(seqID int32, message remote.Message) error {
	return codecutils.SetOrCheckSeqID(seqID, message)
}

// UpdateMsgType updates msg type.
func UpdateMsgType(msgType uint32, message remote.Message) error {
	return codecutils.UpdateMsgType(msgType, message)
}

// NewDataIfNeeded is used to create the data if not exist.
func NewDataIfNeeded(method string, message remote.Message) error {
	return codecutils.NewDataIfNeeded(method, message)
}

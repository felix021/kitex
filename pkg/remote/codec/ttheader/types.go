/*
 * Copyright 2024 CloudWeGo Authors
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

package ttheader

// ProtocolID is the wrapped protocol id used in THeader.
type ProtocolID uint8

// Supported ProtocolID values.
const (
	ProtocolIDThriftBinary    ProtocolID = 0x00
	ProtocolIDThriftCompact   ProtocolID = 0x02 // Kitex not support
	ProtocolIDThriftCompactV2 ProtocolID = 0x03 // Kitex not support
	ProtocolIDKitexProtobuf   ProtocolID = 0x04
	ProtocolIDNotSpecified    ProtocolID = 0xff
	ProtocolIDDefault                    = ProtocolIDThriftBinary
)

type InfoIDType uint8

type HeaderFlags uint16

const (
	HeaderFlagSupportOutOfOrder HeaderFlags = 0x01
	HeaderFlagDuplexReverse     HeaderFlags = 0x08
	HeaderFlagSASL              HeaderFlags = 0x10
)

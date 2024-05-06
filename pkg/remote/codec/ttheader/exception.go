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

import (
	"encoding/binary"
	"errors"
)

// implementation of the thrift.TApplicationException
// MAGIC:            0x8001 (2 bytes)
// Message Type:     0x0003 (2 bytes)
// Method:           length(4 bytes) + value(var-length)
// SeqID:            (4 bytes)
// Fields:
// - Message:        field_type(0x0b, binary) + field_id(1) + length(4 bytes) + value(var-length)
// - Exception Type: field_type(0x08, int32) + field_id(1) + value(4 bytes)

const (
	sizeMagic         = 2
	sizeMessageType   = 2
	sizeMethodLength  = 4
	sizeSeqID         = 4
	sizeMessageMeta   = 7
	sizeExceptionType = 7
	sizeStop          = 1
	sizeFixed         = sizeMagic + sizeMessageType + sizeMethodLength + sizeSeqID + sizeMessageMeta + sizeExceptionType + sizeStop

	thriftMagic                = 0x8001
	thriftMessageTypeException = 0x3

	thriftStop       = 0
	thriftTypeInt32  = 0x8
	thriftTypeBinary = 0xb

	fieldIDMessage       = 1
	fieldIDExceptionType = 2
)

var (
	ErrInvalidThriftMagic       = errors.New("invalid thrift magic")
	ErrInvalidThriftMessageType = errors.New("invalid thrift message type")
)

type exception struct {
	MethodName    string
	SeqID         int32
	Message       string
	ExceptionType int
}

func newException(methodName string, seqID int32, message string, typeID int) *exception {
	return &exception{
		MethodName:    methodName,
		SeqID:         seqID,
		Message:       message,
		ExceptionType: typeID,
	}
}

func (e *exception) BytesLength() int {
	return sizeFixed + len(e.MethodName) + len(e.Message)
}

func (e *exception) Bytes() ([]byte, error) {
	buf := make([]byte, e.BytesLength())
	idx := writeExceptionHeader(buf)
	idx += writeMethod(buf[idx:], e.MethodName)
	idx += writeSeqID(buf[idx:], e.SeqID)
	idx += writeMessage(buf[idx:], e.Message)
	idx += writeExceptionType(buf[idx:], e.ExceptionType)
	return buf, nil
}

func writeExceptionHeader(buf []byte) int {
	binary.BigEndian.PutUint16(buf[0:], thriftMagic)
	binary.BigEndian.PutUint16(buf[2:], thriftMessageTypeException)
	return sizeMagic + sizeMessageType
}

func writeMethod(buf []byte, name string) int {
	binary.BigEndian.PutUint32(buf, uint32(len(name)))
	copy(buf[4:], name)
	return sizeMethodLength + len(name)
}

func writeSeqID(bytes []byte, id int32) int {
	binary.BigEndian.PutUint32(bytes, uint32(id))
	return sizeSeqID
}

func writeMessage(buf []byte, message string) int {
	buf[0] = thriftTypeBinary
	binary.BigEndian.PutUint16(buf[1:], fieldIDMessage)
	binary.BigEndian.PutUint32(buf[3:], uint32(len(message)))
	copy(buf[sizeMessageMeta:], message)
	return sizeMessageMeta + len(message)
}

func writeExceptionType(buf []byte, id int) int {
	buf[0] = thriftTypeInt32
	binary.BigEndian.PutUint16(buf[1:], fieldIDExceptionType)
	binary.BigEndian.PutUint32(buf[3:], uint32(id))
	return sizeExceptionType
}

func (e *exception) read(reader *bytesReader) (err error) {
	if err = readMagic(reader); err != nil {
		return
	}
	if err = readMessageType(reader); err != nil {
		return
	}
	if e.MethodName, err = readMethod(reader); err != nil {
		return
	}
	if e.SeqID, err = reader.ReadInt32(); err != nil {
		return
	}
	return e.readFields(reader)
}

func (e *exception) readFields(reader *bytesReader) (err error) {
	for {
		var tp byte
		if tp, err = reader.ReadByte(); err != nil {
			return err
		} else if tp == thriftStop {
			return nil
		}
		var id uint16
		if id, err = reader.ReadUint16(); err != nil {
			return err
		}
		if tp == thriftTypeBinary && id == fieldIDMessage {
			var size uint32
			if size, err = reader.ReadUint32(); err != nil {
				return err
			}
			if e.Message, err = reader.ReadString(int(size)); err != nil {
				return err
			}
		} else if tp == thriftTypeInt32 && id == fieldIDExceptionType {
			value, err := reader.ReadUint32()
			if err != nil {
				return err
			}
			e.ExceptionType = int(value)
		} else {
			continue // ignore other fields
		}
	}
}

func readMethod(reader *bytesReader) (string, error) {
	size, err := reader.ReadUint32()
	if err != nil {
		return "", err
	}
	return reader.ReadString(int(size))
}

func readMessageType(reader *bytesReader) error {
	msgType, err := reader.ReadUint16()
	if err != nil {
		return err
	}
	if msgType != thriftMessageTypeException {
		return ErrInvalidThriftMessageType
	}
	return nil
}

func readMagic(reader *bytesReader) error {
	magic, err := reader.ReadUint16()
	if err != nil {
		return err
	}
	if magic != thriftMagic {
		return ErrInvalidThriftMagic
	}
	return nil
}

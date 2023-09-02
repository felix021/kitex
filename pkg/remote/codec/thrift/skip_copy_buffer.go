package thrift

import (
	"github.com/felix021/thrift-util/buffer/iface"
)

const (
	defaultInitialCap = 128 // bytes
)

var (
	_ iface.NextBuffer = (*SkipCopyBuffer)(nil)

	initialCap = defaultInitialCap
)

func SetInitialCap(cap int) {
	initialCap = cap
}

type SkipCopyBuffer struct {
	p   *BinaryProtocol
	out []byte
}

func NewSkipCopyBuffer(p *BinaryProtocol) *SkipCopyBuffer {
	return &SkipCopyBuffer{
		p:   p,
		out: make([]byte, 0, initialCap),
	}
}

func (s *SkipCopyBuffer) Next(n int) (buf []byte, err error) {
	if buf, err = s.p.next(n); err == nil {
		s.out = append(s.out, buf...)
	}
	return
}

func (s *SkipCopyBuffer) Skip(n int) (err error) {
	_, err = s.Next(n)
	return
}

func (s *SkipCopyBuffer) Buffer() (buf []byte) {
	return s.out
}

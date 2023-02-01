package protobuf

import (
	"github.com/bytedance/gopkg/lang/mcache"
	"testing"
)

func Test_mallocWithFirstByteZeroed(t *testing.T) {
	type args struct {
		size int
		data []byte
	}
	tests := []struct {
		name string
		args args
		want byte
	}{
		{
			name: "test_with_no_data",
			args: args{
				size: 4,
				data: nil,
			},
			want: 0,
		},
		{
			name: "test_with_zeroed_data",
			args: args{
				size: 8,
				data: []byte{0, 0, 0, 0, 0, 0, 0, 0},
			},
			want: 0,
		},
		{
			name: "test_with_non_zeroed_data",
			args: args{
				size: 16,
				data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.data != nil {
				buf := mcache.Malloc(tt.args.size)
				copy(buf, tt.args.data)
				mcache.Free(buf)
			}
			if got := mallocWithFirstByteZeroed(tt.args.size); got[0] != tt.want {
				t.Errorf("mallocWithFirstByteZeroed() = %v, want %v", got, tt.want)
			}
		})
	}
}

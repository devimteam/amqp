package amqp

import (
	"errors"
	"testing"
)

func TestWrapError(t *testing.T) {
	type args struct {
		errs []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			name:    "empty",
			args:    args{errs: []interface{}{""}},
			wantErr: "",
		},
		{
			name:    "case 1",
			args:    args{errs: []interface{}{"err1", errors.New("err2")}},
			wantErr: "err1: err2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WrapError(tt.args.errs...).Error(); err != tt.wantErr {
				t.Errorf("WrapError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"reflect"
	"testing"
)

func Test_getMsgKeyFromBody(t *testing.T) {
	type args struct {
		body    []byte
		jsonKey string
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test1", args{[]byte("{\"key1\":\"test1\"}"), "key1"}, nil, true},
		{"test2", args{[]byte("{\"key1\":{\"key2\":\"test12\"}}"), "key1"}, []byte("test12"), false},
		{"test3", args{[]byte("{\"key11\":{\"key3\":\"test12\"}}"), "key11"}, nil, true},
		{"test4", args{[]byte("{\"key11\":{\"key2\":12}}"), "key11"}, []byte("12"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getMsgKeyFromBody(tt.args.body, tt.args.jsonKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("getMsgKeyFromBody() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getMsgKeyFromBody() = %v, want %v", got, tt.want)
			}
		})
	}
}

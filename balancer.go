package main

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type rangeBalancer struct {
	kgo.GroupBalancer
	rpcEndpoint string
}

func NewRangeBalancer(rpcEndpoint string) kgo.GroupBalancer {
	return &rangeBalancer{GroupBalancer: kgo.RangeBalancer(), rpcEndpoint: rpcEndpoint}
}

func (r *rangeBalancer) JoinGroupMetadata(interests []string, _ map[string][]int32, _ int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = interests
	meta.UserData = []byte(r.rpcEndpoint)
	return meta.AppendTo([]byte{})
}

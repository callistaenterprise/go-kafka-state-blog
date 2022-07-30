module github.com/bjorngylling/go-kafka-state

go 1.18

replace github.com/twmb/franz-go v1.4.2 => ../franz-go

require (
	github.com/twmb/franz-go v1.6.0
	github.com/twmb/franz-go/pkg/kmsg v1.1.0
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

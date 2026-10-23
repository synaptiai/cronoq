module github.com/synaptiai/cronoq/deploy/helm/test/e2e

go 1.26.0

require github.com/synaptiai/cronoq/sdks/go v0.0.0

require (
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/synaptiai/cronoq/sdks/go => ../../../../sdks/go

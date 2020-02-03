//go:generate protoc --proto_path=..:. -I .. --go_out=plugins=grpc:. insidesvc.proto

package insidesvc

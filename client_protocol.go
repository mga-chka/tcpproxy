package main

import (
	"errors"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

func readClientProtocolV2(src *proto.Reader, version int) error {
	n, err := src.UVarInt()
	if err != nil {
		return err
	}
	code := proto.ClientCode(n)

	if !code.IsAClientCode() {
		return fmt.Errorf("got unkown header client code: %d ", code)
	}
	err = nil
	switch code {
	case proto.ClientCodeHello:
		fmt.Println("+++++ client sent proto.ClientHello")
		err = handleClientHello(src, version)
	case proto.ClientCodeQuery:
		fmt.Println("+++++ client sent proto.ClientCodeQuery")
		err = handleClientQuery(src, version)
	case proto.ClientCodeData:
		fmt.Println("++++ client sent proto.ClientCodeData")
		err = handleClientData(src, version)
	case proto.ClientCodeCancel:
		fmt.Println("++++ client sent proto.ClientCodeCancel")
		err = handleClientCancel(src, version)
	case proto.ClientCodePing:
		fmt.Println("++++ client sent proto.ClientCodePing")
		err = nil
	case proto.ClientTablesStatusRequest:
		fmt.Println("++++ client sent proto.ClientTablesStatusRequest")
		println("error readClientProtocolV2")
		err = errors.New("test")
	}

	return err
}

func endClientHandshake(src *proto.Reader, version int) error {
	v, err := src.Str()
	if err != nil {
		return fmt.Errorf("quota_key :", err)
	}
	println("quota_key = ", v)
	return nil
}

func handleClientHello(src *proto.Reader, version int) error {
	v := proto.ClientHello{}
	err := v.Decode(src)
	if err != nil {
		fmt.Println("----error in v.Decode(src)")
		return err
	}
	println("Client Name:= " + v.Name)
	println("ProtocolVersion:= ", v.ProtocolVersion)
	println("end readClientProtocolV2")
	return nil
}
func handleClientQuery(src *proto.Reader, version int) error {

	var q proto.Query
	if err := q.DecodeAware(src, version); err != nil {
		return err
	}
	println("query_id: ", q.ID)
	println("query_body: ", q.Body)
	return nil
}

func handleClientData(src *proto.Reader, version int) error {
	var data proto.ClientData
	if err := data.DecodeAware(src, version); err != nil {
		return fmt.Errorf("decode %w", err)
	}

	var block proto.Block
	if err := block.DecodeBlock(src, version, nil); err != nil {
		return fmt.Errorf("decode block %w", err)
	}

	if block.Rows > 0 || block.Columns > 0 {
		return errors.New("input not implemented")
	}
	println("TableName: ", data.TableName)

	return nil
}

func handleClientCancel(src *proto.Reader, version int) error {
	println("throwing error to cancel stream")
	return errors.New("cancel stream")
}

func readClientProtocol(isFirstClientPacket *bool, srcType SrcType, buf []byte, written int64) error {
	if *isFirstClientPacket && srcType == SourceClient {
		n, err := UVarInt(&buf)
		if err != nil {
			return err
		}
		*isFirstClientPacket = false
		expected := proto.ClientCodeHello
		code := proto.ClientCode(n)
		if code != expected {
			return fmt.Errorf("got %d instead of %d", code, expected)
		} else {
			println("Client: got ClientCodeHello")
		}
	}
	return nil
}

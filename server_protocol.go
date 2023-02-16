package main

import (
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
)

func readServerProtocolV2(src *proto.Reader, version int) error {
	n, err := src.UVarInt()
	if err != nil {
		return err
	}
	code := proto.ServerCode(n)
	if !code.IsAServerCode() {
		return fmt.Errorf("got unkown header server code: %d ", code)
	}
	switch code {
	case proto.ServerCodeHello:
		fmt.Println("===== server sent proto.ServerCodeHello")
		err = handleServerHello(src, version)
	case proto.ServerCodeData:
		fmt.Println("===== server sent proto.ServerCodeData")
		err = handleServerCodeData(src, version)
	case proto.ServerCodeException:
		fmt.Println("===== server sent proto.ServerCodeException")
		err = handleServerException(src, version)
	case proto.ServerCodeProgress:
		err = handleServerProgress(src, version)
		fmt.Println("===== server sent proto.ServerCodeProgress")
	case proto.ServerCodePong:
		fmt.Println("===== server sent proto.ServerCodePong")
		err = nil
	case proto.ServerCodeEndOfStream:
		fmt.Println("===== server sent proto.ServerCodeEndOfStream")
		err = handleServerEndOfStream(src, version)
	case proto.ServerCodeProfile:
		fmt.Println("===== server sent proto.ServerCodeProfile")
		handleServerProfile(src, version)
	case proto.ServerCodeTotals:
		fmt.Println("===== server sent proto.ServerCodeTotals")
	case proto.ServerCodeExtremes:
		fmt.Println("===== server sent proto.ServerCodeExtremes")
	case proto.ServerCodeTablesStatus:
		fmt.Println("===== server sent proto.ServerCodeTablesStatus")
	case proto.ServerCodeLog:
		fmt.Println("===== server sent proto.ServerCodeLog")
	case proto.ServerCodeTableColumns:
		fmt.Println("===== server sent proto.ServerCodeTableColumns")
	case proto.ServerPartUUIDs:
		fmt.Println("===== server sent proto.ServerPartUUIDs")
	case proto.ServerReadTaskRequest:
		fmt.Println("===== server sent proto.ServerReadTaskRequest")
	case proto.ServerProfileEvents:
		fmt.Println("===== server sent proto.ServerProfileEvents")
		err = handleProfileEvent(src, version)
	}

	return err
}

func handleProfileEvent(src *proto.Reader, version int) error {

	return decodeBlock(src, version)
}

func handleServerException(src *proto.Reader, version int) error {
	var list []proto.Exception
	for {
		var ex proto.Exception
		if err := ex.DecodeAware(src, version); err != nil {
			return errors.Wrap(err, "decode")
		}

		list = append(list, ex)
		if !ex.Nested {
			break
		}
	}
	top := list[0]
	e := &ch.Exception{
		Code:    top.Code,
		Name:    top.Name,
		Message: top.Message,
		Stack:   top.Stack,
	}
	for _, next := range list[1:] {
		e.Next = append(e.Next, ch.Exception{
			Code:    next.Code,
			Name:    next.Name,
			Message: next.Message,
			Stack:   next.Stack,
		})
	}
	return e
}

func handleServerProgress(src *proto.Reader, version int) error {
	var p proto.Progress
	err := p.DecodeAware(src, version)
	if err != nil {
		return err
	}
	println("rows:= ", p.Rows)
	return nil
}

func handleServerHello(src *proto.Reader, version int) error {
	v := proto.ServerHello{}
	err := v.DecodeAware(src, version)
	if err != nil {
		return err
	}
	println("DisplayName:= " + v.DisplayName)
	return nil
}

func handleServerCodeData(src *proto.Reader, version int) error {
	return decodeBlock(src, version)
}

func decodeBlock(src *proto.Reader, version int) error {
	if proto.FeatureTempTables.In(version) {
		v, err := src.Str()
		if err != nil {
			return errors.Wrap(err, "temp table")
		}
		if v != "" {
			return errors.Errorf("unexpected temp table %q", v)
		}
	}
	// TODO: handle FeatureTempTables & compression

	// if c.compression == proto.CompressionEnabled && opt.Compressible {
	// 	c.reader.EnableCompression()
	// 	defer c.reader.DisableCompression()
	// }
	var block proto.Block
	var result = proto.Results{}
	result2 := result.Auto()

	if err := block.DecodeBlock(src, version, result2); err != nil {
		var badData *compress.CorruptedDataErr
		if errors.As(err, &badData) {
			// Returning wrapped exported error to allow user matching.
			exportedErr := compress.CorruptedDataErr(*badData)
			return errors.Wrap(&exportedErr, "bad block")
		}
		return errors.Wrap(err, "decode block")
	}
	if block.End() {
		return nil
	}

	return nil
}

func handleServerProfile(src *proto.Reader, version int) error {
	var p proto.Profile

	if err := p.DecodeAware(src, version); err != nil {
		return errors.Wrap(err, "decode")
	}
	println("got ", p.Blocks, " blocks and ", p.Rows, " rows")

	return nil
}

func handleServerEndOfStream(src *proto.Reader, version int) error {
	return nil
}

func readServerProtocol(isFirstServerPacket *bool, srcType SrcType, buf []byte, written int64) error {
	if *isFirstServerPacket && srcType == SourceServer {
		n, err := UVarInt(&buf)
		if err != nil {
			return err
		}
		*isFirstServerPacket = false
		expected := (proto.ServerCodeHello)
		code := proto.ServerCode(n)
		if code != expected {
			return fmt.Errorf("got %d instead of %d", code, expected)
		} else {
			println("Server: got ServerCodeHello")
		}
	}
	return nil
}

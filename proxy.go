package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"

	"github.com/ClickHouse/ch-go/proto"
)

var localAddr *string = flag.String("l", "localhost:9001", "local address")
var remoteAddr *string = flag.String("r", "localhost:9000", "remote address")

func main() {
	flag.Parse()
	fmt.Println("custom proxy")
	fmt.Printf("Listening: %v\nProxying: %v\n\n", *localAddr, *remoteAddr)

	listener, err := net.Listen("tcp", *localAddr)
	if err != nil {
		panic(err)
	}
	for {
		clientConn, err := listener.Accept()
		var id = rand.Intn(10_000_000)
		log.Println("======got new connection from client, id: ", id)
		if err != nil {
			log.Println("error accepting connection", err)
			continue
		}
		go func() {
			serverConn, err := net.Dial("tcp", *remoteAddr)
			if err != nil {
				log.Println("error dialing remote addr", err)
				return
			}
			go sniffAndCopyStreamV1(serverConn, clientConn, SourceClient)
			sniffAndCopyStreamV1(clientConn, serverConn, SourceServer)
			serverConn.Close()
			clientConn.Close()
			log.Println("======got end of connection from client, id: ", id)
		}()
	}
}

type SrcType int64

const (
	SourceClient SrcType = 0
	SourceServer         = 1
)

func sniffAndCopyStreamV2(dst io.Writer, src io.Reader, srcType SrcType) (written int64, err error) {
	var buf bytes.Buffer
	tee := io.TeeReader(src, &buf)
	byteReader := proto.NewReader(tee)
	version := 54460
	loppCount := 0
	source := ""
	if srcType == SourceClient {
		source = "ClientV2"
	} else {
		source = "ServerV2"
	}

	for {
		if srcType == SourceClient && loppCount != 1 {
			err = readClientProtocolV2(byteReader, version)
			if err != nil {
				break
			}
		}
		if srcType == SourceClient && loppCount == 1 {
			err = endClientHandshake(byteReader, version)
			if err != nil {
				break
			}
		}
		if srcType == SourceServer {
			err = readServerProtocolV2(byteReader, version)
			if err != nil {
				break
			}
		}

		nr := len(buf.Bytes())
		println(source, " read ", nr, " bytes")

		if nr > 0 {
			nw, ew := dst.Write(buf.Bytes())
			buf.Reset()
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
					break
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		loppCount++
	}

	if err != nil {
		println(source + "stopping stream because of err: ")
		fmt.Println(err)

	} else {
		println(source + ": stopping stream ")
	}

	return written, err
}

// UVarInt reads uint64 from internal reader.
func UVarIntV2(r *io.ByteReader) (uint64, error) {
	n, err := binary.ReadUvarint(*r)
	if err != nil {
		return 0, errors.New("read error")
	}
	return n, nil
}

/*
*
code partially extracted from io.copyBuffer
*/
func sniffAndCopyStreamV1(dst io.Writer, src io.Reader, srcType SrcType) (written int64, err error) {
	var isFirstClientPacket = true
	var isFirstServerPacket = true

	var buf []byte
	size := 32 * 1024
	if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
		if l.N < 1 {
			size = 1
		} else {
			size = int(l.N)
		}
	}
	buf = make([]byte, size)
	for {
		nr, er := src.Read(buf)
		if srcType == SourceClient {
			fmt.Println("clientV1: nr = ", nr)
		}
		if srcType == SourceServer {
			fmt.Println("serverV1: nr = ", nr)
		}
		err := readClientProtocol(&isFirstClientPacket, srcType, buf, written)
		if err != nil {
			return written, err
		}
		err = readServerProtocol(&isFirstServerPacket, srcType, buf, written)
		if err != nil {
			return written, err
		}

		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

// UVarInt reads uint64 from internal reader.
func UVarInt(r *[]byte) (uint64, error) {
	n, size := binary.Uvarint(*r)
	if size <= 0 {
		return 0, errors.New("read error")
	}
	return n, nil
}

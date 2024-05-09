package main

import (
	"bufio"
	"net"
)

func masterHandshake(masterHost, masterPort, port string) net.Conn {
	conn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		panic(err)
	}

	buff := make([]byte, 1024)

	writer := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ping := Value{Typ: "array", Array: []Value{{Typ: "bulk", Bulk: "PING"}}}
	writer.Write(ping.Marshal())
	writer.Flush()

	writer.Read(buff)
	replconf1 := Value{Typ: "array", Array: []Value{
		{Typ: "bulk", Bulk: "REPLCONF"},
		{Typ: "bulk", Bulk: "listening-port"},
		{Typ: "bulk", Bulk: port},
	}}
	writer.Write(replconf1.Marshal())
	writer.Flush()

	writer.Read(buff)
	replconf2 := Value{Typ: "array", Array: []Value{
		{Typ: "bulk", Bulk: "REPLCONF"},
		{Typ: "bulk", Bulk: "capa"},
		{Typ: "bulk", Bulk: "psync2"},
	}}
	writer.Write(replconf2.Marshal())
	writer.Flush()

	writer.Read(buff)
	psync := Value{Typ: "array", Array: []Value{
		{Typ: "bulk", Bulk: "PSYNC"},
		{Typ: "bulk", Bulk: "?"},
		{Typ: "bulk", Bulk: "-1"},
	}}
	writer.Write(psync.Marshal())
	writer.Flush()

	return conn
}

package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

var Config struct {
	host      string
	port      string
	replicaOf string
}

var RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

func handleConnection(conn net.Conn, kv *Kv) {
	defer conn.Close()
	kv.Clients[conn.RemoteAddr().String()] = conn

	for {
		r := NewResp(conn)
		value, err := r.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected: ", conn.RemoteAddr().String())
			} else {
				fmt.Println("ERR IS", err)
			}
			return
		} else {
			fmt.Println("Client connected: ", conn.RemoteAddr().String())
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array, got:", value)
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0, got:" + fmt.Sprint(len(value.Array)))
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			err := writer.Write(Value{Typ: "string", Str: "Invalid command" + command})
			if err != nil {
				fmt.Println("Error writing invalid command message:", err)
				break
			}
			continue
		}

		result := handler(args, kv)
		fmt.Println(kv.Info.Role, "received:", command, args, "result:", result)
		fmt.Println(kv.Info.Role, "DB VALUE:", kv.SETs)
		writer.Write(result)

		if kv.Info.Role == "master" {

			switch command {
			case "SET":
				wg := sync.WaitGroup{}
				for _, conn := range kv.Slaves {
					wg.Add(1)
					go propagateToSlave(conn, value, &wg)
				}
				wg.Wait()

			case "PSYNC":
				decodedStr, _ := base64.StdEncoding.DecodeString(RDB)
				writer.Write(Value{Typ: "bulk", Bulk: string(decodedStr), NoCRLF: true})
				if kv.Info.Role == "master" {

					kv.Slaves = append(kv.Slaves, writer.writer)

				}
			default:
				// do nothing
			}
		}

	}
}

func propagateToSlave(conn io.Writer, value Value, wg *sync.WaitGroup) {
	fmt.Println("replicating command")
	defer wg.Done()
	writer := NewWriter(conn)
	writer.Write(value)
}

func main() {
	flag.StringVar(&Config.port, "port", "6379", "port for the redis server")
	flag.StringVar(&Config.host, "host", "0.0.0.0", "port for the redis server")
	flag.StringVar(&Config.replicaOf, "replicaof", "master", "replicate and give the slave role")
	flag.Parse()

	kv := NewKv()
	kv.Info.Host = Config.host
	kv.Info.Port = Config.port

	if Config.replicaOf == "master" {
		kv.Info.Role = "master"
	} else {
		kv.Info.Role = "slave"
		kv.Info.MasterHost = Config.replicaOf
		kv.Info.MasterPort = flag.Args()[len(flag.Args())-1]
		kv.Info.MasterConn = masterHandshake(kv.Info.MasterHost, kv.Info.MasterPort, kv.Info.Port)
	
		go handleConnection(kv.Info.MasterConn, kv)
	}
	
	// Create a new server
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", kv.Info.Host, kv.Info.Port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Listening on port:", kv.Info.Port)

	defer l.Close()

	for {
		// Listen for connections
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleConnection(conn, kv)
	}
}

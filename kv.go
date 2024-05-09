package main

import (
	"io"
	"net"
	"sync"
)

type Kv struct {
	Info      Info
	SETs      map[string]Value
	SETsMu    sync.RWMutex
	HSETs     map[string]map[string]string
	HSETsMu   sync.RWMutex
	Clients   map[string]net.Conn
	Slaves    []io.Writer
	SlaveMu   sync.RWMutex
}

func NewKv() *Kv {
	return &Kv{
		Info: Info{
			ReplicationID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			ReplicationOffset: 0,
		},

		SETs:    map[string]Value{},
		HSETs:   map[string]map[string]string{},
		Clients: map[string]net.Conn{},
		Slaves:  make([]io.Writer, 0),
	}
}

type Info struct {
	Host                 string
	Port                 string
	Role                 string
	NumCommandsProcessed int
	ReplicationID        string
	ReplicationOffset    int
	MasterHost           string
	MasterPort           string
	MasterConn           net.Conn
}

type Slave struct {
	Host string
	Port string
}

func NewSlave(host, port string) Slave {
	return Slave{Host: host, Port: port}
}

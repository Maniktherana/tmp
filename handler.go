package main

import (
	"strconv"
	"strings"
	"time"
)

var Handlers = map[string]func(
	[]Value,
	*Kv,
) Value{
	"INFO":     info,
	"PING":     ping,
	"ECHO":     echo,
	"REPLCONF": replconf,
	"PSYNC":    psync,
	"SET":      set,
	"GET":      get,
	"HSET":     hset,
	"HGET":     hget,
	"HGETALL":  hgetall,
}

func info(args []Value, kv *Kv) Value {
	info := "role:"
	info += kv.Info.Role
	info += "\n"
	info += "master_replid:"
	info += kv.Info.ReplicationID
	info += "\n"
	info += "master_repl_offset:"
	info += strconv.Itoa(kv.Info.ReplicationOffset)
	return Value{Typ: "bulk", Bulk: info}
}

func ping(args []Value, kv *Kv) Value {
	if len(args) == 0 {
		return Value{Typ: "string", Str: "PONG"}
	}

	return Value{Typ: "string", Str: args[0].Bulk}
}

func echo(args []Value, kv *Kv) Value {
	return Value{Typ: "string", Str: args[0].Bulk}
}

func replconf(args []Value, kv *Kv) Value {
	typ := args[0].Bulk

	if typ == "listening-port" {
		return Value{Typ: "string", Str: "OK"}
	} else if typ == "capa" {
		if args[1].Bulk == "psync2" || args[1].Bulk == "eof" {
			return Value{Typ: "string", Str: "OK"}
		}
	}
	return Value{Typ: "error", Str: "ERR syntax error"}
}

func psync(args []Value, kv *Kv) Value {
	if len(args) != 2 {
		return Value{Typ: "error", Str: "ERR syntax error"}
	}
	first := args[0].Bulk
	second := args[1].Bulk

	if first == "?" && second == "-1" {
		return Value{Typ: "string", Str: "FULLRESYNC " + kv.Info.ReplicationID + " 0"}
	} else {
		return Value{Typ: "error", Str: "ERR syntax error"}
	}
}

func set(args []Value, kv *Kv) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "ERR syntax error"}
	}
	key := args[0].Bulk
	value := args[1].Bulk
	var setter string
	keepttl := false
	var get bool
	var ex, px int
	var val Value

	// Parsing command options
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "NX", "XX":
			setter = strings.ToUpper(args[i].Bulk)
		case "KEEPTTL":
			keepttl = true
		case "GET":
			get = true
		case "EX":
			if keepttl {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				ex, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
		case "PX":
			if keepttl {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				px, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
		default:
			return Value{Typ: "error", Str: "ERR syntax error"}
		}
	}

	// Handling SETTER (XX/NX) options
	switch setter {
	case "NX":
		kv.SETsMu.RLock()
		_, ok := kv.SETs[key]
		kv.SETsMu.RUnlock()
		if ok {
			return Value{Typ: "null"}
		}
	case "XX":
		kv.SETsMu.RLock()
		_, ok := kv.SETs[key]
		kv.SETsMu.RUnlock()
		if !ok {
			return Value{Typ: "null"}
		}
	}

	// Handling expiration
	expiration := time.Now()
	if keepttl {
		kv.SETsMu.RLock()
		v, ok := kv.SETs[key]
		kv.SETsMu.RUnlock()
		if !ok {
			return Value{Typ: "null"}
		}
		expiration = time.Unix(v.Expires, 0)
	} else {
		if ex > 0 {
			expiration = expiration.Add(time.Duration(ex) * time.Second)
		} else if px > 0 {
			expiration = expiration.Add(time.Duration(px) * time.Millisecond)
		}
	}

	// Setting the value
	if expiration.UnixMilli() > time.Now().UnixMilli() {
		val = Value{Typ: "string", Str: value, Expires: expiration.UnixMilli()}
	} else {
		val = Value{Typ: "string", Str: value}
	}
	kv.SETsMu.Lock()
	kv.SETs[key] = val
	kv.SETsMu.Unlock()

	if get {
		return val
	} else {
		return Value{Typ: "string", Str: "OK"}
	}
}

func get(args []Value, kv *Kv) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Bulk

	kv.SETsMu.RLock()
	value, ok := kv.SETs[key]
	kv.SETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	if value.Expires > 0 && value.Expires < time.Now().UnixMilli() {
		kv.SETsMu.Lock()
		delete(kv.SETs, key)
		kv.SETsMu.Unlock()

		return Value{Typ: "null"}
	}

	return value
}

func hset(args []Value, kv *Kv) Value {
	if len(args) != 3 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	kv.HSETsMu.Lock()
	if _, ok := kv.HSETs[hash]; !ok {
		kv.HSETs[hash] = map[string]string{}
	}
	kv.HSETs[hash][key] = value
	kv.HSETsMu.Unlock()

	return Value{Typ: "string", Str: "OK"}
}

func hget(args []Value, kv *Kv) Value {
	if len(args) != 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	kv.HSETsMu.RLock()
	value, ok := kv.HSETs[hash][key]
	kv.HSETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: value}
}

func hgetall(args []Value, kv *Kv) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk

	kv.HSETsMu.RLock()
	value, ok := kv.HSETs[hash]
	kv.HSETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	values := []Value{}
	for key, value := range value {
		values = append(values, Value{Typ: "bulk", Bulk: key})
		values = append(values, Value{Typ: "bulk", Bulk: value})
	}

	return Value{Typ: "bulk", Array: values}
}

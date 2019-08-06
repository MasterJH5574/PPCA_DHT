package main

import (
	"fmt"
	"kademlia"
	"message"
	"net"
	"net/rpc"
	"strconv"
)

type client struct {
	O      *kademlia.Node
	server *rpc.Server
	port   string
}

func NewNode(port int) *client {
	o := new(client)
	o.O = new(kademlia.Node)
	o.port = strconv.Itoa(port)
	o.server = rpc.NewServer()
	err := o.server.Register(o.O)
	if err != nil {
		fmt.Println("Error: Register", err)
		return nil
	}
	o.O.O.Init(o.port)
	return o
}

func (o *client) Run() {
	listen, err := net.Listen("tcp", ":"+o.port)
	if err != nil {
		fmt.Println("Error: Listen error:", err)
		return
	}
	o.O.Listen = listen
	o.O.O.ON = true
	go o.server.Accept(o.O.Listen)
	go o.O.O.ExpireReplicate()
	go o.O.O.Republish()
	go o.O.O.Refresh()
}

func (o *client) Create() {
	message.PrintTime()
	fmt.Println("create: success at", o.O.O.IP)
}

func (o *client) Join(addr string) {
	o.O.O.Join(addr)

	message.PrintTime()
	fmt.Println("join:", o.O.O.IP, "join a ring containing", addr)
}

func (o *client) Quit() {
	if o.O.O.ON == false {
		message.PrintTime()
		fmt.Println("quit directly")
	} else {
		o.O.O.ON = false
		_ = o.O.Listen.Close()
		fmt.Println(o.O.O.IP, "quit")
	}
}

func (o *client) Put(key, value string) {
	success := o.O.O.Publish(key, value, true)
	message.PrintTime()
	if success == false {
		fmt.Println("Put: cannot put", key, value)
	} else {
		fmt.Println("Put: ", key, value)
	}
}

func (o *client) Get(key string) (bool, string) {
	//value, success := o.O.Get(key)
	val, ok := o.O.O.GetValue(key)
	message.PrintTime()
	if ok == false {
		fmt.Println("Get: Not Found: ", key)
		return false, ""
	} else {
		fmt.Println("Get: key =", key, ", val =", val)
		return true, val
	}
}

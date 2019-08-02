package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"test/chord"
)

func NewNode(port int) dhtNode {
	var o client
	o.O = new(chord.RPCNode)
	o.O.O = new(chord.Node)
	o.Port = strconv.Itoa(port)
	o.server = rpc.NewServer()
	err := o.server.Register(o.O)
	if err != nil {
		log.Fatalln("Error: Register", err)
	}
	o.O.O.Init(o.Port)

	var res dhtNode
	res = &o
	return res
}

type client struct {
	O    *chord.RPCNode
	Port string

	wg     *sync.WaitGroup
	server *rpc.Server
	// Listening bool
}

func (o *client) Get(k string) (bool, string) {
	res, success := o.O.O.Get(k)
	return success, res
}

func (o *client) Put(k, v string) bool {
	return o.O.O.Put(k, v)
}

func (o *client) Del(k string) bool {
	return o.O.O.Delete(k)
}

func (o *client) Run() {
	/*err := o.server.Register(o.O)
	if err != nil {
		fmt.Println("Error: rpc.Register error: ", err)
		return
	}*/
	//o.server.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+o.Port)
	if err != nil {
		fmt.Println("Error: Listen error: ", err)
		return
	}
	o.O.Listen = listen
	o.O.O.ON = true
	o.O.O.Init(o.Port)
	go o.server.Accept(o.O.Listen)
}

func (o *client) Create() {
	o.O.O.Create()
	go o.O.O.Stabilize(true)
	go o.O.O.FixFingers()
	go o.O.O.CheckPredecessor()

	fmt.Println("create: success", o.O.O.Addr)
}

func (o *client) Join(addr string) bool {
	res := o.O.O.Join(addr)

	if res == true {
		go o.O.O.Stabilize(true)
		go o.O.O.FixFingers()
		go o.O.O.CheckPredecessor()
		fmt.Println("join:", o.O.O.Addr, "join a ring containing", addr)
	} else {
		fmt.Println("join: join failure", addr)
	}

	return res
}

func (o *client) Quit() {
	if o.O.O.ON == false {
		return
	}
	o.O.O.ON = false
	o.O.O.Quit()
	err := o.O.Listen.Close()
	if err != nil {
		fmt.Println("Error: listen close error: ", err)
	}
}

func (o *client) ForceQuit() {
	o.O.O.ON = false
	err := o.O.Listen.Close()
	if err != nil {
		fmt.Println("Error: listen close error when force quit: ", err)
	}
	fmt.Println("Force quit success")
}

func (o *client) Ping(addr string) bool {
	return o.O.O.Ping(addr)
}

func (o *client) GetAddr() string {
	return o.O.O.Addr
}

func (o *client) Dump() {
	o.O.O.Dump()
}

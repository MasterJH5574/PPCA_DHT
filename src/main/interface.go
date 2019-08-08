package main

import (
	"bufio"
	"dht"
	"fmt"
	"message"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type client struct {
	O    *chord.RPCNode
	Port string
	Name string

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
}

func (o *client) Join(addr string) bool {
	fmt.Printf("You're trying to join a chat room.\n" +
		"Please wait for the agreement of the user of the address.\n")
	res := o.O.O.Join(addr)

	if res == true {
		go o.O.O.Stabilize(true)
		go o.O.O.FixFingers()
		go o.O.O.CheckPredecessor()
		fmt.Printf("You join the chat room successfully! Try chatting with others now!\n")
		time.Sleep(time.Second)
		_ = o.O.O.PrintMessage(chord.StrPair{Str: message.CurrentTime() + " " + o.Name + " joins the chat room.",
			Addr: o.O.O.Addr}, new(int))
	}
	return res
}

func (o *client) Quit() {
	err := o.O.Listen.Close()
	o.O.O.ON = false
	o.O.O.Quit()
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

func (o *client) Say() {
	fmt.Printf(" >>> ")
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}
	text = text[:len(text)-1]
	text = message.CurrentTime() + " " + o.Name + ": " + text
	_ = o.O.O.PrintMessage(chord.StrPair{Str: text, Addr: o.O.O.Addr}, new(int))

	_, totStr := o.Get("chatHistory::totalRecordsPieces")
	tot, _ := strconv.Atoi(totStr)
	o.Put("chatHistory::RecordNo"+strconv.Itoa(tot), text)
	tot++
	o.Put("chatHistory::totalRecordsPieces", strconv.Itoa(tot))
}

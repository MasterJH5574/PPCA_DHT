// command-line user interface

package main

import (
	"bufio"
	chord "dht"
	"fmt"
	"message"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

func getLine() []string {
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return nil
	} else {
		// fmt.Println(text)
		args := strings.Fields(text)
		return args
	}
}

// function Help() shows information
func Help() {
	fmt.Printf("help info")
	// TODO: complete Help()
}

// function Port() set the current port
func Port(newPort string, port *string) {
	portInt, err := strconv.Atoi(newPort)
	if err != nil {
		fmt.Println("Error: ", err)
		message.ShowMoreHelp()
	} else if portInt < 0 || portInt > 65535 {
		fmt.Printf("Error: Invalid port\n")
		message.ShowMoreHelp()
	} else {
		*port = strconv.Itoa(portInt)

		message.PrintTime()
		fmt.Printf("port: set port to %d\n", portInt)
	}
}

// function Create() creates a new chord ring based on the current node
func Create(o *chord.RPCNode, port string, createdOrJoined *bool) {
	err := rpc.Register(*o)
	if err != nil {
		fmt.Println("Error: rpc.Register error: ", err)
		return
	}
	rpc.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error: Listen error: ", err)
		return
	}
	o.Listen = listen
	o.O.Init(port)
	o.O.Create()

	go http.Serve(listen, nil)
	go o.O.Stabilize(true)
	go o.O.FixFingers()
	go o.O.CheckPredecessor()

	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("create: a new ring is created\n")
}

// function Join() let the current node join a chord ring
func Join(o *chord.RPCNode, port, addr string, createdOrJoined *bool) {
	err := rpc.Register(*o)
	if err != nil {
		fmt.Println("Error: rpc.Register error: ", err)
		return
	}
	rpc.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error: Listen error: ", err)
		return
	}
	o.Listen = listen
	o.O.Init(port)
	o.O.Join(addr)

	go http.Serve(listen, nil)
	go o.O.Stabilize(true)
	go o.O.FixFingers()
	go o.O.CheckPredecessor()

	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("join: join a ring containing %s\n", addr)
}

// function Quit()
func Quit(o *chord.RPCNode) {
	o.O.Stabilize(false)
	if o.O.Successor[1].Addr != o.O.Addr {
		o.O.Quit()
	}

	err := o.Listen.Close()
	if err != nil {
		fmt.Println("Error: listen close error: ", err)
	}
	message.PrintTime()
	fmt.Println("quit")
}

func Put(o *chord.RPCNode, key, value string) {
	success := o.O.Put(key, value)
	if success == false {
		fmt.Println("Put: cannot put", key, value)
		return
	}

	message.PrintTime()
	fmt.Println("Put: ", key, value)
}

func PutRandom(o *chord.RPCNode, str string) {
	n, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println("Invalid command: ", err)
		return
	}

	for i := 0; i < n; i++ {
		key, value := randString(32), randString(32)
		Put(o, key, value)
	}

	message.PrintTime()
	fmt.Println("Randomly put finished")
}

func Get(o *chord.RPCNode, key string) {
	value, success := o.O.Get(key)
	if success == false {
		fmt.Println("Get: Not Found: ", key)
		return
	}

	message.PrintTime()
	fmt.Println("Get: the key of ", key, " is ", value)
}

func Delete(o *chord.RPCNode, key string) {
	success := o.O.Delete(key)
	if success != false {
		fmt.Println("Delete: not found: ", key)
		return
	}

	message.PrintTime()
	fmt.Println("Delete ", key)
}

func commandLine() {
	randomInit()

	// create a new node for current server
	var o chord.RPCNode
	o.O = new(chord.Node)

	port := "7722" // abbr of PPCA
	createdOrJoined := false

	for running := true; running == true; {
		args := getLine()
		if len(args) == 0 {
			fmt.Printf("Please enter the command.\n" +
				"With any problem you can enter \"help\" to show help information.\n")
			continue
		}

		switch args[0] {
		case "help":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Help()
			}

		// commands before join or create
		case "port":
			if len(args) != 2 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				Port(args[1], &port)
			}
		case "create":
			if len(args) != 1 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				Create(&o, port, &createdOrJoined)
			}
		case "join":
			if len(args) != 2 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				Join(&o, port, args[1], &createdOrJoined)
			}

		// quitting
		case "quit":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Quit(&o)
				running = false
			}

		// put putRandom get delete
		case "put":
			if len(args) != 3 {
				message.InvalidCommand()
			} else {
				Put(&o, args[1], args[2])
			}
		case "putrandom":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				PutRandom(&o, args[1])
			}
		case "get":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				Get(&o, args[1])
			}
		case "delete":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				Delete(&o, args[1])
			}

		}
	}
}

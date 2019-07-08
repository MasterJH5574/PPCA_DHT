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
		fmt.Printf("Error: ")
		fmt.Println(err)
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
func Create(o *chord.Node, port string, createdOrJoined *bool) {
	err := rpc.Register(*o)
	if err != nil {
		fmt.Printf("Error: rpc.Register error: ")
		fmt.Println(err)
		return
	}
	rpc.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Error: Listen error: ")
		fmt.Println(err)
		return
	}
	o.Init(port)
	o.Create()

	go http.Serve(listen, nil)
	go o.Stabilize()
	go o.FixFingers()
	go o.CheckPredecessor()

	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("create: a new ring is created\n")
}

// function Join() let the current node join a chord ring
func Join(o *chord.Node, port, addr string, createdOrJoined *bool) {
	err := rpc.Register(*o)
	if err != nil {
		fmt.Printf("Error: rpc.Register error: ")
		fmt.Println(err)
		return
	}
	rpc.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Error: Listen error: ")
		fmt.Println(err)
		return
	}
	o.Init(port)
	o.Join(addr)

	go http.Serve(listen, nil)
	go o.Stabilize()
	go o.FixFingers()
	go o.CheckPredecessor()

	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("join: join a ring containing %s\n", addr)
}

// function Quit()
func Quit() {
	// TODO: complete Quit()
}

func commandLine() {
	// create a new node for current server
	var o chord.Node
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
				Quit()
				running = false
			}
		}

	}
}

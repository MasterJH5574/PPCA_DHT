// command-line user interface

package main

import (
	"bufio"
	"fmt"
	"message"
	"os"
	"strconv"
	"strings"
	"sync"
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
func Port(newPort string, port *int) {
	portInt, err := strconv.Atoi(newPort)
	if err != nil {
		fmt.Println("Error: ", err)
		message.ShowMoreHelp()
	} else if portInt < 0 || portInt > 65535 {
		fmt.Printf("Error: Invalid port\n")
		message.ShowMoreHelp()
	} else {
		*port = portInt

		message.PrintTime()
		fmt.Printf("port: set port to %d\n", portInt)
	}
}

// function Create() creates a new chord ring based on the current node
func Create(o *dhtNode, createdOrJoined *bool) {
	(*o).Create()
	*createdOrJoined = true

	//message.PrintTime()
	//fmt.Printf("create: success at %s\n", (*o).GetAddr())
}

// function Join() let the current node join a chord ring
func Join(o *dhtNode, addr string, createdOrJoined *bool) {
	(*o).Join(addr)
	*createdOrJoined = true

	//message.PrintTime()
	//fmt.Printf("join: join a ring containing %s\n", addr)
}

// function Quit()
func Quit(o *dhtNode, createdOrJoined *bool) {
	message.PrintTime()
	if *createdOrJoined == false {
		fmt.Println("quit")
		return
	}

	(*o).Quit()
	fmt.Println("quit")
}

func Put(o *dhtNode, key, value string) {
	message.PrintTime()
	success := (*o).Put(key, value)
	if success == false {
		fmt.Println("Put: cannot put", key, value)
		return
	}

	//message.PrintTime()
	//fmt.Println("Put: ", key, value)
}

func PutRandom(o *dhtNode, str string) {
	n, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println("Invalid command: ", err)
		return
	}

	for i := 0; i < n; i++ {
		message.PrintTime()
		key, value := randString(32), randString(32)
		Put(o, key, value)
	}

	message.PrintTime()
	fmt.Println("Randomly put finished")
}

func Get(o *dhtNode, key string) {
	message.PrintTime()
	//value, success := o.O.Get(key)
	success, _ := (*o).Get(key)
	if success == false {
		//fmt.Println("Get: Not Found: ", key)
		return
	}

	//message.PrintTime()
	//fmt.Println("Get: the key of ", key, " is ", value)
}

func Delete(o *dhtNode, key string) {
	message.PrintTime()
	success := (*o).Del(key)
	if success == false {
		//fmt.Println("Delete: not found: ", key)
		return
	}

	//message.PrintTime()
	//fmt.Println("Delete ", key)
}

func Dump(o *dhtNode) {
	(*o).Dump()
}

func commandLine() {
	randomInit()
	fmt.Println("Hello!")

	// create a new node for current server
	var o *dhtNode
	//o.O = new(chord.Node)

	//port := "7722" // abbr of PPCA
	port := 1000
	createdOrJoined := false

	wg := new(sync.WaitGroup)

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
				o = NewNode(port)
				(*o).Run(wg)
				Create(o, &createdOrJoined)
			}
		case "join":
			if len(args) != 2 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				o = NewNode(port)
				(*o).Run(wg)
				Join(o, args[1], &createdOrJoined)
			}

		// quitting
		case "quit":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Quit(o, &createdOrJoined)
				running = false
			}

		// put putRandom get delete
		case "put":
			if len(args) != 3 {
				message.InvalidCommand()
			} else {
				Put(o, args[1], args[2])
			}
		case "putrandom":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				PutRandom(o, args[1])
			}
		case "get":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				Get(o, args[1])
			}
		case "delete":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				Delete(o, args[1])
			}

		// dump
		case "dump":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Dump(o)
			}

		default:
			fmt.Printf("Please enter the command.\n" +
				"With any problem you can enter \"help\" to show help information.\n")
		}
	}
}

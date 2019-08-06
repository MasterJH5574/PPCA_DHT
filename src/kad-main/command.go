package main

import (
	"bufio"
	"fmt"
	"message"
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

func Create(o *client, createdOrJoined *bool) {
	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("create: success at %s\n", o.O.O.IP)
}

func Join(o *client, addr string, createdOrJoined *bool) {
	o.O.O.Join(addr)
	*createdOrJoined = true

	message.PrintTime()
	fmt.Printf("join: join a ring containing %s\n", addr)
}

func Quit(o *client, createdOrJoined *bool) {
	message.PrintTime()
	if *createdOrJoined == false {
		fmt.Println("quit directly")
		return
	}

	o.O.O.ON = false
	_ = o.O.Listen.Close()
	fmt.Println(o.O.O.IP, "quit")
	*createdOrJoined = false
}

func Put(o *client, key, value string) {
	message.PrintTime()
	success := o.O.O.Publish(key, value)
	if success == false {
		fmt.Println("Put: cannot put", key, value)
	} else {
		fmt.Println("Put: ", key, value)
	}
}

func Get(o *client, key string) (bool, string) {
	message.PrintTime()
	//value, success := o.O.Get(key)
	val, ok := o.O.O.GetValue(key)
	if ok == false {
		fmt.Println("Get: Not Found: ", key)
		return false, ""
	} else {
		fmt.Println("Get: key =", key, ", val =", val)
		return true, val
	}
}

func commandLine() {
	randomInit()
	fmt.Println("Hello!")

	// create a new node for current server
	o := NewNode(1000)
	port := 1000
	createdOrJoined := false

	for running := true; running == true; {
		args := getLine()
		if len(args) == 0 {
			fmt.Printf("Please enter the command.\n" +
				"With any problem you can enter \"help\" to show help information.\n")
			continue
		}

		switch args[0] {
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
				o.Run()
				Create(o, &createdOrJoined)
			}
		case "join":
			if len(args) != 2 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				o = NewNode(port)
				o.Run()
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
		case "get":
			if len(args) != 2 {
				message.InvalidCommand()
			} else {
				Get(o, args[1])
			}

		// dump
		//case "dump":
		//    if len(args) != 1 {
		//        message.InvalidCommand()
		//    } else {
		//        Dump(o)
		//    }

		default:
			fmt.Printf("Please enter the command.\n" +
				"With any problem you can enter \"help\" to show help information.\n")
		}
	}
}

// command-line user interface

package main

import (
	"bufio"
	"fmt"
	"message"
	"os"
	"strconv"
	"strings"
	"time"
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

func getName() string {
	for {
		fmt.Println("Please enter your nick name:")
		fmt.Print(" >>> ")
		Name := getLine()
		if len(Name) != 0 {
			text := Name[0]
			for i := 1; i < len(Name); i++ {
				text = text + " " + Name[i]
			}
			fmt.Printf("Hello, %s!\n", text)
			fmt.Println("You can modify your name by enter \"name\" before creating or joining a chat room.")
			return text
		} else {
			fmt.Println("Invalid name. Please try again.")
		}
	}
}

func getPort() int {
	for {
		fmt.Println("Please enter your port:(an integer between 0 ~ 65535)")
		fmt.Print(" >>> ")
		var port int
		_, _ = fmt.Scanln(&port)
		if port < 0 || port > 65535 {
			fmt.Println("Invalid port. Please try again.")
		} else {
			fmt.Println("You set your port successfully!")
			fmt.Println("You can change your port by enter \"port\" before creating or joining a chat room.")
			return port
		}
	}
}

// function Port() set the current port
func Port(newPort string, port *int) {
	portInt, err := strconv.Atoi(newPort)
	if err != nil || portInt < 0 || portInt > 65535 {
		fmt.Println("Invalid port. Your port should be an integer between 0 ~ 65535.")
	} else {
		*port = portInt
		fmt.Printf("port: set port to %d\n", portInt)
	}
}

// function Create() creates a new chord ring based on the current node
func Create(o *client, createdOrJoined *bool) {
	o.Create()
	*createdOrJoined = true

	fmt.Println(message.CurrentTime(), "You create a chat room successfully! Your address is", o.O.O.Addr)
	o.Put("chatHistory::totalRecordsPieces", "0")
	//message.PrintTime()
	//fmt.Printf("create: success at %s\n", (*o).GetAddr())
}

// function Join() let the current node join a chord ring
func Join(o *client, addr string, createdOrJoined *bool) {
	*createdOrJoined = o.Join(addr)
}

// function Quit()
func Quit(o *client, createdOrJoined *bool) {
	if *createdOrJoined == true {
		o.Quit()
	}
	fmt.Println(message.CurrentTime(), "Quitting")
}

func Say(o *client) {
	o.Say()
}

//func Share(o *client, file string) {
//
//}
//
//func Download(o *client, file string) {
//
//}
//
//func Delete(o *client, file string) {
//
//}

func History(o *client) {
	_, totStr := o.Get("chatHistory::totalRecordsPieces")
	tot, _ := strconv.Atoi(totStr)
	fmt.Println("------- HISTORY -------")
	for i := 0; i < tot; i++ {
		_, totStr = o.Get("chatHistory::RecordNo" + strconv.Itoa(i))
		fmt.Println(totStr)
	}
	fmt.Println(tot, "piece(s) in total")
	fmt.Println("----- HISTORY END -----")
}

//func Put(o *client, key, value string) {
//	//message.PrintTime()
//	success := o.Put(key, value)
//	if success == false {
//		fmt.Println("Put: cannot put", key, value)
//		return
//	}
//
//	//message.PrintTime()
//	//fmt.Println("Put: ", key, value)
//}

//
//func Get(o *dhtNode, key string) {
//	message.PrintTime()
//	//value, success := o.O.Get(key)
//	success, _ := (*o).Get(key)
//	if success == false {
//		//fmt.Println("Get: Not Found: ", key)
//		return
//	}
//
//	//message.PrintTime()
//	//fmt.Println("Get: the key of ", key, " is ", value)
//}
//
//func Delete(o *dhtNode, key string) {
//	message.PrintTime()
//	success := (*o).Del(key)
//	if success == false {
//		//fmt.Println("Delete: not found: ", key)
//		return
//	}
//
//	//message.PrintTime()
//	//fmt.Println("Delete ", key)
//}

func Dump(o *client) {
	o.Dump()
}

func commandLine() {
	randomInit()
	var o client

	name := getName()
	port := getPort()
	message.Before()

	createdOrJoined := false

	for running := true; running; {
		//fmt.Print(" >> ")
		args := getLine()
		if len(args) == 0 {
			fmt.Printf("Please enter your command.\n" +
				"With any problem you can enter \"help\" to show information.\n")
			continue
		}

		switch args[0] {
		case "help":
			if len(args) != 1 {
				message.InvalidCommand()
			} else if createdOrJoined == false {
				message.Before()
			} else {
				message.After()
			}

		// commands before join or create
		case "name":
			if len(args) != 1 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				name = getName()
			}
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
				o = NewNode(port, name)
				o.Run()
				Create(&o, &createdOrJoined)
				if createdOrJoined == true {
					running = false
				}
			}
		case "join":
			if len(args) != 2 {
				message.InvalidCommand()
			} else if createdOrJoined {
				message.HasJoined()
			} else {
				o = NewNode(port, name)
				o.Run()
				Join(&o, args[1], &createdOrJoined)
				if createdOrJoined == true {
					running = false
				}
			}

		// quitting
		case "quit":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Quit(&o, &createdOrJoined)
				running = false
				return
			}

		}
	}

	for {
		o.O.O.PrintLock.Lock()
		//fmt.Print(" >> ")
		args := getLine()
		if len(args) == 0 {
			fmt.Printf("Please enter your command.\n" +
				"With any problem you can enter \"help\" to show information.\n")
			continue
		}

		switch args[0] {
		case "help":
			if len(args) != 1 {
				message.InvalidCommand()
			} else if createdOrJoined == false {
				message.Before()
			} else {
				message.After()
			}
		case "quit":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Quit(&o, &createdOrJoined)
				return
			}

		case "say":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Say(&o)
			}
		case "history":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				History(&o)
			}
		//case "share":
		//	if len(args) != 3 {
		//		message.InvalidCommand()
		//	} else {
		//		Share(&o, args[1])
		//	}
		//case "download":
		//	if len(args) != 2 {
		//		message.InvalidCommand()
		//	} else {
		//		Download(&o, args[1])
		//	}
		//case "delete":
		//	if len(args) != 2 {
		//		message.InvalidCommand()
		//	} else {
		//		Delete(&o, args[1])
		//	}

		// dump
		case "dump":
			if len(args) != 1 {
				message.InvalidCommand()
			} else {
				Dump(&o)
			}

		default:
			fmt.Printf("Please enter the command.\n" +
				"With any problem you can enter \"help\" to show help information.\n")
		}
		o.O.O.PrintLock.Unlock()
		time.Sleep(time.Second / 10)
	}
}

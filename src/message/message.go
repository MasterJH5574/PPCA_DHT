package message

import (
	"fmt"
	"strings"
	"time"
)

func ShowMoreHelp() {
	fmt.Printf("Please enter \"help\" to show more information.\n")
}

// print invalid command info in command-line interface
func InvalidCommand() {
	fmt.Printf("Error: Invalid command.\n")
	ShowMoreHelp()
}

// print joined info in command-line interface
func HasJoined() {
	fmt.Printf("Error: This node has already been in a chord circle.\n" +
		"You cannot enter this command.\n")
	ShowMoreHelp()
}

func PrintTime() {
	t := strings.Fields(time.Now().String())

	fmt.Printf("%s %s ", t[0], t[1][:8])
}

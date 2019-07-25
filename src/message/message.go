package message

import (
	"fmt"
	"strings"
	"time"
)

func ShowMoreHelp() {
	fmt.Println("Please enter \"help\" to show more information.")
}

// print invalid command info in command-line interface
func InvalidCommand() {
	fmt.Println("Invalid command. Please try again.")
	ShowMoreHelp()
}

// print joined info in command-line interface
func HasJoined() {
	fmt.Printf("Error: This node has already been in a chord circle.\n" +
		"You cannot enter this command.\n")
	ShowMoreHelp()
}

func Before() {
	fmt.Printf("What can you do now? Here are 5 options you can choose.\n" +
		" - \"name\":                modify your nick name\n" +
		" - \"port <int>\":          change your port\n" +
		" - \"create\":              create a chat room, you can invite other people to join the room\n" +
		" - \"join <addr>\":         join a chat room through addr\n" +
		" - \"quit\":                exit this program\n")
}

func After() {
	fmt.Printf("What can you do now? Here are 5 options you can choose.\n" +
		" - \"say\":                 say what you want to say to everyone in the chat room\n" +
		" - \"share <filename>\":    share a file to the chat room\n" +
		" - \"download <filename>\": download a file which was shared\n" +
		" - \"delete <filename>\":   delete a file which was shared\n" +
		" - \"quit\":                exit this program\n")
}

func CurrentTime() string {
	t := strings.Fields(time.Now().String())
	//fmt.Printf("%s %s ", t[0], t[1][:8])
	return t[0] + " " + t[1][:8]
}

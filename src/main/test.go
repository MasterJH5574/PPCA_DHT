package main

/*
import (
	chord "dht"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const second = 1000 * time.Millisecond

var MAP map[string]string
var id int
var node [20000]*dhtNode
var PUT int

func KVTest() {
	fmt.Println("Sleep 30 seconds")
	time.Sleep(5 * second)

	// insert
	fmt.Println("Start to test insert")
	for i := 0; i < 300; i++ {
		str := strconv.Itoa(PUT)
		//k, v := randString(10), randString(10)
		//MAP[k] = v
		MAP[str] = str
		p := rand.Int() % id
		//(*node[p]).Put(k, v)
		(*node[p]).Put(str, str)
		PUT++
	}

	// check correctness
	fmt.Println("Start to check correctness")
	cnt := 0
	for k, v := range MAP {
		p := rand.Int() % id
		_, res := (*node[p]).Get(k)
		if res != v {
			log.Fatalln("Get incorrect when get key", k)
		}
		cnt++
		if cnt == 200 {
			break
		}
	}

	// delete
	fmt.Println("Start to test delete")
	cnt = 0
	var str [150]string
	for k := range MAP {
		str[cnt] = k
		cnt++
		if cnt == 150 {
			break
		}
	}
	for _, k := range str {
		(*node[rand.Int()%id]).Del(k)
		delete(MAP, k)
	}

	fmt.Println("Sleep 10 seconds")
	time.Sleep(5 * second)
}

func test() {
	//randomInit()
	rand.Seed(1)
	MAP = make(map[string]string)

	id = 0

	wg := new(sync.WaitGroup)

	node[id] = NewNode(2000)
	(*node[id]).Run(wg)
	(*node[id]).Create()
	id++

	localAddr := chord.GetLocalAddress()

	for t := 0; t < 5; t++ {
		fmt.Println("Start to test join")
		for i := 0; i < 15; i++ {
			node[id] = NewNode(id + 2000)
			(*node[id]).Run(wg)
			(*node[id]).Join(localAddr + ":" + strconv.Itoa(2000+rand.Int()%id))
			id++

			time.Sleep(1 * second)
		}

		fmt.Println("Sleep 5 seconds")
		time.Sleep(5 * second)
		for i := 0; i < id; i++ {
			(*node[i]).Dump()
		}

		KVTest()

		fmt.Println("Start to test quit")
		for i := 5; i >= 1; i-- {
			(*node[id-i]).ForceQuit()
			time.Sleep(2 * second)
		}
		id -= 5

		fmt.Println("Sleep 5 seconds")
		time.Sleep(10 * second)
		for i := 0; i < id; i++ {
			(*node[i]).Dump()
		}

		KVTest()

	}

}
*/

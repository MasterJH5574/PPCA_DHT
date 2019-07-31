package main

type dhtNode interface {
	Get(k string) (bool, string)
	Put(k string, v string) bool
	Del(k string) bool
	Run()
	Create()
	Join(addr string) bool
	Quit()
	ForceQuit()
	Ping(addr string) bool

	GetAddr() string
	Dump()
}

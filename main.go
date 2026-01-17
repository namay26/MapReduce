package main

import (
	server "MapReduce/rpc_server"
	"MapReduce/worker"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

func main() {
	if err := server.Register(); err != nil {
		log.Fatal(err)
	}

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	var wg sync.WaitGroup

	inputs := []string{"foo", "bar", "baz", "foo bar baz"}

	master := worker.NewMaster(&wg, inputs, 4)
	defer master.Cleanup()

	master.Start()
	wg.Wait()

	fmt.Println(master.Outputs)
}

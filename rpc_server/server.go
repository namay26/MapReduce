package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"strings"
)

type Tasks struct{}

type MapArgs struct {
	Key     string
	Content string
}

type MapResults struct {
	Word string
	Amt  string
}

type ReduceArgs struct {
	Word string
	List []MapResults
}

type ReduceResults struct {
	Value string
}

func (t *Tasks) FMap(args *MapArgs, reply *[]MapResults) error {
	if args == nil {
		return errors.New("arguments cannot be nil")
	}

	words := strings.Fields(args.Content)

	for _, k := range words {
		*reply = append(*reply, MapResults{
			Word: strings.ToLower(k),
			Amt:  "1",
		})
	}
	return nil
}

func (t *Tasks) FReduce(args *ReduceArgs, reply *ReduceResults) error {
	if args == nil {
		return errors.New("arguments cannot be nil")
	}
	result := 0
	for _, value := range args.List {
		if value.Word == args.Word {
			result = result + 1
		}
	}

	reply.Value = strconv.Itoa(result)
	return nil
}

func main() {
	task := new(Tasks)
	rpc.Register(task)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Task Server is listening on port 8080...")
	rpc.Accept(listener)
}

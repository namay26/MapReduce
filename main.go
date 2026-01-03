package main

import (
	"fmt"
	"net/rpc"
)

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

func main() {
	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer client.Close()

	MapArgs := MapArgs{
		Key:     "doc-0",
		Content: "foo bar baz foo",
	}

	var mpr []MapResults

	err = client.Call("Tasks.FMap", MapArgs, &mpr)
	if err != nil {
		fmt.Println("Error calling Tasks.FMap:", err)
		return
	}

	var rdr ReduceResults

	RedArgs := ReduceArgs{
		Word: "foo",
		List: mpr,
	}

	err = client.Call("Tasks.FReduce", RedArgs, &rdr)
	if err != nil {
		fmt.Println("Error calling Tasks.FReduce:", err)
		return
	}

	fmt.Println(rdr)
}

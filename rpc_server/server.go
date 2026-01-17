package server

import (
	"MapReduce/types"
	"errors"
	"net/rpc"
	"strings"
)

type Tasks struct{}

func (t *Tasks) FMap(args *types.MapArgs, reply *[]types.MapResults) error {
	if args == nil {
		return errors.New("arguments cannot be nil")
	}

	words := strings.Fields(args.Content)

	for _, k := range words {
		*reply = append(*reply, types.MapResults{
			Word: strings.ToLower(k),
			Amt:  1,
		})
	}
	return nil
}

func (t *Tasks) FReduce(args *types.ReduceArgs, reply *types.ReduceResults) error {
	if args == nil {
		return errors.New("arguments cannot be nil")
	}

	res := make(map[string]int)

	for key := range args.List {
		res[key] = len(args.List[key])
	}

	reply.Value = res
	return nil
}

func Register() error {
	return rpc.Register(new(Tasks))
}

package models

import (
	"fmt"
	"sort"

	"6.824/porcupine"
)

const (
	Get Operation = iota
	Put
	Append
)

type (
	Operation uint8

	KvInput struct {
		Op    Operation
		Key   string
		Value string
	}

	KvOutput struct {
		Value string
	}
)

var KvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(KvInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	Init: func() any {
		// note: we are modeling a single key's value here;
		// we're partitioning by key, so this is okay
		return ""
	},
	Step: func(state, input, output any) (bool, any) {
		inp := input.(KvInput)
		out := output.(KvOutput)
		st := state.(string)
		switch inp.Op {
		case Get:
			return out.Value == st, state
		case Put:
			return true, inp.Value
		case Append:
			return true, (st + inp.Value)
		default:
			panic("Unrecognized operation")
		}
	},
	DescribeOperation: func(input, output any) string {
		inp := input.(KvInput)
		out := output.(KvOutput)
		switch inp.Op {
		case Get:
			return fmt.Sprintf("get('%s') -> '%s'", inp.Key, out.Value)
		case Put:
			return fmt.Sprintf("put('%s', '%s')", inp.Key, inp.Value)
		case Append:
			return fmt.Sprintf("append('%s', '%s')", inp.Key, inp.Value)
		default:
			return "<invalid>"
		}
	},
}

package mr

import (
    "sort"
    "strconv"
    "strings"
    "testing"
)

const _testDataDir = "testdata"

type Output struct {
    Key   string
    Value string
}

func Test_Worker(t *testing.T) {
    tests := []struct {
        name        string
        mapInput    []*MapReply
        reduceInput []*ReduceReply
        mapf        MapFunc
        reducef     ReduceFunc
        want        []Output
    }{
        {
            name: "word count func",
            mapInput: []*MapReply{
                {
                    InFile:        _testDataDir + "/file1.txt",
                    MapTaskID:     0,
                    NumReduceTask: 3,
                },
                {
                    InFile:        _testDataDir + "/file2.txt",
                    MapTaskID:     1,
                    NumReduceTask: 3,
                },
                {
                    InFile:        _testDataDir + "/file3.txt",
                    MapTaskID:     2,
                    NumReduceTask: 3,
                },
                {
                    InFile:        _testDataDir + "/file4.txt",
                    MapTaskID:     3,
                    NumReduceTask: 3,
                },
            },
            reduceInput: []*ReduceReply{
                {
                    ReduceTaskID: 0,
                    NumMapTask:   4,
                },
                {
                    ReduceTaskID: 1,
                    NumMapTask:   4,
                },
                {
                    ReduceTaskID: 2,
                    NumMapTask:   4,
                },
            },
            mapf: func(_, content string) []KeyValue {
                lines := strings.Split(content, "\n")

                var out []KeyValue
                for _, line := range lines {
                    words := strings.Split(line, " ")
                    for _, word := range words {
                        out = append(out, KeyValue{
                            Key:   word,
                            Value: "1",
                        })
                    }
                }

                return out
            },
            reducef: func(_ string, vals []string) string {
                return strconv.Itoa(len(vals))
            },
            want: []Output{
                {
                    Key:   "bla",
                    Value: "3",
                },
                {
                    Key:   "Winners",
                    Value: "2",
                },
                {
                    Key:   "win",
                    Value: "2",
                },
                {
                    Key:   "Count",
                    Value: "3",
                },
                {
                    Key:   "3",
                    Value: "3",
                },
            },
        },
    }

    for _, tc := range tests {
        tc := tc
        t.Run(tc.name, func(t *testing.T) {
            for _, reply := range tc.mapInput {
                runMap(reply, tc.mapf)
            }
            for _, reply := range tc.reduceInput {
                runReduce(reply, tc.reducef)
            }

            numReduceTask := len(tc.reduceInput)
            got := getOutput(numReduceTask)
            want := tc.want
            sort.Slice(want, func(i, j int) bool {
                return want[i].Key < want[j].Key
            })
            sort.Slice(got, func(i, j int) bool {
                return got[i].Key < got[j].Key
            })
            for i := range want {
                if want[i] != got[i] {
                    t.Fatalf("i: %d, want: %v, got: %v\n", i, want[i], got[i])
                }
            }
        })
    }
}

func getOutput(numReduceTask int) []Output {
    var out []Output
    for rt := 0; rt < numReduceTask; rt++ {
        fileName := getReduceOutFile(rt)
        lines := strings.Split(readFile(fileName), "\n")
        for _, line := range lines {
            if strings.Trim(line, "") == "" {
                continue
            }
            vals := strings.SplitN(line, " ", 2)
            out = append(out, Output{
                Key:   vals[0],
                Value: vals[1],
            })
        }
    }

    return out
}

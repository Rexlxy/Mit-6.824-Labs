package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// For each map task, Read data into array
	allKeyValues := []KeyValue{}
	for mapTask := 0; mapTask < nMap; mapTask++ {
		filename := reduceName(jobName, mapTask, reduceTask)
		f, err := os.Open(filename)
		if err != nil {
			fmt.Println("Fail to open file ", filename, err)
			continue
		}

		dec := json.NewDecoder(f)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				fmt.Println(err)
				continue
			}
			allKeyValues = append(allKeyValues, kv)
		}
	}

	// sort by keys
	sort.Sort(ByKey(allKeyValues))

	reduced := []KeyValue{}
	// read by keys, collect values
	// call reduceF && put data into result array

	for i := 0; i < len(allKeyValues); {
		key := allKeyValues[i].Key
		values := []string{allKeyValues[i].Value}
		i++
		for i < len(allKeyValues) && allKeyValues[i].Key == key {
			values = append(values, allKeyValues[i].Value)
			i++
		}

		// reduced KeyValue
		kv := KeyValue{key, reduceF(key, values)}
		reduced = append(reduced, kv)
	}

	// write into output file
	f, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	enc := json.NewEncoder(f)
	for _, kv := range reduced {
		enc.Encode(&kv)
	}
	f.Close()
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

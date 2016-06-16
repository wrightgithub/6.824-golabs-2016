package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"os"
	"bufio"
)

func check_err(e error) {
	if e != nil {
		panic(e)
	}
}

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	dat, err := ioutil.ReadFile(inFile)
	check_err(err)
	keyValues := mapF(inFile, string(dat))
	for _, keyValue := range keyValues {
		reducePartition := int(ihash(keyValue.Key))%nReduce
		reduceFileName := reduceName(jobName, mapTaskNumber, reducePartition)
		reduceFile, err := os.OpenFile(reduceFileName, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0777)
		check_err(err)
		reduceFileWriter := bufio.NewWriter(reduceFile)
		enc := json.NewEncoder(reduceFileWriter)
		err = enc.Encode(&keyValue)
		check_err(err)
		reduceFileWriter.Flush()
		reduceFile.Close()
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

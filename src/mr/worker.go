package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//for{
		reply:=GetWork();
		//输出得到的文件名和任务序号
		fmt.Println("reply.Worknum:", reply.Work.Num)
		fmt.Println("reply.Info:",reply.Work.Info)
		if reply.Work.Info=="wait......" {
			time.Sleep(5*time.Second)
		}
		fmt.Println("当前worker编号:",reply.Work.WorkerNum)
		fmt.Println(reply.Work.Filename)
	//}
	// uncomment to send the Example RPC to the master.
	//CallExample()

}
//R完成任务
func FinishWork(args RpcArgs) {

}


//R获取任务
func GetWork() RpcReply {

	// declare an argument structure.
	//args := ExampleArgs{}
	args := RpcArgs{}

	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	//reply := ExampleReply{}
	reply := RpcReply{}

	// send the RPC request, wait for the reply.
	call("Master.RPC", &args, &reply)

	//返回得到的任务信息
	return reply

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

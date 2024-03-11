package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "strconv"
import "encoding/json"
import "io/ioutil"
import "os"
import "strings"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// 实现 json.Marshaler 接口 ,使得可以将自定义类型变为JSON
func (p KeyValue) MarshalJSON() ([]byte, error) {  
	type Alias KeyValue // 创建别名以避免无限递归  
  
	// 将 KeyValue 转换为 Alias 类型，并使用 json.Marshal 编码为 JSON  
	jsonBytes, err := json.Marshal(Alias(p))  
	if err != nil {  
		return nil, err  
	}  

	return jsonBytes, nil  
}

//定义键值对数组类型
// for sorting by key.
type ByKey []KeyValue

//重写方法以使用排序函数
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for{
		reply:=GetWork();
		//输出得到的文件名和任务序号
		//fmt.Println("reply.Worknum:", reply.Work.Num)
		//fmt.Println("reply.Info:",reply.Work.Info)
		if reply.Work.Info=="wait......" {//TODO改为设置状态码，不然提示信息一变就无法跳过for循环
			time.Sleep(5*time.Second)
			continue
		}
		if(reply.Work.Info=="finish!"){//任务完成，退出！
			break;
		}
		//记录任务类型：map or reduce
		typee:=reply.Work.Typee
		if typee==0{//是map任务
			//记录中间文件数量
			nmidFiles:=reply.Work.NmidFiles
			//记录任务序号
			workNum:=reply.Work.Num
			//fmt.Println("当前worker编号:",reply.Work.WorkerNum)
			//fmt.Println("当前处理的文件名：",reply.Work.Filename)
			filename:=reply.Work.Filename
			//开始读入文件
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			//文件内容放入content中
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			//传入<文件名，文件内容>这个键值对给map函数,kva接收中间键值对数组
			kva := mapf(filename, string(content))
			// 创建一个buffer来写入格式化后的JSON  
			//中间键值对写入文件
			//TODO 直接改map，map每遍历到一个word就输出到对应的文件
			for i:=0;i<len(kva);i++{
				var buffer []byte
				hashcode:=ihash(kva[i].Key)
				target:=hashcode%nmidFiles
				//创建文件，文件命名规范:mr_mid_第几个reduce任务(target)_该map任务的任务序号
				oname := "mr_mid_"+strconv.Itoa(target)+"_"+strconv.Itoa(workNum)+".jsonl"
				//打开文件，若没有就创建
				file, err := os.OpenFile(oname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
				if err != nil {  
					fmt.Println("Error opening/creating file:", oname, err)  
					return  
				}
				//当前遍历到的KeyValue转json
				jsonData, err := json.Marshal(kva[i]) 
				//_, err = file.Write(jsonData)
				//写入buffer,并添加一个换行符号
				buffer = append(buffer, jsonData...)  
				buffer = append(buffer, '\n')
				//err1 := os.WriteFile(file, buffer, 0644)
				_, err = file.Write(buffer)
				if err != nil {  
					fmt.Printf("Error writing to file: %v\n", err)  
					return  
				}  
				file.Close();
			}
			args:=RpcArgs{
				Num:reply.Work.WorkerNum,//worker编号
				WorkNum:workNum,//任务编号
			}
			//告诉msater任务完成
			FinishWork(args)
		}else{//是reduce任务
			//记录任务编号
			workNum:=reply.Work.Num
			//fmt.Println("当前worker编号:",reply.Work.WorkerNum)
			//fmt.Println("当前处理的文件名：",reply.Work.Filename)
			filename:=reply.Work.Filename
			//开始读入文件
			jsonData, err := ioutil.ReadFile(filename+".json")  
			if err != nil {  
				log.Fatalf("Error reading JSON file: %v", err)  
			}
			// 将内容按行分割  
			lines := strings.Split(string(jsonData), "\n")
			//fmt.Println(len(lines))
			// 定义一个Person类型的切片  
			var kvs ByKey  
		
			// 遍历每一行并解析JSON对象，放入kvs切片中
			for _, line := range lines {  
				var kv KeyValue  
				// 跳过空行  
				if line == "" {  
					continue  
				}  
				// 解析JSON对象到kv变量中  
				err := json.Unmarshal([]byte(line), &kv)  
				if err != nil {  
					log.Printf("Error parsing JSON line: %v", err)  
					continue  
				}  
				// 将解析后的对象添加到切片中  
				kvs = append(kvs, kv)
			}

			//对切片排序
			sort.Sort(kvs);
			
			//创建输出文件
			outputName:="mr-out-"+strconv.Itoa(workNum)
			outputFile, _ := os.Create(outputName)
			
			//找出kvs切片中key相同的，并传给reduce记数
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)

				// this is the correct format for each line of Reduce output.
				//按格式写入文件
				fmt.Fprintf(outputFile, "%v %v\n", kvs[i].Key, output)

				i = j
			}
			//构造请求信息
			args:=RpcArgs{
				Num:reply.Work.WorkerNum,//worker编号
				WorkNum:workNum,//任务编号
			}
			//告诉msater任务完成
			FinishWork(args)
		}
	}
	// uncomment to send the Example RPC to the master.
	//CallExample()
}
//完成任务
func FinishWork(args RpcArgs) {
	reply := RpcReply{}
	call("Master.RPCFinish", &args, &reply)
}


//获取任务
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

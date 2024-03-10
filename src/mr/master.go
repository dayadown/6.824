package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// 任务的信息
type Works struct{
	Typee int//任务类型，0为map任务，1为reduce任务
	Filename string//若为map人物需要指明文件名
	Num int//任务编号
	Status int//任务状态，0：未分配，1：已分配未完成，2：已完成
	NmidFiles int//中间文件数量
	Info string//传递的信息
	WorkerNum int//给worker分配的编号
}

//实现接口
func (work Works) Equals(other Comparable) bool{ 
	 // 这里需要进行类型断言来确保other是一个Works类型的实例  
	 otherWorks, ok := other.(Works)  
	 if !ok {  
		 // 如果不是Works类型，则返回false  
		 return false  
	 }  
	 // 比较WorkerNum字段  
	 return work.WorkerNum == otherWorks.WorkerNum 
}



type Master struct {
	// Your definitions here.
	mapworkQueue1 *Queue//未被分配的map任务
	mapworkQueue2 *Queue//已被分配但未完成的map任务
	workerQueue *QueueNorm//被分配任务的worker编号集合
	workerNumStream int//worker编号流，用于递增产生编号

}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RPC(args *RpcArgs, reply *RpcReply) error {
	//有未被分配的map任务
	if m.mapworkQueue1.Size()>0 {
		workTemp,_:= m.mapworkQueue1.Out()
		work, ok := workTemp.(Works)
		if !ok{
			return nil
		}
		//分配worker编号
		work.WorkerNum=m.workerNumStream
		//master需要记住这些正在工作的worker
		m.workerQueue.Add(m.workerNumStream)
		//编号递增
		m.workerNumStream++
		//通过引用返回给调用的worker
		reply.Work=work
		//更改任务状态为1，并放入1任务队列
		work.Status=1
		m.mapworkQueue2.Add(work)
		return nil
	}else if m.mapworkQueue2.Size()>0{//有map任务未完成，通知worker等待
		work:=Works{
			Info:"wait......",
		}
		reply.Work=work
		return nil
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//产生master的方法，全局只运行一次
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	n := len(files)
	m.mapworkQueue1=new(Queue)
	m.mapworkQueue2=new(Queue)
	m.workerQueue=new(QueueNorm)
	m.workerNumStream=0
	for i:=0;i<n;i++{
		w:=Works{
			Typee:0,
			Filename:files[i],
			Num:i,
			Status:0,
			NmidFiles:nReduce,
			Info:"map tasks,do it!",
		}
		m.mapworkQueue1.Add(w)
	}
	m.server()
	return &m
}
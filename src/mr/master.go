package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "strconv"
import "path/filepath"
import "strings"
import "io/ioutil"
import "sync"
import "time"

var mutex sync.Mutex//定义全局锁
var times int = 1

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
	 return work.Num == otherWorks.Num 
}



type Master struct {
	// Your definitions here.
	mapworkQueue1 *Queue//未被分配的map任务
	mapworkQueue2 *Queue//已被分配但未完成的map任务
	workerQueue *QueueNorm//被分配任务的worker编号集合
	reduceworkQueue1 *Queue//未被分配的reduce任务
	reduceworkQueue2 *Queue//已被分配但未完成的reduce任务
	workerNumStream int//worker编号流，用于递增产生编号
	status int//记录整个mapreduce任务的阶段,0:map阶段  1:map后创建reduce任务阶段 2：完成
	NFinalmidFiles int//中间文件数量

}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RPC(args *RpcArgs, reply *RpcReply) error {
	//加锁
	mutex.Lock()
	defer mutex.Unlock()//defer确保锁释放（防止有时候程序运行到一半就异常退出，没有执行到释放锁的地方）
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
		
		if times==0{//有worker挂了
			fmt.Println("有worker挂了....")
			//将mapworkQueue2中的任务重新放回mapworkQueue1
			for i:=0;i<m.mapworkQueue2.Size();i++{
				workredo,_:=m.mapworkQueue2.Out()
				m.workerQueue.Out()
				//类型转换
				workRedo,ok:=workredo.(Works)
				if(ok){
					m.mapworkQueue1.Add(workRedo)
					//删除其产生的中间文件
					// 设置当前目录为搜索起点  
					startDir := "."
					midFile:="_"+strconv.Itoa(workRedo.Num)+".jsonl"
					hasMoreFiles:=true 
					for hasMoreFiles {  
						hasMoreFiles = false  
				
						// 使用filepath.Walk遍历目录  
						err := filepath.Walk(startDir, func(path string, info os.FileInfo, err error) error {  
							if err != nil {  
								fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)  
								return err  
							}  
							if !info.IsDir() { // 忽略目录  
								// 检查文件名是否以"_i.jsonl"结尾  
								if strings.HasSuffix(filepath.Base(path), midFile) {  
									hasMoreFiles = true // 标记还有文件需要处理     
				
									// 删除中间文件  
									err = os.Remove(path)  
									if err != nil {  
										fmt.Printf("Error deleting file %s: %v\n", path, err)  
										return nil  
									}  
								}
							}  
							return nil  
						})  
				
						if err != nil {  
							fmt.Printf("Error walking the path %v: %v\n", startDir, err)  
							break  
						}  
				
						// 如果没有找到任何文件，退出循环  
						if !hasMoreFiles {  
							break  
						}  
					}
				}else{
					log.Fatalf("cannot convert!")
				}
			}

			//重置计数器
			times=1

			work:=Works{
				Info:"wait......",
			}
			reply.Work=work
			return nil
		}
		times--
		//等10s
		time.Sleep(10*time.Second)
		work:=Works{
			Info:"wait......",
		}
		reply.Work=work
		return nil
	}else {
		
		//上一阶段是map阶段，那现在该创建reduce任务了
		if m.status==0{
			//修改状态为创建reduce任务阶段
			m.status=1
			//fmt.Println("所有map任务完成,等待创建reduce任务。。。。")
			work:=Works{
				Info:"wait......",
			}
			reply.Work=work

			//收集map产生的中间文件,并产生终极中间文件
			for i:=0;i<m.NFinalmidFiles;i++ {
				// 设置当前目录为搜索起点  
				startDir := "."  
				
				filename:="mr_mid_"+strconv.Itoa(i)
				newFilename:="mr_mid_final_"+strconv.Itoa(i)
				// 创建或清空新文件，用于写入内容  
				outputFile, err := os.Create(newFilename+".json")  
				if err != nil {  
					panic(err)  
				}  
				defer outputFile.Close()  
			
				// 标志位，表示是否还有文件需要处理  
				hasMoreFiles := true  
			
				for hasMoreFiles {  
					hasMoreFiles = false  
			
					// 使用filepath.Walk遍历目录  
					err := filepath.Walk(startDir, func(path string, info os.FileInfo, err error) error {  
						if err != nil {  
							fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)  
							return err  
						}  
						if !info.IsDir() { // 忽略目录  
							// 检查文件名是否以"mr_mid_i"开头  
							if strings.HasPrefix(filepath.Base(path), filename) {  
								hasMoreFiles = true // 标记还有文件需要处理    
			
								// 读取文件内容  
								data, err := ioutil.ReadFile(path)  
								if err != nil {  
									fmt.Printf("Error reading file %s: %v\n", path, err)  
									return nil  
								}  
			
								// 将内容追加到新文件中  
								_, err = outputFile.Write(data)  
								if err != nil {  
									panic(err) // 写入新文件出错，程序终止  
								}  
			
								// 删除原始文件  
								err = os.Remove(path)  
								if err != nil {  
									fmt.Printf("Error deleting file %s: %v\n", path, err)  
									return nil  
								}  
			
								//fmt.Println("Deleted file:", path)
							}
						}  
						return nil  
					})  
			
					if err != nil {  
						fmt.Printf("Error walking the path %v: %v\n", startDir, err)  
						break  
					}  
			
					// 如果没有找到任何文件，退出循环  
					if !hasMoreFiles {  
						break  
					}  
				}
				//创建reduce任务
				reduceWork:=Works{
					Typee:1,//任务类型，0为map任务，1为reduce任务
					Filename:newFilename,//要处理的文件名
					Num:i,//任务编号
					Status:0,//任务状态，0：未分配，1：已分配未完成，2：已完成
					NmidFiles:m.NFinalmidFiles,//中间文件数量
					Info:"reduce work,do it!",//传递的信息
				}
				m.reduceworkQueue1.Add(reduceWork)  
			}
			return nil
		}else if m.status==1 {//上一阶段为创建reduce任务阶段，现在该分发reduce任务了
			if m.reduceworkQueue1.Size()>0 {
				workTemp,_:= m.reduceworkQueue1.Out()
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
				m.reduceworkQueue2.Add(work)
				return nil
			}else if m.reduceworkQueue2.Size()>0 {//有reduce任务未完成，通知等待
				//每次通知worker等待后都将times-1，减到0就认为有worker挂了没执行完任务，将任务重新扔回queue1
				
				if times==0{//有worker挂了
					fmt.Println("有worker挂了....")
					//将mapworkQueue2中的任务重新放回mapworkQueue1
					for i:=0;i<m.mapworkQueue2.Size();i++{
						workredo,_:=m.mapworkQueue2.Out()
						//类型转换
						workRedo,ok:=workredo.(Works)
						if(ok){
							m.mapworkQueue1.Add(workRedo)
						}else{
							log.Fatalf("cannot convert!")
						}
					}
					//重置计数器
					times=1
					work:=Works{
						Info:"wait......",
					}
					reply.Work=work
					return nil
				}
				times--
				time.Sleep(10*time.Second)
				work:=Works{
					Info:"wait......",
				}
				reply.Work=work
				return nil
			}else{//reduce任务队列也没了，分发出去的reduce也做完了，改状态为完成
				m.status=2
				work:=Works{
					Info:"finish!",
				}
				reply.Work=work
				return nil
			}
		}else{//完成
			work:=Works{
				Info:"finish!",
			}
			reply.Work=work
			return nil
		}
	}
	return nil
}

//接受任务完成的请求
func (m *Master) RPCFinish(args *RpcArgs, reply *RpcReply) error {
	//加锁
	mutex.Lock()
	defer mutex.Unlock()
	//完成任务的worker编号
	num:=args.Num
	//完成的任务编号
	workNum:=args.WorkNum
	work:=Works{
		Num:workNum,
	}
	if m.status==0 {
		//删除队列中的worker和任务
		//fmt.Println("删除队列中的worker和任务")
		m.mapworkQueue2.Delete(m.mapworkQueue2.FindIndex(work));
		m.workerQueue.Delete(m.workerQueue.FindIndexNormal(num));
	}else if m.status==1 {
		m.reduceworkQueue2.Delete(m.reduceworkQueue2.FindIndex(work));
		m.workerQueue.Delete(m.workerQueue.FindIndexNormal(num));
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
	if m.status==2{
		ret=true
	}


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
	m.reduceworkQueue1=new(Queue)
	m.reduceworkQueue2=new(Queue)
	m.workerNumStream=0
	m.status=0
	m.NFinalmidFiles=nReduce
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
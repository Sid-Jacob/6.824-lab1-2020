package mr

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// for sorting by key.
type Alpha []string

// for sorting by key.
func (a Alpha) Len() int      { return len(a) }
func (a Alpha) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Alpha) Less(i, j int) bool {
	ret := strings.Compare(a[i], a[j])
	if ret < 0 {
		return true
	} else {
		return false
	}
}

type Master struct {
	// Your definitions here.
	// WorkerID int // 当前worker最大编号
	// WorkerState []int // 0-idle 1-in-progress(map/reduce) 2-failed

	filenames []string       //需要处理的files，默认放在当前目录
	Allocated map[string]int //文件是否已被分配给worker，美10s自动清零！！！注意map、reduce可以重用
	MapState  map[string]int //文件map状态，0-not_fin 1-done
	Mapped    int            // 0-not_fin 1-all_done

	nReduce     int // 输入的参数nReduce（输入的文件会被划分成几个task来处理）
	AllocatedR  []int
	ReduceState []int
	Reduced     int // 0-not_fin 1-all_done
	// inFilePath []string // intermediate file store path
	// inFileSize []int    // intermediate file size
	Mut sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if m.Reduced == 1 {
		// m.unite()
		fmt.Println("ALL DONE")
		ret = true
	}

	return ret
}

// obsolete
func (m *Master) unite() {
	fmt.Println("uniting...")
	var filenames []string
	var pattern string
	// 找出所有给序号为Y的reduce任务：mr-X-Y
	pattern = "mr-out-([0-9]*)"
	curdir, _ := os.Getwd()
	filenames, _ = GetFileName(curdir, pattern)

	fmt.Println(filenames)

	// step2 unite the files & sort
	oname := "mr-out-0"
	// oname := "ret"
	_dir := oname
	var ofile *os.File

	exist, err := PathExists(_dir)
	if err != nil {
		// fmt.Printf("get dir error![%v]\n", err)
		return
	}
	var str []string
	if exist {
		ofile, _ = os.OpenFile(oname, os.O_RDWR, 0666)
		// ofile, _ = os.OpenFile(oname, os.O_WRONLY|os.O_APPEND, 0666)
	} else {
		ofile, _ = os.Create(oname)
		// fmt.Println("mr-out-o not found")
		// ofile, _ = os.Create(oname)
	}

	for o, i := range filenames {
		if o != -1 {
			from, _ := os.Open(i)
			defer from.Close()
			br := bufio.NewReader(from)
			for {
				content, _, c := br.ReadLine()
				if c == io.EOF {
					break
				}
				str = append(str, string(content))
			}
			// content, _ := ioutil.ReadAll(from)
			// str = append(str, string(content))
			// fmt.Fprintln(ofile, str)
		}
	}

	sort.Sort(Alpha(str))
	tmp := ""
	for _, i := range str {
		tmp = tmp + i + "\n"
	}

	fmt.Fprintf(ofile, "%s", tmp)

	ofile.Close()
	return
}

func (m *Master) reallocate() {

	for {
		m.Mut.Lock()

		if m.Reduced == 1 {
			break
		}
		m.Mut.Unlock()
		time.Sleep(2 * time.Second)
		m.Mut.Lock()
		for _, j := range m.filenames {
			if m.Allocated[j] == 1 {
				m.Allocated[j] = 0
			}
			if m.Allocated[j] == 2 {
				fmt.Println("map", j, "done")
			}
		}
		m.Mut.Unlock()
		m.Mut.Lock()
		for i := 0; i < m.nReduce; i++ {
			if m.AllocatedR[i] == 1 {
				m.AllocatedR[i] = 0
			}
			if m.AllocatedR[i] == 2 {
				fmt.Println("reduce", i, "done")
			}
		}
		m.Mut.Unlock()

	}
}

func (m *Master) GiveTask(status *Status, task *Task) error {
	// task := Task{}
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if m.Mapped == 0 {
		task.State = 0
		flag := 0
		for i, j := range m.filenames {
			if m.Allocated[j] == 0 {
				flag = 1
				task.XY = i
				task.NReduce = m.nReduce
				task.Filename = j
				m.Allocated[j] = 1
				break
			}
		}
		if flag == 0 {
			task.State = 3 //idle
		}
	} else if m.Reduced == 0 {
		task.State = 1
		task.XY = -1
		flag := 0
		for i := 0; i < m.nReduce; i++ {
			if m.AllocatedR[i] == 0 {
				flag = 1
				task.XY = i
				m.AllocatedR[i] = 1
				break
				// task.nReduce = m.nReduce
				// task.filename = ""
			}
		}
		if flag == 0 {
			task.State = 3 //idle
		}
	} else {
		task.State = 2
	}
	return nil
}

func (m *Master) DoneTask(task *Task, status *Status) error {

	status.OK = true
	switch task.State {
	case 0:
		m.Mut.Lock()
		m.MapState[task.Filename] = 1
		m.Allocated[task.Filename] = 2 //map阶段不需要10s再分配！！！
		m.Mapped = 1
		for _, i := range m.MapState {
			if i == 0 {
				m.Mapped = 0
			}
		}
		m.Mut.Unlock()
		// if m.Mapped == 1 {
		// 	fmt.Println("MAPPED DONE!!")
		// }
	case 1:
		m.Mut.Lock()
		m.ReduceState[task.XY] = 1
		m.AllocatedR[task.XY] = 2 //reduce阶段不需要10s再分配！！！
		m.Reduced = 1
		for _, i := range m.ReduceState {
			if i == 0 {
				// fmt.Println(o, "not finished")
				m.Reduced = 0
			}
		}
		m.Mut.Unlock()
		// if m.Reduced == 1 {
		// 	fmt.Println("REDUCED DONE!!")
		// }
	}
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// m.filenames = append(m.filenames, files...)
	m.filenames = files
	m.nReduce = nReduce
	m.Mapped = 0
	m.Reduced = 0
	m.Allocated = make(map[string]int)
	m.MapState = make(map[string]int)
	m.ReduceState = make([]int, nReduce, nReduce)
	m.AllocatedR = make([]int, nReduce, nReduce)
	for _, name := range m.filenames {
		m.MapState[name] = 0
		m.Allocated[name] = 0
	}
	// for i := 0; i < m.nReduce; i++ {
	// 	m.ReduceState[i] = 0
	// }

	fmt.Println("nReduce = ", m.nReduce)

	go m.reallocate()

	m.server()

	return &m
}

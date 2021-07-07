package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"regexp"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type args struct {
}

type reply struct {
}

type WorkerTask struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	WorkerID int // identity of assigned Worker
	State    int // default:idle 0:map 1:reduce 2:stop

	nReduce  int
	filename string
	XY       int //taskID:mapIP/reduceID
}

// return filename, state
func (w *WorkerTask) AskTask() {
	// fmt.Println("AskTask...")
	asktask := Status{}
	asktask.OK = true
	task := Task{}

	// call("Master.Example", &asktask, &task)
	call("Master.GiveTask", &asktask, &task)
	if task.State != 2 {
		w.XY = task.XY
		w.filename = task.Filename
		w.State = task.State
		w.nReduce = task.NReduce
	}
}

// do map task
func (w *WorkerTask) Map(X, nReduce int, filename string) {
	// fmt.Println("Map... %s", filename)
	intermediate := []KeyValue{}
	// step1 read file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("map:cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// step2 do map
	kva := w.mapf(filename, string(content))
	intermediate = kva
	// intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// step3 write file
	// X-map task seq num, Y-reduce seq num

	for i := 0; i < len(intermediate); i++ {
		Y := ihash(intermediate[i].Key) % w.nReduce
		oname := "mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(Y)

		_dir := oname
		exist, err := PathExists(_dir)
		if err != nil {
			// fmt.Printf("get dir error![%v]\n", err)
			return
		}
		var ofile *os.File
		if exist {
			ofile, _ = os.OpenFile(oname, os.O_WRONLY|os.O_APPEND, 0666)
		} else {
			ofile, _ = os.Create(oname)
		}

		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
		encoder := json.NewEncoder(ofile)
		kv := intermediate[i]

		encoder.Encode(&kv)

		ofile.Close()
	}
	return
}

// do reduce task
func (w *WorkerTask) Reduce(Y int) {
	// fmt.Printf("Reduce... id = %d \n", Y)
	// step1 look for files
	var filenames []string
	var pattern string
	// 找出所有给序号为Y的reduce任务：mr-X-Y
	pattern = fmt.Sprintf("mr-([0-9]*)-%d", Y)
	curdir, _ := os.Getwd()
	filenames, _ = GetFileName(curdir, pattern)

	// fmt.Println(Y)

	// step2 read file & recover intermediate
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		// intermediate = append(intermediate, kva...)
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	// if Y == 4 {
	// 	fmt.Println(intermediate)
	// }

	// do reduce & write file
	// oname := "mr-out-0"
	// _dir := oname
	// exist, err := PathExists(_dir)
	// if err != nil {
	// 	// fmt.Printf("get dir error![%v]\n", err)
	// 	return
	// }
	// var ofile *os.File
	// if exist {
	// 	ofile, _ = os.OpenFile(oname, os.O_WRONLY|os.O_APPEND, 0666)
	// } else {
	// 	ofile, _ = os.Create(oname)
	// }
	oname := "mr-out-" + strconv.Itoa(Y)
	ofile, _ := os.Create(oname)

	var tmp string
	// fmt.Println("len(intermediate) = ", len(intermediate))
	for i := 0; i < len(intermediate); {
		// if i > len(intermediate)-100 {
		// 	fmt.Println("i = ", i)
		// }
		//
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		tmp = tmp + fmt.Sprintf("%v %v\n", intermediate[i].Key, output)

		i = j
		if i >= len(intermediate) {
			// fmt.Println("break..")
			break
		}
	}
	// fmt.Println(tmp)
	fmt.Fprintf(ofile, tmp)
	ofile.Close()
	return
}

// notify master Worker has done the job
func (w *WorkerTask) DoneTask(state int, filename string, XY int) {
	// fmt.Printf("DoneTask... %d \n", state)
	w.XY = -1
	w.filename = ""
	w.State = -1
	w.nReduce = -1

	task := Task{}
	task.State = state
	task.XY = XY
	task.Filename = filename

	status := Status{}

	call("Master.DoneTask", &task, &status)

}

// shut Worker down (all the tasks has been done)
// func (w *WorkerTask) stop(state, XY int) {
// fmt.Println("shutting down...")
// 	exit()
// }

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

	// Your Worker implementation here.
	w := new(WorkerTask)
	w.mapf = mapf
	w.reducef = reducef

	// w.server()

	for {
		w.AskTask()
		switch w.State {
		case 0:
			// defer w.DoneTask(0, w.filename, w.XY)
			// 防止所有文件都正在加工，此时也会返回空filename，并行化测试要点
			if w.filename != "" {
				w.Map(w.XY, w.nReduce, w.filename)
				w.DoneTask(0, w.filename, w.XY)
			}

		case 1:
			// defer w.DoneTask(1, "", w.XY)
			if w.XY != -1 {
				w.Reduce(w.XY)
				w.DoneTask(1, "", w.XY)
			}
		case 2:
			// w.stop()
			// return
		default:
			time.Sleep(10 * time.Millisecond)
			// continue
		}
		// time.Sleep(200 * time.Millisecond)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// func (w *WorkerTask) server() {
// 	rpc.Register(w)
// 	rpc.HandleHTTP()
// 	//l, e := net.Listen("tcp", ":1234")
// 	sockname := masterSock()
// 	os.Remove(sockname)
// 	l, e := net.Listen("unix", sockname)
// 	if e != nil {
// 		log.Fatal("listen error:", e)
// 	}
// 	go http.Serve(l, nil)
// }

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

//判断文件夹是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 返回filename目录下和pattern匹配的文件名
func GetFileName(filename, pattern string) ([]string, error) {
	var filenames []string
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	fi, err := file.Stat()
	if err != nil {
		fmt.Println(err)
	}
	if !fi.IsDir() {
		fmt.Println(filename, " is not a dir")
	}
	reg, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println(err)
	}
	// 遍历目录
	filepath.Walk(filename,
		func(path string, f os.FileInfo, err error) error {
			if err != nil {
				fmt.Println(err)
				return err
			}
			if f.IsDir() {
				return nil
			}
			// 匹配目录
			matched := reg.MatchString(f.Name())
			if matched {
				// 闭包
				filenames = append(filenames, f.Name())
				// fmt.Println(path, f.Name())
			}
			return nil
		})
	return filenames, err
}

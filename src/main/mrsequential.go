package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue//set up a type named ByKey, which type is a slice, and the element is KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }



func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}//make sure the comment contains 3 args


	//mapf 和 reducef 函数加载：
// loadPlugin(os.Args[1]) 函数加载了一个名为 xxx.so 的共享库，
//并返回两个函数 mapf 和 reducef。这些函数用于执行 Map 和 Reduce 阶段的操作，通常来自用户编写的插件。
// 循环遍历输入文件：
// 接下来的代码使用一个循环遍历命令行参数中的输入文件。这个循环从 os.Args[2:] 开始，
//因为前两个参数通常是程序名称和插件名称。
// 对于每个输入文件，它会打开文件，读取文件内容，然后调用 mapf 函数将文件名和文件内容传递给 Map 函数。
//Map 函数的输出（键值对）将被累积到 intermediate 切片中。
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
		//... 是一个扩展操作符（variadic operator）。 扩展操作符的作用是将切片 kva 中的元素逐个展开，
		//作为参数传递给 append 函数。
		//这样可以将 kva 中的元素一个一个地追加到 intermediate 中，而不是将整个 kva 切片作为一个元素追加。
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//这段代码是一个函数 loadPlugin，它用于动态加载一个插件文件（通常是一个共享库文件），
//并从该插件文件中获取两个特定的函数：Map 和 Reduce。
//这些函数通常用于 MapReduce 框架中的 Map 和 Reduce 阶段的自定义操作。
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

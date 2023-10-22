# MIT6.824

MIT6.824(现6.5840)分布式系统lab1-lab4,渐进式实现一个KV分布式系统。

课程主页: <https://pdos.csail.mit.edu/6.824/labs/lab-mr.html>

前置要求：Go语言基础，重点掌握管道相关知识；MapReduce论文

## Lab1 MapReduce

Map是接收一组键值对，并且通过特定方式对其排序（在Lab1提到的word count例子中，键是词，值是词出现的次数，排序之后同一个词会集中出现）。
Reduce则合并相同键的值，以word count为例，Reduce是将相同单词的值（即出现的次数）相加。
![Alt text](image.png)

工作流程图里说得很清楚：

- 系统会启动一个或多个Master，需要执行任务的机器启动Worker来处理任务。Master主要职责是分配任务给Worker，Master可以随机选择空闲的Worker来分配任务，或者Worker主动向Master请求任务；
- 获得map任务的Worker，读取数据分片，生成一组key/value键值对的中间值集合，并将数据写到本地文件，这里每个map任务数据分为R份(Master创建reduce任务的数量)，通过用户定义的分区函数(如hash(key) mod R)决定将key存在哪个文件；
- 获得reduce任务的Worker，通过远程调用请求数据，数据加载完毕后，对数据进行排序，之后遍历数据，将相同key的数据进行合并，最终输出结果；
- 当所有的map和reduce任务完成了，整个MapReduce程序就处理完毕了，Worker得到处理后的数据，通常会保存在不同的小文件中，合并这些文件之后就是最重要的结果。

——转自知乎划拳小匠，网站：<https://zhuanlan.zhihu.com/p/187507617>

Tips: Go语言的通道的用法

1. **通道的创建：**
   你可以使用内置的 `make` 函数来创建一个通道。通道有两种类型：带缓冲和不带缓冲。带缓冲的通道可以容纳一定数量的数据项，而不带缓冲的通道在发送和接收数据时需要 Goroutines 同时准备好。

   ```go
   // 创建一个不带缓冲的通道
   ch := make(chan int)
   
   // 创建一个带缓冲的通道，容量为 10
   ch := make(chan int, 10)
   ```

2. **通道的发送和接收：**
   使用通道的 `<-` 操作符进行数据的发送和接收。发送操作将数据发送到通道，接收操作从通道中接收数据。

   ```go
   // 发送数据到通道
   ch <- 42
   
   // 从通道中接收数据
   data := <-ch
   ```

3. **通道的关闭：**
   通道可以被关闭，以表示没有更多的数据将被发送到通道。通常在发送者知道不再需要发送数据时关闭通道，接收者可以通过检查通道是否关闭来判断是否还有数据。

   ```go
   close(ch)
   ```

成功输出中间文件：
   ![Alt text](image-1.png)
# UIUC2014-Mini-Program

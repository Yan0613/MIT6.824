# MIT6.824

MIT6.824(现6.5840)分布式系统lab1-lab4,渐进式实现一个KV分布式系统。

## 课程主页: <https://pdos.csail.mit.edu/6.824/labs/lab-mr.html>

## 个人课程理解

### Lab1 MapReduce

![Alt text](image.png)

- 系统会启动一个或多个Master，需要执行任务的机器启动Worker来处理任务。Master主要职责是分配任务给Worker，Master可以随机选择空闲的Worker来分配任务，或者Worker主动向Master请求任务；
- 获得map任务的Worker，读取数据分片，生成一组key/value键值对的中间值集合，并将数据写到本地文件，这里每个map任务数据分为R份(Master创建reduce任务的数量)，通过用户定义的分区函数(如hash(key) mod R)决定将key存在哪个文件；
- 获得reduce任务的Worker，通过远程调用请求数据，数据加载完毕后，对数据进行排序，之后遍历数据，将相同key的数据进行合并，最终输出结果；
- 当所有的map和reduce任务完成了，整个MapReduce程序就处理完毕了，Worker得到处理后的数据，通常会保存在不同的小文件中，合并这些文件之后就是最重要的结果。——转自知乎划拳小匠，网站：<https://zhuanlan.zhihu.com/p/187507617>

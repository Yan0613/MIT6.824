//前置知识
//RPC（Remote Procedure Call）是一种计算机通信协议，用于实现分布式系统中的远程通信。
// 它允许一个计算机程序调用另一个地址空间（通常是在不同的机器上运行）的过程或函数，
// 就像调用本地函数一样，而不需要显式地处理底层网络通信细节。
// RPC的主要目标是简化分布式应用程序的编程，
// 使远程过程调用看起来就像本地过程调用一样。

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

//coordinatorSock() 函数：这个函数用于生成一个唯一的 UNIX 域套接字名称，用于协调器（coordinator）。
// UNIX 域套接字是一种在同一台机器上的不同进程之间进行本地通信的机制。
// 这个函数通过在文件路径中添加用户ID（通过 os.Getuid() 获取）来生成唯一的套接字名称。
// 这个函数的返回值是一个字符串，表示套接字的路径。

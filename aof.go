package main

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"syscall"
// 	"time"

// 	"github.com/iceber/iouring-go"
// )

// // var aofWriter *bufio.Writer
// // var aofFile *os.File
// // var aofFd int
// // var aofMu sync.RWMutex
// // var aofChan = make(chan []string, 20000) // 缓冲通道，防止阻塞

// // 初始化AOF
// // func openAOF(path string) error {
// // 	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// // 	if err != nil {
// // 		return err
// // 	}
// // 	aofFd = int(f.Fd())
// // 	aofFile = f
// // 	//aofWriter = bufio.NewWriter(aofFile)
// // 	return nil
// // }

// // // 追加命令到channel
// // func appendAOF(cmd []string) {
// // 	if aofWriter == nil {
// // 		return
// // 	}
// // 	select {
// // 	case aofChan <- cmd: // 非阻塞发送
// // 	default:
// // 		log.Println("aofChan is full, command discarded")
// // 	}
// // }

// // // 加载时启用旧AOF
// // func loadAOF(path string) error {
// // 	f, err := os.Open(path)
// // 	if os.IsNotExist(err) {
// // 		return nil
// // 	}
// // 	if err != nil {
// // 		return err
// // 	}
// // 	defer f.Close()

// // 	data, err := io.ReadAll(f)
// // 	if err != nil {
// // 		return err
// // 	}
// // 	if len(data) == 0 {
// // 		return nil
// // 	}

// // 	for len(data) > 0 {
// // 		val, remain, err := resp.ParseRESP(data)
// // 		if err != nil {
// // 			return err
// // 		}
// // 		if val == nil {
// // 			break
// // 		}
// // 		HandleCommand(val)
// // 		data = remain
// // 	}
// // 	return nil
// // }

// // func bgReWriteAOF() {
// // 	pid, _, _ := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
// // 	if pid == 0 {
// // 		//子进程,遍历写temp-aof.aod
// // 		temp, _ := os.Create("temp-aof.aof")
// // 		w := bufio.NewWriter(temp)
// // 		Store.SegScan(func(cmd []string) {
// // 			arr := make(resp.Array, len(cmd))
// // 			for i, c := range cmd {
// // 				arr[i] = resp.BulkString(c)
// // 			}
// // 			w.Write(arr.ToBytes())
// // 		})
// // 		w.Flush()
// // 		temp.Close()
// // 		os.Exit(0)
// // 	}
// // 	//父进程,后台wait+rename
// // 	go func() {
// // 		syscall.Wait4(int(pid), nil, 0, nil)
// // 		os.Rename("temp-aof.aof", "appendonly.aof")

// // 		//aofMu.Lock()
// // 		aofWriter.Flush()
// // 		aofFile.Close()
// // 		aofFile, _ = os.OpenFile("appendonly.aof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// // 		aofWriter = bufio.NewWriter(aofFile)
// // 		//aofMu.Unlock()

// // 	}()
// // }

// // // 启动AOF异步写入协程
// // func startAsyncAOF() {
// // 	go func() {
// // 		ticker := time.NewTicker(100 * time.Millisecond)
// // 		defer ticker.Stop()

// // 		batch := make([][]string, 0, 500)

// // 		for {
// // 			select {
// // 			case cmd := <-aofChan:
// // 				batch = append(batch, cmd)
// // 				if len(batch) >= 500 {
// // 					flushAOFBatch(batch)
// // 					batch = batch[:0]
// // 				}
// // 			case <-ticker.C:
// // 				if len(batch) > 0 {
// // 					flushAOFBatch(batch)
// // 					batch = batch[:0]
// // 				}
// // 			}
// // 		}
// // 	}()
// // }

// //	func flushAOFBatch(batch [][]string) {
// //		go func() {
// //			// 非阻塞刷盘：即使刷盘慢，也不影响主请求
// //			tmpBuf := bytes.NewBuffer(nil)
// //			for _, cmd := range batch {
// //				arr := make(resp.Array, len(cmd))
// //				for i, c := range cmd {
// //					arr[i] = resp.BulkString(c)
// //				}
// //				tmpBuf.Write(arr.ToBytes())
// //			}
// //			aofMu.Lock()
// //			defer aofMu.Unlock()
// //			if _, err := aofWriter.Write(tmpBuf.Bytes()); err != nil {
// //				log.Println("flushAOFBatch write error:", err)
// //			}
// //			if err := aofWriter.Flush(); err != nil {
// //				log.Println("flushAOFBatch Flush error:", err)
// //			}
// //		}()
// //	}
// var aofFd int
// var ring *iouring.IOURing

// func initAOFIOUring(path string) error {
// 	ring, err := iouring.New(1024)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer ring.Close()

// 	aofFile, err := os.OpenFile("appendonly.aof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// 	if err != nil {
// 		log.Fatalf("openAOF:%v", err)
// 		return err
// 	}
// 	aofFd = int(aofFile.Fd())

// 	go handleCompletionEvents()

// 	return nil
// }

// func handleCompletionEvents() {
// 	resultCh := make(chan iouring.Result, 128)

// 	for {
// 		// 提交空请求触发完成事件检查
// 		req := iouring.PrepWrite(aofFd, []byte{})
// 		_, _ = ring.SubmitRequest(req, resultCh)

// 		// 等待完成事件（超时时间可调整）
// 		select {
// 		case res := <-resultCh:
// 			if res.Err != nil {
// 				log.Printf("Completion error: %v", res.Err)
// 			}
// 		case <-time.After(100 * time.Millisecond):
// 			// 定期检查避免阻塞
// 		}
// 	}
// }

// func submitAOFWrite(data []byte) error {
// 	// 1. 构造写请求
// 	req, err := iouring.PrepWrite(aofFd, data)
// 	if err != nil {
// 		return fmt.Errorf("create write request failed: %v", err)
// 	}

// 	// 2. 创建结果通道
// 	resultCh := make(chan iouring.Result, 1)

// 	// 3. 提交请求（需传入结果通道）
// 	_, err = ring.SubmitRequest(req, resultCh)
// 	if err != nil {
// 		return fmt.Errorf("submit request failed: %v", err)
// 	}

// 	return nil
// }

// func waitAOFWriteCompletion() error {
// 	cqes, err := ring.wait()
// 	if err != nil {
// 		return err
// 	}

// 	for _, cqe := range cqes {
// 		if cqe.Res < 0 {
// 			return syscall.Errno(-cqe.Res)
// 		}
// 	}
// 	return nil
// }

// func cleanup() {
// 	ring.Close()
// }

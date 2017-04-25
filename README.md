# mux
a simple multiplexer

## Usage 

从 net.Conn 上创建新的Mux  
```
conn, err := net.Dial("tcp4", "...")
if err != nil {
    log.Fatal(err)
}

mx, err := mux.NewMux(conn)
if err != nil {
    log.Fatal(err)
}
```

在mux上调用Dial新建连接  
```
var conn net.Conn
var err error 
conn, err = mx.Dial()
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
io.WriteString(conn, "hello world!")
```

在mux上调用Accept获取连接
```
conn, err := mx.Accept()
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
buf := make([]byte, 512)
n, err := conn.Read(buf)
if err != nil {
    log.Fatal(err)
}
log.Println(string(buf[:n]))
```

Accept/Dial函数在多路复用流两边都可以调用  
返回的conn符合net.Conn接口,但是请不要对其进行并发读或并发写,应该保证同一时刻只有一个goroutine会进行读/写  
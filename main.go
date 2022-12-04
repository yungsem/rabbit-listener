package main

import (
	"flag"
	"github.com/yungsem/gox/logx"
	"github.com/yungsem/rabbit-listener/handler"
	"github.com/yungsem/rabbitx"
)

var (
	username = flag.String("username", "admin", "username of rabbitmq server")
	password = flag.String("password", "admin123456", "password of rabbitmq server")
	host     = flag.String("host", "1.15.181.131", "host of rabbitmq server")
	port     = flag.String("port", "5682", "port of rabbitmq server")
	queue    = flag.String("queue", "test_listener", "queue to listen")
)

var (
	Log = logx.NewStdoutLog(logx.InfoStr)
)

func init() {
	flag.Parse()
}

func main() {
	// 创建 rabbitx
	Log.Info("init rabbitx with a new connection")
	r, err := rabbitx.New(*username, *password, *host, *port)
	if err != nil {
		Log.ErrorE(err)
	}

	// 监听并处理
	Log.InfoF("start listening %s", *queue)
	var timeCmpHandler *handler.TimeCmpHandler
	err = r.Consume(*queue, true, timeCmpHandler)
	if err != nil {
		Log.ErrorE(err)
	}
}

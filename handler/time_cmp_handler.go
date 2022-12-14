package handler

import (
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yungsem/gox/filex"
	"github.com/yungsem/gox/logx"
	"time"
)

var (
	Log = logx.NewFileLog(logx.InfoStr, "logs")
)

// DiffLowerLimit 时间差值的下限，大于该值的消息要记录下来
var DiffLowerLimit int

// TimeCmpHandler 表示专门做时间对比的处理器
type TimeCmpHandler struct {
}

// Handle 处理消费队列里的消息
func (h *TimeCmpHandler) Handle(delivery amqp.Delivery) {
	// 打印收到的消息
	Log.InfoF("got message: %s", delivery.Body)
	// 对比消息体里的时间和收到消息的实际，差异大于 5 秒，则写入文件
	compare(delivery.Body)
}

/*
{
    "trx_name": "eqp_data",
    "msg_id": 1670285870,
    "token": "",
    "rpt_time": "2022-12-06T00:17:50.463511",
    "box_code": "2205107750",
    "encrypt": 0,
    "data": {
        "eqp_code": "MPTEST-BNL-12",
        "product_model": "",
        "product_code": "",
        "params": [
            {
                "k": "eqp_echo",
                "v": 1
            },
            {
                "k": "EQP_STATUS",
                "v": "IDLE"
            }
        ]
    }
}
*/

// message 表示消息体
type message struct {
	TrxName  string    `json:"trx_name"`
	MsgId    int       `json:"msg_id"`
	Token    string    `json:"token"`
	RptTime  string    `json:"rpt_time"`
	BoxCode  string    `json:"box_code"`
	Encrypt  int       `json:"encrypt"`
	Now      time.Time `json:"now"`
	Duration string    `json:"duration"`
	Data     data      `json:"data"`
}

type data struct {
	EqpCode      string  `json:"eqp_code"`
	ProductModel string  `json:"product_model"`
	ProductCode  string  `json:"product_code"`
	Params       []param `json:"params"`
}

type param struct {
	K string      `json:"k"`
	V interface{} `json:"v"`
}

// compare 对比消息体里的时间和收到消息的实际，差异大于 5 秒，则写入文件
func compare(body []byte) {
	// now 作为收到消息的时间
	now := time.Now()

	// 反序列号 msg
	var msg message
	err := json.Unmarshal(body, &msg)
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// 格式化 msg 里的 rptTime
	rptTime, err := time.ParseInLocation("2006-01-02T15:04:05.999999", msg.RptTime, time.Local)
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// now-rptTime ，并取绝对值
	diff := now.Sub(rptTime).Abs()

	// 差值大于 50 秒，写入文件
	if diff > time.Duration(DiffLowerLimit)*time.Second {
		// 创建文件 diff.log
		file, fileErr := filex.OpenFile("diff.log")
		if fileErr != nil {
			Log.ErrorE(fileErr)
		}
		defer file.Close()

		// 扩展 msg ，设置两个字段：Now 和 Duration
		msg.Now = now
		msg.Duration = diff.String()

		// 序列化
		bytes, jsonErr := json.Marshal(msg)
		if jsonErr != nil {
			Log.ErrorE(jsonErr)
		}

		// 写入 diff.log
		newLine := []byte("\n")
		bytes = append(bytes, newLine...)
		n, wErr := file.Write(bytes)
		if wErr != nil {
			Log.ErrorE(wErr)
		}

		Log.InfoF("successfully write %d bytes", n)
	}
}

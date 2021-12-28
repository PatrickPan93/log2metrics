package logjob

import (
	"crypto/md5"
	"encoding/hex"
	"log"
	"log2metrics/src/common"
	"log2metrics/src/modules/agent/config"
	"log2metrics/src/modules/agent/consumer"
	"log2metrics/src/modules/agent/reader"
)

type LogJob struct {
	r        *reader.Reader          // 日志生产者(读取日志)
	c        *consumer.ConsumerGroup // 日志消费者组
	Strategy *config.LogStrategy     // 日志策略
}

func (lj *LogJob) hash() string {
	md5obj := md5.New()
	// filepath和metric name用作hash的生成条件
	md5obj.Write([]byte(lj.Strategy.FilePath))
	md5obj.Write([]byte(lj.Strategy.MetricName))
	return hex.EncodeToString(md5obj.Sum(nil))
}

func (lj *LogJob) start() {

	filePath := lj.Strategy.FilePath
	// stream
	stream := make(chan string, common.LogQueueSize)
	// new reader
	r, err := reader.NewReader(filePath, stream)
	if err != nil {
		log.Printf("%+v\n", err)
	}
	// 赋值给logJob
	lj.r = r

	// new consumerGroup
	cg := consumer.NewConsumerGroup(filePath, stream, lj.Strategy)

	lj.c = cg
	// 启动r和c
	// 先启动消费者
	lj.c.Start()
	// 后生产者
	go lj.r.Start()
	log.Printf("[lojob.start: create logJob successfully][filepath:%s][sid:%s]", filePath, lj.Strategy.MetricName)
}

func (lj *LogJob) stop() {
	// 先停生产者
	lj.r.Stop()
	// 再停消费者
	lj.c.Stop()
}

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
	cg       *consumer.ConsumerGroup // 日志消费者组
	Strategy *config.LogStrategy     // 日志策略
}

func (lj *LogJob) hash() string {
	md5obj := md5.New()
	// filepath和metric name用作hash的生成条件
	md5obj.Write([]byte(lj.Strategy.FilePath))
	md5obj.Write([]byte(lj.Strategy.MetricName))
	return hex.EncodeToString(md5obj.Sum(nil))
}

func (lj *LogJob) start(cq chan *consumer.AnalysisPoint) {

	// 获取当前策略的文件路径
	filePath := lj.Strategy.FilePath
	// 初始化string chan, 日志采集完毕后通过该chan与消费者构成生产者消费者模型
	stream := make(chan string, common.LogQueueSize)

	// 构建reader, 后面会作为logJob的reader结构体成员
	r, err := reader.NewReader(filePath, stream)
	if err != nil {
		log.Printf("%+v\n", err)
	}
	// 实例化reader成员
	lj.r = r

	// 生成消费者组, 传入filePath，
	//  与生产者构成生产消费模型的stream(日志传输chan),
	//  log的策略,
	//  还有AnalysisPoint Chan, 为counterQueue, 消费者在正则处理完毕后会构造AnalysisPoint通过该chan给counter进行消费
	cg := consumer.NewConsumerGroup(filePath, stream, lj.Strategy, cq)

	// 将消费者组赋予当前logJob
	lj.cg = cg

	// 启动消费者组从stream chan消费日志
	lj.cg.Start()
	// 启动生产者读取日志
	go lj.r.Start()

	// 打印当前MetricsName和对应的日志文件路径
	log.Printf("[lojob.start: create logJob successfully][filepath:%s][sid:%s]", filePath, lj.Strategy.MetricName)
}

func (lj *LogJob) stop() {
	// 先停生产者
	lj.r.Stop()
	// 再停消费者
	lj.cg.Stop()
}

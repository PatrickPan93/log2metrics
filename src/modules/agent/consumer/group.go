package consumer

import (
	"fmt"
	"log"
	"log2metrics/src/modules/agent/config"
)

// ConsumerGroup 定义消费者组
type ConsumerGroup struct {
	Consumers   []*Consumer
	ConsumerNum int
}

func (cg *ConsumerGroup) Start() {
	for i := 0; i < cg.ConsumerNum; i++ {
		cg.Consumers[i].Start()
	}
}

func (cg *ConsumerGroup) Stop() {
	for i := 0; i < cg.ConsumerNum; i++ {
		cg.Consumers[i].Stop()
	}
}

func NewConsumerGroup(filePath string, stream chan string, strategy *config.LogStrategy) *ConsumerGroup {
	//TODO 缺少analysisPoint
	cNUm := 10

	cg := &ConsumerGroup{
		Consumers:   make([]*Consumer, 0),
		ConsumerNum: cNUm,
	}

	log.Printf("[new ConsumerGroup][file:%s][num:%d]", filePath, cNUm)

	// 根据消费组成员数量决定Consumer数量,并生成对应consumer
	for i := 0; i < cNUm; i++ {
		mark := fmt.Sprintf("[log.consumer][file:%s][num:%d/%d]", filePath, i+1, cNUm)
		c := &Consumer{
			FilePath:    filePath,
			Stream:      stream,
			Strategy:    strategy,
			Mark:        mark,
			Close:       make(chan struct{}),
			IsAnalysing: false,
		}
		// append消费者
		cg.Consumers = append(cg.Consumers, c)
	}
	return cg
}

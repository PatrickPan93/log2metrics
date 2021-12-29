package consumer

import (
	"fmt"
	"log"
	"log2metrics/src/common"
	"log2metrics/src/modules/agent/config"
)

// ConsumerGroup 定义消费者组
type ConsumerGroup struct {
	Consumers   []*Consumer
	ConsumerNum int
}

func (cg *ConsumerGroup) Start() {
	// 根据消费者组的结构体成员数量,生成对应数量的消费者
	for i := 0; i < cg.ConsumerNum; i++ {
		// 调用start方法
		cg.Consumers[i].Start()
	}
}

func (cg *ConsumerGroup) Stop() {
	for i := 0; i < cg.ConsumerNum; i++ {
		cg.Consumers[i].Stop()
	}
}

func NewConsumerGroup(filePath string, stream chan string, strategy *config.LogStrategy, cq chan *AnalysisPoint) *ConsumerGroup {

	cg := &ConsumerGroup{
		Consumers:   make([]*Consumer, 0),
		ConsumerNum: common.ConsumerNumber,
	}

	log.Printf("[new ConsumerGroup][file:%s][num:%d]", filePath, common.ConsumerNumber)

	// 根据消费组成员数量决定Consumer数量,并生成对应consumer
	for i := 0; i < common.ConsumerNumber; i++ {
		mark := fmt.Sprintf("[log.consumer][file:%s][num:%d/%d]", filePath, i+1, common.ConsumerNumber)
		c := &Consumer{
			FilePath:     filePath,
			Stream:       stream,
			Strategy:     strategy,
			Mark:         mark,
			Close:        make(chan struct{}),
			IsAnalysing:  false,
			CounterQueue: cq,
		}
		// append消费者
		cg.Consumers = append(cg.Consumers, c)
	}
	return cg
}

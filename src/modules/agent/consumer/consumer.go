package consumer

import (
	"log"
	"log2metrics/src/modules/agent/config"
	"time"
)

// Consumer consumer 对象
type Consumer struct {
	FilePath    string
	Stream      chan string
	Strategy    *config.LogStrategy
	Mark        string // worker name
	Close       chan struct{}
	IsAnalysing bool // 判断是否正在分析
}

func (c *Consumer) Start() {
	go func() {
		c.Work()
	}()
}

func (c *Consumer) Stop() {
	close(c.Close)
}

func (c *Consumer) Work() {
	log.Printf("[Consumer:%v] starting", c.Mark)

	var (
		anaCnt, anaSwp int64
	)
	// 统计分析go routine
	analysisClose := make(chan struct{})
	go func() {
		for {
			select {
			case <-analysisClose:
				return
			case <-time.After(10 * time.Second):
				a := anaCnt
				log.Printf("[Consumer:%v] analysis %d line in last 10s", c.Mark, a-anaSwp)
				anaSwp = a
			}
		}
	}()

	// 从stream中读取文件流, 调用analysis方法进行规则处理
	for {
		select {
		case line := <-c.Stream:
			anaCnt++
			c.IsAnalysing = true
			c.analysis(line)
			c.IsAnalysing = false
			// 控制统计go routine生命周期
		case <-c.Close:
			analysisClose <- struct{}{}
			return
		}
	}
}

// consumer处理文本动作
func (c *Consumer) analysis(line string) {
	log.Printf("[Consumer:%v] analysising line %s", c.Mark, line)
}

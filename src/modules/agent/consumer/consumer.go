package consumer

import (
	"bytes"
	"log"
	"log2metrics/src/modules/agent/config"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Consumer consumer 对象
type Consumer struct {
	FilePath     string
	Stream       chan string
	Strategy     *config.LogStrategy
	Mark         string // worker name
	Close        chan struct{}
	CounterQueue chan *AnalysisPoint // 统计Queue
	IsAnalysing  bool                // 判断是否正在分析
}

// AnalysisPoint 从Consumer 往计算部分推的point
type AnalysisPoint struct {
	Value           float64 // 可能是数字的正则结果, cnt 记数的时候是NaN
	MetricsName     string  // Metrics name
	LogFunc         string  // 计算的方法, cnt/max/min
	SortLabelString string  // 标签排序的结果
	LabelMap        map[string]string
}

func (c *Consumer) Start() {
	go func() {
		// 调用消费者实际的Work方法
		c.Work()
	}()
}

func (c *Consumer) Stop() {
	close(c.Close)
}

func (c *Consumer) Work() {
	// 输出日志
	log.Printf("[Consumer:%v] starting", c.Mark)

	// 与生产者相同,会进行过去10s的日志分析数量统计
	var (
		anaCnt, anaSwp int64
	)

	// 统计分析goroutine生命周期控制channel
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

	// 从stream中读取日志, 调用analysis方法进行规则处理
	for {
		select {
		case line := <-c.Stream:
			// 处理数量自增
			anaCnt++
			// 调整日志处理中标记位
			c.IsAnalysing = true
			// 调用analysis方法进行日志处理
			c.analysis(line)
			// 处理完毕后恢复标记位
			c.IsAnalysing = false

		case <-c.Close:
			// 控制统计go routine生命周期
			analysisClose <- struct{}{}
			return
		}
	}
}

// consumer处理文本动作
func (c *Consumer) analysis(line string) {
	log.Printf("[Consumer:%v] analysising line %s", c.Mark, line)

	defer func() {
		if err := recover(); err != nil {
			log.Printf("consumer.analysis: [analysis.panic][mark:%v][err:%v]\n", c.Mark, err)
		}
	}()

	// 开始处理用户正则
	var (
		patternReg *regexp.Regexp
		value      = math.NaN()
		vString    string // 非cnt的正则 数字分组

	)
	// 从消费者结构体成员获取来自配置的主正则pattern
	patternReg = c.Strategy.PatternReg

	// 通过正则进行字符串匹配,结果会返回[]string
	v := patternReg.FindStringSubmatch(line)
	/*
		len = 0 说明没匹配中,该行需要被丢弃
		len = 1 说明匹配中了,但是小括号分组没匹配到
		len > 1 说明正则匹配中了, 小括号分组也匹配到
	*/
	// 没匹配到,直接return
	if len(v) == 0 {
		return
	}

	// 如果全匹配到了则取第二位(小括号内容)
	if len(v) > 1 {
		vString = v[1]
	}

	// 尝试将vString转化为float,如果成功说明匹配到了200 (code=200)
	value, _ = strconv.ParseFloat(vString, 64)

	// TODO analysis 如果等于1呢？

	// 处理tag的正则
	labelMap := map[string]string{}
	// 从配置中获取到tag的Regexp
	for key, regTag := range c.Strategy.TagRegs {
		labelMap[key] = ""
		// 通过正则继续尝试在文本进行匹配
		t := regTag.FindStringSubmatch(line)
		// 如果t不为空, 而且长度大于1(说明小括号内容匹配成功)
		if t != nil && len(t) > 1 {
			// key为配置里面的key , value为匹配到的内容
			/*
					如:
					tags:
					    level: ".*level=(.*?) .*"
				key则为level, value为(.*?)匹配到的内容
			*/
			labelMap[key] = t[1]
		}
	}

	// 构造AnalysisPoint
	ret := &AnalysisPoint{
		Value:           value,
		MetricsName:     c.Strategy.MetricName,
		LogFunc:         c.Strategy.Func,
		SortLabelString: SortedTags(labelMap),
		LabelMap:        labelMap,
	}
	// 将结果推送到放入到CounterQueue中, Counter会对该Queue进行消费进行对应计算方式(sum\max\min...)的处理
	c.CounterQueue <- ret
}

// SortedTags tags排序
func SortedTags(tags map[string]string) string {
	if tags == nil {
		return ""
	}

	size := len(tags)
	if size == 0 {
		return ""
	}

	ret := new(bytes.Buffer)

	if size == 1 {
		for k, v := range tags {
			ret.WriteString(k)
			ret.WriteString("=")
			ret.WriteString(v)
		}
		return ret.String()
	}

	keys := make([]string, size)
	i := 0
	for k := range tags {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	for j, key := range keys {
		ret.WriteString(key)
		ret.WriteString("=")
		ret.WriteString(tags[key])
		if j != size-1 {
			ret.WriteString(",")
		}
	}

	return ret.String()
}

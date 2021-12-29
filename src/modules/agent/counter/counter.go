package counter

import (
	"context"
	"log"
	"log2metrics/src/common"
	"log2metrics/src/modules/agent/consumer"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PointCounterManager struct {
	sync.RWMutex
	CounterQueue chan *consumer.AnalysisPoint
	// key是标签排序后的string
	TagStringMap map[string]*PointCounter
	MetricsMap   map[string]*prometheus.GaugeVec
}

// PointCounter 统计实体 与AnalysisPoint有关系
type PointCounter struct {
	sync.RWMutex
	Count int64   // 日志条数记数
	Sum   float64 // 正则数字的Sum
	Max   float64 // 正则数字的Max
	Min   float64 // 正则数字的Min
	Avg   float64 // 正则数字的Avg
	Ts    int64

	MetricsName     string // Metrics name
	LogFunc         string // 计算的方法, cnt/max/min
	SortLabelString string // 标签排序的结果
	LabelMap        map[string]string
}

func NewPointCounter(metricsName string, sortLabelString string, logFunc string, labelMap map[string]string) *PointCounter {
	return &PointCounter{
		MetricsName:     metricsName,
		LogFunc:         logFunc,
		SortLabelString: sortLabelString,
		LabelMap:        labelMap,
	}
}

// Update 更新PointCounter的值
func (pc *PointCounter) Update(value float64) {
	pc.Lock()
	defer pc.Unlock()
	// 计算sum
	pc.Sum = pc.Sum + value

	// 更新最大值
	if math.IsNaN(pc.Max) || value > pc.Max {
		pc.Max = value
	}
	// 更新最小值
	if math.IsNaN(pc.Min) || value < pc.Min {
		pc.Min = value
	}
	pc.Count++

	pc.Ts = time.Now().Unix()
}

func NewPointCounterManager(cq chan *consumer.AnalysisPoint, metricsMap map[string]*prometheus.GaugeVec) *PointCounterManager {
	return &PointCounterManager{
		CounterQueue: cq,
		TagStringMap: make(map[string]*PointCounter),
		MetricsMap:   metricsMap,
	}
}

// SetPc Set 更新实体方法
func (pcm *PointCounterManager) SetPc(seriesId string, pc *PointCounter) {
	pcm.Lock()
	defer pcm.Unlock()
	pcm.TagStringMap[seriesId] = pc
}

// GetPcByUniqueName 获取实体方法
func (pcm *PointCounterManager) GetPcByUniqueName(seriesId string) *PointCounter {
	pcm.RLock()
	defer pcm.RUnlock()
	return pcm.TagStringMap[seriesId]
}

func (pcm *PointCounterManager) UpdateManager(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("PointCounterManager.UpdateManager.receive_quit_signal_and_quit")
			return nil
			// 从CounterQueue接收来自于consumer推送的AnalysisPoint进行处理
		case ap := <-pcm.CounterQueue:
			log.Printf("PointCounterManager.UpdateManager: received ap name %s", ap.MetricsName)
			/*
				尝试根据metricsName + sortLabelString
				获取PointCounter(每个metricsName+SortLabelString会生成一个对应的统计实体,来表示该实体各种func对应的值
				如Max\Min\Avg
			*/
			pc := pcm.GetPcByUniqueName(ap.MetricsName + ap.SortLabelString)
			// 如果为空则设置对应实体的PointCounter
			if pc == nil {
				// 构造PointCounter
				pc = NewPointCounter(ap.MetricsName, ap.SortLabelString, ap.LogFunc, ap.LabelMap)
				// 设置PointCounter到PointCounterManager中(以MetricsName+SortLabelString为key, value为PointCouter)
				pcm.SetPc(ap.MetricsName+ap.SortLabelString, pc)
			}
			// 不为空, 那么说明PointCounter存在,直接更新它的值
			pc.Update(ap.Value)
		}
	}
}

func (pcm *PointCounterManager) SetMetrics() {
	// 获取锁
	pcm.Lock()
	defer pcm.Unlock()

	// 从TagStringMap获取PointCounter(key为metricsName+sortLabelString)
	for _, pc := range pcm.TagStringMap {
		pc := pc
		// 如果不存在对应的PointCounter,则continue
		metric, loaded := pcm.MetricsMap[pc.MetricsName]
		if !loaded {
			log.Printf("[metrics.notfound[name:%v]", pc.MetricsName)
			continue
		}
		// 如果存在
		log.Printf("[PointCounterManager.SetMetrics][pc: %+v]", pc)
		var value float64

		// 那么会根据PointCounter所需要的计算类型,进行赋值
		switch pc.LogFunc {
		case common.LogFuncCnt:
			value = float64(pc.Count)
		case common.LogFuncSum:
			value = pc.Sum
		case common.LogFuncMax:
			value = pc.Max
		case common.LogFuncMin:
			value = pc.Min
		case common.LogFuncAvg:
			value = pc.Sum / float64(pc.Count)
		}
		// 并以pc.labelMap为label为label, value为对应function的value进行更新
		metric.With(pc.LabelMap).Set(value)
	}
}

// SetMetricsManager 更新set metrics
func (pcm *PointCounterManager) SetMetricsManager(ctx context.Context) error {
	// 由ticker事件驱动的metrics更新
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("PointCounterManager.SetMetricsManager.receive_quit_signal_and_quit")
			return nil
		case <-ticker.C:
			// 调用SetMetrics方法
			pcm.SetMetrics()
		}
	}
}

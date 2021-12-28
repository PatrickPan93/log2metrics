package metric

import (
	"log2metrics/src/modules/agent/config"

	"github.com/prometheus/client_golang/prometheus"
)

// CreateMetrics 根据配置生成prometheus metrics
func CreateMetrics(ss []*config.LogStrategy) map[string]*prometheus.GaugeVec {
	pgvMap := make(map[string]*prometheus.GaugeVec, 0)
	for _, s := range ss {
		var labels []string
		for k := range s.Tags {
			labels = append(labels, k)
		}
		m := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: s.MetricName,
			Help: s.MetricHelp,
		}, labels)
		pgvMap[s.MetricName] = m
	}
	return pgvMap
}

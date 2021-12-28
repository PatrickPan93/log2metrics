package config

import (
	"io/ioutil"
	"log"
	"regexp"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Config struct {
	RpcServerAddr string         `yaml:"rpc_server_addr"`
	LogStrategies []*LogStrategy ` yaml:"log_strategies"`
	HttpAddr      string         `yaml:"http_addr"`
}

// LogStrategy 定义log配置结构体
type LogStrategy struct {
	ID         int64             `json:"id" yaml:"-"`
	MetricName string            `json:"metric_name" yaml:"metric_name"`
	MetricHelp string            `json:"metric_help" yaml:"metric_help"`
	FilePath   string            `json:"file_path" yaml:"file_path"`
	Pattern    string            `json:"pattern" yaml:"pattern"`
	Func       string            `json:"func" yaml:"func"`
	Tags       map[string]string `json:"tags" yaml:"tags"`
	Creator    string            `json:"creator" yaml:"creator"`
	// 通过解析后获取的正则表达式, 上面的是前端配置
	PatternReg *regexp.Regexp            `json:"-" yaml:"-"` // core Reg
	TagRegs    map[string]*regexp.Regexp `json:"-" yaml:"-"` // tags Reg
}

// Load 根据LoadFile读取配置文件后的字符串解析yaml为配置结构体
func Load(bs []byte) (*Config, error) {
	cfg := &Config{}
	err := yaml.Unmarshal(bs, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Load: Loading file filed")
	}
	return cfg, nil
}

// LoadFile 根据conf路径读取内容
func LoadFile(filename string) (*Config, error) {

	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrap(err, "LoadFile: Error while reading file via ReadFile")
	}

	cfg, err := Load(bytes)

	if err != nil {
		return nil, errors.Wrap(err, "LoadFile: Error while reader reading bytes")
	}

	// 加载日志策略配置
	cfg.LogStrategies = setLogRegs(cfg)

	return cfg, nil
}

// 解析用户配置的日志策略正则
func setLogRegs(cfg *Config) []*LogStrategy {
	var res []*LogStrategy

	// 处理主正则
	for _, st := range cfg.LogStrategies {
		st := st
		if len(st.Pattern) != 0 {
			// 编译正则表达式
			reg, err := regexp.Compile(st.Pattern)
			if err != nil {
				log.Printf("%+v", errors.Wrapf(err, "config.setLogRegs: compile pattern regexp failed: %s\n", st.Pattern))
				continue
			}
			st.PatternReg = reg
		}
		// 处理tags正则
		for tagK, tagV := range st.Tags {
			reg, err := regexp.Compile(tagV)
			if err != nil {
				log.Printf("%+v", errors.Wrapf(err, "config.setLogRegs: compile tags pattern regexp failed: %s\n", tagV))
				continue
			}
			if len(st.TagRegs) == 0 {
				st.TagRegs = make(map[string]*regexp.Regexp, 0)
			}
			st.TagRegs[tagK] = reg
		}
		res = append(res, st)
	}
	return res
}

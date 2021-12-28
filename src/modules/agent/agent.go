package main

import (
	"fmt"
	"log"
	"log2metrics/src/modules/agent/config"
	"os"
	"path/filepath"

	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// 命令行解析
	app = kingpin.New(filepath.Base(os.Args[0]), "The log2metrics")
	// 指定配置文件参数
	configFile = app.Flag("config.file", "log2metrics configuration file").Short('c').Default("log2metrics-agent.yaml").String()
)

func main() {
	// Help Info
	app.HelpFlag.Short('h')
	// 基于prometheus库通过build ldflags版本信息注入
	promlogConfig := promlog.Config{}
	app.Version(version.Print("log2metrics"))
	promlogflag.AddFlags(app, &promlogConfig)

	// Get Param From Command line from $1 to the end
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.Println("Start loading config...")
	agentConfig, err := config.LoadFile(*configFile)
	if err != nil {
		log.Printf("%+v\n", err)
		return
	}

	fmt.Println(agentConfig.LogStrategies[0].MetricName)
	fmt.Println(agentConfig.LogStrategies[1].MetricName)
	log.Println("Loading config successfully")
}

package main

import (
	"context"
	"log"
	"log2metrics/src/common/nginx_log_generator"
	"log2metrics/src/modules/agent/config"
	"log2metrics/src/modules/agent/logjob"
	"log2metrics/src/modules/agent/metric"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"

	"github.com/oklog/run"

	"github.com/prometheus/client_golang/prometheus"

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
	log.Println("Loading config successfully")

	// 拿出metricsMap注册
	metricsMap := metric.CreateMetrics(agentConfig.LogStrategies)
	for _, m := range metricsMap {
		prometheus.MustRegister(m)
	}

	// new logJobManager
	logJobManager := logjob.NewLogJobManager()

	logJobSyncChan := make(chan []*logjob.LogJob, 1)

	jobs := make([]*logjob.LogJob, 0)
	for _, i := range agentConfig.LogStrategies {
		i := i
		job := &logjob.LogJob{Strategy: i}
		jobs = append(jobs, job)
	}

	logJobSyncChan <- jobs

	var g run.Group
	ctx, cancel := context.WithCancel(context.Background())

	{
		// 接收signal的chan
		signalChan := make(chan os.Signal, 1)
		// 接收cancel信息的chan
		cancelChan := make(chan struct{})
		// 监听来自系统的terminal相关信号
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
		// the first: execution func
		// the second: the error handling func
		// 通过第一个g.add 来控制整体go goroutine的生命周期.
		// 假设第一组g.add的os.notify嗅探到了term信号,便会执行cancel()
		// 由于其它g.add都有listen <-ctx.Done()，所以当主routine嗅探到term信号后执行cancel()，其它go routine都会开始退出
		g.Add(func() error {
			select {
			case <-signalChan:
				log.Println("notify a SIGTERM syscall.. process will exit soon")
				// cancel() if signalChan got an term signal
				cancel()
				return nil
			case <-cancelChan:
				log.Println("Received a cancel event")
				return nil
			}
		},
			func(error) {
				close(cancelChan)
			})
	}
	{
		g.Add(func() error {
			err := logJobManager.SyncManager(ctx, logJobSyncChan)
			if err != nil {
				log.Printf("%+v", err)
			}
			return nil
		}, func(err error) {
			cancel()
		})
	}
	{
		if agentConfig.LocalConfig.NginxLogGenerating {
			g.Add(func() error {
				nginx_log_generator.Run(ctx)
				return errors.New("nginx_log_generator: running error")
			}, func(err error) {
				cancel()
			})
		}
	}

	g.Run()
}

package main

import (
	"context"
	"log"
	"log2metrics/src/common"
	"log2metrics/src/common/nginx_log_generator"
	"log2metrics/src/modules/agent/config"
	"log2metrics/src/modules/agent/consumer"
	"log2metrics/src/modules/agent/counter"
	"log2metrics/src/modules/agent/logjob"
	"log2metrics/src/modules/metrics"
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
	metricsMap := metrics.CreateMetrics(agentConfig.LogStrategies)
	for _, m := range metricsMap {
		prometheus.MustRegister(m)
	}

	// 统计指标的同步Queue
	cq := make(chan *consumer.AnalysisPoint, common.CounterQueueSize)
	// 统计指标管理器
	PointCounterManager := counter.NewPointCounterManager(cq, metricsMap)
	// 日志job管理器
	logJobManager := logjob.NewLogJobManager(cq)
	// 把配置文件的logJob传入
	logJobSyncChan := make(chan []*logjob.LogJob, 1)

	// 从配置里面获取到jobs列表后通过channel发送给logJobManager
	jobs := make([]*logjob.LogJob, 0)
	for _, i := range agentConfig.LogStrategies {
		i := i
		job := &logjob.LogJob{Strategy: i}
		// 叠加job到jobs中
		jobs = append(jobs, job)
	}
	logJobSyncChan <- jobs

	var g run.Group
	ctx, cancel := context.WithCancel(context.Background())

	// 主控go routine
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
	if agentConfig.LogCollecting.Enable {
		// logJobManager
		// 1.通过配置生成logJobs
		// 2. 构造logJob的reader\consumerGroup
		// 3. 消费者组进行消费预计正则处理后会将AnalysisPoint送入counterQueue等待PointCounter进行消费
		{

			g.Add(func() error {
				// 传入控制goroutine生命周期的ctx和从配置获取到的logJob传入到的chan
				err := logJobManager.SyncManager(ctx, logJobSyncChan)
				if err != nil {
					log.Printf("%+v", err)
				}
				return nil
			}, func(err error) {
				cancel()
			})
		}

		// PointCounter实体管理器，从counterQueue接收AnalysisPoint并使用PointCounter进行统计处理
		{
			g.Add(func() error {
				// 传入ctx控制PointCounterManager生命周期,调用UpdateManager方法,消费CounterQueue并且构造PointCounter实体进行对应统计function的统计
				err := PointCounterManager.UpdateManager(ctx)
				if err != nil {
					log.Printf("%+v", err)
				}
				return nil
			}, func(err error) {
				cancel()
			})
		}
		// 统计任务实体转化为prometheus metrics的任务, 通过metrics endpoint进行指标展示
		{
			g.Add(func() error {
				// 传入控制goroutine生命周期的ctx
				err := PointCounterManager.SetMetricsManager(ctx)
				if err != nil {
					log.Printf("%+v", err)
				}
				return nil
			}, func(err error) {
				cancel()
			})
		}
		// logJob metrics 结果的httpserver
		{
			// 启动httpserver并注入prometheus的http handler进行内存中metrics的展示
			g.Add(func() error {
				errChan := make(chan error, 1)
				go func() {
					errChan <- metrics.StartMetricWeb(":8080")
				}()

				select {
				case err := <-errChan:
					log.Printf("%+v", errors.Wrap(err, "metrics server running error"))
					return err
				case <-ctx.Done():
					log.Println("metric server receive quit signal.. would be stopped soon")
					return nil

				}
			},
				func(err error) {
					cancel()
				})
		}
	}

	// nginx日志生成器
	{
		if agentConfig.LocalConfig.NginxLogGenerating {
			g.Add(func() error {
				nginx_log_generator.Run(ctx, agentConfig.LocalConfig.RatePerSecond)
				return errors.New("nginx_log_generator: running error")
			}, func(err error) {
				cancel()
			})
		}
	}

	g.Run()
}

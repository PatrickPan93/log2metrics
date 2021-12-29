package reader

import (
	"io"
	"log"
	"time"

	"github.com/hpcloud/tail"
	"github.com/pkg/errors"
)

type Reader struct {
	FilePath    string        // 日志路径
	tailer      *tail.Tail    // tailer对象
	Stream      chan string   //同步日志chan
	CurrentPath string        // 当前路径
	Close       chan struct{} // 	关闭的chan
	FD          uint64        // 文件inode, 用来处理文件滚动时文件名发生变化的情况
}

// NewReader new reader函数
func NewReader(filePath string, stream chan string) (*Reader, error) {
	r := &Reader{
		FilePath: filePath,
		Stream:   stream,
		Close:    make(chan struct{}),
	}
	// SeekEnd 从尾部开始打开文件
	if err := r.openFile(io.SeekEnd, filePath); err != nil {
		return nil, errors.Wrap(err, "")
	}
	return r, nil
}

// 打开文件方法
func (r *Reader) openFile(whence int, filePath string) error {
	// 生成SeekInfo 决定文件从哪里开始读取
	seekInfo := &tail.SeekInfo{
		Offset: 0,
		Whence: whence,
	}
	config := tail.Config{
		Location:  seekInfo, // 文件起始读位置
		ReOpen:    true,     // 重新打开
		MustExist: true,
		Poll:      true, // 轮询
		//RateLimiter: nil,
		Follow: true,
	}
	t, err := tail.TailFile(filePath, config)
	if err != nil {
		return errors.Wrap(err, "reader.openFile: Error while TailFile")
	}
	// 将tailer赋值给reader
	r.tailer = t
	r.CurrentPath = filePath
	r.FD = 0
	return nil
}

func (r *Reader) Start() {
	// 开始读取日志
	r.StartRead()
}

func (r *Reader) Stop() {
	r.StopRead()
	close(r.Close)
}

func (r *Reader) StartRead() {
	// 统计read行数以及drop行数
	var (
		readCnt, readSwp int64
		dropCnt, dropSwp int64
	)

	// 会临时开启goroutine进行日志处理,由于需要控制该goroutine的生命周期, 所以生成channel用以达到该目的
	analysisClose := make(chan struct{})
	// 使用goroutine进行日志分析处理
	go func() {
		for {
			select {
			// 通过外层chan 控制生命周期
			case <-analysisClose:
				return
			// 每十秒跳出select循环
			case <-time.After(10 * time.Second):
			}
			// 先将历史read和drop记录复制给临时变量a、b
			a := readCnt
			b := dropCnt
			// 因为每10s触发一次用以统计过去10s中read和drop的数量, 所以用a、b的值分别减去readSwap、dropSwap就是过去10秒的值
			log.Printf("read [%d] line in last 10s", a-readSwp)
			log.Printf("drop [%d] line in last 10s", b-dropSwp)

			// 把本次的read和drop值赋值给swp，用作下次10s统计
			readSwp = a
			dropSwp = b
		}
	}()

	// 利用tailer进行日志读取
	for line := range r.tailer.Lines {
		// 已读取行数自增统计
		readCnt++
		select {
		// 读取到的日志将会推送到stream中,供消费者组进行消费
		case r.Stream <- line.Text:
		default:
			// 已过滤行数自增统计
			dropCnt++
		}
	}
	// 当读取日志loop退出,则把统计的go routine也退出
	close(analysisClose)
}

func (r *Reader) StopRead() {
	r.tailer.Stop()
}

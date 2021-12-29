package logjob

import (
	"context"
	"log"
	"log2metrics/src/modules/agent/consumer"
	"sync"
)

// LogJobManager logjob manager
type LogJobManager struct {
	targetMtx     sync.Mutex
	activeTargets map[string]*LogJob
	cq            chan *consumer.AnalysisPoint
}

// NewLogJobManager return new logjob manager
func NewLogJobManager(cq chan *consumer.AnalysisPoint) *LogJobManager {
	return &LogJobManager{
		activeTargets: make(map[string]*LogJob),
		cq:            cq,
	}
}

// SyncManager 通过SyncManager来触发Sync Jobs
func (jm *LogJobManager) SyncManager(ctx context.Context, syncChan chan []*LogJob) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("logjob.SyncManager.receive_quit_signal_and_quit")
			jm.StopAll()
			return nil
		case jobs := <-syncChan:
			// 获取到具体的jobs后, 传入jobs参数调用Manager的sync方法
			log.Printf("logjob.SyncManager: start logjobs")
			jm.Sync(jobs)
		}
	}
}

// StopAll 停止所有activeTargets中的jobs
func (jm *LogJobManager) StopAll() {
	jm.targetMtx.Lock()
	defer jm.targetMtx.Unlock()
	for _, v := range jm.activeTargets {
		v.stop()
	}

}

func (jm *LogJobManager) StartAll(jobs []*LogJob) {
	jm.Sync(jobs)
}

// Sync sync方法 增量更新jobs实现
func (jm *LogJobManager) Sync(jobs []*LogJob) {

	log.Printf("LogJobManager.Sync: [num:%d] [res:%+v]", len(jobs), jobs)
	// 初始化存储增量jobs map
	thisNewTargets := make(map[string]*LogJob)
	// 初始化本次全量jobs map
	thisAllTargets := make(map[string]*LogJob)

	// 获取锁
	jm.targetMtx.Lock()

	// 循环jobs
	for _, job := range jobs {
		// 根据job的metricsName和filePath生成hash
		hash := job.hash()
		// 以hash为key job为value放入圈梁jobs map里面
		thisAllTargets[hash] = job
		// 如果在activeTarget Map中找不到当前job的hash key, 说明这个job是个增量job
		if _, loaded := jm.activeTargets[hash]; !loaded {
			// 那么就将这个增量Job 添加到增量job Map中
			thisNewTargets[hash] = job
			// 并且往activeTarget将这个job添加进去
			jm.activeTargets[hash] = job
		}
	}

	// 从当前activeTargets中取出target （key=hash value=job)
	for hash, job := range jm.activeTargets {
		// 用activeTarget中job的hash匹配这次的全量新对象hash
		if _, loaded := thisAllTargets[hash]; !loaded {
			// 如果获取不到说明本次activeTarget里面的job是需要被删除
			log.Printf("LogJobManager.Sync: stop %+v stra:%+v", job, job.Strategy)
			// 停止job
			job.stop()
			// 从activeTargets map中删除对象以hash为key的job
			delete(jm.activeTargets, hash)
		}
	}
	// 释放锁
	jm.targetMtx.Unlock()

	// 开启新的job(每个strategy都会生成一个job)
	for _, job := range thisNewTargets {
		//fmt.Println("lj")
		job := job
		// 启动job并且传入cq 用以传到AnalysisPoint到计算部分
		job.start(jm.cq)

	}

}

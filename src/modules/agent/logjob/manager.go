package logjob

import (
	"context"
	"log"
	"sync"
)

// LogJobManager logjob manager
type LogJobManager struct {
	targetMtx     sync.Mutex
	activeTargets map[string]*LogJob
}

// NewLogJobManager return new logjob manager
func NewLogJobManager() *LogJobManager {
	return &LogJobManager{
		activeTargets: make(map[string]*LogJob),
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
	thisNewTargets := make(map[string]*LogJob)
	thisAllTargets := make(map[string]*LogJob)

	jm.targetMtx.Lock()

	// 循环jobs
	for _, t := range jobs {
		// 生成hash
		hash := t.hash()
		// 以hash为key t为value放入这次的AllTargets中
		thisAllTargets[hash] = t
		// 如果在active的target中找不到该metrics的hash key
		if _, loaded := jm.activeTargets[hash]; !loaded {
			// 那么新target将新增这个metrics
			thisNewTargets[hash] = t
			// 并且active也新增这个hash
			jm.activeTargets[hash] = t
		}
	}

	// 取出当前activejob的hash和t对象
	for hash, t := range jm.activeTargets {
		// 用activejob的hash匹配这次的全量新对象hash
		if _, loaded := thisAllTargets[hash]; !loaded {
			// 如果获取不到说明本次activeJob里面比较的t对象要被删除
			log.Printf("LogJobManager.Sync: stop %+v stra:%+v", t, t.Strategy)
			t.stop()
			// 从activeTargets map中删除对象以hash为key的job
			delete(jm.activeTargets, hash)
		}
	}
	jm.targetMtx.Unlock()

	//fmt.Println(thisNewTargets)
	//fmt.Println(123)
	// 开启新的job
	for _, t := range thisNewTargets {
		//fmt.Println("lj")
		t := t
		t.start()

	}

}

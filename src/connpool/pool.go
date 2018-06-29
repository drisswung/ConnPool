package connpool

import (
	"sync"
	"errors"
	"log"
)

type ResourceAble interface {
	// 关闭资源
	Close()
	// 资源是否还存活
	// 例如可以让资源只有一定的存活时间，超出时间限定，资源将被回收
	IsActive() bool
	// 资源id
	getId() int32
}

type Factory func() (ResourceAble, error)

type Pool interface {
	// 获取资源
	GetResource() (ResourceAble, error)
	// 将资源返回连接池
	ReleaseResource(ResourceAble)
	// 关闭连接池
	Shutdown()
}

type commonPool struct {
	sync.Mutex
	maxConn  int32             //最大连接数
	openConn int32             //已经打开的连接数
	p        chan ResourceAble //资源channel
	factory  Factory           //资源创建工厂
}

func CreatePool(maxConn int32, factory Factory) *commonPool {

	cp := commonPool{
		maxConn: maxConn,
		factory: factory,
	}

	cp.p = make(chan ResourceAble, maxConn)

	for i := 0; i < int(maxConn/2); i++ {
		r, err := cp.factory()
		if err == nil {
			cp.openConn++
			cp.p <- r
		}
	}
	return &cp
}

var TRY_AGAIN = errors.New("try again")

func (cp *commonPool) GetResource() (ResourceAble, error) {
	for {
		r, err := cp.getOrCreate()
		if err == TRY_AGAIN {
			continue
		}
		if err != nil || r == nil {
			continue
		}
		log.Println("get resource, Id:", r.getId())
		return r, nil
	}
}

func (cp *commonPool) getOrCreate() (ResourceAble, error) {

	cp.Lock()
	defer cp.Unlock()

	if len(cp.p) > 0 {
		r := <-cp.p
		return r, nil
	}

	if cp.openConn < cp.maxConn {
		r, err := cp.factory()
		if err == nil {
			cp.openConn++
		}
		return r, err
	}

	return nil, TRY_AGAIN
}

func (cp *commonPool) ReleaseResource(r ResourceAble) {

	cp.Lock()
	defer cp.Unlock()

	if cp.openConn >= cp.maxConn || !r.IsActive() {
		closeResource(r, cp)
		return
	}

	cp.p <- r
	log.Println("resource", r.getId(), "released")
}

func closeResource(r ResourceAble, cp *commonPool) {
	r.Close()
	cp.openConn--
	log.Println("resource", r.getId(), "closed")
}

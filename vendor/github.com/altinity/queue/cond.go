package queue

import "sync"

type CondWaiter interface {
	Wait()
	Signal()
	Broadcast()
}

type Conditioner interface {
	sync.Locker
	CondWaiter
}

func NewCond() Conditioner {
	return NewMutexCond()
}

type MutexCond struct {
	m *sync.Mutex
	c *sync.Cond
}

func NewMutexCond() *MutexCond {
	m := &sync.Mutex{}
	c := sync.NewCond(m)
	return &MutexCond{
		m: m,
		c: c,
	}
}

func (m *MutexCond) Lock() {
	m.m.Lock()
}

func (m *MutexCond) Unlock() {
	m.m.Unlock()
}

func (m *MutexCond) Wait() {
	m.c.Wait()
}

func (m *MutexCond) Signal() {
	m.c.Signal()

}
func (m *MutexCond) Broadcast() {
	m.c.Broadcast()
}

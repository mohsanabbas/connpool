// Package connpool provides a thread-safe, bounded connection pool for net.Conn.
package connpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ErrPoolClosed is returned by Acquire after Close has been called.
var ErrPoolClosed = errors.New("pool closed")

// ErrReleaseForeignConn is returned when Release receives a connection that is
// not owned by this pool.
var ErrReleaseForeignConn = errors.New("release foreign connection")

// ErrReleaseNotInUse is returned when Release receives an owned connection that
var ErrReleaseNotInUse = errors.New("release connection not in use")

var errFactoryReturnedManagedConn = errors.New("factory returned a managed connection")

type connState uint8

const (
	stateIdle connState = iota
	stateInUse
)

// Pool manages a bounded set of net.Conn objects.
type Pool struct {
	factory     func() (net.Conn, error)
	maxSize     int
	idleTimeout time.Duration

	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	idle     []idleEntry
	owned    map[net.Conn]connState
	inUse    int
	creating int

	closeOnce   sync.Once
	fullyClosed chan struct{}
	sweeperStop chan struct{}
	sweeperDone chan struct{}

	errMu    sync.Mutex
	closeErr error
}

type idleEntry struct {
	conn       net.Conn
	releasedAt time.Time
}

// NewPool constructs a Pool.
func NewPool(maxSize int, idleTimeout time.Duration, factory func() (net.Conn, error)) *Pool {
	if maxSize <= 0 {
		panic("max connections must be provided and positive")
	}
	if factory == nil {
		panic("factory must be non-nil")
	}

	p := &Pool{
		factory:     factory,
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		idle:        make([]idleEntry, 0, maxSize),
		owned:       make(map[net.Conn]connState, maxSize),
		fullyClosed: make(chan struct{}),
		sweeperStop: make(chan struct{}),
		sweeperDone: make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)

	if idleTimeout > 0 {
		go p.sweep()
	} else {
		close(p.sweeperDone)
	}

	return p
}

// Acquire returns a connection from the pool, blocking until one is available
func (p *Pool) Acquire(ctx context.Context) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var (
		watcherStop chan struct{}
		ctxDone     = ctx.Done()
	)
	defer func() {
		if watcherStop != nil {
			close(watcherStop)
		}
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.closed {
			return nil, ErrPoolClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		gotIdle, conn, err := p.tryIdleLocked()
		if err != nil {
			return nil, err
		}
		if gotIdle {
			return conn, nil
		}

		if len(p.owned)+p.creating < p.maxSize {
			p.creating++
			p.mu.Unlock()
			c, err := p.factory()
			p.mu.Lock()
			p.creating--

			if err != nil {
				p.cond.Broadcast()
				return nil, err
			}
			if p.closed {
				p.cond.Broadcast()
				p.mu.Unlock()
				closeErr := c.Close()
				p.mu.Lock()
				if closeErr != nil {
					wrapped := fmt.Errorf("close conn after pool close: %w", closeErr)
					p.addCloseErr(wrapped)
					return nil, errors.Join(ErrPoolClosed, wrapped)
				}
				return nil, ErrPoolClosed
			}
			if _, exists := p.owned[c]; exists {
				p.cond.Broadcast()
				return nil, errFactoryReturnedManagedConn
			}
			p.owned[c] = stateInUse
			p.inUse++
			return c, nil
		}

		if watcherStop == nil && ctxDone != nil {
			watcherStop = make(chan struct{})
			go func(stop <-chan struct{}) {
				select {
				case <-ctxDone:
					p.mu.Lock()
					p.cond.Broadcast()
					p.mu.Unlock()
				case <-stop:
				}
			}(watcherStop)
		}
		p.cond.Wait()
	}
}

func (p *Pool) tryIdleLocked() (bool, net.Conn, error) {
	for len(p.idle) > 0 {
		e := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]

		state, ok := p.owned[e.conn]
		if !ok || state != stateIdle {
			continue
		}

		if p.idleTimeout > 0 && time.Since(e.releasedAt) > p.idleTimeout {
			delete(p.owned, e.conn)
			p.cond.Broadcast()
			p.mu.Unlock()
			closeErr := e.conn.Close()
			p.mu.Lock()
			if closeErr != nil {
				wrapped := fmt.Errorf("close expired idle conn: %w", closeErr)
				p.addCloseErr(wrapped)
				return false, nil, wrapped
			}
			continue
		}

		p.owned[e.conn] = stateInUse
		p.inUse++
		return true, e.conn, nil
	}
	return false, nil, nil
}

// Release returns a connection to the pool.
func (p *Pool) Release(conn net.Conn) error {
	if conn == nil {
		return nil
	}

	var (
		toClose net.Conn
		baseErr error
	)

	p.mu.Lock()
	state, ok := p.owned[conn]
	if !ok {
		p.mu.Unlock()
		closeErr := conn.Close()
		if closeErr != nil {
			wrapped := fmt.Errorf("close foreign connection: %w", closeErr)
			return errors.Join(ErrReleaseForeignConn, wrapped)
		}
		return ErrReleaseForeignConn
	}

	if state != stateInUse {
		baseErr = ErrReleaseNotInUse
		if state == stateIdle {
			p.removeIdleLocked(conn)
			delete(p.owned, conn)
			p.cond.Broadcast()
		}
		p.mu.Unlock()
		closeErr := conn.Close()
		if closeErr != nil {
			wrapped := fmt.Errorf("close idle connection: %w", closeErr)
			return errors.Join(baseErr, wrapped)
		}
		return baseErr
	}

	p.inUse--
	if p.closed {
		delete(p.owned, conn)
		toClose = conn
		p.mu.Unlock()

		var retErr error
		if err := toClose.Close(); err != nil {
			wrapped := fmt.Errorf("close all connections during shutdown: %w", err)
			p.addCloseErr(wrapped)
			retErr = wrapped
		}

		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
		return retErr
	}

	p.owned[conn] = stateIdle
	if len(p.idle) < cap(p.idle) {
		p.idle = append(p.idle, idleEntry{conn: conn, releasedAt: time.Now()})
		p.cond.Broadcast()
	} else {
		toClose = conn
		delete(p.owned, conn)
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	if toClose != nil {
		_ = toClose.Close()
	}
	return nil
}

// Close shuts the pool down.
func (p *Pool) Close() error {
	p.closeOnce.Do(p.doClose)
	<-p.fullyClosed
	return p.getCloseErr()
}

func (p *Pool) doClose() {
	defer close(p.fullyClosed)

	p.mu.Lock()
	p.closed = true
	idle := p.detachIdleLocked()
	p.cond.Broadcast()
	p.mu.Unlock()

	for _, c := range idle {
		if err := c.Close(); err != nil {
			p.addCloseErr(fmt.Errorf("close idle connections during shutdown: %w", err))
		}
	}

	p.mu.Lock()
	for p.inUse > 0 || p.creating > 0 {
		p.cond.Wait()
	}
	p.mu.Unlock()

	close(p.sweeperStop)
	<-p.sweeperDone
}

func (p *Pool) addCloseErr(err error) {
	if err == nil {
		return
	}
	p.errMu.Lock()
	p.closeErr = errors.Join(p.closeErr, err)
	p.errMu.Unlock()
}

func (p *Pool) getCloseErr() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.closeErr
}

func (p *Pool) removeIdleLocked(conn net.Conn) {
	for i := range p.idle {
		if p.idle[i].conn == conn {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return
		}
	}
}

func (p *Pool) detachIdleLocked() []net.Conn {
	if len(p.idle) == 0 {
		return nil
	}

	out := make([]net.Conn, 0, len(p.idle))
	for _, e := range p.idle {
		if state, ok := p.owned[e.conn]; ok && state == stateIdle {
			delete(p.owned, e.conn)
			out = append(out, e.conn)
		}
	}
	p.idle = nil
	return out
}

func (p *Pool) notifyLocked() {
	p.cond.Broadcast()
}

func (p *Pool) sweep() {
	defer close(p.sweeperDone)

	interval := p.idleTimeout / 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	if interval > p.idleTimeout {
		interval = p.idleTimeout
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			var expired []net.Conn
			now := time.Now()

			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				continue
			}

			kept := p.idle[:0]
			for _, e := range p.idle {
				state, ok := p.owned[e.conn]
				if !ok || state != stateIdle {
					continue
				}
				if now.Sub(e.releasedAt) > p.idleTimeout {
					delete(p.owned, e.conn)
					expired = append(expired, e.conn)
					continue
				}
				kept = append(kept, e)
			}
			p.idle = kept
			if len(expired) > 0 {
				p.cond.Broadcast()
			}
			p.mu.Unlock()

			for _, c := range expired {
				if err := c.Close(); err != nil {
					p.addCloseErr(fmt.Errorf("close expired conn in sweeper: %w", err))
				}
			}
		case <-p.sweeperStop:
			return
		}
	}
}

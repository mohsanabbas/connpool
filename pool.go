// Package connpool provides a thread-safe, bounded connection pool for net.Conn.
package connpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
)

// ErrPoolClosed is returned by Acquire after Close has been called.
var ErrPoolClosed = errors.New("pool closed")

// ErrReleaseForeignConn is returned when Release or Discard receives a
// connection that is not owned by this pool.
var ErrReleaseForeignConn = errors.New("release foreign connection")

// ErrReleaseNotInUse is returned when Release receives an owned connection that
var ErrReleaseNotInUse = errors.New("release connection not in use")

// ErrFactoryReturnedNilConn is returned when the factory returns a nil connection
// without an error.
var ErrFactoryReturnedNilConn = errors.New("factory returned nil connection")

// ErrFactoryReturnedManagedConn is returned when the factory returns a
// connection already managed by a pool.
var ErrFactoryReturnedManagedConn = errors.New("factory returned a managed connection")

// Factory creates a new connection for the pool. The context is the same context
// passed to Acquire, so factories should use it for dials and handshakes.
type Factory func(context.Context) (net.Conn, error)

// Validator checks a connection before Acquire returns it. A failed idle
// validation discards that connection and the pool keeps looking for another one.
type Validator func(context.Context, net.Conn) error

// ResetFunc prepares a connection for reuse before Release returns it to the
// idle pool.
type ResetFunc func(net.Conn) error

// Option customizes pool behavior.
type Option func(*Pool)

// WithValidator configures a validation hook that runs before a connection is
// handed out by Acquire.
func WithValidator(validate Validator) Option {
	return func(p *Pool) {
		p.validate = validate
	}
}

// WithReset configures a reset hook that runs before a connection is returned to
// the idle pool.
func WithReset(reset ResetFunc) Option {
	return func(p *Pool) {
		p.reset = reset
	}
}

// WithMaxLifetime configures the maximum amount of time a connection can remain
// in the pool before being retired.
func WithMaxLifetime(maxLifetime time.Duration) Option {
	return func(p *Pool) {
		if maxLifetime < 0 {
			panic("max lifetime must be non-negative")
		}
		p.maxLifetime = maxLifetime
	}
}

type connState uint8

const (
	stateIdle connState = iota
	stateInUse
	stateClosing
)

// Pool manages a bounded set of net.Conn objects.
type Pool struct {
	factory     Factory
	maxSize     int
	idleTimeout time.Duration
	maxLifetime time.Duration
	validate    Validator
	reset       ResetFunc

	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	idle     []idleEntry
	owned    map[*pooledConn]connState
	inUse    int
	creating int
	closing  int

	closeStartOnce sync.Once
	fullyClosed    chan struct{}
	sweeperStop    chan struct{}
	sweeperDone    chan struct{}

	errMu    sync.Mutex
	closeErr error
}

type pooledConn struct {
	net.Conn
	pool      *Pool
	createdAt time.Time
}

// Close discards this connection from the pool.
func (c *pooledConn) Close() error {
	if c == nil || c.pool == nil {
		return nil
	}

	err := c.pool.Discard(c)
	if errors.Is(err, ErrReleaseForeignConn) || errors.Is(err, ErrReleaseNotInUse) {
		return nil
	}
	return err
}

// Unwrap returns the underlying connection created by the pool factory.
func (c *pooledConn) Unwrap() net.Conn {
	if c == nil {
		return nil
	}
	return c.Conn
}

func (c *pooledConn) closeUnderlying() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

type idleEntry struct {
	conn       *pooledConn
	releasedAt time.Time
}

// Stats is a snapshot of the pool state.
type Stats struct {
	MaxSize  int
	Idle     int
	InUse    int
	Creating int
	Closing  int
	Closed   bool
}

// NewPool constructs a Pool.
func NewPool(maxSize int, idleTimeout time.Duration, factory Factory, opts ...Option) *Pool {
	if maxSize <= 0 {
		panic("max connections must be provided and positive")
	}
	if idleTimeout < 0 {
		panic("idle timeout must be non-negative")
	}
	if factory == nil {
		panic("factory must be non-nil")
	}

	p := &Pool{
		factory:     factory,
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		idle:        make([]idleEntry, 0, maxSize),
		owned:       make(map[*pooledConn]connState, maxSize),
		fullyClosed: make(chan struct{}),
		sweeperStop: make(chan struct{}),
		sweeperDone: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	p.cond = sync.NewCond(&p.mu)

	if idleTimeout > 0 {
		go p.sweep()
	} else {
		close(p.sweeperDone)
	}

	return p
}

// Underlying returns the connection created by the factory when conn came from
// this package. Other connections are returned unchanged.
func Underlying(conn net.Conn) net.Conn {
	if c, ok := conn.(*pooledConn); ok {
		return c.Unwrap()
	}
	return conn
}

// Stats returns a point-in-time snapshot of the pool state.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return Stats{
		MaxSize:  p.maxSize,
		Idle:     len(p.idle),
		InUse:    p.inUse,
		Creating: p.creating,
		Closing:  p.closing,
		Closed:   p.closed,
	}
}

// Acquire returns a connection from the pool, blocking until one is available
func (p *Pool) Acquire(ctx context.Context) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var stopWake func() bool
	defer func() {
		if stopWake != nil {
			stopWake()
		}
	}()

	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return nil, err
		}

		conn, err := p.tryIdleLocked(ctx)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		if conn != nil {
			p.mu.Unlock()
			return conn, nil
		}

		if p.canCreateLocked() {
			p.creating++
			p.mu.Unlock()

			created, panicked, panicValue, factoryErr := p.callFactory(ctx)

			p.mu.Lock()
			if panicked {
				p.creating--
				p.cond.Broadcast()
				p.mu.Unlock()
				panic(panicValue)
			}
			if factoryErr != nil {
				factoryErr = p.closeFactoryResultAfterErrorLocked(created, factoryErr)
				p.creating--
				p.cond.Signal()
				p.mu.Unlock()
				return nil, factoryErr
			}
			if isNilConn(created) {
				p.creating--
				p.cond.Signal()
				p.mu.Unlock()
				return nil, ErrFactoryReturnedNilConn
			}
			if _, managed := created.(*pooledConn); managed {
				p.creating--
				p.cond.Signal()
				p.mu.Unlock()
				return nil, ErrFactoryReturnedManagedConn
			}

			conn := &pooledConn{
				Conn:      created,
				pool:      p,
				createdAt: time.Now(),
			}
			if err := ctx.Err(); err != nil {
				err = p.closeCreatedConnLocked(conn, err, "close conn after acquire context cancellation")
				p.creating--
				p.cond.Signal()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				err := p.closeCreatedConnLocked(conn, ErrPoolClosed, "close conn after pool close")
				p.creating--
				p.cond.Broadcast()
				p.mu.Unlock()
				return nil, err
			}
			if p.validate != nil {
				panicErr, validateErr := p.validateNewConnLocked(ctx, conn)
				if panicErr != nil {
					p.creating--
					p.cond.Broadcast()
					p.mu.Unlock()
					panic(panicErr)
				}
				if validateErr != nil {
					p.creating--
					p.cond.Signal()
					p.mu.Unlock()
					return nil, validateErr
				}
			}

			p.owned[conn] = stateInUse
			p.inUse++
			p.creating--
			p.cond.Signal()
			p.mu.Unlock()
			return conn, nil
		}

		if stopWake == nil && ctx.Done() != nil {
			stopWake = context.AfterFunc(ctx, func() {
				p.mu.Lock()
				p.cond.Broadcast()
				p.mu.Unlock()
			})
		}
		p.cond.Wait()
	}
}

func (p *Pool) tryIdleLocked(ctx context.Context) (net.Conn, error) {
	for len(p.idle) > 0 {
		now := time.Now()
		entry := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]

		state, ok := p.owned[entry.conn]
		if !ok || state != stateIdle {
			continue
		}

		if p.idleExpired(entry, now) || p.lifetimeExpired(entry.conn, now) {
			delete(p.owned, entry.conn)
			p.closing++
			p.cond.Signal()
			p.mu.Unlock()
			closeErr := entry.conn.closeUnderlying()
			if closeErr != nil {
				p.addCloseErr(fmt.Errorf("close expired idle conn: %w", closeErr))
			}
			p.mu.Lock()
			p.closing--
			p.cond.Signal()
			continue
		}

		if p.validate != nil {
			conn, panicErr, err := p.validateIdleConnLocked(ctx, entry.conn)
			if panicErr != nil {
				p.mu.Unlock()
				panic(panicErr)
			}
			if err != nil || conn != nil {
				return conn, err
			}
			continue
		}

		p.owned[entry.conn] = stateInUse
		p.inUse++
		return entry.conn, nil
	}
	return nil, nil
}

func (p *Pool) validateIdleConnLocked(ctx context.Context, conn *pooledConn) (net.Conn, any, error) {
	p.owned[conn] = stateClosing
	p.closing++
	p.mu.Unlock()
	panicked, panicValue, validateErr := p.callValidator(ctx, conn)
	p.mu.Lock()
	p.closing--

	if panicked {
		delete(p.owned, conn)
		p.closing++
		p.mu.Unlock()
		closeErr := conn.closeUnderlying()
		if closeErr != nil {
			p.addCloseErr(fmt.Errorf("close conn after validator panic: %w", closeErr))
		}
		p.mu.Lock()
		p.closing--
		p.cond.Broadcast()
		return nil, panicValue, nil
	}

	ctxErr := ctx.Err()
	switch {
	case validateErr != nil:
		delete(p.owned, conn)
		p.closing++
		p.mu.Unlock()
		closeErr := conn.closeUnderlying()
		if closeErr != nil {
			p.addCloseErr(fmt.Errorf("close invalid idle conn: %w", closeErr))
		}
		p.mu.Lock()
		p.closing--
		p.cond.Signal()
		return nil, nil, nil
	case ctxErr != nil:
		delete(p.owned, conn)
		p.closing++
		p.mu.Unlock()
		closeErr := conn.closeUnderlying()
		var wrapped error
		if closeErr != nil {
			wrapped = fmt.Errorf("close conn after acquire context cancellation: %w", closeErr)
			p.addCloseErr(wrapped)
		}
		p.mu.Lock()
		p.closing--
		p.cond.Signal()
		if wrapped != nil {
			return nil, nil, errors.Join(ctxErr, wrapped)
		}
		return nil, nil, ctxErr
	case p.closed:
		delete(p.owned, conn)
		p.closing++
		p.mu.Unlock()
		closeErr := conn.closeUnderlying()
		var wrapped error
		if closeErr != nil {
			wrapped = fmt.Errorf("close conn after pool close: %w", closeErr)
			p.addCloseErr(wrapped)
		}
		p.mu.Lock()
		p.closing--
		p.cond.Broadcast()
		if wrapped != nil {
			return nil, nil, errors.Join(ErrPoolClosed, wrapped)
		}
		return nil, nil, ErrPoolClosed
	case p.lifetimeExpired(conn, time.Now()):
		delete(p.owned, conn)
		p.closing++
		p.mu.Unlock()
		closeErr := conn.closeUnderlying()
		if closeErr != nil {
			p.addCloseErr(fmt.Errorf("close expired conn after validation: %w", closeErr))
		}
		p.mu.Lock()
		p.closing--
		p.cond.Signal()
		return nil, nil, nil
	default:
		p.owned[conn] = stateInUse
		p.inUse++
		return conn, nil, nil
	}
}

func (p *Pool) validateNewConnLocked(ctx context.Context, conn *pooledConn) (any, error) {
	p.mu.Unlock()
	panicked, panicValue, validateErr := p.callValidator(ctx, conn)
	p.mu.Lock()

	if panicked {
		_ = p.closeCreatedConnLocked(conn, nil, "close conn after validator panic")
		return panicValue, nil
	}
	if validateErr != nil {
		return nil, p.closeCreatedConnLocked(conn, validateErr, "close invalid created conn")
	}
	if err := ctx.Err(); err != nil {
		return nil, p.closeCreatedConnLocked(conn, err, "close conn after acquire context cancellation")
	}
	if p.closed {
		return nil, p.closeCreatedConnLocked(conn, ErrPoolClosed, "close conn after pool close")
	}
	return nil, nil
}

func (p *Pool) closeFactoryResultAfterErrorLocked(conn net.Conn, baseErr error) error {
	if isNilConn(conn) {
		return baseErr
	}
	p.mu.Unlock()
	closeErr := conn.Close()
	p.mu.Lock()
	if closeErr == nil {
		return baseErr
	}
	wrapped := fmt.Errorf("close factory connection after error: %w", closeErr)
	p.addCloseErr(wrapped)
	return errors.Join(baseErr, wrapped)
}

func (p *Pool) closeCreatedConnLocked(conn *pooledConn, baseErr error, message string) error {
	p.mu.Unlock()
	closeErr := conn.closeUnderlying()
	p.mu.Lock()
	if closeErr == nil {
		return baseErr
	}
	wrapped := fmt.Errorf("%s: %w", message, closeErr)
	p.addCloseErr(wrapped)
	return errors.Join(baseErr, wrapped)
}

// Release returns a connection to the pool.
func (p *Pool) Release(conn net.Conn) error {
	return p.release(conn, false)
}

// Discard removes a connection from the pool and closes it. Use Discard when a
// connection is known to be broken or has unread protocol state.
func (p *Pool) Discard(conn net.Conn) error {
	return p.release(conn, true)
}

func (p *Pool) release(conn net.Conn, discard bool) error {
	if conn == nil {
		return nil
	}

	pooled, ok := conn.(*pooledConn)
	if !ok || pooled.pool != p {
		return ErrReleaseForeignConn
	}

	p.mu.Lock()
	state, ok := p.owned[pooled]
	if !ok {
		p.mu.Unlock()
		return ErrReleaseForeignConn
	}

	switch state {
	case stateIdle:
		return p.releaseIdleLocked(pooled, discard)
	case stateClosing:
		p.mu.Unlock()
		return ErrReleaseNotInUse
	}

	p.owned[pooled] = stateClosing
	closed := p.closed
	if closed || discard {
		return p.closeInUseLocked(pooled, closed)
	}

	if p.reset != nil {
		panicErr, resetErr := p.resetInUseLocked(pooled)
		if panicErr != nil {
			panic(panicErr)
		}
		if resetErr != nil {
			return resetErr
		}
	}

	if p.closed {
		return p.closeInUseLocked(pooled, true)
	}
	if p.lifetimeExpired(pooled, time.Now()) {
		return p.closeInUseLocked(pooled, false)
	}

	p.owned[pooled] = stateIdle
	p.inUse--
	p.idle = append(p.idle, idleEntry{conn: pooled, releasedAt: time.Now()})
	p.cond.Signal()
	p.mu.Unlock()
	return nil
}

func (p *Pool) releaseIdleLocked(conn *pooledConn, discard bool) error {
	p.removeIdleLocked(conn)
	delete(p.owned, conn)
	p.closing++
	p.mu.Unlock()
	closeErr := conn.closeUnderlying()
	var wrapped error
	if closeErr != nil {
		wrapped = fmt.Errorf("close idle connection: %w", closeErr)
		p.addCloseErr(wrapped)
	}
	p.mu.Lock()
	p.closing--
	p.cond.Signal()
	p.mu.Unlock()

	if wrapped == nil {
		if discard {
			return nil
		}
		return ErrReleaseNotInUse
	}
	if discard {
		return wrapped
	}
	return errors.Join(ErrReleaseNotInUse, wrapped)
}

func (p *Pool) closeInUseLocked(conn *pooledConn, duringShutdown bool) error {
	p.mu.Unlock()
	closeErr := conn.closeUnderlying()
	var wrapped error
	if closeErr != nil {
		message := "close discarded connection"
		if duringShutdown {
			message = "close all connections during shutdown"
		}
		wrapped = fmt.Errorf("%s: %w", message, closeErr)
		p.addCloseErr(wrapped)
	}
	p.mu.Lock()
	delete(p.owned, conn)
	p.inUse--
	p.cond.Signal()
	p.mu.Unlock()
	return wrapped
}

func (p *Pool) resetInUseLocked(conn *pooledConn) (any, error) {
	p.mu.Unlock()
	panicked, panicValue, resetErr := p.callReset(conn)
	p.mu.Lock()

	if panicked {
		_ = p.closeInUseLocked(conn, p.closed)
		return panicValue, nil
	}
	if resetErr == nil {
		return nil, nil
	}

	p.mu.Unlock()
	closeErr := conn.closeUnderlying()
	var wrapped error
	if closeErr != nil {
		wrapped = fmt.Errorf("close reset-failed connection: %w", closeErr)
		p.addCloseErr(wrapped)
	}
	p.mu.Lock()
	delete(p.owned, conn)
	p.inUse--
	p.cond.Signal()
	p.mu.Unlock()

	if wrapped == nil {
		return nil, resetErr
	}
	return nil, errors.Join(resetErr, wrapped)
}

// Close shuts the pool down and waits until borrowed connections are released.
func (p *Pool) Close() error {
	return p.CloseContext(context.Background())
}

// CloseContext starts pool shutdown and waits until all borrowed connections and
// in-flight factory calls complete, or until ctx is canceled.
func (p *Pool) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.closeStartOnce.Do(func() {
		go p.doClose()
	})

	select {
	case <-p.fullyClosed:
		return p.getCloseErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) doClose() {
	p.mu.Lock()
	p.closed = true
	idle := p.detachIdleLocked()
	p.cond.Broadcast()
	p.mu.Unlock()

	for _, conn := range idle {
		if err := conn.closeUnderlying(); err != nil {
			p.addCloseErr(fmt.Errorf("close idle connections during shutdown: %w", err))
		}
	}

	close(p.sweeperStop)
	<-p.sweeperDone

	p.mu.Lock()
	for p.inUse > 0 || p.creating > 0 || p.closing > 0 {
		p.cond.Wait()
	}
	p.mu.Unlock()

	close(p.fullyClosed)
}

func (p *Pool) canCreateLocked() bool {
	return len(p.owned)+p.creating+p.closing < p.maxSize
}

func (p *Pool) idleExpired(entry idleEntry, now time.Time) bool {
	return p.idleTimeout > 0 && now.Sub(entry.releasedAt) > p.idleTimeout
}

func (p *Pool) lifetimeExpired(conn *pooledConn, now time.Time) bool {
	return p.maxLifetime > 0 && now.Sub(conn.createdAt) > p.maxLifetime
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

func (p *Pool) removeIdleLocked(conn *pooledConn) {
	for i := range p.idle {
		if p.idle[i].conn == conn {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return
		}
	}
}

func (p *Pool) detachIdleLocked() []*pooledConn {
	if len(p.idle) == 0 {
		return nil
	}

	out := make([]*pooledConn, 0, len(p.idle))
	for _, entry := range p.idle {
		if state, ok := p.owned[entry.conn]; ok && state == stateIdle {
			delete(p.owned, entry.conn)
			out = append(out, entry.conn)
		}
	}
	p.idle = nil
	return out
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

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var expired []*pooledConn
			now := time.Now()

			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				continue
			}

			kept := p.idle[:0]
			for _, entry := range p.idle {
				state, ok := p.owned[entry.conn]
				if !ok || state != stateIdle {
					continue
				}
				if p.idleExpired(entry, now) || p.lifetimeExpired(entry.conn, now) {
					delete(p.owned, entry.conn)
					expired = append(expired, entry.conn)
					continue
				}
				kept = append(kept, entry)
			}
			p.idle = kept
			if len(expired) > 0 {
				p.cond.Signal()
			}
			p.mu.Unlock()

			for _, conn := range expired {
				if err := conn.closeUnderlying(); err != nil {
					p.addCloseErr(fmt.Errorf("close expired conn in sweeper: %w", err))
				}
			}
		case <-p.sweeperStop:
			return
		}
	}
}

func (p *Pool) callFactory(ctx context.Context) (conn net.Conn, panicked bool, panicValue any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			panicValue = recovered
		}
	}()
	conn, err = p.factory(ctx)
	return conn, false, nil, err
}

func (p *Pool) callValidator(ctx context.Context, conn net.Conn) (panicked bool, panicValue any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			panicValue = recovered
		}
	}()
	return false, nil, p.validate(ctx, conn)
}

func (p *Pool) callReset(conn net.Conn) (panicked bool, panicValue any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			panicValue = recovered
		}
	}()
	return false, nil, p.reset(conn)
}

func isNilConn(conn net.Conn) bool {
	if conn == nil {
		return true
	}
	value := reflect.ValueOf(conn)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

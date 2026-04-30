package connpool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type simulatedConn struct {
	id         int
	closeCalls atomic.Int32
	closeErr   error
}

func (conn *simulatedConn) Read(_ []byte) (int, error) {
	return 0, nil
}

func (conn *simulatedConn) Write(_ []byte) (int, error) {
	return 0, nil
}

func (conn *simulatedConn) Close() error {
	conn.closeCalls.Add(1)
	return conn.closeErr
}

func (conn *simulatedConn) LocalAddr() net.Addr {
	return nil
}

func (conn *simulatedConn) RemoteAddr() net.Addr {
	return nil
}

func (conn *simulatedConn) SetDeadline(time.Time) error {
	return nil
}

func (conn *simulatedConn) SetReadDeadline(time.Time) error {
	return nil
}

func (conn *simulatedConn) SetWriteDeadline(time.Time) error {
	return nil
}

type nonComparableConn struct {
	data       []byte
	closeCalls *atomic.Int32
}

func (conn nonComparableConn) Read(_ []byte) (int, error) {
	return 0, nil
}

func (conn nonComparableConn) Write(_ []byte) (int, error) {
	return 0, nil
}

func (conn nonComparableConn) Close() error {
	conn.closeCalls.Add(1)
	return nil
}

func (conn nonComparableConn) LocalAddr() net.Addr {
	return nil
}

func (conn nonComparableConn) RemoteAddr() net.Addr {
	return nil
}

func (conn nonComparableConn) SetDeadline(time.Time) error {
	return nil
}

func (conn nonComparableConn) SetReadDeadline(time.Time) error {
	return nil
}

func (conn nonComparableConn) SetWriteDeadline(time.Time) error {
	return nil
}

func newFactory() (Factory, *atomic.Int32, *[]*simulatedConn) {
	var counter atomic.Int32
	var mu sync.Mutex
	conns := make([]*simulatedConn, 0)

	factory := func(context.Context) (net.Conn, error) {
		id := int(counter.Add(1))
		conn := &simulatedConn{id: id}
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		return conn, nil
	}

	return factory, &counter, &conns
}

func mustRelease(t *testing.T, pool *Pool, conn net.Conn) {
	t.Helper()
	if err := pool.Release(conn); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func mustClosePool(t *testing.T, pool *Pool) {
	t.Helper()
	if err := pool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func simulated(t *testing.T, conn net.Conn) *simulatedConn {
	t.Helper()
	underlying, ok := Underlying(conn).(*simulatedConn)
	if !ok {
		t.Fatalf("expected *simulatedConn, got %T", Underlying(conn))
	}
	return underlying
}

func TestAcquireBlocksAndRespectsContext(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(2, 0, factory)
	defer mustClosePool(t, pool)

	ctx := context.Background()
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	timedCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = pool.Acquire(timedCtx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("acquire returned too fast (%v)", elapsed)
	}

	mustRelease(t, pool, conn1)
	mustRelease(t, pool, conn2)
}

func TestAcquirePassesContextToFactory(t *testing.T) {
	t.Parallel()

	factory := func(ctx context.Context) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := pool.Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline from factory, got %v", err)
	}
}

func TestAcquireClosesFactoryConnWhenContextExpiresAfterCreate(t *testing.T) {
	t.Parallel()

	created := make(chan *simulatedConn, 1)
	factory := func(ctx context.Context) (net.Conn, error) {
		<-ctx.Done()
		conn := &simulatedConn{id: 1}
		created <- conn
		return conn, nil
	}
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := pool.Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
	conn := <-created
	if conn.closeCalls.Load() == 0 {
		t.Fatal("created connection was not closed after context cancellation")
	}
}

func TestAcquireWakesOnRelease(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan net.Conn, 1)
	go func() {
		conn, acquireErr := pool.Acquire(context.Background())
		if acquireErr != nil {
			t.Errorf("background acquire: %v", acquireErr)
			return
		}
		got <- conn
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("background acquire returned before release")
	default:
	}

	mustRelease(t, pool, conn1)

	select {
	case conn2 := <-got:
		if simulated(t, conn2).id != simulated(t, conn1).id {
			t.Fatalf("expected conn id=%d, got id=%d", simulated(t, conn1).id, simulated(t, conn2).id)
		}
		mustRelease(t, pool, conn2)
	case <-time.After(time.Second):
		t.Fatal("background acquire did not wake")
	}
}

func TestIdleTimeoutExpiresConn(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(2, 30*time.Millisecond, factory)
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, pool, conn1)

	time.Sleep(80 * time.Millisecond)

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatalf("got expired conn id=%d", simulated(t, conn1).id)
	}
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("expired conn was not closed")
	}
	mustRelease(t, pool, conn2)
}

func TestCloseSemantics(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(3, 0, factory)

	inflight, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	idleConn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, pool, idleConn)

	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- pool.Close()
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if simulated(t, idleConn).closeCalls.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if simulated(t, idleConn).closeCalls.Load() == 0 {
		t.Fatal("idle conn not closed promptly")
	}
	if simulated(t, inflight).closeCalls.Load() != 0 {
		t.Fatal("in-flight conn closed before release")
	}

	select {
	case err := <-closeReturned:
		t.Fatalf("close returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}

	mustRelease(t, pool, inflight)

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not return")
	}

	if simulated(t, inflight).closeCalls.Load() == 0 {
		t.Fatal("in-flight conn not closed after release")
	}

	mustClosePool(t, pool)
}

func TestCloseContextCanTimeOutAndCloseLater(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := pool.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected close context deadline, got %v", err)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected pool to be closed after CloseContext, got %v", err)
	}

	mustRelease(t, pool, conn)
	mustClosePool(t, pool)
}

func TestAcquireNilContext(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	var nilContext context.Context
	conn, err := pool.Acquire(nilContext)
	if err != nil {
		t.Fatalf("acquire with nil context failed: %v", err)
	}
	mustRelease(t, pool, conn)
}

func TestFactoryErrorDoesNotLeakCapacity(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("dial failed")
		}
		return &simulatedConn{id: 1}, nil
	}

	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("expected factory error on first acquire")
	}

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire should succeed, got %v", err)
	}
	mustRelease(t, pool, conn)
}

func TestFactoryNilDoesNotLeakCapacity(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		switch calls.Add(1) {
		case 1:
			return nil, nil
		case 2:
			var conn *simulatedConn
			return conn, nil
		default:
			return &simulatedConn{id: 3}, nil
		}
	}

	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrFactoryReturnedNilConn) {
		t.Fatalf("expected nil conn error, got %v", err)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrFactoryReturnedNilConn) {
		t.Fatalf("expected typed nil conn error, got %v", err)
	}

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("expected capacity to recover after nil factory result, got %v", err)
	}
	mustRelease(t, pool, conn)
}

func TestNonComparableConnImplementation(t *testing.T) {
	t.Parallel()

	var closeCalls atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		return nonComparableConn{data: []byte{1}, closeCalls: &closeCalls}, nil
	}

	pool := NewPool(1, 0, factory)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire non-comparable conn: %v", err)
	}
	mustRelease(t, pool, conn)
	mustClosePool(t, pool)

	if closeCalls.Load() == 0 {
		t.Fatal("non-comparable underlying conn was not closed")
	}
}

func TestCloseRaceWithSlowFactory(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	unblock := make(chan struct{})
	created := make(chan *simulatedConn, 1)

	factory := func(context.Context) (net.Conn, error) {
		close(started)
		<-unblock
		conn := &simulatedConn{id: 1}
		created <- conn
		return conn, nil
	}

	pool := NewPool(1, 0, factory)

	errCh := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(context.Background())
		errCh <- err
	}()

	<-started
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- pool.Close()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := pool.Acquire(ctx)
		cancel()
		if errors.Is(err, ErrPoolClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool did not transition to closed state in time")
		}
	}

	close(unblock)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not return")
	}

	conn := <-created
	if conn.closeCalls.Load() == 0 {
		t.Fatal("factory conn was not closed during close-race")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
}

func TestReleaseMisuseSafety(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(2, 0, factory)
	defer mustClosePool(t, pool)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, pool, conn)

	foreign := &simulatedConn{id: 999}

	tests := []struct {
		name   string
		action func()
		verify func(t *testing.T)
	}{
		{
			name: "release nil is noop",
			action: func() {
				if err := pool.Release(nil); err != nil {
					t.Fatalf("release nil: %v", err)
				}
			},
			verify: func(t *testing.T) {},
		},
		{
			name: "release foreign does not close foreign",
			action: func() {
				err := pool.Release(foreign)
				if !errors.Is(err, ErrReleaseForeignConn) {
					t.Fatalf("expected ErrReleaseForeignConn, got %v", err)
				}
			},
			verify: func(t *testing.T) {
				if foreign.closeCalls.Load() != 0 {
					t.Fatal("foreign connection was closed")
				}
			},
		},
		{
			name: "double release closes idle conn and keeps pool usable",
			action: func() {
				err := pool.Release(conn)
				if !errors.Is(err, ErrReleaseNotInUse) {
					t.Fatalf("expected ErrReleaseNotInUse, got %v", err)
				}
			},
			verify: func(t *testing.T) {
				if simulated(t, conn).closeCalls.Load() == 0 {
					t.Fatal("double released conn was not closed")
				}
				conn2, err := pool.Acquire(context.Background())
				if err != nil {
					t.Fatalf("pool unusable after double release: %v", err)
				}
				mustRelease(t, pool, conn2)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.action()
			test.verify(t)
		})
	}
}

func TestDiscardClosesAndReplacesConn(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Discard(conn1); err != nil {
		t.Fatalf("discard: %v", err)
	}
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("discarded conn was not closed")
	}

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatal("discarded conn was reused")
	}
	mustRelease(t, pool, conn2)
}

func TestConnCloseDiscardsFromPool(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory)
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn1.Close(); err != nil {
		t.Fatalf("conn close: %v", err)
	}
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("conn close did not close underlying connection")
	}

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatal("closed conn was reused")
	}
	mustRelease(t, pool, conn2)
}

func TestReleasePropagatesCloseErrorDuringShutdown(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	factory := func(context.Context) (net.Conn, error) {
		return &simulatedConn{id: 1, closeErr: closeErr}, nil
	}

	pool := NewPool(1, 0, factory)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- pool.Close()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, acquireErr := pool.Acquire(ctx)
		cancel()
		if errors.Is(acquireErr, ErrPoolClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool did not transition to closed state in time")
		}
	}

	err = pool.Release(conn)
	if err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("expected release close error, got %v", err)
	}

	closeReturnedErr := <-closeDone
	if closeReturnedErr == nil || !errors.Is(closeReturnedErr, closeErr) {
		t.Fatalf("expected close aggregated error, got %v", closeReturnedErr)
	}
}

func TestClosePropagatesIdleCloseErrors(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("idle close failed")
	factory := func(context.Context) (net.Conn, error) {
		return &simulatedConn{id: 1, closeErr: closeErr}, nil
	}

	pool := NewPool(1, 0, factory)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := pool.Release(conn); err != nil {
		t.Fatalf("release: %v", err)
	}

	err = pool.Close()
	if err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("expected idle close error, got %v", err)
	}
}

func TestResetFailureDiscardsConn(t *testing.T) {
	t.Parallel()

	resetErr := errors.New("reset failed")
	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory, WithReset(func(net.Conn) error {
		return resetErr
	}))
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Release(conn1); !errors.Is(err, resetErr) {
		t.Fatalf("expected reset error, got %v", err)
	}
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("reset-failed connection was not closed")
	}

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatal("reset-failed connection was reused")
	}
	_ = pool.Discard(conn2)
}

func TestValidatorRejectsIdleConnAndReplacesIt(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	var validateCalls atomic.Int32
	pool := NewPool(1, 0, factory, WithValidator(func(_ context.Context, conn net.Conn) error {
		if validateCalls.Add(1) == 2 {
			return errors.New("stale")
		}
		return nil
	}))
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, pool, conn1)

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatal("validator-rejected idle conn was reused")
	}
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("validator-rejected idle conn was not closed")
	}
	mustRelease(t, pool, conn2)
}

func TestMaxLifetimeRetiresConn(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(1, 0, factory, WithMaxLifetime(time.Millisecond))
	defer mustClosePool(t, pool)

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	mustRelease(t, pool, conn1)
	if simulated(t, conn1).closeCalls.Load() == 0 {
		t.Fatal("max-lifetime connection was not closed on release")
	}

	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if simulated(t, conn2).id == simulated(t, conn1).id {
		t.Fatal("max-lifetime connection was reused")
	}
	mustRelease(t, pool, conn2)
}

func TestStatsReportsSnapshot(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(2, 0, factory)
	defer mustClosePool(t, pool)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	stats := pool.Stats()
	if stats.MaxSize != 2 || stats.InUse != 1 || stats.Idle != 0 || stats.Closed {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	mustRelease(t, pool, conn)
}

func TestConcurrentStress(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	pool := NewPool(8, 50*time.Millisecond, factory)
	defer mustClosePool(t, pool)

	const goroutines = 50
	const ops = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range ops {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				conn, err := pool.Acquire(ctx)
				cancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrPoolClosed) {
						continue
					}
					t.Errorf("acquire: %v", err)
					return
				}
				mustRelease(t, pool, conn)
			}
		}()
	}

	wg.Wait()
}

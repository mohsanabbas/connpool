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

// simulatedConn satisfies net.Conn and tracks close calls.
type simulatedConn struct {
	id         int
	closeCalls atomic.Int32
	closeErr   error
}

func (c *simulatedConn) Read(_ []byte) (int, error) {
	return 0, nil
}

func (c *simulatedConn) Write(_ []byte) (int, error) {
	return 0, nil
}

func (c *simulatedConn) Close() error {
	c.closeCalls.Add(1)
	return c.closeErr
}

func (c *simulatedConn) LocalAddr() net.Addr {
	return nil
}

func (c *simulatedConn) RemoteAddr() net.Addr {
	return nil
}

func (c *simulatedConn) SetDeadline(time.Time) error {
	return nil
}

func (c *simulatedConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *simulatedConn) SetWriteDeadline(time.Time) error {
	return nil
}

func newFactory() (func() (net.Conn, error), *atomic.Int32, *[]*simulatedConn) {
	var counter atomic.Int32
	var mu sync.Mutex
	conns := make([]*simulatedConn, 0)

	factory := func() (net.Conn, error) {
		id := int(counter.Add(1))
		c := &simulatedConn{
			id: id,
		}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		return c, nil
	}

	return factory, &counter, &conns
}

func mustRelease(t *testing.T, p *Pool, conn net.Conn) {
	t.Helper()
	if err := p.Release(conn); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func mustClosePool(t *testing.T, p *Pool) {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestAcquireBlocksAndRespectsContext(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(2, 0, factory)
	defer mustClosePool(t, p)

	ctx := context.Background()
	c1, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	c2, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	tctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = p.Acquire(tctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("acquire returned too fast (%v)", elapsed)
	}

	mustRelease(t, p, c1)
	mustRelease(t, p, c2)
}

func TestAcquireWakesOnRelease(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(1, 0, factory)
	defer mustClosePool(t, p)

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan net.Conn, 1)
	go func() {
		c, err := p.Acquire(context.Background())
		if err != nil {
			t.Errorf("background acquire: %v", err)
			return
		}
		got <- c
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("background acquire returned before release")
	default:
	}

	mustRelease(t, p, c1)

	select {
	case c2 := <-got:
		if c2.(*simulatedConn).id != c1.(*simulatedConn).id {
			t.Fatalf("expected conn id=%d, got id=%d", c1.(*simulatedConn).id, c2.(*simulatedConn).id)
		}
		mustRelease(t, p, c2)
	case <-time.After(time.Second):
		t.Fatal("background acquire did not wake")
	}
}

func TestIdleTimeoutExpiresConn(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(2, 30*time.Millisecond, factory)
	defer mustClosePool(t, p)

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, p, c1)

	time.Sleep(80 * time.Millisecond)

	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2.(*simulatedConn).id == c1.(*simulatedConn).id {
		t.Fatalf("got expired conn id=%d", c1.(*simulatedConn).id)
	}
	if c1.(*simulatedConn).closeCalls.Load() == 0 {
		t.Fatal("expired conn was not closed")
	}
	mustRelease(t, p, c2)
}

func TestCloseSemantics(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(3, 0, factory)

	inflight, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	idleConn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, p, idleConn)

	closeReturned := make(chan struct{})
	go func() {
		p.Close()
		close(closeReturned)
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if idleConn.(*simulatedConn).closeCalls.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if idleConn.(*simulatedConn).closeCalls.Load() == 0 {
		t.Fatal("idle conn not closed promptly")
	}
	if inflight.(*simulatedConn).closeCalls.Load() != 0 {
		t.Fatal("in-flight conn closed before release")
	}

	select {
	case <-closeReturned:
		t.Fatal("close returned before release")
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := p.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}

	mustRelease(t, p, inflight)

	select {
	case <-closeReturned:
	case <-time.After(time.Second):
		t.Fatal("close did not return")
	}

	if inflight.(*simulatedConn).closeCalls.Load() == 0 {
		t.Fatal("in-flight conn not closed after release")
	}

	mustClosePool(t, p)
}

func TestAcquireNilContext(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(1, 0, factory)
	defer mustClosePool(t, p)

	//nolint:staticcheck // Intentional nil context to verify fallback to Background.
	c, err := p.Acquire(nil)
	if err != nil {
		t.Fatalf("acquire with nil context failed: %v", err)
	}
	mustRelease(t, p, c)
}

func TestFactoryErrorDoesNotLeakCapacity(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	factory := func() (net.Conn, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("dial failed")
		}
		return (&simulatedConn{id: 1}), nil
	}

	p := NewPool(1, 0, factory)
	defer mustClosePool(t, p)

	if _, err := p.Acquire(context.Background()); err == nil {
		t.Fatal("expected factory error on first acquire")
	}

	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire should succeed, got %v", err)
	}
	mustRelease(t, p, c)
}

func TestCloseRaceWithSlowFactory(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	unblock := make(chan struct{})
	created := make(chan *simulatedConn, 1)

	factory := func() (net.Conn, error) {
		close(started)
		<-unblock
		c := &simulatedConn{id: 1}
		created <- c
		return c, nil
	}

	p := NewPool(1, 0, factory)

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(context.Background())
		errCh <- err
	}()

	<-started
	closeDone := make(chan struct{})
	go func() {
		p.Close()
		close(closeDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := p.Acquire(ctx)
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

	c := <-created
	if c.closeCalls.Load() == 0 {
		t.Fatal("factory conn was not closed during close-race")
	}

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
}

func TestReleaseMisuseSafety(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(2, 0, factory)
	defer p.Close()

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustRelease(t, p, conn)

	foreign := &simulatedConn{id: 999}

	tests := []struct {
		name   string
		action func()
		verify func(t *testing.T)
	}{
		{
			name: "release nil is noop",
			action: func() {
				if err := p.Release(nil); err != nil {
					t.Fatalf("release nil: %v", err)
				}
			},
			verify: func(t *testing.T) {},
		},
		{
			name: "release foreign closes foreign",
			action: func() {
				err := p.Release(foreign)
				if !errors.Is(err, ErrReleaseForeignConn) {
					t.Fatalf("expected ErrReleaseForeignConn, got %v", err)
				}
			},
			verify: func(t *testing.T) {
				if foreign.closeCalls.Load() == 0 {
					t.Fatal("foreign connection was not closed")
				}
			},
		},
		{
			name: "double release closes conn and keeps pool usable",
			action: func() {
				err := p.Release(conn)
				if !errors.Is(err, ErrReleaseNotInUse) {
					t.Fatalf("expected ErrReleaseNotInUse, got %v", err)
				}
			},
			verify: func(t *testing.T) {
				if conn.(*simulatedConn).closeCalls.Load() == 0 {
					t.Fatal("double released conn was not closed")
				}
				c2, err := p.Acquire(context.Background())
				if err != nil {
					t.Fatalf("pool unusable after double release: %v", err)
				}
				mustRelease(t, p, c2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.action()
			tt.verify(t)
		})
	}
}

func TestReleasePropagatesCloseErrorDuringShutdown(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	factory := func() (net.Conn, error) {
		return &simulatedConn{id: 1, closeErr: closeErr}, nil
	}

	p := NewPool(1, 0, factory)

	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- p.Close()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, aerr := p.Acquire(ctx)
		cancel()
		if errors.Is(aerr, ErrPoolClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool did not transition to closed state in time")
		}
	}

	err = p.Release(c)
	if err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("expected release close error, got %v", err)
	}

	cerr := <-closeDone
	if cerr == nil || !errors.Is(cerr, closeErr) {
		t.Fatalf("expected close aggregated error, got %v", cerr)
	}
}

func TestClosePropagatesIdleCloseErrors(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("idle close failed")
	factory := func() (net.Conn, error) {
		return &simulatedConn{id: 1, closeErr: closeErr}, nil
	}

	p := NewPool(1, 0, factory)

	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := p.Release(c); err != nil {
		t.Fatalf("release: %v", err)
	}

	err = p.Close()
	if err == nil || !errors.Is(err, closeErr) {
		t.Fatalf("expected idle close error, got %v", err)
	}
}

func TestConcurrentStress(t *testing.T) {
	t.Parallel()

	factory, _, _ := newFactory()
	p := NewPool(8, 50*time.Millisecond, factory)
	defer mustClosePool(t, p)

	const goroutines = 50
	const ops = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				c, err := p.Acquire(ctx)
				cancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrPoolClosed) {
						continue
					}
					t.Errorf("acquire: %v", err)
					return
				}
				mustRelease(t, p, c)
			}
		}()
	}

	wg.Wait()
}

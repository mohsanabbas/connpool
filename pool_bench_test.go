package connpool

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
)

func benchmarkFactory() func() (net.Conn, error) {
	return func() (net.Conn, error) {
		return &simulatedConn{}, nil
	}
}

func BenchmarkPoolAcquireRelease(b *testing.B) {
	cases := []struct {
		name    string
		maxSize int
		procs   int
	}{
		{name: "max1-procs1", maxSize: 1, procs: 1},
		{name: "max4-procs4", maxSize: 4, procs: 4},
		{name: "max16-procs8", maxSize: 16, procs: 8},
		{name: "max64-procs16", maxSize: 64, procs: 16},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			old := runtime.GOMAXPROCS(tc.procs)
			defer runtime.GOMAXPROCS(old)

			p := NewPool(tc.maxSize, 0, benchmarkFactory())
			defer func() {
				if err := p.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}
			}()

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c, err := p.Acquire(ctx)
					if err != nil {
						b.Fatalf("acquire: %v", err)
					}
					if err := p.Release(c); err != nil {
						b.Fatalf("release: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkPoolLockContentionByParallelism(b *testing.B) {
	parLevels := []int{1, 2, 4, 8, 16, 32}

	for _, level := range parLevels {
		level := level
		b.Run(fmt.Sprintf("parallel-%d", level), func(b *testing.B) {
			p := NewPool(8, 0, benchmarkFactory())
			defer func() {
				if err := p.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}
			}()

			ctx := context.Background()
			b.SetParallelism(level)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c, err := p.Acquire(ctx)
					if err != nil {
						b.Fatalf("acquire: %v", err)
					}
					if err := p.Release(c); err != nil {
						b.Fatalf("release: %v", err)
					}
				}
			})
		})
	}
}

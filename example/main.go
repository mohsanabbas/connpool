package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/mohsanabbas/connpool"
)

const (
	poolSize    = 5
	idleTimeout = 30 * time.Second
	maxLifetime = 5 * time.Minute
	opTimeout   = 3 * time.Second
)

// valkeyAddr is the Valkey server address.
var valkeyAddr = func() string {
	if v := os.Getenv("VALKEY_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}()

func main() {
	pool := connpool.NewPool(poolSize, idleTimeout, dialValkey,
		connpool.WithMaxLifetime(maxLifetime),
		connpool.WithValidator(ping),
	)
	defer shutdown(pool)

	log.Printf("stats: %+v", pool.Stats())

	log.Println("--- wave 1: concurrent (10 workers, pool max 5) ---")
	runConcurrent(pool, 10, "worker")
	log.Printf("stats: %+v", pool.Stats())

	log.Println("--- wave 2: sequential reuse proof ---")
	for i := range 5 {
		doWork(pool, i, "reuse")
	}
	log.Printf("stats: %+v", pool.Stats())
}

func dialValkey(ctx context.Context) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", valkeyAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("dialed new connection %s", conn.LocalAddr())
	return conn, nil
}

func ping(_ context.Context, conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return err
	}
	var buf [7]byte // "+PONG\r\n"
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

func shutdown(pool *connpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.CloseContext(ctx); err != nil {
		log.Printf("pool shutdown: %v", err)
	}
}

func runConcurrent(pool *connpool.Pool, n int, prefix string) {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(id int) {
			defer wg.Done()
			doWork(pool, id, prefix)
		}(i)
	}
	wg.Wait()
}

func doWork(pool *connpool.Pool, id int, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("[%s-%d] acquire: %v", prefix, id, err)
		return
	}

	port := connpool.Underlying(conn).LocalAddr().String()
	key := fmt.Sprintf("connpool:%s:%d", prefix, id)
	val := fmt.Sprintf("value-%d", id)

	if err := redisSet(conn, key, val); err != nil {
		log.Printf("[%s-%d] port=%-22s SET: %v", prefix, id, port, err)
		_ = pool.Discard(conn)
		return
	}

	got, err := redisGet(conn, key)
	if err != nil {
		log.Printf("[%s-%d] port=%-22s GET: %v", prefix, id, port, err)
		_ = pool.Discard(conn)
		return
	}

	log.Printf("[%s-%d] port=%-22s SET %s=%q  GET=%q", prefix, id, port, key, val, got)

	if err := pool.Release(conn); err != nil {
		log.Printf("[%s-%d] release: %v", prefix, id, err)
	}
}

// redisSet issues SET key value.
func redisSet(conn net.Conn, key, value string) error {
	req := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)
	if _, err := io.WriteString(conn, req); err != nil {
		return err
	}
	var buf [5]byte // "+OK\r\n"
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return err
	}
	if string(buf[:]) != "+OK\r\n" {
		return fmt.Errorf("unexpected SET response: %q", string(buf[:]))
	}
	return nil
}

// redisGet issues GET key and returns the bulk-string value.
func redisGet(conn net.Conn, key string) (string, error) {
	req := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	if _, err := io.WriteString(conn, req); err != nil {
		return "", err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n') // "$<n>\r\n"
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[0] != '$' {
		return "", fmt.Errorf("unexpected GET response: %q", line)
	}
	var n int
	if _, err := fmt.Sscanf(line[1:], "%d", &n); err != nil {
		return "", fmt.Errorf("parse bulk length %q: %w", line, err)
	}
	buf := make([]byte, n+2) // value + "\r\n"
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

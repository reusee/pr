package pr

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reusee/e4"
)

func TestConsume(
	t *testing.T,
) {

	t.Run("normal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)
		var numPut int64
		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		if c != 8 {
			t.Fatalf("got %d", c)
		}
		if c != numPut {
			t.Fatalf("got %d, %d", c, numPut)
		}
	})

	t.Run("multiple wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)

		var numPut int64
		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		if err := wait(false); err != nil {
			t.Fatal(err)
		}
		if c != 8 {
			t.Fatalf("got %d", c)
		}
		if c != numPut {
			t.Fatalf("got %d, %d", c, numPut)
		}

		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		if err := wait(false); err != nil {
			t.Fatal(err)
		}
		if c != 16 {
			t.Fatalf("got %d", 16)
		}
		if c != numPut {
			t.Fatalf("got %d, %d", c, numPut)
		}
	})

	t.Run("cancel before put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)
		cancel()
		var numPut int
		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		if c != 0 {
			t.Fatalf("got %d", c)
		}
		if numPut != 0 {
			t.Fatalf("got %d", numPut)
		}
	})

	t.Run("close before put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		var numPut int64
		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		if c != 0 {
			t.Fatalf("got %d", c)
		}
		if c != numPut {
			t.Fatalf("got %d", numPut)
		}
	})

	t.Run("concurrent put and close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)
		var numPut int64
		wg := new(sync.WaitGroup)
		for i := 0; i < 128; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				if put(i) {
					atomic.AddInt64(&numPut, 1)
				}
			}()
		}
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if a, b := atomic.LoadInt64(&numPut), atomic.LoadInt64(&c); a != b {
			t.Fatalf("got %d, %d", a, b)
		}
	})

	t.Run("concurrent put and cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				return nil
			},
		)
		var numPut int64
		wg := new(sync.WaitGroup)
		for i := 0; i < 128; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				if put(i) {
					atomic.AddInt64(&numPut, 1)
				}
			}()
		}
		cancel()
		if err := wait(true); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if a, b := atomic.LoadInt64(&numPut), atomic.LoadInt64(&c); a != b {
			t.Fatalf("got %d, %d", a, b)
		}
	})

	t.Run("fn error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				if v.(int) == 3 {
					return errors.New("foo")
				}
				return nil
			},
		)
		var numPut int64
		for i := 0; i < 8; i++ {
			if put(i) {
				numPut++
			}
		}
		err := wait(true)
		if err == nil {
			t.Fatal()
		}
		if err.Error() != "foo" {
			t.Fatalf("got %v", err)
		}
		if numPut != c {
			t.Fatalf("got %d, %d", numPut, c)
		}
		time.Sleep(time.Millisecond * 10)
	})

	t.Run("multiple fn error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		put, wait := Consume(
			ctx,
			128,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				if v.(int) > 1 {
					return errors.New("foo")
				}
				return nil
			},
		)
		var numPut int64
		for i := 0; i < 128; i++ {
			if put(i) {
				numPut++
			}
		}
		err := wait(true)
		if err == nil {
			t.Fatal()
		}
		if err.Error() != "foo" {
			t.Fatalf("got %v", err)
		}
		if numPut != c {
			t.Fatalf("got %d, %d", numPut, c)
		}
	})

	t.Run("put in func", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c int64
		var put Put
		put, wait := Consume(
			ctx,
			8,
			func(i int, v any) error {
				atomic.AddInt64(&c, 1)
				n := v.(int)
				if n == 0 {
					return nil
				}
				put(n - 1)
				return nil
			},
		)
		put(2048)
		if err := wait(false); err != nil {
			t.Fatal(err)
		}
		if c != 2049 {
			t.Fatalf("got %d", c)
		}
	})

}

func TestConsumeE4Error(t *testing.T) {
	put, wait := Consume(context.Background(), 8, func(_ int, v any) error {
		return v.(func() error)()
	})
	put(func() error {
		return func() error {
			e4.Check(io.EOF)
			return nil
		}()
	})
	err := wait(true)
	if !errors.Is(err, io.EOF) {
		t.Fatal()
	}
}

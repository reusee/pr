package pr

//TODO use wait tree

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/reusee/e4"
)

type Put = func(any) bool

type Wait = func(noMorePut bool) error

type ConsumeOption interface {
	IsConsumeOption()
}

type BacklogSize int

func (_ BacklogSize) IsConsumeOption() {}

func Consume(
	ctx context.Context,
	numThread int,
	fn func(threadID int, value any) error,
	options ...ConsumeOption,
) (
	put Put,
	wait Wait,
) {

	backlogSize := int(math.MaxInt32)

	for _, option := range options {
		switch option := option.(type) {
		case BacklogSize:
			backlogSize = int(option)
		default:
			panic(fmt.Errorf("unknown option: %T", option))
		}
	}

	inCh := make(chan any)
	outCh := make(chan any)
	threadWaitGroup := new(sync.WaitGroup)
	errCh := make(chan error, 1)
	valueCond := sync.NewCond(new(sync.Mutex))
	numValue := 0

	threadWaitGroup.Add(1)
	go func() {
		defer threadWaitGroup.Done()
		values := list.New()
		var c chan any
	loop:
		for {

			c = inCh
			if values.Len() > backlogSize {
				c = nil
			}

			if values.Len() > 0 {
				select {

				case outCh <- values.Front().Value:
					values.Remove(values.Front())

				case v, ok := <-c:
					if !ok {
						break loop
					}
					values.PushBack(v)

				case <-ctx.Done():
					break loop

				}

			} else {
				select {

				case v, ok := <-c:
					if !ok {
						break loop
					}
					select {
					case outCh <- v:
					default:
						values.PushBack(v)
					}

				case <-ctx.Done():
					break loop

				}
			}

		}

		elem := values.Front()
		for elem != nil {
			outCh <- elem.Value
			elem = elem.Next()
		}

		close(outCh)

	}()

	var putLock sync.RWMutex
	putClosed := false
	put = func(v any) bool {

		putLock.RLock()
		defer putLock.RUnlock()

		if putClosed {
			return false
		}

		if len(errCh) > 0 {
			return false
		}

		select {

		case inCh <- v:
			valueCond.L.Lock()
			numValue++
			n := numValue
			valueCond.L.Unlock()
			if n == 0 {
				valueCond.Signal()
			}
			return true

		case <-ctx.Done():
			return false

		}
	}

	var closeOnce sync.Once
	closePut := func() {
		closeOnce.Do(func() {
			putLock.Lock()
			defer putLock.Unlock()
			putClosed = true
			close(inCh)
		})
	}

	wait = func(noMorePut bool) error {

		if noMorePut {
			closePut()
			threadWaitGroup.Wait()
		}

		valueCond.L.Lock()
		for numValue != 0 {
			valueCond.Wait()
		}
		valueCond.L.Unlock()

		select {
		case err := <-errCh:
			return err
		default:
		}

		return nil
	}

	for i := 0; i < numThread; i++ {
		i := i
		threadWaitGroup.Add(1)

		go func() {
			defer threadWaitGroup.Done()

			for v := range outCh {
				err := func() (err error) {
					defer e4.Handle(&err)
					return fn(i, v)
				}()
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
				valueCond.L.Lock()
				numValue--
				n := numValue
				valueCond.L.Unlock()
				if n == 0 {
					valueCond.Signal()
				}
			}

		}()

	}

	return

}

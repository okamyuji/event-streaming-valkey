package consumer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/consumer"
)

type runnerFunc func(context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

func waitDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRunAllは停止の合図で全員が止まればnilを返す(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(10*time.Millisecond, cancel)

	err := consumer.RunAll(ctx, []consumer.Runner{runnerFunc(waitDone), runnerFunc(waitDone)})

	if err != nil {
		t.Fatalf("nil のはずが %v", err)
	}
}

func TestRunAllは1つが失敗したら残りを止めてその失敗を返す(t *testing.T) {
	boom := errors.New("partition 1 が壊れた")
	stopped := make(chan struct{})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := consumer.RunAll(ctx, []consumer.Runner{
		runnerFunc(func(ctx context.Context) error {
			err := waitDone(ctx)
			close(stopped)
			return err
		}),
		runnerFunc(func(context.Context) error { return boom }),
	})

	if !errors.Is(err, boom) {
		t.Fatalf("失敗を返すはずが %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("残りの Runner が止まっていない")
	}
}

func TestRunAllは失敗せずに戻ったRunnerも止まった扱いにする(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := consumer.RunAll(ctx, []consumer.Runner{
		runnerFunc(func(context.Context) error { return nil }),
		runnerFunc(waitDone),
	})

	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("予期しない停止として返すはずが %v", err)
	}
}

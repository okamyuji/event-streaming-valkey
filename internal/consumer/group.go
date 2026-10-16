package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Runner 止まるまで動き続ける処理。Worker が満たす。
type Runner interface {
	Run(ctx context.Context) error
}

var errStopped = errors.New("停止の合図がないのに止まった")

// RunAll パーティションごとの Runner を並べて動かす。1つでも止まったら残りも止め、プロセスごと再起動に任せる。
func RunAll(ctx context.Context, runners []Runner) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for i, r := range runners {
		wg.Go(func() {
			err := r.Run(ctx)
			if err == nil {
				err = errStopped
			}
			if ctx.Err() == nil {
				cancel(fmt.Errorf("runner %d: %w", i, err))
			}
		})
	}
	wg.Wait()
	cause := context.Cause(ctx)
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return nil
	}
	return cause
}

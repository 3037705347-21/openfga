package tuplesync

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// parallelMap applies fn to every input with bounded concurrency, preserving
// input order in the result. The first error cancels the group.
func parallelMap[In, Out any](ctx context.Context, limit int, in []In, fn func(context.Context, In) (Out, error)) ([]Out, error) {
	if limit < 1 {
		limit = 1
	}
	out := make([]Out, len(in))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for i := range in {
		i := i
		g.Go(func() error {
			res, err := fn(gctx, in[i])
			if err != nil {
				return err
			}
			out[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

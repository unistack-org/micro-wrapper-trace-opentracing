package opentracing

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/unistack-org/micro/v3/metadata"
	"golang.org/x/sync/errgroup"
)

func TestStartSpanFromIncomingContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	md := metadata.New(2)
	md.Set("key", "val")

	g, gctx := errgroup.WithContext(metadata.NewIncomingContext(ctx, md))

	tracer := opentracing.GlobalTracer()

	for i := 0; i < 8000; i++ {
		g.Go(func() error {
			_, sp, err := StartSpanFromIncomingContext(gctx, tracer, "test")
			if err != nil {
				t.Fatal(err)
			}
			sp.Finish()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

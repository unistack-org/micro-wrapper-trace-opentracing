// Package opentracing provides wrappers for OpenTracing
package opentracing

import (
	"context"
	"fmt"
	"net/textproto"

	opentracing "github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/unistack-org/micro/v3/client"
	"github.com/unistack-org/micro/v3/metadata"
	"github.com/unistack-org/micro/v3/server"
)

var (
	// MetadataKey contains metadata key
	MetadataKey = textproto.CanonicalMIMEHeaderKey("x-request-id")
)

type otWrapper struct {
	opts             Options
	serverHandler    server.HandlerFunc
	serverSubscriber server.SubscriberFunc
	clientCallFunc   client.CallFunc
	client.Client
}

type ClientCallObserver func(context.Context, client.Request, interface{}, []client.CallOption, opentracing.Span, error)
type ClientStreamObserver func(context.Context, client.Request, []client.CallOption, client.Stream, opentracing.Span, error)
type ClientPublishObserver func(context.Context, client.Message, []client.PublishOption, opentracing.Span, error)
type ClientCallFuncObserver func(context.Context, string, client.Request, interface{}, client.CallOptions, opentracing.Span, error)
type ServerHandlerObserver func(context.Context, server.Request, interface{}, opentracing.Span, error)
type ServerSubscriberObserver func(context.Context, server.Message, opentracing.Span, error)

type Options struct {
	Tracer                    opentracing.Tracer
	ClientCallObservers       []ClientCallObserver
	ClientStreamObservers     []ClientStreamObserver
	ClientPublishObservers    []ClientPublishObserver
	ClientCallFuncObservers   []ClientCallFuncObserver
	ServerHandlerObservers    []ServerHandlerObserver
	ServerSubscriberObservers []ServerSubscriberObserver
}

type Option func(*Options)

func NewOptions(opts ...Option) Options {
	options := Options{
		Tracer:                    opentracing.GlobalTracer(),
		ClientCallObservers:       []ClientCallObserver{DefaultClientCallObserver},
		ClientStreamObservers:     []ClientStreamObserver{DefaultClientStreamObserver},
		ClientPublishObservers:    []ClientPublishObserver{DefaultClientPublishObserver},
		ClientCallFuncObservers:   []ClientCallFuncObserver{DefaultClientCallFuncObserver},
		ServerHandlerObservers:    []ServerHandlerObserver{DefaultServerHandlerObserver},
		ServerSubscriberObservers: []ServerSubscriberObserver{DefaultServerSubscriberObserver},
	}

	for _, o := range opts {
		o(&options)
	}

	return options
}

func WithTracer(ot opentracing.Tracer) Option {
	return func(o *Options) {
		o.Tracer = ot
	}
}

func WithClientCallObservers(ob ...ClientCallObserver) Option {
	return func(o *Options) {
		o.ClientCallObservers = ob
	}
}

func WithClientStreamObservers(ob ...ClientStreamObserver) Option {
	return func(o *Options) {
		o.ClientStreamObservers = ob
	}
}

func WithClientPublishObservers(ob ...ClientPublishObserver) Option {
	return func(o *Options) {
		o.ClientPublishObservers = ob
	}
}

func WithClientCallFuncObservers(ob ...ClientCallFuncObserver) Option {
	return func(o *Options) {
		o.ClientCallFuncObservers = ob
	}
}

func WithServerHandlerObservers(ob ...ServerHandlerObserver) Option {
	return func(o *Options) {
		o.ServerHandlerObservers = ob
	}
}

func WithServerSubscriberObservers(ob ...ServerSubscriberObserver) Option {
	return func(o *Options) {
		o.ServerSubscriberObservers = ob
	}
}

func DefaultClientCallObserver(ctx context.Context, req client.Request, rsp interface{}, opts []client.CallOption, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("%s.%s", req.Service(), req.Endpoint()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

func DefaultClientStreamObserver(ctx context.Context, req client.Request, opts []client.CallOption, stream client.Stream, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("%s.%s", req.Service(), req.Endpoint()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

func DefaultClientPublishObserver(ctx context.Context, msg client.Message, opts []client.PublishOption, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("Pub to %s", msg.Topic()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

func DefaultServerHandlerObserver(ctx context.Context, req server.Request, rsp interface{}, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("%s.%s", req.Service(), req.Endpoint()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

func DefaultServerSubscriberObserver(ctx context.Context, msg server.Message, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("Sub from %s", msg.Topic()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

func DefaultClientCallFuncObserver(ctx context.Context, addr string, req client.Request, rsp interface{}, opts client.CallOptions, sp opentracing.Span, err error) {
	sp.SetOperationName(fmt.Sprintf("%s.%s", req.Service(), req.Endpoint()))
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		sp.SetTag("error", true)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if id, ok := md.Get(MetadataKey); ok {
			sp.SetTag(MetadataKey, id)
		}
	}
}

// StartSpanFromOutgoingContext returns a new span with the given operation name and options. If a span
// is found in the context, it will be used as the parent of the resulting span.
func StartSpanFromOutgoingContext(ctx context.Context, tracer opentracing.Tracer, name string, opts ...opentracing.StartSpanOption) (context.Context, opentracing.Span, error) {
	var parentCtx opentracing.SpanContext

	md, ok := metadata.FromIncomingContext(ctx)
	// Find parent span.
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		// First try to get span within current service boundary.
		parentCtx = parentSpan.Context()
	} else if spanCtx, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(md)); err == nil && ok {
		// If there doesn't exist, try to get it from metadata(which is cross boundary)
		parentCtx = spanCtx
	}

	if parentCtx != nil {
		opts = append(opts, opentracing.ChildOf(parentCtx))
	}

	if !ok {
		md = metadata.New(1)
	}
	nmd := metadata.New(1)

	sp := tracer.StartSpan(name, opts...)
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.TextMapCarrier(nmd)); err != nil {
		return nil, nil, err
	}

	for k, v := range nmd {
		md.Set(k, v)
	}

	ctx = metadata.NewOutgoingContext(opentracing.ContextWithSpan(ctx, sp), md)

	return ctx, sp, nil
}

// StartSpanFromIncomingContext returns a new span with the given operation name and options. If a span
// is found in the context, it will be used as the parent of the resulting span.
func StartSpanFromIncomingContext(ctx context.Context, tracer opentracing.Tracer, name string, opts ...opentracing.StartSpanOption) (context.Context, opentracing.Span, error) {
	var parentCtx opentracing.SpanContext

	// Find parent span.
	md, ok := metadata.FromIncomingContext(ctx)
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		// First try to get span within current service boundary.
		parentCtx = parentSpan.Context()
	} else if spanCtx, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(md)); err == nil && ok {
		// If there doesn't exist, try to get it from metadata(which is cross boundary)
		parentCtx = spanCtx
	}

	if parentCtx != nil {
		opts = append(opts, opentracing.ChildOf(parentCtx))
	}

	var nmd metadata.Metadata
	if ok {
		nmd = metadata.New(len(md))
	} else {
		nmd = metadata.New(0)
	}

	sp := tracer.StartSpan(name, opts...)
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.TextMapCarrier(nmd)); err != nil {
		return nil, nil, err
	}

	for k, v := range md {
		nmd.Set(k, v)
	}

	ctx = metadata.NewIncomingContext(opentracing.ContextWithSpan(ctx, sp), nmd)

	return ctx, sp, nil
}

func (ot *otWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	ctx, sp, err := StartSpanFromOutgoingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return err
	}
	defer sp.Finish()

	err = ot.Client.Call(ctx, req, rsp, opts...)

	for _, o := range ot.opts.ClientCallObservers {
		o(ctx, req, rsp, opts, sp, err)
	}

	return err
}

func (ot *otWrapper) Stream(ctx context.Context, req client.Request, opts ...client.CallOption) (client.Stream, error) {
	ctx, sp, err := StartSpanFromOutgoingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return nil, err
	}
	defer sp.Finish()

	stream, err := ot.Client.Stream(ctx, req, opts...)

	for _, o := range ot.opts.ClientStreamObservers {
		o(ctx, req, opts, stream, sp, err)
	}

	return stream, err
}

func (ot *otWrapper) Publish(ctx context.Context, msg client.Message, opts ...client.PublishOption) error {
	ctx, sp, err := StartSpanFromOutgoingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return err
	}
	defer sp.Finish()

	err = ot.Client.Publish(ctx, msg, opts...)

	for _, o := range ot.opts.ClientPublishObservers {
		o(ctx, msg, opts, sp, err)
	}

	return err
}

func (ot *otWrapper) ServerHandler(ctx context.Context, req server.Request, rsp interface{}) error {
	ctx, sp, err := StartSpanFromIncomingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return err
	}
	defer sp.Finish()

	err = ot.serverHandler(ctx, req, rsp)

	for _, o := range ot.opts.ServerHandlerObservers {
		o(ctx, req, rsp, sp, err)
	}

	return err
}

func (ot *otWrapper) ServerSubscriber(ctx context.Context, msg server.Message) error {
	ctx, sp, err := StartSpanFromIncomingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return err
	}
	defer sp.Finish()

	err = ot.serverSubscriber(ctx, msg)

	for _, o := range ot.opts.ServerSubscriberObservers {
		o(ctx, msg, sp, err)
	}

	return err
}

// NewClientWrapper accepts an open tracing Trace and returns a Client Wrapper
func NewClientWrapper(opts ...Option) client.Wrapper {
	return func(c client.Client) client.Client {
		options := NewOptions()
		for _, o := range opts {
			o(&options)
		}
		return &otWrapper{opts: options, Client: c}
	}
}

// NewClientCallWrapper accepts an opentracing Tracer and returns a Call Wrapper
func NewClientCallWrapper(opts ...Option) client.CallWrapper {
	return func(h client.CallFunc) client.CallFunc {
		options := NewOptions()
		for _, o := range opts {
			o(&options)
		}

		ot := &otWrapper{opts: options, clientCallFunc: h}
		return ot.ClientCallFunc
	}
}

func (ot *otWrapper) ClientCallFunc(ctx context.Context, addr string, req client.Request, rsp interface{}, opts client.CallOptions) error {
	ctx, sp, err := StartSpanFromOutgoingContext(ctx, ot.opts.Tracer, "")
	if err != nil {
		return err
	}
	defer sp.Finish()

	err = ot.clientCallFunc(ctx, addr, req, rsp, opts)

	for _, o := range ot.opts.ClientCallFuncObservers {
		o(ctx, addr, req, rsp, opts, sp, err)
	}

	return err
}

// NewServerHandlerWrapper accepts an options and returns a Handler Wrapper
func NewServerHandlerWrapper(opts ...Option) server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		options := NewOptions()
		for _, o := range opts {
			o(&options)
		}

		ot := &otWrapper{opts: options, serverHandler: h}
		return ot.ServerHandler
	}
}

// NewServerSubscriberWrapper accepts an opentracing Tracer and returns a Subscriber Wrapper
func NewServerSubscriberWrapper(opts ...Option) server.SubscriberWrapper {
	return func(h server.SubscriberFunc) server.SubscriberFunc {
		options := NewOptions()
		for _, o := range opts {
			o(&options)
		}

		ot := &otWrapper{opts: options, serverSubscriber: h}
		return ot.ServerSubscriber
	}
}

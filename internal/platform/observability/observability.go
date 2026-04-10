package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Handle struct {
	logger         *slog.Logger
	cfg            config.Service
	server         *http.Server
	tracerProvider *sdktrace.TracerProvider
}

type runtimeState struct {
	service string
	tracer  trace.Tracer

	grpcRequests         *prometheus.CounterVec
	grpcRequestDuration  *prometheus.HistogramVec
	jobOperations        *prometheus.CounterVec
	workerEvents         *prometheus.CounterVec
	schedulerTicks       *prometheus.CounterVec
	schedulerTickLatency prometheus.Histogram
	schedulerRequeued    prometheus.Counter
	schedulerEnqueued    prometheus.Counter
	dispatchEvents       *prometheus.CounterVec
}

var (
	stateMu sync.RWMutex
	state   *runtimeState
)

func Start(ctx context.Context, logger *slog.Logger, cfg config.Service) (*Handle, error) {
	if logger == nil {
		logger = slog.Default()
	}

	registry := prometheus.NewRegistry()
	runtime := &runtimeState{
		service: cfg.Name,
		grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_orchestrator_grpc_server_requests_total",
			Help: "Count of gRPC requests handled by the service.",
		}, []string{"service", "method", "kind", "code"}),
		grpcRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_orchestrator_grpc_server_request_duration_seconds",
			Help:    "Latency distribution of gRPC requests handled by the service.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2, 5},
		}, []string{"service", "method", "kind"}),
		jobOperations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_orchestrator_job_operations_total",
			Help: "Count of control-plane job operations by outcome.",
		}, []string{"service", "operation", "outcome"}),
		workerEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_orchestrator_worker_events_total",
			Help: "Count of worker stream events by outcome.",
		}, []string{"service", "event", "outcome"}),
		schedulerTicks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_orchestrator_scheduler_ticks_total",
			Help: "Count of scheduler ticks by outcome.",
		}, []string{"service", "outcome"}),
		schedulerTickLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_orchestrator_scheduler_tick_duration_seconds",
			Help:    "Duration of scheduler reconciliation ticks.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		}),
		schedulerRequeued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_orchestrator_scheduler_requeued_total",
			Help: "Total number of expired executions requeued by the scheduler.",
		}),
		schedulerEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_orchestrator_scheduler_enqueued_total",
			Help: "Total number of runnable executions pushed into ready queues by the scheduler.",
		}),
		dispatchEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_orchestrator_dispatch_events_total",
			Help: "Count of dispatch-side events by outcome.",
		}, []string{"service", "event", "outcome"}),
	}

	if err := registry.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("register go collector: %w", err)
	}
	if err := registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("register process collector: %w", err)
	}

	for _, collector := range []prometheus.Collector{
		runtime.grpcRequests,
		runtime.grpcRequestDuration,
		runtime.jobOperations,
		runtime.workerEvents,
		runtime.schedulerTicks,
		runtime.schedulerTickLatency,
		runtime.schedulerRequeued,
		runtime.schedulerEnqueued,
		runtime.dispatchEvents,
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register collector: %w", err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	registerPprof(mux)

	handle := &Handle{
		logger: logger,
		cfg:    cfg,
		server: &http.Server{
			Addr:              cfg.MetricsAddress(),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}

	tracerProvider, err := newTracerProvider(ctx, cfg)
	if err != nil {
		return nil, err
	}
	handle.tracerProvider = tracerProvider
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	runtime.tracer = otel.Tracer("github.com/gnix0/task-orchestrator/" + cfg.Name)

	stateMu.Lock()
	state = runtime
	stateMu.Unlock()

	go func() {
		if err := handle.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("observability server exited with error", slog.String("service", cfg.Name), slog.Any("error", err))
		}
	}()

	logger.Info("observability server listening", slog.String("service", cfg.Name), slog.String("address", cfg.MetricsAddress()))
	return handle, nil
}

func (h *Handle) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}

	var shutdownErr error
	if h.server != nil {
		shutdownErr = h.server.Shutdown(ctx)
	}
	if h.tracerProvider != nil {
		if err := h.tracerProvider.Shutdown(ctx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}

	stateMu.Lock()
	state = nil
	stateMu.Unlock()

	return shutdownErr
}

func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	runtime := current()
	if runtime == nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	return runtime.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func RecordGRPCRequest(kind, method string, started time.Time, err error) {
	runtime := current()
	if runtime == nil {
		return
	}

	normalizedMethod := normalizeMethod(method)
	code := status.Code(err).String()
	runtime.grpcRequests.WithLabelValues(runtime.service, normalizedMethod, kind, code).Inc()
	runtime.grpcRequestDuration.WithLabelValues(runtime.service, normalizedMethod, kind).Observe(time.Since(started).Seconds())
}

func RecordJobOperation(operation string, err error) {
	runtime := current()
	if runtime == nil {
		return
	}

	runtime.jobOperations.WithLabelValues(runtime.service, operation, outcomeForError(err)).Inc()
}

func RecordWorkerEvent(event string, err error) {
	runtime := current()
	if runtime == nil {
		return
	}

	runtime.workerEvents.WithLabelValues(runtime.service, event, outcomeForError(err)).Inc()
}

func RecordSchedulerTick(duration time.Duration, isLeader bool, requeued, enqueued int, err error) {
	runtime := current()
	if runtime == nil {
		return
	}

	outcome := outcomeForError(err)
	if err == nil && !isLeader {
		outcome = "skipped_not_leader"
	}
	runtime.schedulerTicks.WithLabelValues(runtime.service, outcome).Inc()
	runtime.schedulerTickLatency.Observe(duration.Seconds())
	if requeued > 0 {
		runtime.schedulerRequeued.Add(float64(requeued))
	}
	if enqueued > 0 {
		runtime.schedulerEnqueued.Add(float64(enqueued))
	}
}

func RecordDispatchEvent(event string, err error) {
	runtime := current()
	if runtime == nil {
		return
	}

	runtime.dispatchEvents.WithLabelValues(runtime.service, event, outcomeForError(err)).Inc()
}

func current() *runtimeState {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return state
}

func newTracerProvider(ctx context.Context, cfg config.Service) (*sdktrace.TracerProvider, error) {
	resource, err := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.Name),
			semconv.DeploymentEnvironmentName(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}

	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource),
	}

	if cfg.TracingEnabled {
		exporterOptions := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.OTLPInsecure {
			exporterOptions = append(exporterOptions, otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
		}

		exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
		if err != nil {
			return nil, fmt.Errorf("create otlp exporter: %w", err)
		}
		options = append(options,
			sdktrace.WithBatcher(exporter),
			sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.TraceSampleRate)),
		)
	} else {
		options = append(options, sdktrace.WithSampler(sdktrace.NeverSample()))
	}

	return sdktrace.NewTracerProvider(options...), nil
}

func outcomeForError(err error) string {
	if err == nil {
		return "success"
	}
	return strings.ToLower(status.Code(err).String())
}

func normalizeMethod(method string) string {
	method = strings.TrimSpace(method)
	if method == "" {
		return "unknown"
	}

	return strings.TrimPrefix(method, "/")
}

func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

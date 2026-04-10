package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type summary struct {
	Profile           string    `json:"profile"`
	Target            string    `json:"target"`
	Workers           int       `json:"workers"`
	DurationSeconds   float64   `json:"duration_seconds"`
	HeartbeatInterval string    `json:"heartbeat_interval"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	Registrations     int       `json:"registrations"`
	HeartbeatsSent    int       `json:"heartbeats_sent"`
	AssignmentsSeen   int       `json:"assignments_seen"`
	Errors            int       `json:"errors"`
	AvgAckLatencyMs   float64   `json:"avg_ack_latency_ms"`
	P50AckLatencyMs   float64   `json:"p50_ack_latency_ms"`
	P95AckLatencyMs   float64   `json:"p95_ack_latency_ms"`
	P99AckLatencyMs   float64   `json:"p99_ack_latency_ms"`
	MaxAckLatencyMs   float64   `json:"max_ack_latency_ms"`
}

type workerResult struct {
	registrations   int
	heartbeatsSent  int
	assignmentsSeen int
	errors          int
	ackLatenciesMs  []float64
}

func main() {
	var (
		target            = flag.String("target", "127.0.0.1:8081", "worker-gateway gRPC target")
		workersCount      = flag.Int("workers", 10, "number of concurrent worker streams")
		duration          = flag.Duration("duration", 30*time.Second, "test duration")
		heartbeatInterval = flag.Duration("heartbeat-interval", 2*time.Second, "delay between worker heartbeats")
		maxConcurrency    = flag.Int("max-concurrency", 4, "worker max concurrency value to advertise")
		outputPath        = flag.String("output", "perf/results/worker-heartbeats-smoke.json", "path to write JSON summary")
		profile           = flag.String("profile", "smoke", "profile label")
	)
	flag.Parse()

	started := time.Now().UTC()
	results := make(chan workerResult, *workersCount)

	var wg sync.WaitGroup
	for index := 0; index < *workersCount; index++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			results <- runWorker(*target, *duration, *heartbeatInterval, *maxConcurrency, workerIndex)
		}(index)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	aggregate := summary{
		Profile:           *profile,
		Target:            *target,
		Workers:           *workersCount,
		DurationSeconds:   duration.Seconds(),
		HeartbeatInterval: heartbeatInterval.String(),
		StartedAt:         started,
	}

	latencies := make([]float64, 0, *workersCount*8)
	for result := range results {
		aggregate.Registrations += result.registrations
		aggregate.HeartbeatsSent += result.heartbeatsSent
		aggregate.AssignmentsSeen += result.assignmentsSeen
		aggregate.Errors += result.errors
		latencies = append(latencies, result.ackLatenciesMs...)
	}

	aggregate.FinishedAt = time.Now().UTC()
	summarizeLatencies(&aggregate, latencies)

	if err := os.MkdirAll(parentDir(*outputPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output directory: %v\n", err)
		os.Exit(1)
	}

	output, err := json.MarshalIndent(aggregate, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal summary: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outputPath, append(output, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
		os.Exit(1)
	}
}

func runWorker(target string, duration, heartbeatInterval time.Duration, maxConcurrency, workerIndex int) workerResult {
	result := workerResult{}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		result.errors++
		return result
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			result.errors++
		}
	}()

	conn.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), duration+15*time.Second)
	defer cancel()

	if err := waitForReady(ctx, conn); err != nil {
		result.errors++
		return result
	}

	client := taskorchestratorv1.NewWorkerServiceClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		result.errors++
		return result
	}
	defer func() {
		if closeErr := stream.CloseSend(); closeErr != nil && !errors.Is(closeErr, context.Canceled) {
			result.errors++
		}
	}()

	workerID := fmt.Sprintf("perf-worker-%02d", workerIndex)
	if err := stream.Send(&taskorchestratorv1.ConnectRequest{
		Payload: &taskorchestratorv1.ConnectRequest_Registration{
			Registration: &taskorchestratorv1.WorkerRegistration{
				WorkerId:       workerID,
				Capabilities:   []string{"perf-heartbeat"},
				MaxConcurrency: int32(maxConcurrency),
				Labels: map[string]string{
					"load_test": "true",
					"profile":   "worker-heartbeat",
				},
			},
		},
	}); err != nil {
		result.errors++
		return result
	}
	result.registrations++
	if _, assignments, err := waitForAck(stream, workerID); err != nil {
		result.errors++
		return result
	} else {
		result.assignmentsSeen += assignments
	}

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		started := time.Now()
		if err := stream.Send(&taskorchestratorv1.ConnectRequest{
			Payload: &taskorchestratorv1.ConnectRequest_Heartbeat{
				Heartbeat: &taskorchestratorv1.WorkerHeartbeat{
					WorkerId:           workerID,
					Status:             taskorchestratorv1.WorkerStatus_WORKER_STATUS_READY,
					InflightExecutions: 0,
				},
			},
		}); err != nil {
			result.errors++
			return result
		}

		if _, assignments, err := waitForAck(stream, workerID); err != nil {
			result.errors++
			return result
		} else {
			result.assignmentsSeen += assignments
		}
		result.heartbeatsSent++
		result.ackLatenciesMs = append(result.ackLatenciesMs, float64(time.Since(started).Microseconds())/1000.0)
		time.Sleep(heartbeatInterval)
	}

	return result
}

func waitForAck(stream taskorchestratorv1.WorkerService_ConnectClient, workerID string) (*taskorchestratorv1.WorkerAck, int, error) {
	assignments := 0

	for {
		response, err := stream.Recv()
		if err != nil {
			return nil, assignments, err
		}

		if assignment := response.GetAssignment(); assignment != nil {
			assignments++
			continue
		}

		if ack := response.GetAck(); ack != nil {
			if ack.GetWorkerId() == workerID || ack.GetWorkerId() == "" {
				return ack, assignments, nil
			}
		}
	}
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		switch conn.GetState() {
		case connectivity.Ready:
			return nil
		case connectivity.Idle:
			conn.Connect()
		case connectivity.Shutdown:
			return errors.New("gRPC connection shut down before reaching ready state")
		}

		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			return ctx.Err()
		}
	}
}

func summarizeLatencies(aggregate *summary, latencies []float64) {
	if len(latencies) == 0 {
		return
	}

	sort.Float64s(latencies)
	total := 0.0
	for _, latency := range latencies {
		total += latency
	}

	aggregate.AvgAckLatencyMs = total / float64(len(latencies))
	aggregate.P50AckLatencyMs = percentile(latencies, 0.50)
	aggregate.P95AckLatencyMs = percentile(latencies, 0.95)
	aggregate.P99AckLatencyMs = percentile(latencies, 0.99)
	aggregate.MaxAckLatencyMs = latencies[len(latencies)-1]
}

func percentile(values []float64, ratio float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if ratio <= 0 {
		return values[0]
	}
	if ratio >= 1 {
		return values[len(values)-1]
	}

	index := int(float64(len(values)-1) * ratio)
	return values[index]
}

func parentDir(path string) string {
	lastSlash := -1
	for index := len(path) - 1; index >= 0; index-- {
		if path[index] == '/' {
			lastSlash = index
			break
		}
	}

	if lastSlash <= 0 {
		return "."
	}

	return path[:lastSlash]
}

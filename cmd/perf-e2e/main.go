package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	dispatchdv1 "github.com/gnix0/dispatchd/gen/go/dispatchd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type summary struct {
	Profile                string    `json:"profile"`
	ControlPlaneTarget     string    `json:"control_plane_target"`
	WorkerGatewayTarget    string    `json:"worker_gateway_target"`
	JobType                string    `json:"job_type"`
	JobsSubmitted          int       `json:"jobs_submitted"`
	JobsSucceeded          int       `json:"jobs_succeeded"`
	JobsFailed             int       `json:"jobs_failed"`
	AssignmentsObserved    int64     `json:"assignments_observed"`
	WorkerHeartbeatsSent   int64     `json:"worker_heartbeats_sent"`
	WorkerResultsSent      int64     `json:"worker_results_sent"`
	DurationSeconds        float64   `json:"duration_seconds"`
	StartedAt              time.Time `json:"started_at"`
	FinishedAt             time.Time `json:"finished_at"`
	SubmitToClaimAvgMs     float64   `json:"submit_to_claim_avg_ms"`
	SubmitToClaimP50Ms     float64   `json:"submit_to_claim_p50_ms"`
	SubmitToClaimP95Ms     float64   `json:"submit_to_claim_p95_ms"`
	SubmitToClaimP99Ms     float64   `json:"submit_to_claim_p99_ms"`
	SubmitToSucceededAvgMs float64   `json:"submit_to_succeeded_avg_ms"`
	SubmitToSucceededP50Ms float64   `json:"submit_to_succeeded_p50_ms"`
	SubmitToSucceededP95Ms float64   `json:"submit_to_succeeded_p95_ms"`
	SubmitToSucceededP99Ms float64   `json:"submit_to_succeeded_p99_ms"`
	MaxSubmitToSucceededMs float64   `json:"max_submit_to_succeeded_ms"`
	FailureMessages        []string  `json:"failure_messages,omitempty"`
}

type jobMetric struct {
	claimLatencyMs     float64
	succeededLatencyMs float64
	err                error
}

func main() {
	var (
		profile           = flag.String("profile", "smoke", "profile label")
		controlTarget     = flag.String("control-plane-target", "127.0.0.1:8080", "control-plane gRPC target")
		workerTarget      = flag.String("worker-gateway-target", "127.0.0.1:8081", "worker-gateway gRPC target")
		jobType           = flag.String("job-type", "perf.execute", "job type used for the e2e benchmark")
		jobsCount         = flag.Int("jobs", 20, "number of jobs to submit")
		maxConcurrency    = flag.Int("max-concurrency", 8, "worker max concurrency to advertise")
		pollInterval      = flag.Duration("poll-interval", 100*time.Millisecond, "job polling interval")
		heartbeatInterval = flag.Duration("heartbeat-interval", 500*time.Millisecond, "worker heartbeat interval")
		timeout           = flag.Duration("timeout", 45*time.Second, "per-job completion timeout")
		outputPath        = flag.String("output", "evidence/performance/end-to-end-smoke.json", "path to write summary JSON")
	)
	flag.Parse()

	started := time.Now().UTC()
	summary := summary{
		Profile:             *profile,
		ControlPlaneTarget:  *controlTarget,
		WorkerGatewayTarget: *workerTarget,
		JobType:             *jobType,
		JobsSubmitted:       *jobsCount,
		StartedAt:           started,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+30*time.Second)
	defer cancel()

	controlConn, err := grpc.NewClient(*controlTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err)
	}
	defer func() { _ = controlConn.Close() }()
	if err := waitForReady(ctx, controlConn); err != nil {
		fail(err)
	}

	workerConn, err := grpc.NewClient(*workerTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err)
	}
	defer func() { _ = workerConn.Close() }()
	if err := waitForReady(ctx, workerConn); err != nil {
		fail(err)
	}

	jobClient := dispatchdv1.NewJobServiceClient(controlConn)
	workerClient := dispatchdv1.NewWorkerServiceClient(workerConn)

	assignmentsObserved := atomic.Int64{}
	heartbeatsSent := atomic.Int64{}
	resultsSent := atomic.Int64{}
	workerErr := make(chan error, 1)

	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() {
		workerErr <- runAutoWorker(workerCtx, workerClient, *jobType, *maxConcurrency, *heartbeatInterval, &assignmentsObserved, &heartbeatsSent, &resultsSent)
	}()

	time.Sleep(500 * time.Millisecond)

	results := make(chan jobMetric, *jobsCount)
	var wg sync.WaitGroup
	for index := 0; index < *jobsCount; index++ {
		submitResp, err := jobClient.SubmitJob(ctx, &dispatchdv1.SubmitJobRequest{
			JobType:        *jobType,
			Payload:        []byte(fmt.Sprintf(`{"job_index":%d}`, index)),
			Priority:       10,
			IdempotencyKey: fmt.Sprintf("%s-%d-%d", *profile, index, time.Now().UnixNano()),
		})
		if err != nil {
			fail(fmt.Errorf("submit job %d: %w", index, err))
		}

		wg.Add(1)
		go func(jobID string, submittedAt time.Time) {
			defer wg.Done()
			metric, err := monitorJob(ctx, jobClient, jobID, submittedAt, *pollInterval, *timeout)
			if err != nil {
				results <- jobMetric{err: fmt.Errorf("monitor job %s: %w", jobID, err)}
				return
			}
			results <- metric
		}(submitResp.GetJob().GetJobId(), time.Now())
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	claimLatencies := make([]float64, 0, *jobsCount)
	successLatencies := make([]float64, 0, *jobsCount)
	failures := make([]string, 0)
	for metric := range results {
		if metric.err != nil {
			failures = append(failures, metric.err.Error())
			continue
		}
		summary.JobsSucceeded++
		claimLatencies = append(claimLatencies, metric.claimLatencyMs)
		successLatencies = append(successLatencies, metric.succeededLatencyMs)
	}

	summary.JobsFailed = len(failures)
	summary.AssignmentsObserved = assignmentsObserved.Load()
	summary.WorkerHeartbeatsSent = heartbeatsSent.Load()
	summary.WorkerResultsSent = resultsSent.Load()
	summary.FailureMessages = failures
	summary.FinishedAt = time.Now().UTC()
	summary.DurationSeconds = summary.FinishedAt.Sub(summary.StartedAt).Seconds()

	fillLatencyStats(&summary, claimLatencies, successLatencies)

	select {
	case err := <-workerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			summary.JobsFailed++
			summary.FailureMessages = append(summary.FailureMessages, fmt.Sprintf("worker: %v", err))
		}
	default:
	}

	if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
		fail(err)
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*outputPath, append(encoded, '\n'), 0o644); err != nil {
		fail(err)
	}
}

func runAutoWorker(
	ctx context.Context,
	client dispatchdv1.WorkerServiceClient,
	jobType string,
	maxConcurrency int,
	heartbeatInterval time.Duration,
	assignmentsObserved, heartbeatsSent, resultsSent *atomic.Int64,
) error {
	stream, err := client.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.CloseSend() }()

	workerID := "perf-e2e-worker"
	sendMu := sync.Mutex{}
	send := func(request *dispatchdv1.ConnectRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(request)
	}

	if err := send(&dispatchdv1.ConnectRequest{
		Payload: &dispatchdv1.ConnectRequest_Registration{
			Registration: &dispatchdv1.WorkerRegistration{
				WorkerId:       workerID,
				Capabilities:   []string{jobNamespace(jobType)},
				MaxConcurrency: int32(maxConcurrency),
				Labels: map[string]string{
					"profile": "perf-e2e",
				},
			},
		},
	}); err != nil {
		return err
	}

	recvErr := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if response.GetAssignment() == nil {
				continue
			}
			assignmentsObserved.Add(1)
			if err := send(&dispatchdv1.ConnectRequest{
				Payload: &dispatchdv1.ConnectRequest_Result{
					Result: &dispatchdv1.TaskResult{
						ExecutionId: response.GetAssignment().GetExecutionId(),
						Success:     true,
						Metadata: map[string]string{
							"worker_id": workerID,
							"profile":   "perf-e2e",
						},
					},
				},
			}); err != nil {
				recvErr <- err
				return
			}
			resultsSent.Add(1)
		}
	}()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-recvErr:
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		case <-ticker.C:
			if err := send(&dispatchdv1.ConnectRequest{
				Payload: &dispatchdv1.ConnectRequest_Heartbeat{
					Heartbeat: &dispatchdv1.WorkerHeartbeat{
						WorkerId:           workerID,
						Status:             dispatchdv1.WorkerStatus_WORKER_STATUS_READY,
						InflightExecutions: 0,
					},
				},
			}); err != nil {
				return err
			}
			heartbeatsSent.Add(1)
		}
	}
}

func monitorJob(
	ctx context.Context,
	client dispatchdv1.JobServiceClient,
	jobID string,
	submittedAt time.Time,
	pollInterval, timeout time.Duration,
) (jobMetric, error) {
	deadline := time.Now().Add(timeout)
	claimObserved := false
	metric := jobMetric{}

	for time.Now().Before(deadline) {
		if !claimObserved {
			listResp, err := client.ListExecutions(ctx, &dispatchdv1.ListExecutionsRequest{JobId: jobID})
			if err != nil {
				return jobMetric{}, err
			}
			for _, execution := range listResp.GetExecutions() {
				if execution.GetClaimedAt() != nil || execution.GetStatus() == dispatchdv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED {
					metric.claimLatencyMs = float64(time.Since(submittedAt).Microseconds()) / 1000.0
					claimObserved = true
					break
				}
			}
		}

		getResp, err := client.GetJob(ctx, &dispatchdv1.GetJobRequest{JobId: jobID})
		if err != nil {
			return jobMetric{}, err
		}
		switch getResp.GetJob().GetStatus() {
		case dispatchdv1.JobStatus_JOB_STATUS_SUCCEEDED:
			if !claimObserved {
				metric.claimLatencyMs = float64(time.Since(submittedAt).Microseconds()) / 1000.0
			}
			metric.succeededLatencyMs = float64(time.Since(submittedAt).Microseconds()) / 1000.0
			return metric, nil
		case dispatchdv1.JobStatus_JOB_STATUS_FAILED, dispatchdv1.JobStatus_JOB_STATUS_CANCELED:
			return jobMetric{}, fmt.Errorf("job finished with status %s", getResp.GetJob().GetStatus().String())
		}

		select {
		case <-ctx.Done():
			return jobMetric{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}

	return jobMetric{}, fmt.Errorf("timed out waiting for job %s", jobID)
}

func fillLatencyStats(summary *summary, claimLatencies, successLatencies []float64) {
	if len(claimLatencies) > 0 {
		sort.Float64s(claimLatencies)
		total := 0.0
		for _, value := range claimLatencies {
			total += value
		}
		summary.SubmitToClaimAvgMs = total / float64(len(claimLatencies))
		summary.SubmitToClaimP50Ms = percentile(claimLatencies, 0.50)
		summary.SubmitToClaimP95Ms = percentile(claimLatencies, 0.95)
		summary.SubmitToClaimP99Ms = percentile(claimLatencies, 0.99)
	}

	if len(successLatencies) > 0 {
		sort.Float64s(successLatencies)
		total := 0.0
		for _, value := range successLatencies {
			total += value
		}
		summary.SubmitToSucceededAvgMs = total / float64(len(successLatencies))
		summary.SubmitToSucceededP50Ms = percentile(successLatencies, 0.50)
		summary.SubmitToSucceededP95Ms = percentile(successLatencies, 0.95)
		summary.SubmitToSucceededP99Ms = percentile(successLatencies, 0.99)
		summary.MaxSubmitToSucceededMs = successLatencies[len(successLatencies)-1]
	}
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

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	client := healthpb.NewHealthClient(conn)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("gRPC connection did not report serving health before deadline")
}

func jobNamespace(jobType string) string {
	for index, char := range jobType {
		if char == '.' {
			return jobType[:index]
		}
	}
	return jobType
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

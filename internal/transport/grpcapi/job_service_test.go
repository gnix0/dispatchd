package grpcapi

import (
	"context"
	"testing"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJobServiceMethodsReturnUnimplemented(t *testing.T) {
	service := &JobService{}

	testCases := []struct {
		name string
		call func() error
	}{
		{
			name: "SubmitJob",
			call: func() error {
				_, err := service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{})
				return err
			},
		},
		{
			name: "CancelJob",
			call: func() error {
				_, err := service.CancelJob(context.Background(), &taskorchestratorv1.CancelJobRequest{})
				return err
			},
		},
		{
			name: "GetJob",
			call: func() error {
				_, err := service.GetJob(context.Background(), &taskorchestratorv1.GetJobRequest{})
				return err
			},
		},
		{
			name: "ListExecutions",
			call: func() error {
				_, err := service.ListExecutions(context.Background(), &taskorchestratorv1.ListExecutionsRequest{})
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("expected Unimplemented code, got %v", status.Code(err))
			}
		})
	}
}

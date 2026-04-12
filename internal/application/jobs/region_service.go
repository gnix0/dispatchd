package jobs

import (
	"context"
	"fmt"
	"strings"
)

type regionalService struct {
	next           Service
	region         string
	primaryRegion  string
	allowMutations bool
}

func NewRegionalService(next Service, region, primaryRegion string, allowMutations bool) Service {
	return &regionalService{
		next:           next,
		region:         strings.TrimSpace(region),
		primaryRegion:  strings.TrimSpace(primaryRegion),
		allowMutations: allowMutations,
	}
}

func (s *regionalService) SubmitJob(ctx context.Context, input SubmitJobInput) (Job, error) {
	if !s.allowMutations {
		return Job{}, fmt.Errorf("%w: region %s is passive; primary region is %s", ErrMutationsDisabled, s.region, s.primaryRegion)
	}
	return s.next.SubmitJob(ctx, input)
}

func (s *regionalService) GetJob(ctx context.Context, jobID string) (Job, error) {
	return s.next.GetJob(ctx, jobID)
}

func (s *regionalService) CancelJob(ctx context.Context, jobID string) (Job, error) {
	if !s.allowMutations {
		return Job{}, fmt.Errorf("%w: region %s is passive; primary region is %s", ErrMutationsDisabled, s.region, s.primaryRegion)
	}
	return s.next.CancelJob(ctx, jobID)
}

func (s *regionalService) ListExecutions(ctx context.Context, jobID string) ([]Execution, error) {
	return s.next.ListExecutions(ctx, jobID)
}

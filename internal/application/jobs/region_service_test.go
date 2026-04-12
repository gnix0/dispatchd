package jobs

import (
	"context"
	"errors"
	"testing"
)

func TestRegionalServiceBlocksMutationsInPassiveRegion(t *testing.T) {
	base := NewInMemoryService()
	service := NewRegionalService(base, "region-b", "region-a", false)

	_, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType: "email.send",
		Payload: []byte("payload"),
	})
	if !errors.Is(err, ErrMutationsDisabled) {
		t.Fatalf("expected mutations disabled error, got %v", err)
	}
}

func TestRegionalServiceAllowsReadsInPassiveRegion(t *testing.T) {
	base := NewInMemoryService()
	job, err := base.SubmitJob(context.Background(), SubmitJobInput{
		JobType: "email.send",
		Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("expected seed submit to succeed, got %v", err)
	}

	service := NewRegionalService(base, "region-b", "region-a", false)
	got, err := service.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("expected read in passive region to succeed, got %v", err)
	}
	if got.ID != job.ID {
		t.Fatalf("expected job %q, got %q", job.ID, got.ID)
	}
}

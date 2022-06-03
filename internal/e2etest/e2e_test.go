package e2etest

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DataDog/temporalite/temporaltest"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	codec "github.com/DataDog/temporal-large-payload-codec"
	"github.com/DataDog/temporal-large-payload-codec/internal/driver/memory"
	"github.com/DataDog/temporal-large-payload-codec/internal/server"
)

func TestWorker(t *testing.T) {
	// Create test remote codec service
	testCodecServer := httptest.NewServer(server.NewHttpHandler(&memory.Driver{}))
	defer testCodecServer.Close()
	// Create test codec (to be used from Go SDK)
	testCodec, err := codec.New(
		codec.WithURL(testCodecServer.URL),
		codec.WithHTTPClient(testCodecServer.Client()),
		codec.WithMinBytes(32),
	)
	if err != nil {
		t.Fatal(err)
	}
	testDataConverter := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), testCodec)

	// Create test Temporal server and client
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	c := ts.NewClientWithOptions(client.Options{
		DataConverter: testDataConverter,
	})

	// Register a new worker
	w := worker.New(c, "large_payloads", worker.Options{})
	defer w.Stop()
	w.RegisterWorkflow(Workflow)
	w.RegisterActivity(LargePayloadActivity)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	// Start a workflow that executes an activity with a "large" response payload
	wfr, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		TaskQueue:                "large_payloads",
		WorkflowExecutionTimeout: time.Second * 10,
	}, Workflow)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for workflow to complete and fail test if workflow errors.
	if err := wfr.Get(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	// Validate that activity result was encoded via remote codec
	wfHistory := c.GetWorkflowHistory(context.Background(), wfr.GetID(), wfr.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for wfHistory.HasNext() {
		event, err := wfHistory.Next()
		if err != nil {
			t.Fatal(err)
		}
		t.Log(event.GetEventType().String())
		if event.GetEventType() == enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			payload := event.GetActivityTaskCompletedEventAttributes().GetResult().GetPayloads()[0]
			if _, ok := payload.GetMetadata()["temporal.io/remote-codec"]; !ok {
				t.Errorf("activity payload not encoded with remote codec, got: %s", payload.String())
			}
		}
	}
}

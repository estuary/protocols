package capture

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
)

func TestPushServerLifecycle(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var ctx, cancel = context.WithCancel(context.Background())
	push, err := NewPushServer(
		ctx,
		func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
			return new(pf.MockCombiner), nil
		},
		pf.NewFullRange(),
		&spec,
		"a-version",
	)
	require.NoError(t, err)

	var captured []json.RawMessage
	var reducedCheckpoint pf.DriverCheckpoint

	// drain takes Combined documents from the MockCombiner, appending them into
	// |captured|, and reduces the driver checkpoint into |reducedCheckpoint|.
	var drain = func() {
		var combiner = push.Combiners()[0].(*pf.MockCombiner)
		captured = append(captured, combiner.Combined...)
		combiner.Combined = nil

		require.NoError(t, reducedCheckpoint.Reduce(push.DriverCheckpoint()))
	}

	// Start Serve() delivering into |startCommitCh|.
	// On |cancel| it will gracefully stop.
	var startCommitCh = make(chan error)
	go push.Serve(func(err error) { startCommitCh <- err })

	var acksCh = make(chan struct{})

	require.NoError(t, push.Push(
		[]Documents{*makeDocs(0, "one"), *makeDocs(0, "two")},
		*makeCheckpoint(map[string]int{"a": 1}),
		acksCh,
	))

	// Expect Serve notified our callback.
	require.NoError(t, <-startCommitCh)
	drain()

	// Tell Serve of a pending log commit.
	var commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))

	// Two new Pushes arrive.
	require.NoError(t, push.Push(
		[]Documents{*makeDocs(0, "three")},
		*makeCheckpoint(map[string]int{"b": 1}),
		acksCh,
	))
	require.NoError(t, push.Push(
		[]Documents{*makeDocs(0, "four", "five")},
		*makeCheckpoint(map[string]int{"b": 2}),
		acksCh,
	))

	commitOp.Resolve(nil)
	<-acksCh // Expect first Push is acknowledged.

	// We were notified that the next commit is ready.
	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	_, _ = <-acksCh, <-acksCh // Next two Pushes are acknowledged.

	// A checkpoint without Documents is also valid.
	require.NoError(t, push.Push(
		nil,
		*makeCheckpoint(map[string]int{"a": 2}),
		acksCh,
	))

	// Begin a graceful top of Serve.
	cancel()

	// We were notified that the next commit is ready.
	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	_ = <-acksCh // Last push is acknowledged.

	// Serve has stopped running.
	<-push.ServeOp().Done()

	// We're notified of the close.
	require.Equal(t, io.EOF, <-startCommitCh)
	// The client closes gracefully.
	require.NoError(t, push.Close())
	// A further attempt to push errors, since Serve is no longer listening.
	require.Equal(t, io.EOF, push.Push(nil, pf.DriverCheckpoint{}, acksCh))
	// A further attempt to set a LogCommitOp errors, since Serve is no longer listening.
	require.Equal(t, io.EOF, push.SetLogCommitOp(client.NewAsyncOperation()))

	// Snapshot the recorded observations of drains.
	cupaloy.SnapshotT(t,
		"DRIVER CHECKPOINT:", reducedCheckpoint,
		"CAPTURED", captured,
	)
}

package sql

import (
	"context"
	"encoding/base64"

	pf "github.com/estuary/protocols/flow"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Fence is an installed barrier in a shared checkpoints table which prevents
// other sessions from committing transactions under the fenced ID --
// and prevents this Fence from committing where another session has in turn
// fenced this instance off.
type Fence struct {
	// Checkpoint associated with this Fence.
	Checkpoint []byte

	// fence is the current value of the monotonically increasing integer used to identify unique
	// instances of transactions rpcs.
	Fence int64
	// Full name of the fenced materialization.
	Materialization pf.Materialization
	// [keyBegin, keyEnd) identify the range of keys covered by this Fence.
	KeyBegin uint32
	KeyEnd   uint32

	UpdateSQL string
}

// LogEntry returns a log.Entry with pre-set fields that identify the Shard ID and Fence
func (f *Fence) LogEntry() *log.Entry {
	return log.WithFields(log.Fields{
		"materialization": f.Materialization,
		"keyBegin":        f.KeyBegin,
		"keyEnd":          f.KeyEnd,
		"fence":           f.Fence,
	})
}

// Update the fence and its Checkpoint, returning an error if this Fence
// has in turn been fenced off by another.
// Update takes a ExecFn callback which should be scoped to a database transaction,
// such as sql.Tx or a database-specific transaction implementation.
func (f *Fence) Update(ctx context.Context, execFn ExecFn) error {
	rowsAffected, err := execFn(
		ctx,
		f.UpdateSQL,
		base64.StdEncoding.EncodeToString(f.Checkpoint),
		f.Materialization,
		f.KeyBegin,
		f.KeyEnd,
		f.Fence,
	)
	if err == nil && rowsAffected == 0 {
		err = errors.Errorf("this transactions session was fenced off by another")
	}
	return err
}

// ExecFn executes a |sql| statement with |arguments|, and returns the number of rows affected.
type ExecFn func(ctx context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error)

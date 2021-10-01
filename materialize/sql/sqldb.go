package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"

	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// SqlDbEndpoint is the *database/sql.DB implementation of an endpoint
type SqlDbEndpoint struct {
	// Parsed configuration of this Endpoint, as a driver-specific type.
	Config interface{}
	// Endpoint opened as driver/sql DB.
	DB *sql.DB
	// Generator of SQL for this endpoint.
	Generator *Generator
	// FlowTables
	FlowTables *FlowTables
}

func (e *SqlDbEndpoint) GetGenerator() *Generator {
	return e.Generator
}

func (e *SqlDbEndpoint) GetFlowTables() *FlowTables {
	return e.FlowTables
}

// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func (e *SqlDbEndpoint) LoadSpec(ctx context.Context, materialization pf.Materialization) (version string, _ *pf.MaterializationSpec, _ error) {

	// Fail-fast: surface a connection issue.
	if err := e.DB.PingContext(ctx); err != nil {
		return "", nil, fmt.Errorf("connecting to DB: %w", err)
	}

	var specB64 string
	var spec = new(pf.MaterializationSpec)

	var err = e.DB.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization=%s;",
			e.FlowTables.Specs.Identifier,
			e.Generator.Placeholder(0),
		),
		materialization.String(),
	).Scan(&version, &specB64)

	if err != nil {
		log.WithFields(log.Fields{
			"table": e.FlowTables.Specs.Identifier,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return "", nil, nil
	} else if specBytes, err := base64.StdEncoding.DecodeString(specB64); err != nil {
		return version, nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return version, nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return version, nil, fmt.Errorf("validating spec: %w", err)
	}

	return version, spec, nil
}

// ExecuteStatements executes all of the statements provided in a single transaction.
// It will skip a transaction if there is only one statement
func (e *SqlDbEndpoint) ExecuteStatements(ctx context.Context, statements []string) error {

	if len(statements) == 1 {
		log.WithField("sql", statements[0]).Debug("executing single statement")
		if _, err := e.DB.Exec(statements[0]); err != nil {
			return fmt.Errorf("executing single statement: %w", err)
		}
	} else if len(statements) > 1 {
		log.Debug("starting transaction")
		var txn, err = e.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("DB.BeginTx: %w", err)
		}
		for i, statement := range statements {
			log.WithField("sql", statement).Debug("executing statement")
			if _, err := txn.Exec(statement); err != nil {
				_ = txn.Rollback()
				return fmt.Errorf("executing statement %d: %w", i, err)
			}
		}
		if err := txn.Commit(); err != nil {
			return err
		}
		log.Debug("committed transaction")
	}
	return nil

}

// NewFence installs and returns a new *Fence. On return, all older fences of
// this |shardFqn| have been fenced off from committing further transactions.
func (e *SqlDbEndpoint) NewFence(ctx context.Context, materialization pf.Materialization, keyBegin, keyEnd uint32) (*Fence, error) {
	var txn, err = e.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("db.BeginTx: %w", err)
	}

	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET fence=fence+1
				WHERE materialization=%s
				AND key_end>=%s
				AND key_begin<=%s
			;
			`,
			e.FlowTables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
		),
		materialization,
		keyBegin,
		keyEnd,
	); err != nil {
		return nil, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end]
	// which fully overlaps our range.
	var fence int64
	var readBegin, readEnd uint32
	var checkpointB64 string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT fence, key_begin, key_end, checkpoint
				FROM %s
				WHERE materialization=%s
				AND key_begin<=%s
				AND key_end>=%s
				ORDER BY key_end - key_begin ASC
				LIMIT 1
			;
			`,
			e.FlowTables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
		),
		materialization,
		keyBegin,
		keyEnd,
	).Scan(&fence, &readBegin, &readEnd, &checkpointB64); err == sql.ErrNoRows {
		// A checkpoint doesn't exist. Use an implicit checkpoint value.
		fence = 1
		// Initialize a checkpoint such that the materialization starts from
		// scratch, regardless of the runtime's internal checkpoint.
		checkpointB64 = base64.StdEncoding.EncodeToString(pm.ExplicitZeroCheckpoint)
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return nil, fmt.Errorf("scanning fence and checkpoint: %w", err)
	}

	// If a checkpoint for this exact range doesn't exist, insert it now.
	if readBegin == keyBegin && readEnd == keyEnd {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, checkpoint, fence) VALUES (%s, %s, %s, %s, %s);",
			e.FlowTables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
			e.Generator.Placeholder(3),
			e.Generator.Placeholder(4),
		),
		materialization,
		keyBegin,
		keyEnd,
		checkpointB64,
		fence,
	); err != nil {
		return nil, fmt.Errorf("inserting fence: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(checkpointB64)
	if err != nil {
		return nil, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return nil, fmt.Errorf("txn.Commit: %w", err)
	}

	// Craft SQL which is used for future commits under this fence.
	var updateSQL = fmt.Sprintf(
		"UPDATE %s SET checkpoint=%s WHERE materialization=%s AND key_begin=%s AND key_end=%s AND fence=%s;",
		e.FlowTables.Checkpoints.Identifier,
		e.Generator.Placeholder(0),
		e.Generator.Placeholder(1),
		e.Generator.Placeholder(2),
		e.Generator.Placeholder(3),
		e.Generator.Placeholder(4),
	)

	return &Fence{
		Checkpoint:      checkpoint,
		Fence:           fence,
		Materialization: materialization,
		KeyBegin:        keyBegin,
		KeyEnd:          keyEnd,
		UpdateSQL:       updateSQL,
	}, nil
}

// UpdateFenceQuery returns the sql.DB compliant query+args suitable for updating the fence value from a transaction.
// If this query does not affect any rows it should be considered failed and the fence has failed to update.
func (e *SqlDbEndpoint) UpdateFence(fence *Fence) (string, []interface{}) {

	// Craft SQL which is used for future commits under this fence.
	return fmt.Sprintf(
			"UPDATE %s SET checkpoint=%s WHERE materialization=%s AND key_begin=%s AND key_end=%s AND fence=%s;",
			e.FlowTables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
			e.Generator.Placeholder(3),
			e.Generator.Placeholder(4),
		),
		[]interface{}{
			base64.StdEncoding.EncodeToString(fence.Checkpoint),
			fence.Materialization,
			fence.KeyBegin,
			fence.KeyEnd,
			fence.Fence,
		}

}

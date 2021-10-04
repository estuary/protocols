package sql

import (
	"context"

	pf "github.com/estuary/protocols/flow"
)

// Endpoint is an sql compatible endpoint that allows dialect specific tasks and generators.
type Endpoint interface {

	// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
	LoadSpec(ctx context.Context, materialization pf.Materialization) (string, *pf.MaterializationSpec, error)

	// CreateTableStatement returns the SQL statement to create the specified table in correct dialect.
	CreateTableStatement(table *Table) (string, error)

	// ExecuteStatements takes a slice of SQL statements and executes them as a single transaction
	// (assuming it is possible for the dialect) and rolls back in case of failure.
	ExecuteStatements(ctx context.Context, statements []string) error

	// NewFence installs and returns a new *Fence. On return, all older fences of
	// this |shardFqn| have been fenced off from committing further transactions.
	NewFence(ctx context.Context, materialization pf.Materialization, keyBegin, keyEnd uint32) (*Fence, error)

	// Generator returns the dialect specific SQL generator for the endpoint.
	Generator() Generator

	// FlowTables returns the FlowTables definitions for this endpoint.
	FlowTables() FlowTables
}

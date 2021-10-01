package sql

import (
	"context"

	pf "github.com/estuary/protocols/flow"
)

// Endpoint is an sql compatible endpoint that allows dialect specific tasks and generators.
type Endpoint interface {

	// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
	LoadSpec(ctx context.Context, materialization pf.Materialization) (string, *pf.MaterializationSpec, error)

	// ExecuteStatements takes a slice of SQL statements and executes them as a single transaction and rolls back
	// in case of failure. If one statement is provided it will execute it outside of a transaction for databases
	// that do not support transactions for certain kinds of statements. (Table structure manipulation)
	ExecuteStatements(ctx context.Context, statements []string) error

	// NewFence installs and returns a new *Fence. On return, all older fences of
	// this |shardFqn| have been fenced off from committing further transactions.
	NewFence(ctx context.Context, materialization pf.Materialization, keyBegin, keyEnd uint32) (*Fence, error)

	// GetGenerator gets the sql dialect generator for this endpoint.
	GetGenerator() *Generator

	// GetFlowTables returns the FlowTables definitions for this endpoint.
	GetFlowTables() *FlowTables
}

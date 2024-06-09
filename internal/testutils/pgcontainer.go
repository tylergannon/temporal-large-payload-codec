package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const createTableStatement = "CREATE TABLE workflow_arguments (id BIGSERIAL PRIMARY KEY, sha256 BYTEA NOT NULL, metadata BYTEA NOT NULL, data BYTEA NOT NULL);"

type ExecStruct struct {
	Cmd []string
}

func (e ExecStruct) AsCommand() []string {
	return e.Cmd
}
func (e ExecStruct) Options() []testcontainers.ExecOptions {
	var res = make([]testcontainers.ExecOptions, 0)
	return res
}

type DbSetup struct {
	Container testcontainers.Container
	Pool      *pgxpool.Pool
	ConnStr   string
	Error     error
}

func (t *DbSetup) Close(ctx context.Context, duration *time.Duration) {
	if t.Pool != nil {
		t.Pool.Close()
	}
	t.Container.Stop(ctx, duration)
}
func SetupTestDatabase(ctx context.Context) DbSetup {
	// 1. Create PostgreSQL container request
	containerReq := testcontainers.ContainerRequest{
		Image:        "postgres:alpine",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
		},
	}

	// 2. Start PostgreSQL container
	dbContainer, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerReq,
			Started:          true,
		})
	if err != nil {
		return DbSetup{Error: fmt.Errorf("failed to start container: %w", err)}
	}

	// 3.1 Get host and port of PostgreSQL container
	host, err := dbContainer.Host(context.Background())
	if err != nil {
		return DbSetup{Error: err}
	}

	port, err := dbContainer.MappedPort(context.Background(), "5432")

	if err != nil {
		return DbSetup{Error: fmt.Errorf("failed to get port: %w", err)}
	}

	// 3.2 Create db connection string and connect
	dbURI := fmt.Sprintf("postgres://postgres:postgres@%v:%v/testdb", host, port.Port())
	connPool, err := pgxpool.New(context.Background(), dbURI)
	if err != nil {
		return DbSetup{Error: err, Container: dbContainer}
	}

	conn, err := connPool.Acquire(ctx)
	if err != nil {
		return DbSetup{Error: err, Container: dbContainer}
	}

	defer conn.Release()

	_, err = conn.Exec(ctx, createTableStatement)
	if err != nil {
		return DbSetup{Error: fmt.Errorf("unable to create table: %w", err)}
	}

	return DbSetup{Container: dbContainer, Pool: connPool, ConnStr: dbURI, Error: nil}
}

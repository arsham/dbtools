package dbtools_test

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/arsham/dbtools/v3/mocks"
	"github.com/arsham/retry/v3"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// assertInError returns true if the needle is found in stack, which is created
// either with pkg/errors help or Go's error wrap. It will fall back to
// checking the contents of the needle.Error() is in haystack.Error().
func assertInError(t *testing.T, haystack, needle error) bool {
	t.Helper()
	if haystack == nil || needle == nil {
		t.Errorf("want %v in %v", needle, haystack)
		return false
	}
	if errors.Is(haystack, needle) {
		return true
	}
	return assert.Containsf(t, haystack.Error(), needle.Error(),
		"want\n\t%v\nin\n\t%v", needle, haystack,
	)
}

func randomString(count int) string {
	const runes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, count)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

// getDB returns an address to a running postgres database inside a container.
// The container will be removed after test is finished running.
func getDB(t *testing.T) string {
	t.Helper()
	// If you faced with any issues setting up containers, comment this out:
	testcontainers.Logger = log.New(&ioutils.NopWriter{}, "", 0)

	var (
		pgContainer *postgres.PostgresContainer
		r           = &retry.Retry{
			Attempts: 30,
			Delay:    300 * time.Millisecond,
		}
		ctx = context.Background()
	)

	err := r.Do(func() error {
		var (
			containerName = "dbtools_" + randomString(50)
			err           error
		)

		pgContainer, err = postgres.RunContainer(ctx,
			testcontainers.WithImage("docker.io/postgres:15-alpine"),
			testcontainers.WithHostConfigModifier(func(c *container.HostConfig) {
				c.Memory = 256 * 1024 * 1024
			}),
			testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
				req.Name = containerName
				return nil
			}),
			postgres.WithDatabase("dbtools"),
			postgres.WithUsername("dbtools"),
			postgres.WithPassword(randomString(20)),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Second)),
		)
		if err != nil {
			pgContainer.Terminate(ctx)
			return err
		}
		return nil
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	})

	addr, err := pgContainer.ConnectionString(ctx)
	require.NoError(t, err)

	return addr
}

type exampleConn struct{}

func (e *exampleConn) Begin(context.Context) (pgx.Tx, error) {
	tx := &mocks.PGXTx{}
	tx.On("Rollback", mock.Anything).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)
	return tx, nil
}

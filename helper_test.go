package dbtools_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/arsham/dbtools/v2/mocks"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
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

func init() {
	rand.Seed(time.Now().UnixNano())
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
func getDB(t *testing.T) (addr string) {
	t.Helper()
	ctx := context.Background()
	env := make(map[string]string)
	env["POSTGRES_PASSWORD"] = "1234"
	req := testcontainers.ContainerRequest{
		Image:        "postgres:12-alpine",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		AutoRemove:   true,
		Env:          env,
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	ip, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	t.Cleanup(func() {
		container.Terminate(ctx)
	})
	return buildQueryString("postgres", "1234", "postgres", ip, port.Port())
}

// buildQueryString builds a query string.
func buildQueryString(user, pass, dbname, host, port string) string {
	parts := []string{}
	if user != "" {
		parts = append(parts, fmt.Sprintf("user=%s", user))
	}
	if pass != "" {
		parts = append(parts, fmt.Sprintf("password=%s", pass))
	}
	if dbname != "" {
		parts = append(parts, fmt.Sprintf("dbname=%s", dbname))
	}
	if host != "" {
		parts = append(parts, fmt.Sprintf("host=%s", host))
	}
	if port != "" {
		parts = append(parts, fmt.Sprintf("port=%s", port))
	}
	return strings.Join(parts, " ")
}

type exampleConn struct{}

func (e *exampleConn) Begin(context.Context) (pgx.Tx, error) {
	tx := &mocks.PGXTx{}
	tx.On("Rollback", mock.Anything).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)
	return tx, nil
}

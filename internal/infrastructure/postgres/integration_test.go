package postgres_test

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	appuser "go-backend-service-blueprint/internal/app/user"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	"go-backend-service-blueprint/internal/infrastructure/postgres"
	"go-backend-service-blueprint/pkg/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage = "postgres:17.2-alpine"
	migrationsDir = "./migrations"
)

var (
	testDB  *database.AppDB
	discard = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// TestMain owns one container for the whole package: migrations run once and
// every test truncates what it touched instead of paying for a fresh database.
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("blueprint_test"),
		tcpostgres.WithUsername("blueprint"),
		tcpostgres.WithPassword("blueprint"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}

	code := func() int {
		defer func() {
			if err := testcontainers.TerminateContainer(container); err != nil {
				log.Printf("terminate postgres container: %v", err)
			}
		}()

		testDB, err = connect(ctx, container)
		if err != nil {
			log.Fatalf("connect to postgres container: %v", err)
		}
		defer testDB.Close()

		return m.Run()
	}()

	os.Exit(code)
}

func connect(ctx context.Context, container *tcpostgres.PostgresContainer) (*database.AppDB, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, err
	}

	db, err := database.InitDB(&database.DBConfig{
		Host:           host,
		Port:           port.Port(),
		Username:       "blueprint",
		Password:       "blueprint",
		DBName:         "blueprint_test",
		SSLMode:        "disable",
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 5,
	})
	if err != nil {
		return nil, err
	}

	if err := db.RunMigrations(migrationsDir); err != nil {
		db.Close()

		return nil, err
	}

	return db, nil
}

func freshDB(t *testing.T) *database.AppDB {
	t.Helper()

	if testing.Short() {
		t.Skip("integration test needs a docker daemon")
	}

	_, err := testDB.Postgresql.Exec(context.Background(), "TRUNCATE users, operations")
	require.NoError(t, err)

	return testDB
}

func lockTimeout() <-chan time.Time {
	return time.After(5 * time.Second)
}

func countUsers(t *testing.T, email string) int {
	t.Helper()

	var count int
	require.NoError(t, testDB.Postgresql.
		QueryRow(context.Background(), "SELECT count(*) FROM users WHERE email = $1", email).
		Scan(&count))

	return count
}

type failingOperations struct {
	domainoperation.Repository
}

func (failingOperations) Create(context.Context, *domainoperation.Operation) error {
	return errors.New("operations table is unavailable")
}

func TestCreateCommitsTheUserAndItsWelcomeOperationTogether(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	operations := postgres.NewOperationRepository(db)

	useCases := appuser.NewUseCases(
		postgres.NewUserRepository(db),
		operations,
		postgres.NewTxManager(db),
		appuser.NewUnlimited(),
		0,
		discard,
	)

	created, err := useCases.Create(ctx, appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})
	require.NoError(t, err)

	stored, err := useCases.Get(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, "jane@example.com", stored.Email)

	planned, err := operations.ListPlannedBatch(ctx, time.Now().UTC(), uuid.Nil, 10)
	require.NoError(t, err)
	require.Len(t, planned, 1)
	assert.Equal(t, domainoperation.OperationTypeSendWelcomeEmail, planned[0].OperationType)
	assert.Equal(t, created.UUID, planned[0].EntityID)
	assert.Equal(t, domainoperation.StatusPlanned, planned[0].Status)
	assert.JSONEq(t, `{}`, string(planned[0].Payload))
}

// TestCreateRollsBackTheUserWhenTheOperationInsertFails is the regression test
// for a repository that talked to the pool instead of the transaction: the user
// was committed on its own connection and survived the rollback.
func TestCreateRollsBackTheUserWhenTheOperationInsertFails(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	useCases := appuser.NewUseCases(
		postgres.NewUserRepository(db),
		failingOperations{Repository: postgres.NewOperationRepository(db)},
		postgres.NewTxManager(db),
		appuser.NewUnlimited(),
		0,
		discard,
	)

	_, err := useCases.Create(ctx, appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})

	require.Error(t, err)
	assert.Equal(t, 0, countUsers(t, "jane@example.com"))
}

func TestCreateReportsADuplicateEmail(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	useCases := appuser.NewUseCases(
		postgres.NewUserRepository(db),
		postgres.NewOperationRepository(db),
		postgres.NewTxManager(db),
		appuser.NewUnlimited(),
		0,
		discard,
	)

	_, err := useCases.Create(ctx, appuser.CreateCommand{Email: "jane@example.com", FullName: "Jane"})
	require.NoError(t, err)

	_, err = useCases.Create(ctx, appuser.CreateCommand{Email: "JANE@example.com", FullName: "Jane Again"})

	require.ErrorIs(t, err, domainuser.ErrAlreadyExists)
	assert.Equal(t, 1, countUsers(t, "jane@example.com"))
}

func TestGetByUUIDReportsAMissingUser(t *testing.T) {
	db := freshDB(t)

	_, err := postgres.NewUserRepository(db).GetByUUID(context.Background(), uuid.New())

	require.ErrorIs(t, err, domainuser.ErrNotFound)
}

func TestListPlannedBatchPagesByKeysetAndHonoursPlannedAt(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	operations := postgres.NewOperationRepository(db)

	due := make([]*domainoperation.Operation, 0, 3)
	for range 3 {
		op := domainoperation.New(domainoperation.OperationTypeRecalculateStats, uuid.New(), nil, now, now)
		require.NoError(t, operations.Create(ctx, op))
		due = append(due, op)
	}

	future := domainoperation.New(
		domainoperation.OperationTypeRecalculateStats,
		uuid.New(),
		nil,
		now.Add(time.Hour),
		now,
	)
	require.NoError(t, operations.Create(ctx, future))

	seen := make([]string, 0, len(due))
	after := uuid.Nil
	for {
		batch, err := operations.ListPlannedBatch(ctx, now, after, 2)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}

		for _, op := range batch {
			seen = append(seen, op.UUID.String())
		}
		after = batch[len(batch)-1].UUID
	}

	assert.ElementsMatch(t, []string{due[0].UUID.String(), due[1].UUID.String(), due[2].UUID.String()}, seen)
	assert.IsIncreasing(t, seen)
}

func TestLockedOperationIsSettledAndLeavesTheQueue(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	operations := postgres.NewOperationRepository(db)

	op := domainoperation.New(domainoperation.OperationTypeRecalculateStats, uuid.New(), nil, now, now)
	require.NoError(t, operations.Create(ctx, op))

	err := postgres.NewTxManager(db).WithinTx(ctx, func(ctx context.Context) error {
		locked, lockErr := operations.LockForUpdate(ctx, op.UUID)
		if lockErr != nil {
			return lockErr
		}

		locked.Complete([]byte(`{"ok":true}`), now)

		return operations.Update(ctx, locked)
	})
	require.NoError(t, err)

	planned, err := operations.ListPlannedBatch(ctx, now, uuid.Nil, 10)
	require.NoError(t, err)
	assert.Empty(t, planned)
}

// TestLockForUpdateSkipsARowHeldByAnotherTransaction proves the NOWAIT
// contract: the second worker gets an answer immediately instead of waiting for
// the first one to commit.
func TestLockForUpdateSkipsARowHeldByAnotherTransaction(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	operations := postgres.NewOperationRepository(db)
	manager := postgres.NewTxManager(db)

	op := domainoperation.New(domainoperation.OperationTypeRecalculateStats, uuid.New(), nil, now, now)
	require.NoError(t, operations.Create(ctx, op))

	locked := make(chan struct{})
	release := make(chan struct{})
	holder := make(chan error, 1)

	go func() {
		holder <- manager.WithinTx(ctx, func(ctx context.Context) error {
			if _, err := operations.LockForUpdate(ctx, op.UUID); err != nil {
				return err
			}
			close(locked)
			<-release

			return nil
		})
	}()

	// A failing assertion below must still let the holder commit, otherwise the
	// leaked row lock blocks the TRUNCATE of every later test in this package.
	var holderErr error
	stopHolder := sync.OnceFunc(func() {
		close(release)
		holderErr = <-holder
	})
	t.Cleanup(stopHolder)

	select {
	case <-locked:
	case <-lockTimeout():
		t.Fatal("first worker never took the lock")
	}

	done := make(chan error, 1)
	go func() {
		done <- manager.WithinTx(ctx, func(ctx context.Context) error {
			_, err := operations.LockForUpdate(ctx, op.UUID)

			return err
		})
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, domainoperation.ErrLocked)
	case <-lockTimeout():
		t.Fatal("second worker blocked on the locked row instead of skipping it")
	}

	stopHolder()
	require.NoError(t, holderErr)

	reread, err := operations.ListPlannedBatch(ctx, now, uuid.Nil, 10)
	require.NoError(t, err)
	require.Len(t, reread, 1)
	assert.Equal(t, domainoperation.StatusPlanned, reread[0].Status)
}

func TestLockForUpdateReportsAMissingOperation(t *testing.T) {
	db := freshDB(t)

	err := postgres.NewTxManager(db).WithinTx(context.Background(), func(ctx context.Context) error {
		_, lockErr := postgres.NewOperationRepository(db).LockForUpdate(ctx, uuid.New())

		return lockErr
	})

	require.ErrorIs(t, err, domainoperation.ErrNotFound)
}

package models

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netlify/gotrue/storage"
	"github.com/sirupsen/logrus"
	metricglobal "go.opentelemetry.io/otel/metric/global"
	metricinstrument "go.opentelemetry.io/otel/metric/instrument"
	otelasyncint64instrument "go.opentelemetry.io/otel/metric/instrument/asyncint64"
)

var cleanupStatements []string
var cleanupAffectedRows otelasyncint64instrument.Counter

func init() {
	tableRefreshTokens := RefreshToken{}.TableName()
	tableSessions := Session{}.TableName()

	// These statements intentionally use a FOR UPDATE SKIP LOCKED clause
	// in the select portion which ensures that the transaction performing
	// the cleanup does not wait on other transactions on the database that
	// are touching those rows. This ensures that cleanups always run as
	// fast as possible while consuming minimal resources to do it.
	cleanupStatements = append(cleanupStatements,
		// deletes 100 refresh tokens without waiting on locked rows
		fmt.Sprintf("delete %s where id in (select id from %s where revoked is true and updated_at < (now() - interval '10 minutes') limit 100 for update skip locked);", tableRefreshTokens, tableRefreshTokens),

		// sets refresh tokens as revoked on expired sessions since 10 minutes ago without waiting on locked rows
		fmt.Sprintf("update %s set revoked = true where id in (select id from %s join %s on %s.session_id = %s.id where %s.session_not_after < (now() - interval '10 minutes') and %s.revoked is false limit 100 for update skip locked);", tableRefreshTokens, tableRefreshTokens, tableSessions, tableRefreshTokens, tableSessions, tableSessions, tableRefreshTokens),

		// deletes sessions that expired 48h ago (gives ample time to clean up their refresh tokens in tiny batches)
		fmt.Sprintf("delete %s where id (select id from %s where session_not_after < (now() - interval '48 hours') limit 100 for update skip locked)", tableSessions, tableSessions),
	)

	var err error
	cleanupAffectedRows, err = metricglobal.Meter("gotrue").AsyncInt64().Counter(
		"gotrue_cleanup_affected_rows",
		metricinstrument.WithDescription("Number of affected rows from cleaning up stale entities"),
	)
	if err != nil {
		logrus.WithError(err).Error("unable to get gotrue.gotrue_cleanup_rows counter metric")
	}
}

// Cleanup removes stale entities in the database. You can call it on each
// request or as a periodic background job. It does quick lockless updates or
// deletes, has an execution timeout and acquire timeout so that cleanups do
// not affect performance of other database jobs.
func Cleanup(db *storage.Connection) (int, error) {
	ctx, cancel := context.WithTimeout(db.Context(), 50*time.Millisecond)
	defer cancel()

	acquiredSignal := make(chan struct{})

	go func() {
		select {
		case <-acquiredSignal:
			// connection was acquired
			return

		case <-time.After(5 * time.Millisecond):
			// don't even try to clean up, it's taking a while to
			// acquire a free connection
			cancel()
			return
		}
	}()

	affectedRows := 0

	if err := db.WithContext(ctx).Transaction(func(tx *storage.Connection) error {
		ctx := tx.Context()

		if err := ctx.Err(); err != nil {
			// check if context timed out or was cancelled
			// not returning an error since this is OK
			return nil
		}

		close(acquiredSignal)

		for _, statement := range cleanupStatements {
			if err := ctx.Err(); err != nil {
				// check if context timed out or was cancelled
				// not returning an error since this is OK and
				// we want to keep any changes to the DB made
				// prior to timing out
				return nil
			}

			count, err := tx.RawQuery(statement).ExecWithCount()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// keep changes to the database
					return nil
				}

				return err
			}

			affectedRows += count
		}

		return nil
	}); err != nil {
		return affectedRows, err
	}

	if cleanupAffectedRows != nil {
		cleanupAffectedRows.Observe(db.Context(), int64(affectedRows))
	}

	return affectedRows, nil
}

package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestUnroutablePublishIsNotSuccess(t *testing.T) {
	returns := make(chan amqp.Return, 1)
	require.NoError(t, unroutable(returns))

	returns <- amqp.Return{ReplyCode: amqp.NoRoute, ReplyText: "NO_ROUTE"}
	err := unroutable(returns)
	require.ErrorIs(t, err, ErrPublishUnroutable)
	require.Contains(t, err.Error(), "NO_ROUTE")

	returns <- amqp.Return{ReplyText: "stale"}
	drainReturns(returns)
	require.NoError(t, unroutable(returns))
}

func TestReturnHelpersSurviveAClosedChannel(t *testing.T) {
	returns := make(chan amqp.Return)
	close(returns)

	require.NoError(t, unroutable(returns))
	drainReturns(returns)
}

package rabbitmq

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	heartbeat   = 10 * time.Second
	dialTimeout = 10 * time.Second
)

type Config struct {
	Host           string
	Port           int
	User           string
	Password       string
	VHost          string
	ConnectionName string
}

func (c Config) URL() string {
	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.User, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
	}
	if c.VHost != "" && c.VHost != "/" {
		u.Path = "/" + c.VHost
		u.RawPath = "/" + url.PathEscape(c.VHost)
	}

	return u.String()
}

func Dial(cfg Config) (*amqp.Connection, error) {
	properties := amqp.NewConnectionProperties()
	if cfg.ConnectionName != "" {
		properties.SetClientConnectionName(cfg.ConnectionName)
	}

	conn, err := amqp.DialConfig(cfg.URL(), amqp.Config{
		Heartbeat:  heartbeat,
		Properties: properties,
		Dial:       amqp.DefaultDial(dialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), err)
	}

	return conn, nil
}

func DeadLetterQueueName(queue string) string {
	return queue + ".dead_letter"
}

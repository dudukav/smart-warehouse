package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

const defaultTimeoutSeconds = 5

type CassandraConfig struct {
	Hosts       []string
	Keyspace    string
	Consistency gocql.Consistency
	Timeout     time.Duration
}

type CassandraStore struct {
	session *gocql.Session
}

func NewCassandraStore(cfg *CassandraConfig) (*CassandraStore, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)

	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = cfg.Consistency
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.Timeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create cassandra session: %w", err)
	}

	return &CassandraStore{
		session: session,
	}, nil
}

func (s *CassandraStore) Close() {
	s.session.Close()
}

func DefaultCassandraConfig() CassandraConfig {
	return CassandraConfig{
		Hosts: []string{
			"cassandra-1",
			"cassandra-2",
			"cassandra-3",
		},
		Keyspace:    "smart_warehouse",
		Consistency: gocql.Quorum,
		Timeout:     defaultTimeoutSeconds * time.Second,
	}
}

func (s *CassandraStore) Ping(ctx context.Context) error {
	return s.session.Query(`SELECT now() FROM system.local`).WithContext(ctx).Exec()
}

func (s *CassandraStore) newLoggedBatch(ctx context.Context) *gocql.Batch {
	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.WithContext(ctx)
	return batch
}

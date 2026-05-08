CREATE KEYSPACE IF NOT EXISTS smart_warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};
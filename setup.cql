CREATE KEYSPACE db3
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'DC1': 2
};

USE db3;

CREATE TABLE IF NOT EXISTS telemetry (
    timestamp      double,
    device         text,
    carbonmonoxide double,
    humidity       double,
    light          tinyint,
    lpg            double,
    motion         tinyint,
    smoke          double,
    temperature    double,
    PRIMARY KEY (device, timestamp) 
) WITH CLUSTERING ORDER BY (timestamp DESC);
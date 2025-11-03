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

CREATE TABLE IF NOT EXISTS device_configuration_history (
    device text,
    valid_to_timestamp double,
    firmware_version text,
    PRIMARY KEY (device, valid_to_timestamp)
) WITH CLUSTERING ORDER BY (valid_to_timestamp DESC);
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

CREATE MATERIALIZED VIEW IF NOT EXISTS config_by_firmware_version AS
    SELECT device, valid_to_timestamp, firmware_version
    FROM device_configuration_history
    WHERE device IS NOT NULL AND valid_to_timestamp IS NOT NULL AND firmware_version IS NOT NULL
    PRIMARY KEY (firmware_version, valid_to_timestamp, device)
WITH CLUSTERING ORDER BY (valid_to_timestamp DESC);
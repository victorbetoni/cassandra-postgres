CREATE TABLE public.telemetry (
    id             BIGSERIAL PRIMARY KEY, 
    timestamp      DOUBLE PRECISION NOT NULL, 
    device         VARCHAR(50) NOT NULL,
    carbonmonoxide DOUBLE PRECISION,
    humidity       DOUBLE PRECISION,
    light          SMALLINT,
    lpg            DOUBLE PRECISION,
    motion         SMALLINT,
    smoke          DOUBLE PRECISION,
    temperature    DOUBLE PRECISION
);

CREATE INDEX idx_telemetry_device ON public.telemetry (device);
CREATE INDEX idx_telemetry_device_ts ON public.telemetry (device, timestamp DESC);
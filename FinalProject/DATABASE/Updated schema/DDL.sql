-- Task C3:
DROP SCHEMA IF EXISTS raforka_updated CASCADE;
CREATE SCHEMA raforka_updated;

-- 1. Owners
CREATE TABLE raforka_updated.owner (
    id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- 2. Base entity table for stations
CREATE TABLE raforka_updated.station (
    id             INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    type           TEXT NOT NULL,
    station_type   TEXT NOT NULL,
    installed_date DATE,
    owner_id       INTEGER NOT NULL REFERENCES raforka_updated.owner(id),
    x_coordinates  DOUBLE PRECISION NOT NULL,
    y_coordinates  DOUBLE PRECISION NOT NULL  
);

-- 3. Power plant specialization
CREATE TABLE raforka_updated.power_plant (
    power_plant_id INTEGER PRIMARY KEY
        REFERENCES raforka_updated.station(id)
);

-- 4. Substation specialization
CREATE TABLE raforka_updated.substation (
    substation_id INTEGER PRIMARY KEY
        REFERENCES raforka_updated.station(id)
);

-- 5. Customers
CREATE TABLE raforka_updated.customer (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          TEXT NOT NULL,
    ssn           TEXT NOT NULL UNIQUE,
    founded_year  INTEGER,
    x_coordinates DOUBLE PRECISION,
    y_coordinates DOUBLE PRECISION,
    owner_id      INTEGER NOT NULL REFERENCES raforka_updated.owner(id),

    CONSTRAINT chk_customer_founded_year
        CHECK (
            founded_year IS NULL OR
            (founded_year >= 1800 AND founded_year <= EXTRACT(YEAR FROM CURRENT_DATE))
        ),
    CONSTRAINT chk_customer_ssn_format
        CHECK (ssn ~ '^\d{10}$'),
    CONSTRAINT chk_customer_coords_both_or_neither
        CHECK ((x_coordinates IS NULL) = (y_coordinates IS NULL))
);

-- 6. Production
CREATE TABLE raforka_updated.production (
    power_plant_id INTEGER NOT NULL
        REFERENCES raforka_updated.power_plant(power_plant_id),
    timestamp      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value_kwh      NUMERIC NOT NULL,
    PRIMARY KEY (power_plant_id, timestamp),

    CONSTRAINT chk_production_kwh_positive
        CHECK (value_kwh > 0)
);

-- 7. Injection
CREATE TABLE raforka_updated.injects_to (
    power_plant_id       INTEGER NOT NULL,
    production_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    substation_id        INTEGER NOT NULL
        REFERENCES raforka_updated.substation(substation_id),
    value_kwh            FLOAT NOT NULL,
    timestamp            TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (power_plant_id, production_timestamp, substation_id),
    FOREIGN KEY (power_plant_id, production_timestamp)
        REFERENCES raforka_updated.production(power_plant_id, timestamp),

    CONSTRAINT chk_injects_kwh_positive
        CHECK (value_kwh > 0),

    CONSTRAINT chk_injection_after_production
        CHECK (timestamp >= production_timestamp)
);

-- 8. Withdrawal
CREATE TABLE raforka_updated.withdraws_from (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   INTEGER NOT NULL
        REFERENCES raforka_updated.customer(id),
    substation_id INTEGER NOT NULL
        REFERENCES raforka_updated.substation(substation_id),
    timestamp     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value_kwh     NUMERIC NOT NULL,

    CONSTRAINT chk_withdraws_kwh_positive
        CHECK (value_kwh > 0)
);

-- 9. Substation-to-substation connections
CREATE TABLE raforka_updated.connects_to (
    from_substation_id INTEGER NOT NULL
        REFERENCES raforka_updated.substation(substation_id),
    to_substation_id   INTEGER NOT NULL
        REFERENCES raforka_updated.substation(substation_id),
    distance           DOUBLE PRECISION NOT NULL,
    value_kwh          DOUBLE PRECISION,
    max_capacity_mw    DOUBLE PRECISION DEFAULT 0,
    PRIMARY KEY (from_substation_id, to_substation_id),

    CONSTRAINT no_self_connection
        CHECK (from_substation_id <> to_substation_id),

    CONSTRAINT chk_distance_positive
        CHECK (distance > 0),

    CONSTRAINT chk_capacity_non_negative
        CHECK (max_capacity_mw >= 0),

    CONSTRAINT chk_connects_flow_non_negative
        CHECK (value_kwh IS NULL OR value_kwh >= 0)
);
-- Task D

CREATE INDEX idx_production_plant_timestamp
    ON raforka_updated.production (power_plant_id, timestamp);


CREATE INDEX idx_production_timestamp
    ON raforka_updated.production (timestamp);


CREATE INDEX idx_injects_to_plant_timestamp
    ON raforka_updated.injects_to (power_plant_id, timestamp);


CREATE INDEX idx_injects_to_timestamp
    ON raforka_updated.injects_to (timestamp);


CREATE INDEX idx_injects_to_substation
    ON raforka_updated.injects_to (substation_id);


CREATE INDEX idx_withdraws_timestamp
    ON raforka_updated.withdraws_from (timestamp);


CREATE INDEX idx_withdraws_customer
    ON raforka_updated.withdraws_from (customer_id);


CREATE INDEX idx_withdraws_substation
    ON raforka_updated.withdraws_from (substation_id);


CREATE INDEX idx_connects_from
    ON raforka_updated.connects_to (from_substation_id);

CREATE INDEX idx_connects_to
    ON raforka_updated.connects_to (to_substation_id);

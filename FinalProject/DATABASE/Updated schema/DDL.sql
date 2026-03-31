-- Task C3:

-- Reset the Schema
DROP SCHEMA IF EXISTS raforka_updated CASCADE;
CREATE SCHEMA raforka_updated;

-- 1. Owners Table
CREATE TABLE raforka_updated.owner(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- 2. Base Station Table
CREATE TABLE raforka_updated.station (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL, 
    station_type TEXT NOT NULL,
    installed_date DATE,
    owner_id INTEGER REFERENCES raforka_updated.owner(id),
    x_coordinates DOUBLE PRECISION NOT NULL,
    y_coordinates DOUBLE PRECISION NOT NULL
);

-- 3. Power Plant Specialization
CREATE TABLE raforka_updated.power_plant (
    power_plant_id INTEGER PRIMARY KEY REFERENCES raforka_updated.station(id)
);

-- 4. Substation Specialization
CREATE TABLE raforka_updated.substation (
    substation_id INTEGER PRIMARY KEY REFERENCES raforka_updated.station(id)
);

-- 5. Customers Table
CREATE TABLE raforka_updated.customer (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    ssn TEXT NOT NULL UNIQUE,
    founded_year INTEGER,
    x_coordinates DOUBLE PRECISION,
    y_coordinates DOUBLE PRECISION, 
    owner_id INTEGER NOT NULL REFERENCES raforka_updated.owner(id)
);

-- 6. Production (Weak Entity)
CREATE TABLE raforka_updated.production (
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(power_plant_id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL,
    PRIMARY KEY (power_plant_id, timestamp)
);

-- 7. Injection (Connecting Production to Substation)
-- This represents the 'connects_to' diamond between Production and Substation
CREATE TABLE raforka_updated.injects_to(
    power_plant_id INTEGER NOT NULL,
    production_timestamp TIMESTAMP NOT NULL,
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    value_kwh NUMERIC NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (power_plant_id, production_timestamp, substation_id),
    FOREIGN KEY (power_plant_id, production_timestamp) 
        REFERENCES raforka_updated.production(power_plant_id, timestamp)
);

-- 8. Withdrawal (Represented by 'withdraws' relationship)
CREATE TABLE raforka_updated.withdraws_from(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES raforka_updated.customer(id),
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id), 
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL
);

-- 9. Substation Connections (Self-referencing 'connects_to')
CREATE TABLE raforka_updated.connects_to(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    from_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    to_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    distance DOUBLE PRECISION NOT NULL, 
    value_kwh DOUBLE PRECISION,         -- To store the estimated flow from Part F
    max_capacity_mw DOUBLE PRECISION DEFAULT 0,
    CONSTRAINT no_self_connection CHECK (from_substation_id <> to_substation_id),
    CONSTRAINT unique_connection UNIQUE (from_substation_id, to_substation_id)
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
-- Task C3

CREATE SCHEMA raforka_updated;

ALTER SCHEMA raforka_updated OWNER TO bjarki1312;

SET default_tablespace = '';

SET default_table_access_method = heap;

-- Table for owners
CREATE TABLE raforka_updated.owner(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE raforka_updated.station (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    station_type TEXT NOT NULL,
    installed_date DATE NOT NULL,
    owner_id INTEGER REFERENCES raforka_updated.owner(id),
    X_COORDINATE DOUBLE PRECISION,
    Y_COORDINATE DOUBLE PRECISION
);

-- Power plant specialization
CREATE TABLE raforka_updated.power_plant (
    power_plant_id INTEGER PRIMARY KEY REFERENCES raforka_updated.station(id)
);

-- Substation specialization
CREATE TABLE raforka_updated.substation (
    substation_id INTEGER PRIMARY KEY REFERENCES raforka_updated.station(id)
);

-- Customers with proper types
CREATE TABLE raforka_updated.customer (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    SSN TEXT NOT NULL UNIQUE,
    founded_year INTEGER NOT NULL,
    X_COORDINATE DOUBLE PRECISION,
    Y_COORDINATES DOUBLE PRECISION,
    owner_id INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES raforka_updated.owner(id)
);

-- Raw energy production at a power plant
CREATE TABLE raforka_updated.production (
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(power_plant_id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL,
    PRIMARY KEY (power_plant_id, timestamp)
);

-- Energy injected at a substation from a power plant
CREATE TABLE raforka_updated.injection (
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(power_plant_id),
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL,
    PRIMARY KEY (power_plant_id, substation_id, timestamp)
);

-- Energy withdrawn at S3 by a customer
CREATE TABLE raforka_updated.withdrawal (
    customer_id INTEGER NOT NULL REFERENCES raforka_updated.customer(id),
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id), 
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL,
    PRIMARY KEY (customer_id, substation_id, timestamp)
);

-- Plant to substation (P → S)
CREATE TABLE raforka_updated.plant_connection (
    id SERIAL PRIMARY KEY,
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(power_plant_id),
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    distance_km DOUBLE PRECISION,
    CONSTRAINT unique_plant_substation UNIQUE (power_plant_id, substation_id)
);

-- Substation to substation (S → S)
CREATE TABLE raforka_updated.substation_connection (
    id SERIAL PRIMARY KEY,
    from_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    to_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    distance_km DOUBLE PRECISION,
    CONSTRAINT no_self_connection CHECK (from_substation_id <> to_substation_id)
);



-- Task D1
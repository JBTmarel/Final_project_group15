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
    X_HNIT DOUBLE PRECISION,
    Y_HNIT DOUBLE PRECISION
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
    kennitala INTEGER NOT NULL UNIQUE,
    founded_year INTEGER NOT NULL,
    X_HNIT DOUBLE PRECISION,
    Y_HNIT DOUBLE PRECISION,
    owner_id INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES raforka_updated.owner(id)
);

-- Raw energy production at a power plant
CREATE TABLE raforka_updated.production (
    id SERIAL PRIMARY KEY,
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(power_plant_id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL
);

-- Energy injected into a substation after plant-level losses
CREATE TABLE raforka_updated.injection (
    id SERIAL PRIMARY KEY,
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(substation_id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL
);

-- Energy withdrawn at S3 by a customer
CREATE TABLE raforka_updated.withdrawal (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES raforka_updated.customer(id),
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL
);

-- Plant to substation (P → S)
CREATE TABLE raforka_updated.plant_connection (
    id SERIAL PRIMARY KEY,
    power_plant_id INTEGER NOT NULL REFERENCES raforka_updated.power_plant(id),
    substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(id),
    distance_km DOUBLE PRECISION
);

-- Substation to substation (S → S)
CREATE TABLE raforka_updated.substation_connection (
    id SERIAL PRIMARY KEY,
    from_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(id),
    to_substation_id INTEGER NOT NULL REFERENCES raforka_updated.substation(id),
    distance_km DOUBLE PRECISION
);


-- Task D1
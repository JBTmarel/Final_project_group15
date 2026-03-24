-- Task C3

CREATE SCHEMA raforka_updated;

ALTER SCHEMA raforka_updated OWNER TO bjarki1312;

SET default_tablespace = '';

SET default_table_access_method = heap;


-- Separate table for power plants only
CREATE TABLE raforka_updated.power_plant (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT,
    installed_date DATE,
    X_HNIT DOUBLE PRECISION,
    Y_HNIT DOUBLE PRECISION
);

-- Separate table for substations only
CREATE TABLE raforka_updated.substation (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    X_HNIT DOUBLE PRECISION,
    Y_HNIT DOUBLE PRECISION
);

-- Customers with proper types
CREATE TABLE raforka_updated.customer (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    kennitala TEXT,
    owner TEXT,
    founded_year INTEGER,
    X_HNIT DOUBLE PRECISION,
    Y_HNIT DOUBLE PRECISION
);

-- Measurements with proper foreign keys
CREATE TABLE raforka_updated.measurement (
    id SERIAL PRIMARY KEY,
    power_plant_id INTEGER REFERENCES raforka_updated.power_plant(id),
    customer_id INTEGER REFERENCES raforka_updated.customer(id),
    measurement_type TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value_kwh NUMERIC NOT NULL
);

-- Transmission lines between substations (for Part F)
CREATE TABLE raforka_updated.transmission_line (
    id SERIAL PRIMARY KEY,
    from_plant_id INTEGER REFERENCES raforka_updated.power_plant(id),
    from_substation_id INTEGER REFERENCES raforka_updated.substation(id),
    to_substation_id INTEGER REFERENCES raforka_updated.substation(id),
    distance_km DOUBLE PRECISION
);

-- Task D1
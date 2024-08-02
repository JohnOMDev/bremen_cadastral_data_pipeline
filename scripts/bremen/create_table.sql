USE DATABASE syte;

USE SCHEMA bremen;

CREATE TABLE IF NOT EXISTS buildings (
    identifier VARCHAR PRIMARY KEY,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    area FLOAT,
    num_floors INTEGER,
    on_parcel VARCHAR,
    type VARCHAR,
    building_date date,
    fast_api_sync timestamp without time zone default (now() at time zone 'utc')
);

CREATE TABLE IF NOT EXISTS parcels (
    identifier VARCHAR PRIMARY KEY,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    area FLOAT,
    location_text VARCHAR,
    cadastral_identifier VARCHAR,
    municipal VARCHAR,
    district VARCHAR,
    fast_api_sync timestamp without time zone default (now() at time zone 'utc')
);

/***********************************************************
*
** Create Dimension Tables for Competition and Venue data 
*
***********************************************************/

-- Create extension for geometry support
CREATE EXTENSION IF NOT EXISTS postgis;

-- Dimension tables
-- Region
CREATE TABLE IF NOT EXISTS dim_regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL,
    area_sqkm NUMERIC(20, 6),
    length_km NUMERIC(20, 6),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Zone
CREATE TABLE IF NOT EXISTS dim_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(255) NOT NULL,
    region_id INTEGER REFERENCES dim_regions(region_id),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Cluster
CREATE TABLE IF NOT EXISTS dim_clusters (
    cluster_id SERIAL PRIMARY KEY,
    cluster_name VARCHAR(255) NOT NULL,
    region_id INTEGER REFERENCES dim_regions(region_id),
    zone_id INTEGER REFERENCES dim_zones(zone_id),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Status
CREATE TABLE IF NOT EXISTS dim_status (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Venue
CREATE TABLE IF NOT EXISTS test.dim_venues (
    venue_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    status_id INTEGER REFERENCES test.dim_status(status_id),
    has_subvenue BOOLEAN,
    region_id INTEGER REFERENCES test.dim_regions(region_id),
    zone_id INTEGER REFERENCES test.dim_zones(zone_id),
    cluster_id INTEGER REFERENCES test.dim_clusters(cluster_id),
    latitude NUMERIC (21, 4),
    longitude NUMERIC (21, 4),
    capacity INTEGER,
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL,
    CONSTRAINT venue_date_range CHECK (valid_from < valid_to)
);

-- Sub-Venue
CREATE TABLE IF NOT EXISTS dim_subvenues (
    subvenue_id SERIAL PRIMARY KEY,
    subvenue_name VARCHAR(100) NOT NULL,
    venue_id INTEGER REFERENCES dim_venues(venue_id),
    capacity INTEGER,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL,
    CONSTRAINT subvenue_date_range CHECK (valid_from < valid_to)
);

-- Sport
CREATE TABLE IF NOT EXISTS dim_sports (
    sport_id INTEGER PRIMARY KEY,
    sport_discipline VARCHAR(255) NOT NULL UNIQUE,
    venue_id INTEGER REFERENCES dim_venues(venue_id),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Calendar
CREATE TABLE IF NOT EXISTS dim_calendar (
    day_id INTEGER PRIMARY KEY,
    event_day INTEGER NOT NULL,
    model_date DATE NOT NULL,
    actual_date DATE NOT NULL,
    day_of_week VARCHAR(50) NOT NULL,
    competition_type VARCHAR(50) NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- None Competition Facilities
CREATE TABLE IF NOT EXISTS dim_non_comp_facs (
    nc_fac_id SERIAL PRIMARY KEY,
    nc_fac_name VARCHAR(255) NOT NULL,
	nc_fac_code VARCHAR(10),
	details VARCHAR(255),
	status_id INTEGER REFERENCES dim_status(status_id),
    region_id INTEGER REFERENCES dim_regions(region_id),
    latitude NUMERIC (21, 4),
    longitude NUMERIC (21, 4),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

/****************************************************
** Create Dimension Tables for Games Route Network **
*****************************************************/